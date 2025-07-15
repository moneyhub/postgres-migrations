const pg = require("pg")
const fs = require("fs")
const crypto = require("crypto")
const bluebird = require("bluebird")
const path = require("path")
const SQL = require("sql-template-strings")
const dedent = require("dedent-js")

const runMigration = require("./run-migration")
const {createLockTableIfNotExists, generateMigrationHash, verifyLockDoesNotExist, removeLock, insertLock} = require("./lock")
const {createSchemaIfNotExists, setSchema} = require("./schema")
const {quoteIdent} = require("./utils")

module.exports = migrate

const getPid = async (client) => {
  const res = await client.query("SELECT pg_backend_pid()")
  return res.rows[0].pg_backend_pid
}

const acquireApplicationLock = async (client, lockId, shouldBlock = false, log) => {
  if (shouldBlock) {
    await client.query("SELECT pg_advisory_lock($1)", [lockId])
  } else {
    const result = await client.query("SELECT pg_try_advisory_lock($1)", [lockId])
    if (!result.rows[0].pg_try_advisory_lock) {
      throw new Error(`Failed to acquire application lock with ID: ${lockId}`)
    }
  }
  log(`Acquired application lock with ID: ${lockId}`)
}

const releaseApplicationLock = async (client, lockId, log) => {
  try {
    await client.query("SELECT pg_advisory_unlock($1)", [lockId])
    log(`Released application lock with ID: ${lockId}`)
  } catch (error) {
    log(`Failed to release (may not exist) application lock with ID: ${lockId}`)
  }
}

// eslint-disable-next-line max-statements
function migrate(dbConfig = {}, migrationsDirectory, config = {}) { // eslint-disable-line complexity
  if (
    typeof dbConfig.database !== "string" ||
    typeof dbConfig.user !== "string" ||
    typeof dbConfig.password !== "string" ||
    typeof dbConfig.host !== "string" ||
    typeof dbConfig.port !== "number"
  ) {
    return Promise.reject(new Error("Database config problem"))
  }
  if (typeof migrationsDirectory !== "string") {
    return Promise.reject(new Error("Must pass migrations directory as a string"))
  }

  const log = config.logger || (() => {})
  const {migrationTimeout = 60000} = config
  const numberMigrationsToLoad = config.numberMigrationsToLoad
  const schema = config.schema || "public"
  const shouldBlockOnAppLock = config.shouldBlockOnAppLock || false
  const applicationId = config.applicationId ?? Math.floor(Math.random() * 2147483647)

  const client = new pg.Client(dbConfig)

  let hash, pid, queryCancelled = false, timeoutId
  log("Attempting database migration")

  return bluebird.resolve()
    .then(() => client.connect())
    .then(() => log("Connected to database"))
    .then(() => acquireApplicationLock(client, applicationId, shouldBlockOnAppLock, log))
    .then(() => createSchemaIfNotExists(client, schema))
    .then(() => setSchema(client, log, schema))
    .then(() => getPid(client))
    .then(res => {
      log(`Retrieved backend PID: ${res}`)
      pid = res
    })
    .then(() => createLockTableIfNotExists(client, schema))
    .then(() => loadMigrationFiles(migrationsDirectory, log, numberMigrationsToLoad))
    .then(filterMigrations(client, schema))
    .tap(migrations => {
      hash = generateMigrationHash(migrations)
    })
    .tap(() => verifyLockDoesNotExist(client, hash, schema))
    .tap(() => insertLock(client, hash, schema))
    .then((migrations) => {
      timeoutId = setTimeout(async () => {
        queryCancelled = true
        const cancelClient = new pg.Client(dbConfig)
        await cancelClient.connect()
        await cancelClient.query(`SELECT pg_cancel_backend(${pid})`)

        log("Cancelling migration connection")

        cancelClient.end()
      }, migrationTimeout)

      return migrations
    })
    .each(runMigration(client))
    .then(logResult(log))
    .tap(async () => {
      if (hash) {
        await removeLock(client, hash, schema)
      }
    })
    .catch((err) => {
      const message = queryCancelled ? "Timeout" : err.message
      log(`Migration failed. Reason: ${message}`)
      throw err
    })
    .finally(async () => {
      clearTimeout(timeoutId)
      await releaseApplicationLock(client, applicationId, log)
      client.end()
    })
}

function logResult(log) {
  return (completedMigrations) => {
    if (completedMigrations.length === 0) {
      log("No migrations applied")
    } else {
      const names = completedMigrations.map((m) => m.name)
      log(`Successfully applied migrations: ${names}`)
    }

    return completedMigrations
  }
}

// Work out which migrations to apply
function filterMigrations(client, schema = "public") {
  return (migrations) => {
    // Arrange in ID order
    const orderedMigrations = migrations.sort((a, b) => a.id - b.id)

    // Assert their IDs are consecutive integers
    migrations.forEach((mig, i) => {
      if (mig.id !== i) {
        throw new Error("Found a non-consecutive migration ID")
      }
    })

    return doesTableExist(client, "migrations", schema)
      .then((exists) => {
        if (!exists) {
          // Migrations table hasn't been created,
          // so the database is new and we need to run all migrations
          return orderedMigrations
        }

        return client.query(`SELECT * FROM ${quoteIdent(schema)}.migrations ORDER BY id ASC`)
          .then(filterUnappliedMigrations(orderedMigrations))
      })
  }
}

const disableHashCheck = (migrationSQL) => migrationSQL
  .indexOf("-- postgres-migrations disable-hash-check") !== -1

// Remove migrations that have already been applied
function filterUnappliedMigrations(orderedMigrations) {
  return ({rows: appliedMigrations}) => {
    return orderedMigrations.filter((mig) => {
      const migRecord = appliedMigrations[mig.id]
      if (!migRecord) {
        return true
      }
      if (disableHashCheck(mig.sql)) {
        return false
      }
      if (migRecord.hash !== mig.hash) {
        // Someone has altered a migration which has already run - gasp!
        throw new Error(dedent`
          Hashes don't match for migration '${mig.name}'.
          This means that the script has changed since it was applied.`)
      }
      return false
    })
  }
}

const readDir = bluebird.promisify(fs.readdir)
function loadMigrationFiles(directory, log, numberMigrationsToLoad) {
  log(`Loading migrations from: ${directory}`)
  return readDir(directory)
    .then((fileNames) => {
      if (numberMigrationsToLoad) {
        log(`Loading ${numberMigrationsToLoad} migration files`)
        return fileNames.slice(0, numberMigrationsToLoad)
      }
      return fileNames
    })
    .then(fileNames => {
      log(`Found migration files: ${fileNames}`)
      return fileNames
        .filter((fileName) => fileName.toLowerCase().endsWith(".sql"))
        .map((fileName) => path.resolve(directory, fileName))
    })
    .then((fileNames) => {
      // Add a special zeroth migration to create the migrations table
      fileNames.unshift(path.join(__dirname, "migrations/0_create-migrations-table.sql"))
      return fileNames
    })
    .then((fileNames) => bluebird.map(fileNames, loadFile))
}

const readFile = bluebird.promisify(fs.readFile)
function loadFile(filePath) {
  const fileName = path.basename(filePath)

  const id = parseInt(fileName, 10)
  if (isNaN(id)) {
    return Promise.reject(new Error(dedent`
      Migration files should begin with an integer ID.
      Offending file: '${fileName}'`))
  }

  return readFile(filePath, "utf8")
    .then((contents) => {
      const hash = crypto.createHash("sha1")
      hash.update(fileName + contents, "utf8")
      const encodedHash = hash.digest("hex")

      return {
        id,
        name: fileName,
        sql: contents,
        hash: encodedHash,
      }
    })
}

// Check whether table exists in postgres - http://stackoverflow.com/a/24089729
function doesTableExist(client, tableName, schema = "public") {
  return client.query(SQL`
      SELECT EXISTS (
        SELECT 1
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  c.relname = ${tableName}
        AND    c.relkind = 'r'
        AND    n.nspname = ${schema}
      );
    `)
    .then((result) => {
      return result.rows.length > 0 && result.rows[0].exists
    })
}
