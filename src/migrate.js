const pg = require("pg")
const fs = require("fs")
const crypto = require("crypto")
const bluebird = require("bluebird")
const path = require("path")
const dedent = require("dedent-js")

const runMigration = require("./run-migration")
const {createLockTableIfExists, generateMigrationHash, verifyLockDoesNotExist, removeLock, insertLock} = require("./lock")

module.exports = migrate

const getPid = async (client) => {
  const res = await client.query("SELECT pg_backend_pid()")
  return res.rows[0].pg_backend_pid
}

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

  const client = new pg.Client(dbConfig)
  let hash, pid, queryCancelled = false, timeoutId
  log("Attempting database migration")

  return bluebird.resolve()
    .then(() => client.connect())
    .then(() => log("Connected to database"))
    .then(() => client.query(`SET app.schema = '${schema}'`))
    .then(() => log(`Using schema: ${schema}`))
    .then(() => getPid(client))
    .then(res => {
      log(`Retrieved backend PID: ${res}`)
      pid = res
    })
    .then(() => createLockTableIfExists(client))
    .then(() => loadMigrationFiles(migrationsDirectory, log, numberMigrationsToLoad))
    .then(filterMigrations(client))
    .tap(migrations => {
      hash = generateMigrationHash(migrations)
    })
    .tap(() => verifyLockDoesNotExist(client, hash))
    .tap(() => insertLock(client, hash))
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
        await removeLock(client, hash)
      }
    })
    .catch((err) => {
      const message = queryCancelled ? "Timeout" : err.message
      log(`Migration failed. Reason: ${message}`)
      throw err
    })
    .finally(() => {
      clearTimeout(timeoutId)
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
function filterMigrations(client) {
  return (migrations) => {
    // Arrange in ID order
    const orderedMigrations = migrations.sort((a, b) => a.id - b.id)

    // Assert their IDs are consecutive integers
    migrations.forEach((mig, i) => {
      if (mig.id !== i) {
        throw new Error("Found a non-consecutive migration ID")
      }
    })

    return doesTableExist(client, "migrations")
      .then((exists) => {
        if (!exists) {
          // Migrations table hasn't been created,
          // so the database is new and we need to run all migrations
          return orderedMigrations
        }

        return client.query("SELECT * FROM migrations ORDER BY id ASC")
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
async function doesTableExist(client, tableName) {
  const schema = client.connectionParameters.schema || "public"
  const result = await client.query(`
    DO $$
    DECLARE
      schema_name text := '${schema}';
      table_exists boolean;
    BEGIN
      EXECUTE 'SELECT EXISTS (
        SELECT 1
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  c.relname = ' || quote_literal(${tableName}) || '
        AND    c.relkind = ''r''
        AND    n.nspname = ' || quote_literal(schema_name) || '
      )'
      INTO table_exists;
    END $$;
  `)
  return result.rows[0].table_exists
}
