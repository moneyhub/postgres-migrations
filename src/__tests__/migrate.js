const test = require("ava")
const fs = require("fs")
const bluebird = require("bluebird")
const {execSync} = require("child_process")
const pg = require("pg")
const SQL = require("sql-template-strings")

const startPostgres = require("./_start-postgres")

const createDb = require("../create")
const migrate = require("../migrate")
const {createLockTableIfExists, insertLock} = require("../lock")
const crypto = require("crypto")

const CONTAINER_NAME = "pg-migrations-test-migrate"
const PASSWORD = startPostgres.PASSWORD

let port

test.before.cb((t) => {
  port = startPostgres(CONTAINER_NAME, t)
})

test("successful first migration", (t) => {
  const databaseName = "migration-test-success-first"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  return createDb(databaseName, dbConfig)
    .then(() => migrate(dbConfig, "src/__tests__/success-first"))
    .then(() => doesTableExist(dbConfig, "success"))
    .then((exists) => {
      t.truthy(exists)
    })
})

test("successful second migration", (t) => {
  const databaseName = "migration-test-success-second"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  return createDb(databaseName, dbConfig)
    .then(() => migrate(dbConfig, "src/__tests__/success-first"))
    .then(() => migrate(dbConfig, "src/__tests__/success-second"))
    .then(() => doesTableExist(dbConfig, "more_success"))
    .then((exists) => {
      t.truthy(exists)
    })
})

test("number of migrations to load", (t) => {
  const databaseName = "migration-test-number-migrations-load"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  return createDb(databaseName, dbConfig)
    .then(() => migrate(dbConfig, "src/__tests__/migrations-to-load", {numberMigrationsToLoad: 2}))
    .then(() => doesTableExist(dbConfig, "load_first"))
    .then((exists) => {
      t.truthy(exists)
    })
    .then(() => doesTableExist(dbConfig, "load_second"))
    .then((exists) => {
      t.truthy(exists)
    })
    .then(() => doesTableExist(dbConfig, "load_third"))
    .then((exists) => {
      t.falsy(exists)
    })
})

test("bad arguments - no db config", (t) => {
  return t.throwsAsync(() => migrate())
    .then((err) => {
      t.regex(err.message, /config/)
    })
})

test("bad arguments - no migrations directory argument", (t) => {
  return t.throwsAsync(() => migrate({
    database: "migration-test-args",
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }))
    .then((err) => {
      t.regex(err.message, /directory/)
    })
})

test("bad arguments - incorrect user", (t) => {
  return t.throwsAsync(() => migrate({
    database: "migration-test-args",
    user: "nobody",
    password: PASSWORD,
    host: "localhost",
    port,
  }, "some/path"))
    .then((err) => {
      t.regex(err.message, /nobody/)
    })
})

test("bad arguments - incorrect password", (t) => {
  return t.throwsAsync(() => migrate({
    database: "migration-test-args",
    user: "postgres",
    password: "not_the_password",
    host: "localhost",
    port,
  }, "some/path"))
    .then((err) => {
      t.regex(err.message, /password/)
    })
})

test("bad arguments - incorrect host", (t) => {
  return t.throwsAsync(() => migrate({
    database: "migration-test-args",
    user: "postgres",
    password: PASSWORD,
    host: "sillyhost",
    port,
  }, "some/path"))
    .then((err) => {
      t.regex(err.message, /sillyhost/)
    })
})

test("bad arguments - incorrect port", (t) => {
  return t.throwsAsync(() => migrate({
    database: "migration-test-args",
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port: 1234,
  }, "some/path"))
    .then((err) => {
      t.regex(err.message, /1234/)
    })
})

test("no database", (t) => {
  return t.throwsAsync(() => migrate({
    database: "migration-test-no-database",
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }, "some/path"))
    .then((err) => {
      t.regex(err.message, /database "migration-test-no-database" does not exist/)
    })
})

test("no migrations dir", (t) => {
  const databaseName = "migration-test-no-dir"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const promise = createDb(databaseName, dbConfig)
    .then(() => {
      return migrate(dbConfig, "some/path")
    })

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /some\/path/)
    })
})

test("empty migrations dir", (t) => {
  const databaseName = "migration-test-empty-dir"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  return createDb(databaseName, dbConfig)
    .then(() => {
      return migrate(dbConfig, "src/__tests__/empty")
    })
    .then(() => t.pass())
})

test("non-consecutive ordering", (t) => {
  const databaseName = "migration-test-non-consec"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const promise = createDb(databaseName, dbConfig)
    .then(() => {
      return migrate(dbConfig, "src/__tests__/non-consecutive")
    })

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /Found a non-consecutive migration ID/)
    })
})

test("not starting from one", (t) => {
  const databaseName = "migration-test-starting-id"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const promise = createDb(databaseName, dbConfig)
    .then(() => {
      return migrate(dbConfig, "src/__tests__/start-from-2")
    })

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /Found a non-consecutive migration ID/)
    })
})

test("negative ID", (t) => {
  const databaseName = "migration-test-negative"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const promise = createDb(databaseName, dbConfig)
    .then(() => {
      return migrate(dbConfig, "src/__tests__/negative")
    })

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /Found a non-consecutive migration ID/)
    })
})

test("no prefix", (t) => {
  const databaseName = "migration-test-prefix"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const promise = createDb(databaseName, dbConfig)
    .then(() => {
      return migrate(dbConfig, "src/__tests__/no-prefix")
    })

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /Migration files should begin with an integer ID/)
      t.regex(err.message, /migrate-this/, "Should name the problem file")
    })
})

test("syntax error", (t) => {
  const databaseName = "migration-test-syntax-error"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const promise = createDb(databaseName, dbConfig)
    .then(() => {
      return migrate(dbConfig, "src/__tests__/syntax-error")
    })

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /syntax error/)
    })
})

test("hash check failure", (t) => {
  const databaseName = "migration-test-hash-check"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const promise = createDb(databaseName, dbConfig)
    .then(() => migrate(dbConfig, "src/__tests__/hash-check/first-run"))
    .then(() => migrate(dbConfig, "src/__tests__/hash-check/second-run"))

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /Hashes don't match/)
      t.regex(err.message, /1_migration/, "Should name the problem file")
    })
})

test("hash check disabled", (t) => {
  const databaseName = "migration-test-hash-disabled-check"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  return createDb(databaseName, dbConfig)
    .then(() => migrate(dbConfig, "src/__tests__/hash-check-disable/first-run"))
    .then(() => migrate(dbConfig, "src/__tests__/hash-check-disable/second-run"))
    .then((exists) => {
      t.truthy(exists)
    })
})

test("hash check disabled and transaction disabled", (t) => {
  const databaseName = "migration-test-hash-disabled-check-transaction-disabled"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  return createDb(databaseName, dbConfig)
    .then(() => migrate(dbConfig, "src/__tests__/hash-check-disable/first-run"))
    .then(() => migrate(dbConfig, "src/__tests__/hash-check-disable/second-run"))
    .then((exists) => {
      t.truthy(exists)
    })
})

test("rollback", (t) => {
  const databaseName = "migration-test-rollback"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const promise = createDb(databaseName, dbConfig)
    .then(() => migrate(dbConfig, "src/__tests__/rollback"))

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /Rolled back/)
      t.regex(err.message, /1_trigger-rollback/)
    })
    .then(() => doesTableExist(dbConfig, "should_get_rolled_back"))
    .then((exists) => {
      t.false(exists, "The table created in the migration should not have been committed.")
    })
})

test("timeout failure", t => {
  const databaseName = "migration-test-timeout"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const promise = createDb(databaseName, dbConfig)
    .then(() => migrate(dbConfig, "src/__tests__/timeout", {
      migrationTimeout: 500,
    }))

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /canceling statement due to user request/)
    })
})

test("locking failure", t => {
  const databaseName = "migration-test-lock"
  const dbConfig = {
    database: databaseName,
    user: "postgres",
    password: PASSWORD,
    host: "localhost",
    port,
  }

  const hash = crypto.createHash("sha1")
  const initMigration = fs.readFileSync("src/migrations/0_create-migrations-table.sql", "utf-8")
  const migration = fs.readFileSync("src/__tests__/lock/1_first.sql", "utf-8")
  hash.update(initMigration)
  hash.update(migration)

  const promise = createDb(databaseName, dbConfig)
    .then(() => insertMigrationLock(dbConfig, hash.digest("hex")))
    .then(() => migrate(dbConfig, "src/__tests__/lock"))

  return t.throwsAsync(() => promise)
    .then((err) => {
      t.regex(err.message, /Current migration is locked:/)
    })
})

test.after.always(() => {
  execSync(`docker rm -f ${CONTAINER_NAME}`)
})

function insertMigrationLock(dbConfig, hash) {
  const client = bluebird.promisifyAll(new pg.Client(dbConfig))
  return client.connect()
    .then(() => createLockTableIfExists(client))
    .then(() => insertLock(client, hash))
    .finally(() => client.end())
}

function doesTableExist(dbConfig, tableName) {
  const client = bluebird.promisifyAll(new pg.Client(dbConfig))
  return client.connect()
    .then(() => client.query(SQL`
        SELECT EXISTS (
          SELECT 1
          FROM   pg_catalog.pg_class c
          JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          WHERE  c.relname = ${tableName}
          AND    c.relkind = 'r'
          AND    n.nspname = current_setting('app.schema')
        );
      `),
    )
    .then((result) => {
      client.end()
      return result.rows.length > 0 && result.rows[0].exists
    })
}
