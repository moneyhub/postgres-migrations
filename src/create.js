const bluebird = require("bluebird")
const pg = require("pg")

module.exports = createDb

const DUPLICATE_DATABASE = "42P04"

// Time out after 10 seconds - should probably be able to override this
const DEFAULT_TIMEOUT = 10000

function createDb(dbName, dbConfig = {}, config = {}) { // eslint-disable-line complexity
  const {user, password, host, port} = dbConfig
  if (typeof dbName !== "string") {
    return Promise.reject(new Error("Must pass database name as a string"))
  }
  if (
    typeof user !== "string" ||
    typeof password !== "string" ||
    typeof host !== "string" ||
    typeof port !== "number"
  ) {
    return Promise.reject(new Error("Database config problem"))
  }

  const log = config.logger || (() => {})

  log(`Attempting to create database: ${dbName}`)

  const client = new pg.Client({
    database: dbConfig.defaultDatabase || "postgres",
    user,
    password,
    host,
    port,
  })

  return bluebird.resolve()
    .then(() => client.connect())
    .timeout(DEFAULT_TIMEOUT)
    // eslint-disable-next-line quotes
    .then(() => client.query(`CREATE DATABASE "${dbName.replace(/"/g, '""')}"`))
    .timeout(DEFAULT_TIMEOUT)
    .then(() => log(`Created database: ${dbName}`))
    .catch((err) => {
      if (err) {
        // we are not worried about duplicate db errors
        if (err.code === DUPLICATE_DATABASE) {
          log(`'${dbName}' database already exists`)
        } else {
          log(err)
          throw new Error(`Error creating database. Caused by: '${err.name}: ${err.message}'`)
        }
      }
    })
    .finally(() => client.end())
}
