const crypto = require("crypto")
const {quoteIdent} = require("./utils")

const createLockTableIfNotExists = (client, schema = "public") => {
  return client.query(
    `CREATE TABLE IF NOT EXISTS ${quoteIdent(schema)}.migration_locks (
      hash varchar(40) NOT NULL PRIMARY KEY
    );`,
  )
}

const generateMigrationHash = (migrations) => {
  const hash = crypto.createHash("sha1")
  migrations.forEach(migration => {
    hash.update(migration.sql)
  })

  return hash.digest("hex")
}

const verifyLockDoesNotExist = async (client, hash, schema = "public") => {
  const result = await client.query(
    `SELECT hash FROM ${quoteIdent(schema)}.migration_locks WHERE hash = $1`,
    [hash],
  )

  if (result.rowCount) {
    throw new Error(`Current migration is locked: ${hash}`)
  }
}

const removeLock = (client, hash, schema = "public") => {
  return client.query(
    `DELETE FROM ${quoteIdent(schema)}.migration_locks WHERE hash = $1`,
    [hash],
  )
}

const insertLock = (client, hash, schema = "public") => {
  return client.query(
    `INSERT INTO ${quoteIdent(schema)}.migration_locks (hash) VALUES ($1)`,
    [hash],
  )
}

module.exports = {
  createLockTableIfNotExists,
  generateMigrationHash,
  verifyLockDoesNotExist,
  removeLock,
  insertLock,
}
