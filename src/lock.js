const SQL = require("sql-template-strings")
const crypto = require("crypto")

const createLockTableIfExists = client => {
  return client.query(
    SQL`
CREATE TABLE IF NOT EXISTS migration_locks (
  hash varchar(40) NOT NULL PRIMARY KEY
);
`,
  )
}

const generateMigrationHash = migrations => {
  const hash = crypto.createHash("sha1")
  migrations.forEach(migration => {
    hash.update(migration.sql)
  })

  return hash.digest("hex")
}

const verifyLockDoesNotExist = async (client, hash) => {
  const result = await client.query(
    SQL`
SELECT hash FROM migration_locks
  WHERE hash = ${hash}
    `,
  )

  if (result.rowCount) {
    throw new Error(`Current migration is locked: ${hash}`)
  }
}

const removeLock = (client, hash) => {
  return client.query(
    SQL`DELETE FROM migration_locks WHERE hash = ${hash};`,
  )
}

const insertLock = (client, hash) => {
  return client.query(
    SQL`INSERT INTO migration_locks (hash) VALUES (${hash})`,
  )
}

module.exports = {
  createLockTableIfExists,
  generateMigrationHash,
  verifyLockDoesNotExist,
  removeLock,
  insertLock,
}
