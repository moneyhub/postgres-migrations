const crypto = require("crypto")

const createLockTableIfExists = client => {
  const plpgsql = `
DO $$
DECLARE
  schema_name text := current_setting('app.schema', true);
BEGIN
  IF schema_name IS NULL THEN
    schema_name := 'public';
  END IF;
  EXECUTE 'CREATE TABLE IF NOT EXISTS ' || quote_ident(schema_name) || '.migration_locks (
    hash varchar(40) NOT NULL PRIMARY KEY
  )'; -- TODO: Linter warning - extra semicolon inside PL/pgSQL block
END
$$
`
  return client.query(plpgsql)
}

const generateMigrationHash = migrations => {
  const hash = crypto.createHash("sha1")
  migrations.forEach(migration => {
    hash.update(migration.sql)
  })

  return hash.digest("hex")
}

const verifyLockDoesNotExist = async (client, hash) => {
  const plpgsql = `
DO $$
DECLARE
  schema_name text := current_setting('app.schema', true);
  lock_exists boolean;
BEGIN
  IF schema_name IS NULL THEN
    schema_name := 'public';
  END IF;
  EXECUTE 'SELECT EXISTS (SELECT 1 FROM ' || quote_ident(schema_name) || '.migration_locks WHERE hash = ' || quote_literal(${hash}) || ')'
    INTO lock_exists; -- TODO: Linter warning - extra semicolon inside PL/pgSQL block
  IF lock_exists THEN
    RAISE EXCEPTION 'Current migration is locked: %', ${hash};
  END IF;
END
$$
`
  await client.query(plpgsql)
}

const removeLock = (client, hash) => {
  const plpgsql = `
DO $$
DECLARE
  schema_name text := current_setting('app.schema', true);
BEGIN
  IF schema_name IS NULL THEN
    schema_name := 'public';
  END IF;
  EXECUTE 'DELETE FROM ' || quote_ident(schema_name) || '.migration_locks WHERE hash = ' || quote_literal(${hash}); -- TODO: Linter warning - extra semicolon inside PL/pgSQL block
END
$$
`
  return client.query(plpgsql)
}

const insertLock = (client, hash) => {
  const plpgsql = `
DO $$
DECLARE
  schema_name text := current_setting('app.schema', true);
BEGIN
  IF schema_name IS NULL THEN
    schema_name := 'public';
  END IF;
  EXECUTE 'INSERT INTO ' || quote_ident(schema_name) || '.migration_locks (hash) VALUES (' || quote_literal(${hash}) || ')'; -- TODO: Linter warning - extra semicolon inside PL/pgSQL block
END
$$
`
  return client.query(plpgsql)
}

module.exports = {
  createLockTableIfExists,
  generateMigrationHash,
  verifyLockDoesNotExist,
  removeLock,
  insertLock,
}
