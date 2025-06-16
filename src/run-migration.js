const bluebird = require("bluebird")
const dedent = require("dedent-js")

module.exports = client => migration => {
  const inTransaction = migration.sql
    .indexOf("-- postgres-migrations disable-transaction") === -1

  const begin = () => inTransaction
    ? client.query("START TRANSACTION")
    : undefined

  const end = () => inTransaction
    ? client.query("COMMIT")
    : undefined

  const cleanup = () => inTransaction
    ? client.query("ROLLBACK")
    : undefined

  return bluebird
    .resolve()
    .then(begin)
    .then(() => client.query(migration.sql))
    .then(() => {
      const plpgsql = `
DO $$
DECLARE
  schema_name text := current_setting('app.schema', true);
BEGIN
  IF schema_name IS NULL THEN
    schema_name := 'public';
  END IF;
  EXECUTE 'INSERT INTO ' || quote_ident(schema_name) || '.migrations (id, name, hash) VALUES (' 
    || quote_literal(${migration.id}) || ', ' 
    || quote_literal(${migration.name}) || ', ' 
    || quote_literal(${migration.hash}) || ')'; -- TODO: Linter warning - extra semicolon inside PL/pgSQL block
END
$$
  `
      return client.query(plpgsql)
    })
    .then(end)
    .catch(err => {
      return bluebird.resolve().tap(cleanup).then(() => {
        throw new Error(
          dedent`
          An error occurred running '${migration.name}'. Rolled back this migration.
          No further migrations were run.
          Reason: ${err.message}`,
        )
      })
    })
}
