const createSchemaIfNotExists = (client, schema) => {
  if (!schema || schema === "public") {
    return Promise.resolve()
  }
  const plpgsql = `
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = '${schema}') THEN
    EXECUTE format('CREATE SCHEMA %I', '${schema}');
  END IF;
END
$$;
`
  return client.query(plpgsql)
}

const setSchema = (client, log, schema) => {
  return client.query(`SET app.schema = '${schema}'`)
    .then(() => client.query(`SET search_path TO '${schema}'`))
    .then(() => log(`Set schema in app.schema and search_path to: ${schema}`))

}

module.exports = {
  createSchemaIfNotExists,
  setSchema,
}
