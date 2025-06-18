DO $$
DECLARE
  schema_name text := current_setting('app.schema', true);
BEGIN
  IF schema_name IS NULL THEN
    schema_name := 'public';
  END IF;

  -- hash here is sha1 hex encoded hash of the file name and contents, to ensure it hasn't been altered since applying the migration
  EXECUTE format('CREATE TABLE IF NOT EXISTS %I.migrations (
    id integer PRIMARY KEY,
    name varchar(100) UNIQUE NOT NULL,
    hash varchar(40) NOT NULL,
    executed_at timestamp DEFAULT current_timestamp
  )', schema_name);
END
$$;
