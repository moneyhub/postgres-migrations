-- postgres-migrations disable-transaction
-- postgres-migrations disable-hash-check
CREATE TABLE IF NOT EXISTS hash_check_disabled_test (
  id integer PRIMARY KEY
);
