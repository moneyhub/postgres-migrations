const test = require("ava")
const sinon = require("sinon")
const dedent = require("dedent-js")

const bluebird = require("bluebird")

const runMigration = require("../../run-migration")

const readFile = bluebird.promisify(require("fs").readFile)

let normalSqlFile
let noTransactionSqlFile

test.before(() => {
  return bluebird.all([
    readFile(__dirname + "/normal.sql", "utf8").then(contents => {
      normalSqlFile = contents
    }),

    readFile(__dirname + "/no-transaction.sql", "utf8").then(contents => {
      noTransactionSqlFile = contents
    }),
  ])
})

function normalizeLines(str) {
  return dedent(str)
    .split("\n")
    .map(line => line.trim())
    .join("\n")
    .trim()
}

function buildMigration(sql) {
  return {
    id: "id",
    name: "name",
    sql,
    hash: "hash",
  }
}

test("runs a simple migration", t => {
  const query = sinon.stub().resolves()
  const run = runMigration({query})

  const migration = buildMigration(normalSqlFile)

  return run(migration).then(() => {
    t.is(query.callCount, 4)
    t.is(
      query.firstCall.args[0],
      "START TRANSACTION",
      "should begin a transaction",
    )

    t.is(
      query.secondCall.args[0],
      migration.sql,
      "should execute the migration",
    )

    const thirdCallExpected = dedent`
      DO $$
      DECLARE
        schema_name text := current_setting('app.schema', true);
      BEGIN
        IF schema_name IS NULL THEN
          schema_name := 'public';
        END IF;
        EXECUTE format('INSERT INTO %I.migrations (id, name, hash) VALUES (%L, %L, %L)',
          schema_name,
          'id',
          'name',
          'hash'
        );
      END
      $$
    `

    t.is(
      normalizeLines(query.thirdCall.args[0]),
      normalizeLines(thirdCallExpected),
      "should record the running of the migration in the database (SQL text)",
    )

    t.is(
      query.lastCall.args[0],
      "COMMIT",
      "should complete the transaction",
    )
  })
})

test("rolls back when there is an error inside a transactiony migration", t => {
  const query = sinon.stub().rejects(new Error("There was a problem"))
  const run = runMigration({query})

  const migration = buildMigration(normalSqlFile)
  t.plan(2)

  return run(migration).catch(e => {
    t.is(query.lastCall.args[0], "ROLLBACK", "should perform a rollback")
    t.true(
      e.message.indexOf("There was a problem") >= 0,
      "should throw an error",
    )
  })
})

test("does not run the migration in a transaction when instructed", t => {
  const query = sinon.stub().resolves()
  const run = runMigration({query})

  const migration = buildMigration(noTransactionSqlFile)

  return run(migration).then(() => {
    t.is(query.callCount, 2)

    t.is(
      query.firstCall.args[0],
      migration.sql,
      "should run the migration",
    )

    const secondCallExpected = dedent`
      DO $$
      DECLARE
        schema_name text := current_setting('app.schema', true);
      BEGIN
        IF schema_name IS NULL THEN
          schema_name := 'public';
        END IF;
        EXECUTE format('INSERT INTO %I.migrations (id, name, hash) VALUES (%L, %L, %L)',
          schema_name,
          'id',
          'name',
          'hash'
        );
      END
      $$
    `

    t.is(
      normalizeLines(query.secondCall.args[0]),
      normalizeLines(secondCallExpected),
      "should record the running of the migration in the database (SQL text)",
    )
  })
})

test(
  "does not roll back when there is an error inside a transactiony migration",
  t => {
    const query = sinon.stub().rejects(new Error("There was a problem"))
    const run = runMigration({query})

    const migration = buildMigration(noTransactionSqlFile)

    return run(migration).catch(e => {
      sinon.assert.neverCalledWith(query, "ROLLBACK")
      t.true(
        e.message.indexOf("There was a problem") >= 0,
        "should throw an error",
      )
    })
  },
)
