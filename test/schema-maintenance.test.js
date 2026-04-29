import test from "node:test";
import assert from "node:assert/strict";
import { SchemaBuilder } from "../src/core/SchemaBuilder.js";
import { DB } from "../src/core/DB.js";

function sqlAdapter(dialect, executed) {
  return {
    dialect,
    placeholder(i) {
      if (dialect === "pg") return `$${i}`;
      if (dialect === "mssql") return `@p${i}`;
      if (dialect === "oracle") return `:p${i}`;
      return "?";
    },
    async execute(qb) {
      const out = qb.compile();
      executed.push(out.sql);
      return [];
    },
  };
}

test("truncateTable compiles for PostgreSQL with restart identity and cascade", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .truncateTable("users", { restartIdentity: true, cascade: true })
    .exec();

  assert.equal(
    executed[0],
    'TRUNCATE TABLE "users" RESTART IDENTITY CASCADE',
  );
});

test("truncateTable on mongo deletes all documents", async () => {
  let receivedFilter = null;
  const adapter = {
    dialect: "mongo",
    db: {
      collection(name) {
        assert.equal(name, "logs");
        return {
          async deleteMany(filter) {
            receivedFilter = filter;
            return { deletedCount: 4 };
          },
        };
      },
    },
  };

  const out = await new SchemaBuilder(adapter).truncateTable("logs").exec();

  assert.deepEqual(receivedFilter, {});
  assert.deepEqual(out, { truncated: true, collection: "logs", deletedCount: 4 });
});

test("analyzeTable compiles for supported SQL dialects", async () => {
  const pgExecuted = [];
  await new SchemaBuilder(sqlAdapter("pg", pgExecuted))
    .analyzeTable("users", { verbose: true })
    .exec();
  assert.equal(pgExecuted[0], 'ANALYZE VERBOSE "users"');

  const mysqlExecuted = [];
  await new SchemaBuilder(sqlAdapter("mysql", mysqlExecuted))
    .analyzeTable("users")
    .exec();
  assert.equal(mysqlExecuted[0], "ANALYZE TABLE `users`");

  const mssqlExecuted = [];
  await new SchemaBuilder(sqlAdapter("mssql", mssqlExecuted))
    .analyzeTable("users", { fullscan: true })
    .exec();
  assert.equal(mssqlExecuted[0], "UPDATE STATISTICS [users] WITH FULLSCAN");

  const oracleExecuted = [];
  await new SchemaBuilder(sqlAdapter("oracle", oracleExecuted))
    .analyzeTable("audit.logs", { cascade: false })
    .exec();
  assert.equal(
    oracleExecuted[0],
    "BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname => 'audit', tabname => 'logs', cascade => FALSE); END;",
  );
});

test("optimizeTable compiles best-effort maintenance SQL", async () => {
  const pgExecuted = [];
  await new SchemaBuilder(sqlAdapter("pg", pgExecuted))
    .optimizeTable("users")
    .exec();
  assert.equal(pgExecuted[0], 'VACUUM (ANALYZE) "users"');

  const mysqlExecuted = [];
  await new SchemaBuilder(sqlAdapter("mysql", mysqlExecuted))
    .optimizeTable("users")
    .exec();
  assert.equal(mysqlExecuted[0], "OPTIMIZE TABLE `users`");

  const mssqlExecuted = [];
  await new SchemaBuilder(sqlAdapter("mssql", mssqlExecuted))
    .optimizeTable("users", { rebuild: true })
    .exec();
  assert.equal(mssqlExecuted[0], "ALTER INDEX ALL ON [users] REBUILD");
});

test("optimizeTable rejects unsupported dialects", () => {
  assert.throws(
    () => new SchemaBuilder(sqlAdapter("oracle", [])).optimizeTable("users"),
    /oracle: optimizeTable is not supported/i,
  );

  assert.throws(
    () =>
      new SchemaBuilder({
        dialect: "mongo",
        db: { collection() {} },
      }).optimizeTable("users"),
    /mongo: optimizeTable is not supported/i,
  );
});

test("vacuumTable and vacuumDatabase compile for PostgreSQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .vacuumTable("users", { analyze: true, verbose: true })
    .exec();

  await new SchemaBuilder(adapter)
    .vacuumDatabase({ full: true })
    .exec();

  assert.equal(executed[0], 'VACUUM (VERBOSE, ANALYZE) "users"');
  assert.equal(executed[1], "VACUUM (FULL)");
});

test("reindexTable compiles for PostgreSQL and SQL Server", async () => {
  const pgExecuted = [];
  await new SchemaBuilder(sqlAdapter("pg", pgExecuted))
    .reindexTable("users", { concurrently: true })
    .exec();
  assert.equal(pgExecuted[0], 'REINDEX TABLE CONCURRENTLY "users"');

  const mssqlExecuted = [];
  await new SchemaBuilder(sqlAdapter("mssql", mssqlExecuted))
    .reindexTable("users", { online: true, fillfactor: 80 })
    .exec();
  assert.equal(mssqlExecuted[0], "ALTER INDEX ALL ON [users] REBUILD WITH (ONLINE = ON, FILLFACTOR = 80)");
});

test("repairTable compiles for MySQL", async () => {
  const executed = [];
  await new SchemaBuilder(sqlAdapter("mysql", executed))
    .repairTable("users", { quick: true, extended: true })
    .exec();

  assert.equal(executed[0], "REPAIR TABLE `users` QUICK EXTENDED");
});

test("DB.maintenance executes helpers immediately", async () => {
  const executed = [];
  const db = new DB(sqlAdapter("pg", executed));

  await db.maintenance().vacuumTable("users", { analyze: true });
  await db.maintenance().reindexTable("users");

  assert.deepEqual(executed, [
    'VACUUM (ANALYZE) "users"',
    'REINDEX TABLE "users"',
  ]);
});
