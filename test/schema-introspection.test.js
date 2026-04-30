import test from "node:test";
import assert from "node:assert/strict";
import { SchemaBuilder } from "../src/core/SchemaBuilder.js";

function sqlAdapter(dialect, executed, rows) {
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
      executed.push({ type: qb._type, sql: out.sql, params: out.params ?? [] });
      return rows;
    },
  };
}

test("showTables compiles and returns names for PostgreSQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed, [{ name: "users" }, { name: "orders" }]);
  const names = await new SchemaBuilder(adapter).showTables();

  assert.deepEqual(names, ["users", "orders"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /pg_catalog\.pg_tables/);
});

test("showDatabases compiles and returns names for MySQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("mysql", executed, [{ name: "mysql" }, { name: "appdb" }]);
  const names = await new SchemaBuilder(adapter).showDatabases();

  assert.deepEqual(names, ["mysql", "appdb"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /information_schema\.schemata/i);
});

test("showSchemas compiles and returns names for PostgreSQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed, [{ name: "public" }, { name: "reporting" }]);
  const names = await new SchemaBuilder(adapter).showSchemas();

  assert.deepEqual(names, ["public", "reporting"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /information_schema\.schemata/i);
});

test("showSequences compiles and returns names for SQL Server", async () => {
  const executed = [];
  const adapter = sqlAdapter("mssql", executed, [{ name: "order_seq" }, { name: "user_seq" }]);
  const names = await new SchemaBuilder(adapter).showSequences({ schema: "dbo" });

  assert.deepEqual(names, ["order_seq", "user_seq"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /sys\.sequences/i);
  assert.deepEqual(executed[0].params, ["dbo"]);
});

test("showTables/showDatabases/showViews/showIndexes return names for Mongo", async () => {
  const adapter = {
    dialect: "mongo",
    db: {
      listCollections(filter) {
        return {
          async toArray() {
            if (filter?.type === "view") {
              return [{ name: "active_users" }, { name: "sales_rollup" }];
            }
            return [{ name: "users" }, { name: "logs" }];
          },
        };
      },
      collection() {
        return {
          listIndexes() {
            return {
              async toArray() {
                return [{ name: "_id_" }, { name: "email_1" }];
              },
            };
          },
        };
      },
      admin() {
        return {
          async listDatabases() {
            return { databases: [{ name: "admin" }, { name: "framecraft" }] };
          },
        };
      },
    },
  };

  const schema = new SchemaBuilder(adapter);
  assert.deepEqual(await schema.showTables(), ["logs", "users"]);
  assert.deepEqual(await schema.showDatabases(), ["admin", "framecraft"]);
  assert.deepEqual(await schema.showViews(), ["active_users", "sales_rollup"]);
  assert.deepEqual(await schema.showIndexes("users"), ["_id_", "email_1"]);
});

test("showViews compiles and returns names for PostgreSQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed, [{ name: "active_users" }, { name: "sales_rollup" }]);
  const names = await new SchemaBuilder(adapter).showViews({ schema: "public" });

  assert.deepEqual(names, ["active_users", "sales_rollup"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /information_schema\.views/i);
  assert.deepEqual(executed[0].params, ["public"]);
});

test("showTriggers compiles and returns names for PostgreSQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed, [{ name: "users_audit_trg" }, { name: "users_touch_updated_at" }]);
  const names = await new SchemaBuilder(adapter).showTriggers({ table: "public.users" });

  assert.deepEqual(names, ["users_audit_trg", "users_touch_updated_at"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /information_schema\.triggers/i);
  assert.deepEqual(executed[0].params, ["public", "users"]);
});

test("showIndexes compiles and returns names for SQL Server", async () => {
  const executed = [];
  const adapter = sqlAdapter("mssql", executed, [{ name: "IX_users_email" }, { name: "PK_users" }]);
  const names = await new SchemaBuilder(adapter).showIndexes("dbo.users");

  assert.deepEqual(names, ["IX_users_email", "PK_users"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /sys\.indexes/i);
  assert.deepEqual(executed[0].params, ["users", "dbo"]);
});

test("showConstraints compiles and returns names for MySQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("mysql", executed, [{ name: "PRIMARY" }, { name: "users_email_unique" }]);
  const names = await new SchemaBuilder(adapter).showConstraints({ table: "users", type: "UNIQUE" });

  assert.deepEqual(names, ["PRIMARY", "users_email_unique"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /information_schema\.table_constraints/i);
  assert.deepEqual(executed[0].params, ["users", "UNIQUE"]);
});

test("showFunctions compiles and returns names for PostgreSQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed, [{ name: "greet_user" }, { name: "slugify_title" }]);
  const names = await new SchemaBuilder(adapter).showFunctions();

  assert.deepEqual(names, ["greet_user", "slugify_title"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /information_schema\.routines/i);
  assert.match(executed[0].sql, /routine_type = 'FUNCTION'/i);
});

test("showProcedures compiles and returns names for SQL Server", async () => {
  const executed = [];
  const adapter = sqlAdapter("mssql", executed, [{ name: "refresh_rollup" }, { name: "sync_users" }]);
  const names = await new SchemaBuilder(adapter).showProcedures({ schema: "dbo" });

  assert.deepEqual(names, ["refresh_rollup", "sync_users"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /information_schema\.routines/i);
  assert.match(executed[0].sql, /routine_type = 'PROCEDURE'/i);
  assert.deepEqual(executed[0].params, ["dbo"]);
});

test("showAggregates compiles and returns names for PostgreSQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed, [{ name: "array_concat_agg" }, { name: "jsonb_merge_agg" }]);
  const names = await new SchemaBuilder(adapter).showAggregates();

  assert.deepEqual(names, ["array_concat_agg", "jsonb_merge_agg"]);
  assert.equal(executed[0].type, "select");
  assert.match(executed[0].sql, /pg_aggregate/i);
});

test("showFunctions rejects Mongo, showTriggers rejects Mongo, and showAggregates rejects non-PostgreSQL", async () => {
  const mongoSchema = new SchemaBuilder({ dialect: "mongo", db: {} });
  const mysqlSchema = new SchemaBuilder(sqlAdapter("mysql", [], []));

  await assert.rejects(() => mongoSchema.showFunctions(), /mongo: showFunctions is not supported/);
  await assert.rejects(() => mongoSchema.showSchemas(), /mongo: showSchemas is not supported/);
  await assert.rejects(() => mongoSchema.showSequences(), /mongo: showSequences is not supported/);
  await assert.rejects(() => mongoSchema.showConstraints(), /mongo: showConstraints is not supported/);
  await assert.rejects(() => mongoSchema.showTriggers(), /mongo: showTriggers is not supported/);
  await assert.rejects(() => mysqlSchema.showAggregates(), /mysql: showAggregates is only supported on PostgreSQL/);
  await assert.rejects(() => mysqlSchema.showSequences(), /mysql: showSequences is not supported/);
});
