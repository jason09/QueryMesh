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

test("showTables/showDatabases return names for Mongo", async () => {
  const adapter = {
    dialect: "mongo",
    db: {
      listCollections() {
        return {
          async toArray() {
            return [{ name: "users" }, { name: "logs" }];
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
});
