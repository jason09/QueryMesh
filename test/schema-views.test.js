import test from "node:test";
import assert from "node:assert/strict";
import { SchemaBuilder } from "../src/core/SchemaBuilder.js";
import { QueryBuilder } from "../src/core/QueryBuilder.js";

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

test("createView and dropView compile for PostgreSQL", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .createView("v_users", "SELECT id, email FROM users WHERE is_active = true", { ifNotExists: true })
    .exec();

  await new SchemaBuilder(adapter)
    .dropView("v_users", { ifExists: true, cascade: true })
    .exec();

  assert.equal(
    executed[0],
    'CREATE VIEW IF NOT EXISTS "v_users" AS SELECT id, email FROM users WHERE is_active = true',
  );
  assert.equal(executed[1], 'DROP VIEW IF EXISTS "v_users" CASCADE');
});

test("dropView ifExists compiles for SQL Server", async () => {
  const executed = [];
  const adapter = sqlAdapter("mssql", executed);

  await new SchemaBuilder(adapter)
    .dropView("v_users", { ifExists: true })
    .exec();

  assert.equal(
    executed[0],
    "IF OBJECT_ID(N'v_users', N'V') IS NOT NULL DROP VIEW [v_users]",
  );
});

test("createView rejects parameterized query builders", () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);
  const query = new QueryBuilder(adapter, "users").select("id").where("is_active", true);

  assert.throws(
    () => new SchemaBuilder(adapter).createView("v_users", query),
    /does not support parameterized queries/,
  );
});
