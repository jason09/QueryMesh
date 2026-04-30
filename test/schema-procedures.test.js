import test from "node:test";
import assert from "node:assert/strict";
import { SchemaBuilder } from "../src/core/SchemaBuilder.js";

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

test("createProcedure(pg) compiles CREATE OR REPLACE PROCEDURE", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .createProcedure({
      name: "public.refresh_rollup",
      args: ["target_date date"],
      language: "plpgsql",
      orReplace: true,
      body: "BEGIN\n  PERFORM refresh_rollup_for(target_date);\nEND;",
    })
    .exec();

  assert.equal(
    executed[0],
    'CREATE OR REPLACE PROCEDURE "public"."refresh_rollup"(target_date date) LANGUAGE plpgsql AS $$\nBEGIN\n  PERFORM refresh_rollup_for(target_date);\nEND;\n$$',
  );
});

test("createProcedure(mysql) rejects orReplace", () => {
  const adapter = sqlAdapter("mysql", []);

  assert.throws(
    () =>
      new SchemaBuilder(adapter).createProcedure({
        name: "refresh_rollup",
        body: "BEGIN\n  SELECT 1;\nEND",
        orReplace: true,
      }),
    /mysql: createProcedure does not support orReplace/,
  );
});

test("createProcedure(mssql) compiles CREATE OR ALTER PROCEDURE", async () => {
  const executed = [];
  const adapter = sqlAdapter("mssql", executed);

  await new SchemaBuilder(adapter)
    .createProcedure({
      name: "dbo.refresh_rollup",
      args: ["@target_date DATE"],
      orReplace: true,
      body: "BEGIN\n  EXEC dbo.refresh_rollup_inner @target_date;\nEND",
    })
    .exec();

  assert.equal(
    executed[0],
    "CREATE OR ALTER PROCEDURE [dbo].[refresh_rollup] @target_date DATE AS\nBEGIN\n  EXEC dbo.refresh_rollup_inner @target_date;\nEND",
  );
});

test("dropProcedure(oracle) supports ifExists wrapper", async () => {
  const executed = [];
  const adapter = sqlAdapter("oracle", executed);

  await new SchemaBuilder(adapter)
    .dropProcedure("audit.refresh_rollup", { ifExists: true })
    .exec();

  assert.deepEqual(executed, [
    "BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE audit.refresh_rollup'; EXCEPTION WHEN OTHERS THEN NULL; END;",
  ]);
});
