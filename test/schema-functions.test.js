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

test("createFunction(pg) compiles CREATE OR REPLACE FUNCTION", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .createFunction({
      name: "public.greet_user",
      args: ["name text"],
      returns: "text",
      language: "plpgsql",
      orReplace: true,
      body: "BEGIN\n  RETURN 'Hello ' || name;\nEND;",
    })
    .exec();

  assert.equal(
    executed[0],
    'CREATE OR REPLACE FUNCTION "public"."greet_user"(name text) RETURNS text AS $$\nBEGIN\n  RETURN \'Hello \' || name;\nEND;\n$$ LANGUAGE plpgsql',
  );
});

test("dropFunction(pg) compiles signature-aware DROP FUNCTION", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .dropFunction("public.greet_user", { args: ["text"], ifExists: true, cascade: true })
    .exec();

  assert.deepEqual(executed, [
    'DROP FUNCTION IF EXISTS "public"."greet_user"(text) CASCADE',
  ]);
});

test("createFunction(mysql) supports deterministic bodies", async () => {
  const executed = [];
  const adapter = sqlAdapter("mysql", executed);

  await new SchemaBuilder(adapter)
    .createFunction({
      name: "slugify_title",
      args: ["title VARCHAR(255)"],
      returns: "VARCHAR(255)",
      deterministic: true,
      body: "RETURN LOWER(REPLACE(title, ' ', '-'));",
    })
    .exec();

  assert.equal(
    executed[0],
    "CREATE FUNCTION `slugify_title`(title VARCHAR(255)) RETURNS VARCHAR(255) DETERMINISTIC\nRETURN LOWER(REPLACE(title, ' ', '-'));",
  );
});

test("createFunction(mysql) rejects orReplace", () => {
  const adapter = sqlAdapter("mysql", []);

  assert.throws(
    () =>
      new SchemaBuilder(adapter).createFunction({
        name: "slugify_title",
        returns: "VARCHAR(255)",
        body: "RETURN title;",
        orReplace: true,
      }),
    /mysql: createFunction does not support orReplace/,
  );
});

test("createFunction(mssql) compiles CREATE OR ALTER FUNCTION", async () => {
  const executed = [];
  const adapter = sqlAdapter("mssql", executed);

  await new SchemaBuilder(adapter)
    .createFunction({
      name: "dbo.full_name",
      args: ["@first NVARCHAR(50)", "@last NVARCHAR(50)"],
      returns: "NVARCHAR(120)",
      orReplace: true,
      body: "BEGIN\n  RETURN @first + N' ' + @last;\nEND",
    })
    .exec();

  assert.equal(
    executed[0],
    "CREATE OR ALTER FUNCTION [dbo].[full_name](@first NVARCHAR(50), @last NVARCHAR(50)) RETURNS NVARCHAR(120) AS\nBEGIN\n  RETURN @first + N' ' + @last;\nEND",
  );
});

test("dropFunction(oracle) supports ifExists wrapper", async () => {
  const executed = [];
  const adapter = sqlAdapter("oracle", executed);

  await new SchemaBuilder(adapter)
    .dropFunction("audit.touch_user", { ifExists: true })
    .exec();

  assert.deepEqual(executed, [
    "BEGIN EXECUTE IMMEDIATE 'DROP FUNCTION audit.touch_user'; EXCEPTION WHEN OTHERS THEN NULL; END;",
  ]);
});

test("createFunction is SQL-only", () => {
  const adapter = {
    dialect: "mongo",
    async execute() {
      return [];
    },
  };

  assert.throws(
    () =>
      new SchemaBuilder(adapter).createFunction({
        name: "hello",
        returns: "text",
        body: "return 'hello';",
      }),
    /Schema builder is SQL-only/,
  );
});
