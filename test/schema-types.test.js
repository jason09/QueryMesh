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

test("createType(pg) compiles ENUM types from value arrays", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .createType("public.order_status", ["pending", "paid", "cancelled"])
    .exec();

  assert.deepEqual(executed, [
    `CREATE TYPE "public"."order_status" AS ENUM ('pending', 'paid', 'cancelled')`,
  ]);
});

test("createType(pg) compiles composite types with ifNotExists wrapper", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .createType("public.address_t", {
      kind: "composite",
      fields: {
        street: "text",
        zip_code: "text",
      },
    }, { ifNotExists: true })
    .exec();

  assert.deepEqual(executed, [
    'DO $$ BEGIN CREATE TYPE "public"."address_t" AS ("street" text, "zip_code" text); EXCEPTION WHEN duplicate_object THEN NULL; END $$;',
  ]);
});

test("dropType(pg) compiles signature with IF EXISTS and CASCADE", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .dropType("public.order_status", { ifExists: true, cascade: true })
    .exec();

  assert.deepEqual(executed, [
    'DROP TYPE IF EXISTS "public"."order_status" CASCADE',
  ]);
});

test("createType is PostgreSQL-only", () => {
  const adapter = sqlAdapter("mysql", []);

  assert.throws(
    () => new SchemaBuilder(adapter).createType("order_status", ["pending", "paid"]),
    /mysql: createType is only supported on PostgreSQL/,
  );
});
