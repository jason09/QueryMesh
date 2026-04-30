import test from "node:test";
import assert from "node:assert/strict";
import { SchemaBuilder } from "../src/core/SchemaBuilder.js";

function sqlAdapter(dialect, executed) {
  return {
    dialect,
    async execute(qb) {
      const out = qb.compile();
      executed.push(out.sql);
      return [];
    },
  };
}

test("createAggregate(pg) compiles PostgreSQL aggregate definition", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .createAggregate({
      name: "public.array_concat_agg",
      args: ["anycompatiblearray"],
      definition: {
        SFUNC: "array_cat",
        STYPE: "anycompatiblearray",
        INITCOND: "{}",
        PARALLEL: "SAFE",
      },
    })
    .exec();

  assert.equal(
    executed[0],
    'CREATE AGGREGATE "public"."array_concat_agg"(anycompatiblearray) (\n  SFUNC = array_cat,\n  STYPE = anycompatiblearray,\n  INITCOND = \'{}\',\n  PARALLEL = SAFE\n)',
  );
});

test("dropAggregate(pg) compiles DROP AGGREGATE", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .dropAggregate("public.array_concat_agg", {
      args: ["anycompatiblearray"],
      ifExists: true,
      cascade: true,
    })
    .exec();

  assert.deepEqual(executed, [
    'DROP AGGREGATE IF EXISTS "public"."array_concat_agg"(anycompatiblearray) CASCADE',
  ]);
});

test("createAggregate is PostgreSQL-only", () => {
  const adapter = sqlAdapter("mysql", []);

  assert.throws(
    () =>
      new SchemaBuilder(adapter).createAggregate({
        name: "array_concat_agg",
        definition: { SFUNC: "array_cat", STYPE: "text" },
      }),
    /mysql: createAggregate is only supported on PostgreSQL/,
  );
});
