import test from "node:test";
import assert from "node:assert/strict";
import { QueryBuilder } from "../src/core/QueryBuilder.js";

function fakeAdapter(dialect) {
  return {
    dialect,
    placeholder(i) {
      if (dialect === "pg") return `$${i}`;
      if (dialect === "mssql") return `@p${i}`;
      if (dialect === "oracle") return `:p${i}`;
      return "?";
    },
    supportsReturning() {
      return dialect === "pg";
    },
  };
}

test("UNION / UNION ALL compile for SQL", () => {
  const q1 = new QueryBuilder(fakeAdapter("pg"), "users").select("id").where("country", "US");
  const q2 = new QueryBuilder(fakeAdapter("pg"), "admins").select("id").where("enabled", true);
  const q3 = new QueryBuilder(fakeAdapter("pg"), "guests").select("id").where("active", true);

  const out = q1.union(q2).unionAll(q3).compile();
  assert.equal(
    out.sql,
    '(SELECT "id" FROM "users" WHERE "country" = $1) UNION (SELECT "id" FROM "admins" WHERE "enabled" = $2) UNION ALL (SELECT "id" FROM "guests" WHERE "active" = $3)',
  );
  assert.deepEqual(out.params, ["US", true, true]);
});

test("insertSelect compiles INSERT ... SELECT", () => {
  const src = new QueryBuilder(fakeAdapter("pg"), "users_archive")
    .select(["id", "email"])
    .where("is_active", true);

  const out = new QueryBuilder(fakeAdapter("pg"), "users")
    .insertSelect(["id", "email"], src)
    .compile();

  assert.equal(
    out.sql,
    'INSERT INTO "users" ("id", "email") SELECT "id", "email" FROM "users_archive" WHERE "is_active" = $1',
  );
  assert.deepEqual(out.params, [true]);
});

test("ANY / ALL compile", () => {
  const sub = new QueryBuilder(fakeAdapter("pg"), "limits").select("max_score");
  const out = new QueryBuilder(fakeAdapter("pg"), "users")
    .whereAny("score", ">", [10, 20, 30])
    .whereAll("score", "<=", sub)
    .compile();

  assert.equal(
    out.sql,
    'SELECT * FROM "users" WHERE "score" > ANY ($1) AND "score" <= ALL (SELECT "max_score" FROM "limits")',
  );
  assert.deepEqual(out.params, [[10, 20, 30]]);
});

test("NOT, IS and NULL helpers compile", () => {
  const out = new QueryBuilder(fakeAdapter("pg"), "users")
    .whereNot((q) => q.where("a", 1).orWhere("b", 2))
    .whereIs("flag", true)
    .whereIsNot("deleted_at", null)
    .compile();

  assert.equal(
    out.sql,
    'SELECT * FROM "users" WHERE NOT ("a" = $1 OR "b" = $2) AND "flag" IS TRUE AND "deleted_at" IS NOT NULL',
  );
  assert.deepEqual(out.params, [1, 2]);
});

test("MIN/MAX aggregates compile", () => {
  const out = new QueryBuilder(fakeAdapter("pg"), "users")
    .min("age", "min_age")
    .max("age", "max_age")
    .compile();

  assert.equal(
    out.sql,
    'SELECT MIN("age") AS "min_age", MAX("age") AS "max_age" FROM "users"',
  );
});

test("Mongo rejects unsupported SQL-only features", () => {
  assert.throws(
    () => new QueryBuilder(fakeAdapter("mongo"), "users").whereAny("score", ">", [1, 2]).compile(),
    /does not support SQL ANY\/ALL/,
  );
  assert.throws(
    () => {
      const q1 = new QueryBuilder(fakeAdapter("mongo"), "a").select("*");
      const q2 = new QueryBuilder(fakeAdapter("mongo"), "b").select("*");
      q1.union(q2).compile();
    },
    /does not support UNION/,
  );
});
