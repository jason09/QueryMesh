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

test("SQL: explicit grouped where callbacks compile with parentheses", () => {
  const qb = new QueryBuilder(fakeAdapter("pg"), "users")
    .where((q) => q.where("a", "x").where("b", "y"))
    .orWhere((q) => q.where("c", "z").orWhere("d", "xx"));

  const out = qb.compile();
  assert.equal(
    out.sql,
    'SELECT * FROM "users" WHERE ("a" = $1 AND "b" = $2) OR ("c" = $3 OR "d" = $4)',
  );
  assert.deepEqual(out.params, ["x", "y", "z", "xx"]);
});

test("SQL: nested grouped where callbacks compile correctly", () => {
  const qb = new QueryBuilder(fakeAdapter("pg"), "users")
    .where((q) => q.where("a", "x").where("b", "y"))
    .orWhere((q) =>
      q.where((g) => g.where("c", "z").orWhere("d", "xx"))
        .orWhere((g) => g.where("a1", "x1").where("b1", "y1")),
    );

  const out = qb.compile();
  assert.equal(
    out.sql,
    'SELECT * FROM "users" WHERE ("a" = $1 AND "b" = $2) OR (("c" = $3 OR "d" = $4) OR ("a1" = $5 AND "b1" = $6))',
  );
  assert.deepEqual(out.params, ["x", "y", "z", "xx", "x1", "y1"]);
});

test("Mongo: explicit grouped where callbacks preserve logic", () => {
  const qb = new QueryBuilder(fakeAdapter("mongo"), "users")
    .where((q) => q.where("a", "x").where("b", "y"))
    .orWhere((q) => q.where("c", "z").orWhere("d", "xx"));

  const out = qb.compile();
  assert.equal(out.mongo.op, "aggregate");
  assert.deepEqual(out.mongo.pipeline[0], {
    $match: {
      $or: [
        { $and: [{ a: "x" }, { b: "y" }] },
        { $or: [{ c: "z" }, { d: "xx" }] },
      ],
    },
  });
});

test("Unsafe operators are rejected", () => {
  const qb = new QueryBuilder(fakeAdapter("pg"), "users");
  assert.throws(() => qb.whereOp("a", "= 1 OR 1=1 --", 1), /Unsafe whereOp operator/);
  assert.throws(
    () => qb.join("inner", "roles", "users.role_id", "= roles.id; DROP TABLE users; --", "roles.id"),
    /Unsafe join operator/,
  );
});
