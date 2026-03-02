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
      return dialect === "pg" || dialect === "mssql";
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

test("ANY / ALL array compile for mysql/mssql/oracle", () => {
  for (const dialect of ["mysql", "mssql", "oracle"]) {
    const out = new QueryBuilder(fakeAdapter(dialect), "users")
      .whereAny("score", ">", [10, 20])
      .whereAll("age", "<", [60, 70])
      .compile();

    if (dialect === "mysql") {
      assert.equal(
        out.sql,
        "SELECT * FROM `users` WHERE (`score` > ? OR `score` > ?) AND (`age` < ? AND `age` < ?)",
      );
    }
    if (dialect === "mssql") {
      assert.equal(
        out.sql,
        "SELECT * FROM [users] WHERE ([score] > @p1 OR [score] > @p2) AND ([age] < @p3 AND [age] < @p4)",
      );
    }
    if (dialect === "oracle") {
      assert.equal(
        out.sql,
        "SELECT * FROM \"users\" WHERE (\"score\" > :p1 OR \"score\" > :p2) AND (\"age\" < :p3 AND \"age\" < :p4)",
      );
    }

    assert.deepEqual(out.params, [10, 20, 60, 70]);
  }
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

test("MSSQL returning compiles as OUTPUT", () => {
  const insertOut = new QueryBuilder(fakeAdapter("mssql"), "users")
    .insert({ email: "a@b.com" })
    .returning(["id", "email"])
    .compile();
  assert.equal(
    insertOut.sql,
    "INSERT INTO [users] ([email]) OUTPUT inserted.[id], inserted.[email] VALUES (@p1)",
  );
  assert.deepEqual(insertOut.params, ["a@b.com"]);

  const updateOut = new QueryBuilder(fakeAdapter("mssql"), "users")
    .update({ email: "z@b.com" })
    .where("id", 1)
    .returning("id")
    .compile();
  assert.equal(
    updateOut.sql,
    "UPDATE [users] SET [email] = @p1 OUTPUT inserted.[id] WHERE [id] = @p2",
  );
  assert.deepEqual(updateOut.params, ["z@b.com", 1]);

  const deleteOut = new QueryBuilder(fakeAdapter("mssql"), "users")
    .delete()
    .where("id", 1)
    .returning("id")
    .compile();
  assert.equal(
    deleteOut.sql,
    "DELETE FROM [users] OUTPUT deleted.[id] WHERE [id] = @p1",
  );
  assert.deepEqual(deleteOut.params, [1]);
});

test("onConflictDoUpdate compiles for mssql and oracle via MERGE", () => {
  const mssqlOut = new QueryBuilder(fakeAdapter("mssql"), "users")
    .insert({ id: 1, email: "a@b.com", name: "A" })
    .onConflictDoUpdate("email", { name: "B" })
    .returning("id")
    .compile();

  assert.equal(
    mssqlOut.sql,
    "MERGE INTO [users] AS target USING (SELECT @p1 AS [id], @p2 AS [email], @p3 AS [name]) AS source ON (target.[email] = source.[email]) WHEN MATCHED THEN UPDATE SET target.[name] = @p4 WHEN NOT MATCHED THEN INSERT ([id], [email], [name]) VALUES (source.[id], source.[email], source.[name]) OUTPUT inserted.[id]",
  );
  assert.deepEqual(mssqlOut.params, [1, "a@b.com", "A", "B"]);

  const oracleOut = new QueryBuilder(fakeAdapter("oracle"), "users")
    .insert({ id: 1, email: "a@b.com", name: "A" })
    .onConflictDoUpdate("email", { name: "B" })
    .compile();

  assert.equal(
    oracleOut.sql,
    "MERGE INTO \"users\" target USING (SELECT :p1 AS \"id\", :p2 AS \"email\", :p3 AS \"name\" FROM dual) source ON (target.\"email\" = source.\"email\") WHEN MATCHED THEN UPDATE SET target.\"name\" = :p4 WHEN NOT MATCHED THEN INSERT (\"id\", \"email\", \"name\") VALUES (source.\"id\", source.\"email\", source.\"name\")",
  );
  assert.deepEqual(oracleOut.params, [1, "a@b.com", "A", "B"]);
});

test("Mongo compiles ANY/ALL with literal arrays", () => {
  const out = new QueryBuilder(fakeAdapter("mongo"), "users")
    .whereAny("score", ">", [10, 20, 30])
    .whereAll("score", "<", [100, 200])
    .compile();

  assert.equal(out.mongo.op, "find");
  assert.deepEqual(out.mongo.filter, {
    $and: [
      { score: { $gt: 10 } },
      { score: { $lt: 100 } },
    ],
  });
});

test("Mongo compiles non-equality joins with $lookup pipeline", () => {
  const out = new QueryBuilder(fakeAdapter("mongo"), "orders")
    .join("left", "users", "orders.user_id", ">", "users.id")
    .compile();

  assert.equal(out.mongo.op, "aggregate");
  assert.deepEqual(out.mongo.pipeline[0], {
    $lookup: {
      from: "users",
      let: { __qm_join_left_1: "$user_id" },
      pipeline: [
        { $match: { $expr: { $gt: ["$$__qm_join_left_1", "$id"] } } },
      ],
      as: "users",
    },
  });
  assert.deepEqual(out.mongo.pipeline[1], {
    $unwind: { path: "$users", preserveNullAndEmptyArrays: true },
  });
});

test("Mongo compiles onConflictDoUpdate insert as upsert", () => {
  const out = new QueryBuilder(fakeAdapter("mongo"), "users")
    .insert({ email: "a@b.com", name: "A" })
    .onConflictDoUpdate("email", { name: "B" })
    .compile();

  assert.equal(out.mongo.op, "upsertOne");
  assert.deepEqual(out.mongo.filter, { email: "a@b.com" });
  assert.deepEqual(out.mongo.update, {
    $setOnInsert: { email: "a@b.com", name: "A" },
    $set: { name: "B" },
  });
});

test("Mongo compiles UNION / UNION ALL via union op", () => {
  const q1 = new QueryBuilder(fakeAdapter("mongo"), "a").select("id").where("x", 1);
  const q2 = new QueryBuilder(fakeAdapter("mongo"), "b").select("id").where("y", 2);
  const q3 = new QueryBuilder(fakeAdapter("mongo"), "c").select("id").where("z", 3);

  const out = q1.union(q2).unionAll(q3).compile();
  assert.equal(out.mongo.op, "union");
  assert.equal(out.mongo.base.op, "find");
  assert.equal(out.mongo.base.collection, "a");
  assert.equal(out.mongo.unions.length, 2);
  assert.equal(out.mongo.unions[0].all, false);
  assert.equal(out.mongo.unions[0].source.collection, "b");
  assert.equal(out.mongo.unions[1].all, true);
  assert.equal(out.mongo.unions[1].source.collection, "c");
});

test("Mongo compiles insertSelect with QueryBuilder source", () => {
  const src = new QueryBuilder(fakeAdapter("mongo"), "users_archive")
    .select(["uid", "email"])
    .where("is_active", true);

  const out = new QueryBuilder(fakeAdapter("mongo"), "users")
    .insertSelect(["id", "email"], src)
    .compile();

  assert.equal(out.mongo.op, "insertSelect");
  assert.equal(out.mongo.collection, "users");
  assert.equal(out.mongo.source.collection, "users_archive");
  assert.deepEqual(out.mongo.mappings, [
    { target: "id", source: "uid" },
    { target: "email", source: "email" },
  ]);
});

test("Mongo rejects unsupported SQL-only features", () => {
  assert.throws(
    () => new QueryBuilder(fakeAdapter("mongo"), "users").whereAny("score", ">", new QueryBuilder(fakeAdapter("mongo"), "limits").select("max")).compile(),
    /supports only literal arrays/,
  );
  assert.throws(
    () => new QueryBuilder(fakeAdapter("mongo"), "users")
      .select("*")
      .union("SELECT 1")
      .compile(),
    /supports only QueryBuilder sources for mongo/,
  );
  assert.throws(
    () =>
      new QueryBuilder(fakeAdapter("mongo"), "users")
        .insert({ email: "a@b.com" })
        .onDuplicateKeyUpdate({ email: "b@c.com" })
        .compile(),
    /does not support onDuplicateKeyUpdate/,
  );
  assert.throws(
    () =>
      new QueryBuilder(fakeAdapter("mongo"), "users")
        .insertSelect(["id"], "SELECT id FROM x")
        .compile(),
    /supports only QueryBuilder source/,
  );
});
