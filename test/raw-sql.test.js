import test from "node:test";
import assert from "node:assert/strict";
import { DB } from "../src/core/DB.js";
import { PgAdapter } from "../src/adapters/PgAdapter.js";
import { MySqlAdapter } from "../src/adapters/MySqlAdapter.js";
import { MsSqlAdapter } from "../src/adapters/MsSqlAdapter.js";
import { OracleAdapter } from "../src/adapters/OracleAdapter.js";
import { MongoAdapter } from "../src/adapters/MongoAdapter.js";

test("DB.query executes raw SQL on PostgreSQL and exposes rows/rowCount", async () => {
  const calls = [];
  const adapter = new PgAdapter({
    async query(sql, params) {
      calls.push({ sql, params });
      if (String(sql).startsWith("SELECT")) return { rows: [{ id: 1 }], rowCount: 1 };
      return { rows: [], rowCount: 2 };
    },
  });
  const db = new DB(adapter);

  const rows = await db.query("SELECT * FROM users WHERE id = ?", [1]);
  const result = await db.query("UPDATE users SET active = ? WHERE id = ?", [false, 1]);

  assert.deepEqual(rows, [{ id: 1 }]);
  assert.equal(rows.rows, rows);
  assert.equal(rows.rowCount, 1);
  assert.deepEqual(result, []);
  assert.equal(result.rows, result);
  assert.equal(result.rowCount, 2);
  assert.deepEqual(calls, [
    { sql: "SELECT * FROM users WHERE id = $1", params: [1] },
    { sql: "UPDATE users SET active = $1 WHERE id = $2", params: [false, 1] },
  ]);
});

test("DB.query rewrites native placeholders and skips quoted question marks", async () => {
  const calls = [];
  const adapter = new PgAdapter({
    async query(sql, params) {
      calls.push({ sql, params });
      return { rows: [{ ok: true }], rowCount: 1 };
    },
  });
  const db = new DB(adapter);

  await db.query("SELECT '?' AS marker, * FROM users WHERE id = @p1 AND email = :p2", [7, "a@b.com"]);

  assert.deepEqual(calls, [
    {
      sql: "SELECT '?' AS marker, * FROM users WHERE id = $1 AND email = $2",
      params: [7, "a@b.com"],
    },
  ]);
});

test("DB.query executes raw SQL on MySQL with pg-style placeholders", async () => {
  const calls = [];
  const adapter = new MySqlAdapter({
    query(sql, params, cb) {
      calls.push({ sql, params });
      cb(null, String(sql).startsWith("SELECT") ? [{ id: 2 }] : { affectedRows: 1, insertId: 10 });
    },
  });
  const db = new DB(adapter);

  const rows = await db.query("SELECT * FROM users WHERE id = $1", [2]);
  const result = await db.query("UPDATE users SET active = $1 WHERE id = $2", [false, 2]);

  assert.deepEqual(rows, [{ id: 2 }]);
  assert.equal(rows.rows, rows);
  assert.equal(rows.rowCount, 1);
  assert.deepEqual(result, []);
  assert.equal(result.rowCount, 1);
  assert.equal(result.affectedRows, 1);
  assert.equal(result.insertId, 10);
  assert.deepEqual(calls, [
    { sql: "SELECT * FROM users WHERE id = ?", params: [2] },
    { sql: "UPDATE users SET active = ? WHERE id = ?", params: [false, 2] },
  ]);
});

test("DB.query executes raw SQL on SQL Server with portable placeholders", async () => {
  const calls = [];
  const adapter = new MsSqlAdapter({
    request() {
      const inputs = [];
      return {
        input(name, value) {
          inputs.push({ name, value });
        },
        async query(sql) {
          calls.push({ sql, inputs: [...inputs] });
          if (String(sql).startsWith("SELECT")) return { recordset: [{ id: 3 }], rowsAffected: [1] };
          return { recordset: [], rowsAffected: [4] };
        },
      };
    },
  });
  const db = new DB(adapter);

  const rows = await db.query("SELECT * FROM users WHERE id = ?", [3]);
  const result = await db.query("UPDATE users SET active = ? WHERE id = ?", [false, 3]);

  assert.deepEqual(rows, [{ id: 3 }]);
  assert.equal(rows.rows, rows);
  assert.equal(rows.rowCount, 1);
  assert.deepEqual(result, []);
  assert.equal(result.rowCount, 4);
  assert.deepEqual(result.rowsAffected, [4]);
  assert.deepEqual(calls, [
    { sql: "SELECT * FROM users WHERE id = @p1", inputs: [{ name: "p1", value: 3 }] },
    {
      sql: "UPDATE users SET active = @p1 WHERE id = @p2",
      inputs: [{ name: "p1", value: false }, { name: "p2", value: 3 }],
    },
  ]);
});

test("DB.query executes raw SQL on Oracle with portable placeholders", async () => {
  const calls = [];
  let closeCount = 0;
  const adapter = new OracleAdapter({
    async getConnection() {
      return {
        async execute(sql, binds, opts) {
          calls.push({ sql, binds, opts });
          if (String(sql).startsWith("SELECT")) return { rows: [{ ID: 4 }], rowsAffected: 0 };
          return { rows: [], rowsAffected: 5 };
        },
        async close() {
          closeCount += 1;
        },
      };
    },
  });
  const db = new DB(adapter);

  const rows = await db.query("SELECT * FROM users WHERE id = ?", [4]);
  const result = await db.query("UPDATE users SET active = ? WHERE id = ?", [false, 4]);

  assert.deepEqual(rows, [{ ID: 4 }]);
  assert.equal(rows.rows, rows);
  assert.equal(rows.rowCount, 1);
  assert.deepEqual(result, []);
  assert.equal(result.rowCount, 5);
  assert.equal(result.rowsAffected, 5);
  assert.equal(closeCount, 2);
  assert.deepEqual(calls, [
    { sql: "SELECT * FROM users WHERE id = :p1", binds: { p1: 4 }, opts: { autoCommit: true } },
    {
      sql: "UPDATE users SET active = :p1 WHERE id = :p2",
      binds: { p1: false, p2: 4 },
      opts: { autoCommit: true },
    },
  ]);
});

test("DB.exec rejects raw SQL and points users to DB.query", async () => {
  const db = new DB(new PgAdapter({ async query() { return { rows: [], rowCount: 0 }; } }));

  await assert.rejects(
    () => db.exec("UPDATE users SET active = false"),
    /db\.exec\(\) no longer executes raw SQL\. Use db\.query\(sql, params\) instead/,
  );
});

test("DB.query and DB.exec reject raw SQL on Mongo", async () => {
  const db = new DB(new MongoAdapter({ collection() {} }));

  await assert.rejects(() => db.query("SELECT * FROM users"), /mongo: raw SQL query is not supported/);
  await assert.rejects(
    () => db.exec("UPDATE users SET active = false"),
    /db\.exec\(\) no longer executes raw SQL/,
  );
});
