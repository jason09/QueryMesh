import test from "node:test";
import assert from "node:assert/strict";
import { DB } from "../src/core/DB.js";
import { PgAdapter } from "../src/adapters/PgAdapter.js";
import { MySqlAdapter } from "../src/adapters/MySqlAdapter.js";
import { MsSqlAdapter } from "../src/adapters/MsSqlAdapter.js";
import { OracleAdapter } from "../src/adapters/OracleAdapter.js";
import { MongoAdapter } from "../src/adapters/MongoAdapter.js";

test("DB.query and DB.exec execute raw SQL on PostgreSQL", async () => {
  const calls = [];
  const adapter = new PgAdapter({
    async query(sql, params) {
      calls.push({ sql, params });
      if (String(sql).startsWith("SELECT")) return { rows: [{ id: 1 }], rowCount: 1 };
      return { rows: [], rowCount: 2 };
    },
  });
  const db = new DB(adapter);

  const rows = await db.query("SELECT * FROM users WHERE id = $1", [1]);
  const result = await db.exec("UPDATE users SET active = false WHERE id = $1", [1]);

  assert.deepEqual(rows, [{ id: 1 }]);
  assert.deepEqual(result, { rowCount: 2, rows: [] });
  assert.deepEqual(calls, [
    { sql: "SELECT * FROM users WHERE id = $1", params: [1] },
    { sql: "UPDATE users SET active = false WHERE id = $1", params: [1] },
  ]);
});

test("DB.query and DB.exec execute raw SQL on MySQL", async () => {
  const calls = [];
  const adapter = new MySqlAdapter({
    query(sql, params, cb) {
      calls.push({ sql, params });
      cb(null, String(sql).startsWith("SELECT") ? [{ id: 2 }] : { affectedRows: 1 });
    },
  });
  const db = new DB(adapter);

  assert.deepEqual(await db.query("SELECT * FROM users WHERE id = ?", [2]), [{ id: 2 }]);
  assert.deepEqual(await db.exec("UPDATE users SET active = ? WHERE id = ?", [false, 2]), { affectedRows: 1 });
  assert.deepEqual(calls, [
    { sql: "SELECT * FROM users WHERE id = ?", params: [2] },
    { sql: "UPDATE users SET active = ? WHERE id = ?", params: [false, 2] },
  ]);
});

test("DB.query and DB.exec execute raw SQL on SQL Server", async () => {
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

  assert.deepEqual(await db.query("SELECT * FROM users WHERE id = @p1", [3]), [{ id: 3 }]);
  assert.deepEqual(
    await db.exec("UPDATE users SET active = @p1 WHERE id = @p2", [false, 3]),
    { rowsAffected: [4], recordset: [] },
  );
  assert.deepEqual(calls, [
    { sql: "SELECT * FROM users WHERE id = @p1", inputs: [{ name: "p1", value: 3 }] },
    {
      sql: "UPDATE users SET active = @p1 WHERE id = @p2",
      inputs: [{ name: "p1", value: false }, { name: "p2", value: 3 }],
    },
  ]);
});

test("DB.query and DB.exec execute raw SQL on Oracle", async () => {
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

  assert.deepEqual(await db.query("SELECT * FROM users WHERE id = :p1", [4]), [{ ID: 4 }]);
  assert.deepEqual(
    await db.exec("UPDATE users SET active = :p1 WHERE id = :p2", [false, 4]),
    { rowsAffected: 5, rows: [] },
  );
  assert.equal(closeCount, 2);
  assert.deepEqual(calls, [
    { sql: "SELECT * FROM users WHERE id = :p1", binds: { p1: 4 }, opts: { autoCommit: false } },
    {
      sql: "UPDATE users SET active = :p1 WHERE id = :p2",
      binds: { p1: false, p2: 4 },
      opts: { autoCommit: true },
    },
  ]);
});

test("DB.query and DB.exec reject raw SQL on Mongo", async () => {
  const db = new DB(new MongoAdapter({ collection() {} }));

  await assert.rejects(() => db.query("SELECT * FROM users"), /mongo: raw SQL query is not supported/);
  await assert.rejects(() => db.exec("UPDATE users SET active = false"), /mongo: raw SQL exec is not supported/);
});
