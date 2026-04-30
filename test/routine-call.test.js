import test from "node:test";
import assert from "node:assert/strict";
import { DB } from "../src/core/DB.js";
import { PgAdapter } from "../src/adapters/PgAdapter.js";
import { MySqlAdapter } from "../src/adapters/MySqlAdapter.js";
import { MsSqlAdapter } from "../src/adapters/MsSqlAdapter.js";
import { OracleAdapter } from "../src/adapters/OracleAdapter.js";
import { MongoAdapter } from "../src/adapters/MongoAdapter.js";

test("DB.call executes PostgreSQL procedures with CALL", async () => {
  const calls = [];
  const adapter = new PgAdapter({
    async query(sql, params) {
      calls.push({ sql, params });
      return { rows: [], rowCount: 1 };
    },
  });
  const db = new DB(adapter);

  const result = await db.call("public.refresh_rollup", ["2026-01-01"]);

  assert.deepEqual(result, []);
  assert.equal(result.rowCount, 1);
  assert.deepEqual(calls, [
    { sql: 'CALL "public"."refresh_rollup"($1)', params: ["2026-01-01"] },
  ]);
});

test("DB.call executes PostgreSQL scalar functions with SELECT", async () => {
  const calls = [];
  const adapter = new PgAdapter({
    async query(sql, params) {
      calls.push({ sql, params });
      return { rows: [{ value: 5 }], rowCount: 1 };
    },
  });
  const db = new DB(adapter);

  const result = await db.call("public.add_one", [4], { kind: "function" });

  assert.deepEqual(result, [{ value: 5 }]);
  assert.equal(result.rowCount, 1);
  assert.deepEqual(calls, [
    { sql: 'SELECT "public"."add_one"($1) AS "value"', params: [4] },
  ]);
});

test("DB.call maps PostgreSQL OUT params from returned row", async () => {
  const calls = [];
  const adapter = new PgAdapter({
    async query(sql, params) {
      calls.push({ sql, params });
      return { rows: [{ total: 7 }], rowCount: 1 };
    },
  });
  const db = new DB(adapter);

  const result = await db.call("public.count_users", [
    { mode: "out", name: "total" },
  ]);

  assert.deepEqual(result, [{ total: 7 }]);
  assert.deepEqual(result.out, { total: 7 });
  assert.deepEqual(calls, [
    { sql: 'CALL "public"."count_users"()', params: [] },
  ]);
});

test("DB.call executes MySQL scalar functions with custom alias", async () => {
  const calls = [];
  const adapter = new MySqlAdapter({
    query(sql, params, cb) {
      calls.push({ sql, params });
      cb(null, [{ slug: "hello-world" }]);
    },
  });
  const db = new DB(adapter);

  const result = await db.call("slugify_title", ["Hello World"], { kind: "function", as: "slug" });

  assert.deepEqual(result, [{ slug: "hello-world" }]);
  assert.equal(result.rowCount, 1);
  assert.deepEqual(calls, [
    { sql: "SELECT `slugify_title`(?) AS `slug`", params: ["Hello World"] },
  ]);
});

test("DB.call executes MySQL procedures with OUT/INOUT via session variables", async () => {
  const calls = [];
  const adapter = new MySqlAdapter({
    query(sql, params, cb) {
      calls.push({ sql, params });
      cb(null, [{ affectedRows: 0 }, [{ total: 11, current_status: "ready" }]]);
    },
  }, { config: { multipleStatements: true } });
  const db = new DB(adapter);

  const result = await db.call("refresh_rollup", [
    { mode: "in", value: "2026-01-01" },
    { mode: "out", name: "total" },
    { mode: "inout", name: "current_status", value: "queued" },
  ]);

  assert.deepEqual(result, []);
  assert.deepEqual(result.out, { total: 11, current_status: "ready" });
  assert.deepEqual(calls, [
    {
      sql: "SET @qm_out_3 = ?; CALL `refresh_rollup`(?, @qm_out_2, @qm_out_3); SELECT @qm_out_2 AS `total`, @qm_out_3 AS `current_status`",
      params: ["queued", "2026-01-01"],
    },
  ]);
});

test("DB.call executes SQL Server procedures with EXEC", async () => {
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
          return { recordset: [], rowsAffected: [1] };
        },
      };
    },
  });
  const db = new DB(adapter);

  const result = await db.call("dbo.refresh_rollup", ["2026-01-01"]);

  assert.deepEqual(result, []);
  assert.equal(result.rowCount, 1);
  assert.deepEqual(calls, [
    {
      sql: "EXEC [dbo].[refresh_rollup] @p1",
      inputs: [{ name: "p1", value: "2026-01-01" }],
    },
  ]);
});

test("DB.call executes SQL Server procedures with OUT params", async () => {
  const calls = [];
  const adapter = new MsSqlAdapter({
    request() {
      const inputs = [];
      const outputs = [];
      return {
        input(name, value) {
          inputs.push({ name, value });
        },
        output(name, type, value) {
          outputs.push({ name, type, value });
        },
        async query(sql) {
          calls.push({ sql, inputs: [...inputs], outputs: [...outputs] });
          return { recordset: [], rowsAffected: [0], output: { total: 19, status: "done" } };
        },
      };
    },
  });
  const db = new DB(adapter);

  const result = await db.call("dbo.refresh_rollup", [
    { mode: "in", name: "target_date", value: "2026-01-01" },
    { mode: "out", name: "total", type: "Int" },
    { mode: "inout", name: "status", type: "VarChar", value: "queued" },
  ]);

  assert.deepEqual(result, []);
  assert.deepEqual(result.out, { total: 19, status: "done" });
  assert.deepEqual(calls, [
    {
      sql: "EXEC [dbo].[refresh_rollup] @target_date = @target_date, @total = @total OUTPUT, @status = @status OUTPUT",
      inputs: [{ name: "target_date", value: "2026-01-01" }],
      outputs: [
        { name: "total", type: "Int", value: undefined },
        { name: "status", type: "VarChar", value: "queued" },
      ],
    },
  ]);
});

test("DB.call executes Oracle procedures with PL/SQL block", async () => {
  const calls = [];
  let closeCount = 0;
  const adapter = new OracleAdapter({
    async getConnection() {
      return {
        async execute(sql, binds, opts) {
          calls.push({ sql, binds, opts });
          return { rows: [], rowsAffected: 1 };
        },
        async close() {
          closeCount += 1;
        },
      };
    },
  });
  const db = new DB(adapter);

  const result = await db.call("audit.refresh_rollup", ["2026-01-01"]);

  assert.deepEqual(result, []);
  assert.equal(result.rowCount, 1);
  assert.equal(closeCount, 1);
  assert.deepEqual(calls, [
    {
      sql: 'BEGIN "audit"."refresh_rollup"(:p1); END;',
      binds: { p1: "2026-01-01" },
      opts: { autoCommit: true },
    },
  ]);
});

test("DB.call executes Oracle procedures with OUT params", async () => {
  const calls = [];
  let closeCount = 0;
  const adapter = new OracleAdapter({
    async getConnection() {
      return {
        async execute(sql, binds, opts) {
          calls.push({ sql, binds, opts });
          return { rows: [], rowsAffected: 0, outBinds: { total: 33, state: "ready" } };
        },
        async close() {
          closeCount += 1;
        },
      };
    },
  }, {
    features: {
      driverImporter: async () => ({
        BIND_OUT: "BIND_OUT",
        BIND_INOUT: "BIND_INOUT",
        STRING: "STRING",
        NUMBER: "NUMBER",
      }),
    },
  });
  const db = new DB(adapter);

  const result = await db.call("audit.refresh_rollup", [
    { mode: "in", name: "run_date", value: "2026-01-01" },
    { mode: "out", name: "total", type: "NUMBER" },
    { mode: "inout", name: "state", type: "STRING", value: "queued", size: 50 },
  ]);

  assert.deepEqual(result, []);
  assert.deepEqual(result.out, { total: 33, state: "ready" });
  assert.equal(closeCount, 1);
  assert.equal(calls[0].sql, 'BEGIN "audit"."refresh_rollup"(:run_date, :total, :state); END;');
  assert.deepEqual(calls[0].opts, { autoCommit: true });
  assert.deepEqual(calls[0].binds, {
    run_date: "2026-01-01",
    total: { dir: "BIND_OUT", type: "NUMBER" },
    state: { dir: "BIND_INOUT", type: "STRING", maxSize: 50, val: "queued" },
  });
});

test("DB.callTable executes set-returning/table-valued functions", async () => {
  const calls = [];
  const adapter = new PgAdapter({
    async query(sql, params) {
      calls.push({ sql, params });
      return { rows: [{ id: 1 }], rowCount: 1 };
    },
  });
  const db = new DB(adapter);

  const result = await db.callTable("public.list_active_users", [true]);

  assert.deepEqual(result, [{ id: 1 }]);
  assert.deepEqual(calls, [
    { sql: 'SELECT * FROM "public"."list_active_users"($1)', params: [true] },
  ]);
});

test("DB.callTable rejects unsupported dialects and OUT params", async () => {
  const mysql = new DB(new MySqlAdapter({
    query(sql, params, cb) {
      cb(null, []);
    },
  }));
  const mongo = new DB(new MongoAdapter({ collection() {} }));

  await assert.rejects(
    () => mysql.callTable("list_users"),
    /mysql: db\.callTable\(\) is not supported/,
  );
  await assert.rejects(
    () => mongo.callTable("list_users"),
    /mongo: db\.callTable\(\) is not supported/,
  );
  await assert.rejects(
    () => new DB(new PgAdapter({ async query() { return { rows: [], rowCount: 0 }; } })).callTable("x", [{ mode: "out", name: "total" }]),
    /callTable\(\) does not support OUT\/INOUT params/,
  );
});

test("DB.call rejects MongoDB", async () => {
  const db = new DB(new MongoAdapter({ collection() {} }));

  await assert.rejects(
    () => db.call("refresh_rollup"),
    /mongo: db\.call\(\) is not supported/,
  );
});
