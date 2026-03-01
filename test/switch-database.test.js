import test from "node:test";
import assert from "node:assert/strict";
import { DB } from "../src/core/DB.js";
import { PgAdapter } from "../src/adapters/PgAdapter.js";
import { MySqlAdapter } from "../src/adapters/MySqlAdapter.js";
import { MsSqlAdapter } from "../src/adapters/MsSqlAdapter.js";
import { OracleAdapter } from "../src/adapters/OracleAdapter.js";
import { MongoAdapter } from "../src/adapters/MongoAdapter.js";

test("DB.switchDatabase mutates adapter and returns same DB instance", async () => {
  const adapterA = {
    dialect: "pg",
    async switchDatabase(name) {
      return { dialect: "pg", currentDatabase: name, switchDatabase: this.switchDatabase };
    },
  };

  const db = new DB(adapterA);
  const out = await db.switchDatabase("app_b");
  assert.equal(out, db);
  assert.equal(db.adapter.currentDatabase, "app_b");
});

test("DB.useDatabase is alias of switchDatabase", async () => {
  const adapterA = {
    dialect: "pg",
    async switchDatabase(name) {
      return { dialect: "pg", currentDatabase: name, switchDatabase: this.switchDatabase };
    },
  };
  const db = new DB(adapterA);
  await db.useDatabase("app_c");
  assert.equal(db.adapter.currentDatabase, "app_c");
});

test("PgAdapter switchDatabase creates new pool with updated database", async () => {
  let closed = false;
  let capturedCfg = null;

  const oldPool = {
    async query() { return { rows: [], rowCount: 0 }; },
    async end() { closed = true; },
  };

  class FakePool {
    constructor(cfg) { capturedCfg = cfg; }
    async query() { return { rows: [], rowCount: 0 }; }
    async end() {}
  }

  const a = new PgAdapter(oldPool, {
    config: { connectionString: "postgres://u:p@localhost:5432/app_a?sslmode=require" },
    features: { driverImporter: async () => ({ Pool: FakePool }) },
  });

  const next = await a.switchDatabase("app_b");
  assert.equal(next.dialect, "pg");
  assert.equal(closed, true);
  assert.match(String(capturedCfg.connectionString), /\/app_b/);
});

test("MySqlAdapter switchDatabase creates new pool with updated database", async () => {
  let closed = false;
  let capturedCfg = null;

  const oldDriver = {
    query(_sql, _params, cb) { cb?.(null, []); },
    end(cb) { closed = true; cb?.(); },
  };

  const a = new MySqlAdapter(oldDriver, {
    config: { host: "127.0.0.1", user: "root", database: "app_a" },
    features: {
      driverImporter: async () => ({
        createPool(cfg) {
          capturedCfg = cfg;
          return {
            query(_sql, _params, cb) { cb?.(null, []); },
            end(cb) { cb?.(); },
          };
        },
      }),
    },
  });

  const next = await a.switchDatabase("app_b");
  assert.equal(next.dialect, "mysql");
  assert.equal(closed, true);
  assert.equal(capturedCfg.database, "app_b");
});

test("MsSqlAdapter switchDatabase creates new pool with updated database", async () => {
  let closed = false;
  let capturedCfg = null;

  const oldPool = {
    request() {
      return {
        input() {},
        async query() { return { recordset: [], rowsAffected: [0] }; },
      };
    },
    async close() { closed = true; },
  };

  class FakeConnectionPool {
    constructor(cfg) { capturedCfg = cfg; }
    async connect() { return this; }
    request() {
      return {
        input() {},
        async query() { return { recordset: [], rowsAffected: [0] }; },
      };
    }
    async close() {}
  }

  const a = new MsSqlAdapter(oldPool, {
    config: { server: "localhost", user: "sa", password: "pass", database: "master" },
    features: { driverImporter: async () => ({ ConnectionPool: FakeConnectionPool }) },
  });

  const next = await a.switchDatabase("app_b");
  assert.equal(next.dialect, "mssql");
  assert.equal(closed, true);
  assert.equal(capturedCfg.database, "app_b");
});

test("OracleAdapter switchDatabase creates new pool with updated connectString", async () => {
  let closed = false;
  let capturedCfg = null;

  const oldPool = {
    async getConnection() {
      return { async execute() { return { rows: [] }; }, async close() {} };
    },
    async close() { closed = true; },
  };

  const newPool = {
    async getConnection() {
      return { async execute() { return { rows: [] }; }, async close() {} };
    },
    async close() {},
  };

  const a = new OracleAdapter(oldPool, {
    config: { user: "u", password: "p", connectString: "svcA" },
    features: {
      driverImporter: async () => ({
        async createPool(cfg) { capturedCfg = cfg; return newPool; },
      }),
    },
  });

  const next = await a.switchDatabase("svcB");
  assert.equal(next.dialect, "oracle");
  assert.equal(closed, true);
  assert.equal(capturedCfg.connectString, "svcB");
});

test("MongoAdapter switchDatabase reuses client and swaps db handle", async () => {
  const client = {
    db(name) {
      return {
        databaseName: name,
        collection() { return {}; },
      };
    },
  };

  const db = {
    client,
    databaseName: "app_a",
    collection() { return {}; },
  };

  const a = new MongoAdapter(db, { config: { dbName: "app_a" } });
  const next = await a.switchDatabase("app_b");
  assert.equal(next.dialect, "mongo");
  assert.equal(next.db.databaseName, "app_b");
  assert.equal(next.db.client, client);
  assert.equal(next.config.dbName, "app_b");
});

test("DB.switchDialect reconnects and closes current adapter by default", async () => {
  let closed = false;
  let capturedCfg = null;

  class FakePool {
    constructor(cfg) { capturedCfg = cfg; }
    async query() { return { rows: [], rowCount: 0 }; }
    async end() {}
  }

  const db = new DB({
    dialect: "mysql",
    async close() { closed = true; },
  });

  const out = await db.switchDialect(
    "pg",
    { server: "localhost", user: "u", password: "p", database: "app_b" },
    {
      importer: async (pkg) => {
        assert.equal(pkg, "pg");
        return { Pool: FakePool };
      },
    },
  );

  assert.equal(out, db);
  assert.equal(closed, true);
  assert.equal(db.adapter.dialect, "pg");
  assert.equal(capturedCfg.host, "localhost");
  assert.equal(capturedCfg.user, "u");
  assert.equal(capturedCfg.password, "p");
  assert.equal(capturedCfg.database, "app_b");
});

test("DB.switchDialect uses current adapter feature importer when opts.importer is missing", async () => {
  let importedPkg = null;
  let capturedCfg = null;

  const db = new DB({
    dialect: "pg",
    features: {
      driverImporter: async (pkg) => {
        importedPkg = pkg;
        return {
          createPool(cfg) {
            capturedCfg = cfg;
            return {
              query(_sql, _params, cb) {
                const done = typeof _params === "function" ? _params : cb;
                done?.(null, []);
              },
              end(cb) { cb?.(); },
            };
          },
        };
      },
    },
  });

  await db.switchDialect("mysql", { host: "127.0.0.1", user: "root", database: "app_b" });
  assert.equal(importedPkg, "mysql");
  assert.equal(db.adapter.dialect, "mysql");
  assert.equal(capturedCfg.database, "app_b");
});

test("DB.switchDialect merges current and provided features", async () => {
  let importedPkg = null;

  class FakePool {
    async query() { return { rows: [], rowCount: 0 }; }
    async end() {}
  }

  const db = new DB({
    dialect: "mysql",
    features: {
      keepExisting: true,
      driverImporter: async (pkg) => {
        importedPkg = pkg;
        return { Pool: FakePool };
      },
    },
  });

  await db.switchDialect("pg", { database: "app_b" }, {
    features: { extraFlag: true },
  });

  assert.equal(importedPkg, "pg");
  assert.equal(db.adapter.features.keepExisting, true);
  assert.equal(db.adapter.features.extraFlag, true);
});

test("DB.switchDialect respects closeCurrent=false", async () => {
  let closed = false;

  class FakePool {
    async query() { return { rows: [], rowCount: 0 }; }
    async end() {}
  }

  const db = new DB({
    dialect: "mysql",
    async close() { closed = true; },
  });

  await db.switchDialect("pg", { database: "app_b" }, {
    closeCurrent: false,
    importer: async () => ({ Pool: FakePool }),
  });

  assert.equal(db.adapter.dialect, "pg");
  assert.equal(closed, false);
});

test("DB.useDialect is alias of switchDialect", async () => {
  class FakePool {
    async query() { return { rows: [], rowCount: 0 }; }
    async end() {}
  }

  const db = new DB({ dialect: "mysql" });
  await db.useDialect("pg", { database: "app_b" }, {
    importer: async () => ({ Pool: FakePool }),
  });

  assert.equal(db.adapter.dialect, "pg");
});
