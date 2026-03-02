import test from "node:test";
import assert from "node:assert/strict";
import { connect } from "../src/index.js";

function missingImporter(pkg) {
  const err = new Error(`Cannot find package '${pkg}' imported from /tmp/app.js`);
  err.code = "ERR_MODULE_NOT_FOUND";
  throw err;
}

test("connect shows npm install command when a dialect package is missing", async () => {
  await assert.rejects(
    () => connect({ dialect: "pg", config: {}, importer: missingImporter }),
    /Missing driver package "pg" for dialect "pg"\. Install it with: npm install pg/,
  );

  await assert.rejects(
    () => connect({ dialect: "mongo", config: {}, importer: missingImporter }),
    /Missing driver package "mongodb" for dialect "mongo"\. Install it with: npm install mongodb/,
  );
});

test("connect keeps non-module import errors unchanged", async () => {
  const boom = new Error("native binding failed");
  await assert.rejects(
    () => connect({ dialect: "mysql", config: {}, importer: () => { throw boom; } }),
    boom,
  );
});

test("connect accepts mongodb alias with connectionString", async () => {
  const calls = { uri: null, dbName: null, connected: false };
  class FakeMongoClient {
    constructor(uri) {
      calls.uri = uri;
    }
    async connect() {
      calls.connected = true;
    }
    db(name) {
      calls.dbName = name;
      return { collection() {}, databaseName: name };
    }
  }

  const db = await connect({
    dialect: "mongodb",
    config: { connectionString: "mongodb://localhost:27017/framecraft" },
    importer: async (pkg) => {
      assert.equal(pkg, "mongodb");
      return { MongoClient: FakeMongoClient };
    },
  });

  assert.equal(db.adapter.dialect, "mongo");
  assert.equal(calls.uri, "mongodb://localhost:27017/framecraft");
  assert.equal(calls.dbName, "framecraft");
  assert.equal(calls.connected, true);
});

test("connect maps pg server alias to host", async () => {
  let captured = null;
  class FakePool {
    constructor(cfg) {
      captured = cfg;
    }
  }

  const db = await connect({
    dialect: "pg",
    config: { server: "localhost", user: "sa", password: "pass", database: "master" },
    importer: async (pkg) => {
      assert.equal(pkg, "pg");
      return { Pool: FakePool };
    },
  });

  assert.equal(db.adapter.dialect, "pg");
  assert.equal(captured.host, "localhost");
  assert.equal(captured.user, "sa");
  assert.equal(captured.password, "pass");
  assert.equal(captured.database, "master");
});

test("connect supports pg default export interop (Pool under default)", async () => {
  let captured = null;
  class FakePool {
    constructor(cfg) {
      captured = cfg;
    }
  }

  const db = await connect({
    dialect: "pg",
    config: { server: "localhost", user: "sa", password: "pass", database: "master" },
    importer: async (pkg) => {
      assert.equal(pkg, "pg");
      return { default: { Pool: FakePool } };
    },
  });

  assert.equal(db.adapter.dialect, "pg");
  assert.equal(captured.host, "localhost");
  assert.equal(captured.user, "sa");
  assert.equal(captured.password, "pass");
  assert.equal(captured.database, "master");
});

test("connect flattens pg config.options into Pool config", async () => {
  let captured = null;
  class FakePool {
    constructor(cfg) {
      captured = cfg;
    }
  }

  await connect({
    dialect: "pg",
    config: {
      server: "localhost",
      user: "sa",
      password: "pass",
      database: "master",
      options: { ssl: true, max: 20, idleTimeoutMillis: 5000 },
    },
    importer: async () => ({ Pool: FakePool }),
  });

  assert.equal(captured.host, "localhost");
  assert.equal(captured.user, "sa");
  assert.equal(captured.password, "pass");
  assert.equal(captured.database, "master");
  assert.equal(captured.ssl, true);
  assert.equal(captured.max, 20);
  assert.equal(captured.idleTimeoutMillis, 5000);
  assert.equal(Object.prototype.hasOwnProperty.call(captured, "options"), false);
});

test("connect maps mysql server alias to host and flattens options", async () => {
  let captured = null;
  const fakePool = {
    query(_sql, _params, cb) {
      const done = typeof _params === "function" ? _params : cb;
      done?.(null, []);
    },
    end(cb) { cb?.(); },
  };

  const db = await connect({
    dialect: "mysql",
    config: {
      server: "localhost",
      port: 3307,
      user: "root",
      password: "pass",
      dbName: "app_db",
      options: { connectTimeout: 1500, multipleStatements: true },
    },
    importer: async (pkg) => {
      assert.equal(pkg, "mysql");
      return {
        createPool(cfg) {
          captured = cfg;
          return fakePool;
        },
      };
    },
  });

  assert.equal(db.adapter.dialect, "mysql");
  assert.equal(captured.host, "localhost");
  assert.equal(captured.port, 3307);
  assert.equal(captured.user, "root");
  assert.equal(captured.password, "pass");
  assert.equal(captured.database, "app_db");
  assert.equal(captured.connectTimeout, 1500);
  assert.equal(captured.multipleStatements, true);
  assert.equal(Object.prototype.hasOwnProperty.call(captured, "options"), false);
});

test("connect mongo supports options and server/port config", async () => {
  const calls = { uri: null, options: null, dbName: null, connected: false };
  class FakeMongoClient {
    constructor(uri, options) {
      calls.uri = uri;
      calls.options = options;
    }
    async connect() {
      calls.connected = true;
    }
    db(name) {
      calls.dbName = name;
      return { collection() {}, databaseName: name };
    }
  }

  const db = await connect({
    dialect: "mongo",
    config: {
      server: "localhost",
      port: 27018,
      database: "framecraft",
      options: { maxPoolSize: 5, serverSelectionTimeoutMS: 1200 },
    },
    importer: async (pkg) => {
      assert.equal(pkg, "mongodb");
      return { MongoClient: FakeMongoClient };
    },
  });

  assert.equal(db.adapter.dialect, "mongo");
  assert.equal(calls.uri, "mongodb://localhost:27018/framecraft");
  assert.equal(calls.dbName, "framecraft");
  assert.equal(calls.connected, true);
  assert.equal(calls.options.maxPoolSize, 5);
  assert.equal(calls.options.serverSelectionTimeoutMS, 1200);
});

test("connect supports mongoose dialect using existing mongoose connection", async () => {
  let importerCalled = false;
  const client = { startSession() { return null; } };
  const fakeDb = {
    databaseName: "framecraft",
    collection() { return {}; },
  };
  const mongoose = {
    connection: {
      db: fakeDb,
      getClient() { return client; },
    },
  };

  const db = await connect({
    dialect: "mongoose",
    config: { mongoose },
    importer: async () => {
      importerCalled = true;
      throw new Error("should not import mongodb when mongoose connection is provided");
    },
  });

  assert.equal(importerCalled, false);
  assert.equal(db.adapter.dialect, "mongo");
  assert.equal(db.adapter.db, fakeDb);
  assert.equal(db.adapter.db.client, client);
});
