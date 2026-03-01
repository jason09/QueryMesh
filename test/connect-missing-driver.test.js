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
