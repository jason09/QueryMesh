import test from "node:test";
import assert from "node:assert/strict";
import { ToolsManager } from "../src/tools/ToolsManager.js";

function makeRunner(installed = {}, versions = {}) {
  return (command, args = []) => {
    if (command === "which") {
      const c = args[0];
      if (installed[c]) return { status: 0, stdout: `/usr/bin/${c}\n`, stderr: "" };
      return { status: 1, stdout: "", stderr: "" };
    }
    if (command === "where") {
      const c = args[0];
      if (installed[c]) return { status: 0, stdout: `C:\\tools\\${c}.exe\r\n`, stderr: "" };
      return { status: 1, stdout: "", stderr: "" };
    }
    if (installed[command] && versions[command]) {
      return { status: 0, stdout: `${versions[command]}\n`, stderr: "" };
    }
    return { status: 1, stdout: "", stderr: "not found" };
  };
}

test("ToolsManager detects installed cli tools and versions", () => {
  const runner = makeRunner(
    { psql: true, pg_dump: true, pg_restore: true, mysql: false },
    { psql: "psql (PostgreSQL) 16.3" },
  );
  const db = { adapter: { dialect: "pg", execute: async () => [] } };
  const t = new ToolsManager(db, { runner, platform: "darwin" });

  assert.equal(t.isPostgresInstalled(), true);
  assert.equal(t.isMySqlInstalled(), false);
  assert.match(t.getCliVersion("psql") || "", /PostgreSQL/);

  const status = t.getToolingStatus("pg");
  assert.equal(status.allInstalled, true);
  assert.equal(status.tools.length, 3);
});

test("ToolsManager getVersion + ping for SQL dialect", async () => {
  const db = {
    adapter: {
      dialect: "pg",
      execute: async (qb) => {
        const { sql } = qb.compile();
        if (/SELECT version\(\)/i.test(sql)) return [{ version: "PostgreSQL 16.3" }];
        if (/SELECT 1 AS ok/i.test(sql)) return [{ ok: 1 }];
        return [];
      },
    },
  };
  const t = new ToolsManager(db, { runner: makeRunner({ psql: true }, { psql: "psql (PostgreSQL) 16.3" }) });
  assert.equal(await t.getVersion(), "PostgreSQL 16.3");
  assert.equal(await t.ping(), true);
});

test("ToolsManager getVersion + ping for mongo dialect", async () => {
  const db = {
    adapter: {
      dialect: "mongo",
      db: {
        admin() {
          return {
            async command(cmd) {
              if (cmd.buildInfo) return { version: "8.0.1" };
              if (cmd.ping) return { ok: 1 };
              return {};
            },
          };
        },
      },
    },
  };
  const t = new ToolsManager(db, { runner: makeRunner({ mongorestore: true }, { mongorestore: "mongorestore version: 100.9.0" }) });
  assert.equal(await t.getVersion(), "8.0.1");
  assert.equal(await t.ping(), true);
  assert.equal(t.isMongoInstalled(), true);
});
