import test from "node:test";
import assert from "node:assert/strict";
import { PgAdapter } from "../src/adapters/PgAdapter.js";
import { MySqlAdapter } from "../src/adapters/MySqlAdapter.js";
import { MongoAdapter } from "../src/adapters/MongoAdapter.js";

test("pg getExportCommand maps friendly options to flags", () => {
  const a = new PgAdapter({ query: async () => ({ rows: [], rowCount: 0 }) }, {});
  const op = a.getExportCommand({
    format: "tar",
    file: "dump.tar",
    schemaOnly: true,
    create: true,
    clean: true,
    excludeTables: ["secret"],
    schemas: ["public"],
    pgDumpArgs: ["--column-inserts"],
  });

  assert.equal(op.cmd, "pg_dump");
  const args = op.args.join(" ");
  assert.ok(args.includes("-Ft"));
  assert.ok(args.includes("--schema-only"));
  assert.ok(args.includes("--create"));
  assert.ok(args.includes("--clean"));
  assert.ok(args.includes("--exclude-table=secret"));
  assert.ok(args.includes("--schema=public"));
  assert.ok(args.includes("--column-inserts"));
});

test("mysql getExportCommand uses mysqldump and applies tables", () => {
  const a = new MySqlAdapter({ query(_sql, _params, cb) { cb?.(null, []); } }, {});
  const op = a.getExportCommand({
    file: "dump.sql",
    tables: ["users", "orders"],
    mysqlDumpArgs: ["--single-transaction"],
  });
  assert.equal(op.cmd, "mysqldump");
  const args = op.args.join(" ");
  assert.ok(args.includes("users"));
  assert.ok(args.includes("orders"));
  assert.ok(args.includes("--single-transaction"));
});

test("mongo getExportCommand uses mongodump archive and gzip", () => {
  const a = new MongoAdapter({}, {});
  const op = a.getExportCommand({
    file: "mongo.archive",
    format: "archive",
    gzip: true,
    mongoDumpArgs: ["--numParallelCollections=4"],
  });
  assert.equal(op.cmd, "mongodump");
  const args = op.args.join(" ");
  assert.ok(args.includes("--archive=mongo.archive"));
  assert.ok(args.includes("--gzip"));
  assert.ok(args.includes("--numParallelCollections=4"));
});

test("pg getImportCommand streams custom/tar through stdin for progress", () => {
  const a = new PgAdapter({ query: async () => ({ rows: [], rowCount: 0 }) }, {});

  const custom = a.getImportCommand({ format: "custom", file: "dump.custom" });
  assert.equal(custom.cmd, "pg_restore");
  assert.equal(custom.pipeStdin, true);
  assert.ok(custom.args.includes("-Fc"));
  assert.ok(!custom.args.includes("dump.custom"));

  const tar = a.getImportCommand({ format: "tar", file: "dump.tar" });
  assert.equal(tar.cmd, "pg_restore");
  assert.equal(tar.pipeStdin, true);
  assert.ok(tar.args.includes("-Ft"));
  assert.ok(!tar.args.includes("dump.tar"));

  const dir = a.getImportCommand({ format: "directory", file: "dump_dir" });
  assert.equal(dir.cmd, "pg_restore");
  assert.equal(dir.pipeStdin, false);
  assert.ok(dir.args.includes("dump_dir"));
});

test("mongo getImportCommand streams archive through stdin for progress", () => {
  const a = new MongoAdapter({}, {});
  const op = a.getImportCommand({
    format: "archive",
    file: "mongo.archive",
    gzip: true,
  });
  assert.equal(op.cmd, "mongorestore");
  assert.equal(op.pipeStdin, true);
  assert.ok(op.args.includes("--archive"));
  assert.ok(!op.args.includes("--archive=mongo.archive"));
  assert.ok(op.args.includes("--gzip"));
});
