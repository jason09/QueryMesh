import test from "node:test";
import assert from "node:assert/strict";
import { SchemaBuilder } from "../src/core/SchemaBuilder.js";

function sqlAdapter(dialect, executed) {
  return {
    dialect,
    placeholder(i) {
      if (dialect === "pg") return `$${i}`;
      if (dialect === "mssql") return `@p${i}`;
      if (dialect === "oracle") return `:p${i}`;
      return "?";
    },
    async execute(qb) {
      const out = qb.compile();
      executed.push(out.sql);
      return [];
    },
  };
}

test("createTrigger(pg) executes function + trigger without splitting function body", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .createTrigger({
      name: "trg_users_touch",
      table: "users",
      timing: "BEFORE",
      events: ["UPDATE"],
      body: "NEW.updated_at = CURRENT_TIMESTAMP;",
    })
    .exec();

  assert.equal(executed.length, 2);
  assert.match(executed[0], /CREATE OR REPLACE FUNCTION "trg_users_touch_fn"\(\) RETURNS trigger AS \$\$/);
  assert.match(executed[0], /NEW\.updated_at = CURRENT_TIMESTAMP;/);
  assert.match(executed[0], /RETURN NEW;/);
  assert.equal(
    executed[1],
    'CREATE TRIGGER "trg_users_touch" BEFORE UPDATE ON "users" FOR EACH ROW EXECUTE FUNCTION "trg_users_touch_fn"()',
  );
});

test("dropTrigger(pg) drops trigger and helper function", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .dropTrigger("trg_users_touch", { table: "users", ifExists: true })
    .exec();

  assert.deepEqual(executed, [
    'DROP TRIGGER IF EXISTS "trg_users_touch" ON "users"',
    'DROP FUNCTION IF EXISTS "trg_users_touch_fn"()',
  ]);
});

test("alterTable executes all generated statements in order", async () => {
  const executed = [];
  const adapter = sqlAdapter("pg", executed);

  await new SchemaBuilder(adapter)
    .alterTable("users", (t) => {
      t.string("display_name", 120);
      t.dropColumn("old_field");
    })
    .exec();

  assert.equal(executed.length, 2);
  assert.equal(
    executed[0],
    'ALTER TABLE "users" ADD COLUMN "display_name" VARCHAR(120)',
  );
  assert.equal(
    executed[1],
    'ALTER TABLE "users" DROP COLUMN "old_field"',
  );
});

test("alterTable(mysql) works when only post-statements are generated", async () => {
  const executed = [];
  const adapter = sqlAdapter("mysql", executed);

  await new SchemaBuilder(adapter)
    .alterTable("users", (t) => {
      t.dropConstraint("fk_users_company");
    })
    .exec();

  assert.deepEqual(executed, [
    "ALTER TABLE `users` DROP FOREIGN KEY `fk_users_company`",
    "ALTER TABLE `users` DROP INDEX `fk_users_company`",
  ]);
});

test("createTrigger(mysql) with multiple events executes one statement per event", async () => {
  const executed = [];
  const adapter = sqlAdapter("mysql", executed);

  await new SchemaBuilder(adapter)
    .createTrigger({
      name: "trg_users_touch",
      table: "users",
      timing: "AFTER",
      events: ["INSERT", "UPDATE"],
      body: "SET NEW.updated_at = CURRENT_TIMESTAMP;",
    })
    .exec();

  assert.equal(executed.length, 2);
  assert.match(executed[0], /CREATE TRIGGER `trg_users_touch_insert` AFTER INSERT ON `users` FOR EACH ROW BEGIN/);
  assert.match(executed[1], /CREATE TRIGGER `trg_users_touch_update` AFTER UPDATE ON `users` FOR EACH ROW BEGIN/);
});
