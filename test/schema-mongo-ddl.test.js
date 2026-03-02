import test from "node:test";
import assert from "node:assert/strict";
import { SchemaBuilder } from "../src/core/SchemaBuilder.js";

test("dropTable on mongo drops collection", async () => {
  let dropped = false;
  const adapter = {
    dialect: "mongo",
    db: {
      collection(name) {
        assert.equal(name, "users");
        return {
          async drop() {
            dropped = true;
            return true;
          },
        };
      },
    },
  };

  const out = await new SchemaBuilder(adapter).dropTable("users").exec();
  assert.equal(dropped, true);
  assert.deepEqual(out, { dropped: true, collection: "users" });
});

test("dropTable on mongo respects ifExists when collection is missing", async () => {
  const adapter = {
    dialect: "mongo",
    db: {
      collection() {
        return {
          async drop() {
            const e = new Error("ns not found");
            e.code = 26;
            e.codeName = "NamespaceNotFound";
            throw e;
          },
        };
      },
    },
  };

  const out = await new SchemaBuilder(adapter).dropTable("ghost", { ifExists: true }).exec();
  assert.deepEqual(out, { dropped: false, collection: "ghost", skipped: true });
});

test("dropTable on mongo throws when missing collection and ifExists=false", async () => {
  const adapter = {
    dialect: "mongo",
    db: {
      collection() {
        return {
          async drop() {
            const e = new Error("namespace not found");
            e.code = 26;
            throw e;
          },
        };
      },
    },
  };

  await assert.rejects(
    () => new SchemaBuilder(adapter).dropTable("ghost", { ifExists: false }).exec(),
    /namespace not found/i,
  );
});
