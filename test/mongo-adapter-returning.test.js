import test from "node:test";
import assert from "node:assert/strict";
import { MongoAdapter } from "../src/adapters/MongoAdapter.js";

test("MongoAdapter executes union op with UNION and UNION ALL semantics", async () => {
  const data = {
    a: [{ id: 1 }, { id: 2 }],
    b: [{ id: 2 }, { id: 3 }],
    c: [{ id: 2 }],
  };

  const db = {
    collection(name) {
      return {
        find() {
          return {
            project() { return this; },
            sort() { return this; },
            skip() { return this; },
            limit() { return this; },
            async toArray() { return data[name].map(x => ({ ...x })); },
          };
        },
        aggregate() {
          return {
            async toArray() { return []; },
          };
        },
      };
    },
  };

  const adapter = new MongoAdapter(db);
  const qb = {
    _returning: null,
    compile() {
      return {
        mongo: {
          op: "union",
          collection: "a",
          base: { op: "find", collection: "a", filter: {} },
          unions: [
            { all: false, source: { op: "find", collection: "b", filter: {} } },
            { all: true, source: { op: "find", collection: "c", filter: {} } },
          ],
        },
      };
    },
  };

  const rows = await adapter.execute(qb);
  assert.deepEqual(rows, [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 2 }]);
});

test("MongoAdapter executes insertSelect and maps source columns", async () => {
  const inserted = [];
  const sourceRows = [
    { uid: "u1", email: "a@querymesh.dev" },
    { uid: "u2", email: "b@querymesh.dev" },
  ];

  const db = {
    collection(name) {
      if (name === "users_archive") {
        return {
          find() {
            return {
              project() { return this; },
              sort() { return this; },
              skip() { return this; },
              limit() { return this; },
              async toArray() { return sourceRows; },
            };
          },
          aggregate() {
            return {
              async toArray() { return []; },
            };
          },
        };
      }
      if (name === "users") {
        return {
          async insertMany(docs) {
            inserted.push(...docs);
            return { insertedIds: { 0: "i1", 1: "i2" } };
          },
        };
      }
      throw new Error(`unexpected collection: ${name}`);
    },
  };

  const adapter = new MongoAdapter(db);
  const qb = {
    _returning: null,
    compile() {
      return {
        mongo: {
          op: "insertSelect",
          collection: "users",
          source: { op: "find", collection: "users_archive", filter: {} },
          mappings: [
            { target: "id", source: "uid" },
            { target: "email", source: "email" },
          ],
        },
      };
    },
  };

  const res = await adapter.execute(qb);
  assert.deepEqual(inserted, [
    { id: "u1", email: "a@querymesh.dev" },
    { id: "u2", email: "b@querymesh.dev" },
  ]);
  assert.deepEqual(res, { insertedIds: { 0: "i1", 1: "i2" } });
});

test("MongoAdapter executes upsertOne and returns projected row", async () => {
  const calls = [];
  let findOneCount = 0;

  const col = {
    async findOne(filter, opts) {
      findOneCount += 1;
      calls.push({ fn: "findOne", filter, opts });
      if (findOneCount === 1) return { _id: 42 };
      return { email: "next@querymesh.dev" };
    },
    async updateOne(filter, update, opts) {
      calls.push({ fn: "updateOne", filter, update, opts });
      return { matchedCount: 1, modifiedCount: 1, upsertedId: null };
    },
  };

  const db = {
    collection(name) {
      assert.equal(name, "users");
      return col;
    },
  };

  const adapter = new MongoAdapter(db);
  const qb = {
    _returning: ["email"],
    compile() {
      return {
        mongo: {
          op: "upsertOne",
          collection: "users",
          filter: { email: "a@querymesh.dev" },
          update: {
            $setOnInsert: { email: "a@querymesh.dev", name: "A" },
            $set: { name: "B" },
          },
        },
      };
    },
  };

  const rows = await adapter.execute(qb);
  assert.deepEqual(rows, [{ email: "next@querymesh.dev" }]);
  assert.deepEqual(calls[0].opts?.projection, { _id: 1 });
  assert.equal(calls[1].opts?.upsert, true);
  assert.deepEqual(calls[2].filter, { _id: 42 });
  assert.deepEqual(calls[2].opts?.projection, { email: 1, _id: 0 });
});

test("MongoAdapter updateMany returning keeps row order and projection", async () => {
  let findCount = 0;
  const observedProjections = [];

  const col = {
    find(filter) {
      findCount += 1;
      if (findCount === 1) {
        assert.deepEqual(filter, { status: 1 });
        return {
          project(projection) {
            observedProjections.push(projection);
            return this;
          },
          async toArray() {
            return [{ _id: "b" }, { _id: "a" }];
          },
        };
      }

      assert.deepEqual(filter, { _id: { $in: ["b", "a"] } });
      return {
        project(projection) {
          observedProjections.push(projection);
          return this;
        },
        async toArray() {
          return [
            { _id: "a", email: "a@querymesh.dev" },
            { _id: "b", email: "b@querymesh.dev" },
          ];
        },
      };
    },
    async updateMany(filter, update) {
      assert.deepEqual(filter, { status: 1 });
      assert.deepEqual(update, { $set: { status: 2 } });
      return { matchedCount: 2, modifiedCount: 2 };
    },
  };

  const db = { collection: () => col };
  const adapter = new MongoAdapter(db);
  const qb = {
    _returning: ["email"],
    compile() {
      return {
        mongo: {
          op: "updateMany",
          collection: "users",
          filter: { status: 1 },
          update: { $set: { status: 2 } },
        },
      };
    },
  };

  const rows = await adapter.execute(qb);
  assert.deepEqual(rows, [
    { email: "b@querymesh.dev" },
    { email: "a@querymesh.dev" },
  ]);
  assert.deepEqual(observedProjections[0], { _id: 1 });
  assert.deepEqual(observedProjections[1], { email: 1, _id: 1 });
});
