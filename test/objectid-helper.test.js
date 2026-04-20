import test from "node:test";
import assert from "node:assert/strict";
import { toObjectId, toObjectIdSync } from "../src/index.js";

class FakeObjectId {
  constructor(value) {
    const s = String(value ?? "");
    if (!/^[a-f\d]{24}$/i.test(s)) throw new Error("invalid");
    this.value = s.toLowerCase();
    this._bsontype = "ObjectId";
  }

  toHexString() {
    return this.value;
  }
}

test("toObjectId converts string value using provided ObjectId constructor", async () => {
  const id = await toObjectId("507f1f77bcf86cd799439011", { ObjectId: FakeObjectId });
  assert.ok(id instanceof FakeObjectId);
  assert.equal(id.toHexString(), "507f1f77bcf86cd799439011");
});

test("toObjectId returns object-id-like value as-is", async () => {
  const existing = {
    _bsontype: "ObjectId",
    toHexString() { return "507f1f77bcf86cd799439011"; },
  };
  const id = await toObjectId(existing, { ObjectId: FakeObjectId });
  assert.equal(id, existing);
});

test("toObjectId supports extended-json $oid object", async () => {
  const id = await toObjectId({ $oid: "507f1f77bcf86cd799439011" }, { ObjectId: FakeObjectId });
  assert.ok(id instanceof FakeObjectId);
  assert.equal(id.toHexString(), "507f1f77bcf86cd799439011");
});

test("toObjectId shows npm install message when mongodb package is missing", async () => {
  await assert.rejects(
    () => toObjectId("507f1f77bcf86cd799439011", {
      importer: async () => {
        const err = new Error("Cannot find package \"mongodb\"");
        err.code = "ERR_MODULE_NOT_FOUND";
        throw err;
      },
    }),
    /Install it with: npm install mongodb/,
  );
});

test("toObjectId validates input value", async () => {
  await assert.rejects(
    () => toObjectId("bad-id", { ObjectId: FakeObjectId }),
    /Invalid ObjectId value: bad-id/,
  );
});

test("toObjectIdSync converts string value using provided ObjectId constructor", () => {
  const id = toObjectIdSync("507f1f77bcf86cd799439011", FakeObjectId);
  assert.ok(id instanceof FakeObjectId);
  assert.equal(id.toHexString(), "507f1f77bcf86cd799439011");
});

test("toObjectIdSync returns object-id-like value as-is", () => {
  const existing = {
    _bsontype: "ObjectId",
    toHexString() { return "507f1f77bcf86cd799439011"; },
  };
  const id = toObjectIdSync(existing, FakeObjectId);
  assert.equal(id, existing);
});

test("toObjectIdSync requires ObjectId constructor when conversion is needed", () => {
  assert.throws(
    () => toObjectIdSync("507f1f77bcf86cd799439011"),
    /toObjectIdSync requires ObjectId constructor/,
  );
});

test("toObjectIdSync validates input value", () => {
  assert.throws(
    () => toObjectIdSync("bad-id", FakeObjectId),
    /Invalid ObjectId value: bad-id/,
  );
});
