import test from "node:test";
import assert from "node:assert/strict";
import { MsSqlAdapter } from "../src/adapters/MsSqlAdapter.js";

test("MsSqlAdapter returns recordset for returning mutations", async () => {
  const captured = {};
  const pool = {
    request() {
      return {
        input(name, value) {
          captured[name] = value;
        },
        async query(sql) {
          assert.equal(sql, "UPDATE [users] SET [email] = @p1 OUTPUT inserted.[id] WHERE [id] = @p2");
          return {
            recordset: [{ id: 7 }],
            rowsAffected: [1],
          };
        },
      };
    },
  };

  const adapter = new MsSqlAdapter(pool);
  const qb = {
    _type: "update",
    _returning: ["id"],
    compile() {
      return {
        sql: "UPDATE [users] SET [email] = @p1 OUTPUT inserted.[id] WHERE [id] = @p2",
        params: ["a@b.com", 7],
      };
    },
  };

  const out = await adapter.execute(qb);
  assert.deepEqual(out, [{ id: 7 }]);
  assert.deepEqual(captured, { p1: "a@b.com", p2: 7 });
});
