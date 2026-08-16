# QueryMesh

![QueryMesh logo](./assets/querymesh-logo.svg)

QueryMesh is a compact **query builder + light ORM** for Node.js with multi-database support.

HTML documentation: [overview](./docs/index.html) and [API reference](./docs/api.html)

## Supported dialects

- **PostgreSQL** (`pg`)
- **MySQL** (`mysql`)
- **SQL Server** (`mssql`)
- **Oracle** (`oracledb`)
- **MongoDB** (`mongo`, `mongodb`, `mongoose`) — same fluent builder, but executed as real MongoDB operations.

## Cross-dialect support (what is not universal)

| Feature | pg | mysql | mssql | oracle | mongo |
|---|---|---|---|---|---|
| Basic query builder (`where`, `NOT`, `IS`, `NULL`, `MIN/MAX`) | ✅ | ✅ | ✅ | ✅ | ✅ |
| `UNION` / `UNION ALL` | ✅ | ✅ | ✅ | ✅ | ✅ (QueryBuilder sources) |
| `INSERT ... SELECT` | ✅ | ✅ | ✅ | ✅ | ✅ (QueryBuilder source + explicit select list) |
| `ANY/ALL` with subquery/raw source | ✅ | ✅ | ✅ | ✅ | ❌ |
| `ANY/ALL` with literal array | ✅ | ✅ | ✅ | ✅ | ✅ |
| `.onConflictDoUpdate(...)` | ✅ | ❌ | ✅ (single-row insert) | ✅ (single-row insert) | ✅ (single-row insert) |
| `.onDuplicateKeyUpdate(...)` | ❌ | ✅ | ❌ | ❌ | ❌ |
| `.returning(...)` | ✅ (native) | ❌ | ✅ (native `OUTPUT`) | ❌ | ✅ (best-effort on mutations) |
| Functions (`createFunction/dropFunction`) | ✅ | ✅ | ✅ | ✅ | ❌ |
| Procedures (`createProcedure/dropProcedure`) | ✅ | ✅ | ✅ | ✅ | ❌ |
| Aggregate objects (`createAggregate/dropAggregate`) | ✅ | ❌ | ❌ | ❌ | ❌ |
| Routine calls (`db.call(...)`) | ✅ | ✅ | ✅ | ✅ | ❌ |
| Table-valued / set-returning helper (`db.callTable(...)`) | ✅ | ❌ | ✅ | ✅ | ❌ |
| Raw select expressions (`selectRaw/selectExpr`) | ✅ | ✅ | ✅ | ✅ | ❌ |
| PostgreSQL types (`createType/dropType`) | ✅ | ❌ | ❌ | ❌ | ❌ |
| `schema().createView()/dropView()` | ✅ | ✅ | ✅ | ✅ | ❌ |
| Triggers (`createTrigger/dropTrigger`) | ✅ | ✅ | ✅ | ✅ | ❌ |
| Schema DDL (`createTable/alterTable/dropTable`) | ✅ | ✅ | ✅ | ✅ | ⚠️ (`dropTable` only) |
| Catalog helpers (`showSchemas/showSequences/showConstraints`) | ✅ | ⚠️ (`showSchemas`, `showConstraints`) | ✅ | ✅ | ❌ |
| Introspection helpers (`showViews/showTriggers/showIndexes`) | ✅ | ✅ | ✅ | ✅ | ⚠️ (`showViews`/`showIndexes` only) |
| Maintenance (`truncateTable`) | ✅ | ✅ | ✅ | ✅ | ✅ (`deleteMany({})`) |
| Maintenance (`analyzeTable`) | ✅ | ✅ | ✅ | ✅ | ❌ |
| Maintenance (`optimizeTable`) | ✅ (`VACUUM`) | ✅ | ✅ | ❌ | ❌ |
| Maintenance (`vacuumTable` / `vacuumDatabase`) | ✅ | ❌ | ❌ | ❌ | ❌ |
| Maintenance (`reindexTable`) | ✅ | ❌ | ✅ | ❌ | ❌ |
| Maintenance (`repairTable`) | ❌ | ✅ | ❌ | ❌ | ❌ |

Mongo notes:
- Mongo joins use `$lookup`; comparison join operators are supported (`=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`).
- Mongo `UNION/UNION ALL` supports QueryBuilder sources (raw/string SQL sources are not supported).
- Mongo `insertSelect` supports QueryBuilder source with explicit selected columns.
- Mongo `schema().dropTable(name)` drops a collection; `createTable/alterTable` remain SQL-only.
- Mongo `schema().truncateTable(name)` deletes all documents with `deleteMany({})`.
- Mongo functions/procedures are not supported by QueryMesh schema APIs.
- Custom aggregate objects are advanced PostgreSQL-only schema features in QueryMesh.
- `db.call(...)` and `db.callTable(...)` are SQL-only.
- Mongo triggers are not supported by QueryMesh (`createTrigger/dropTrigger` are SQL dialect features).
- `selectRaw(...)`, `selectExpr(...)`, and SQL `raw(...)` fragments are SQL-only builder features.
- `showSchemas()`, `showSequences()`, and `showConstraints()` are SQL-only.
- Right/full join semantics are not fully equivalent to SQL joins in Mongo pipelines.
- For Mongo schema APIs, use `showTables`, `showDatabases`, `showViews`, `showIndexes`, and `getDesc` for introspection.
- `optimizeTable` is a best-effort mapping, not identical SQL across dialects: pg uses `VACUUM`, mysql uses `OPTIMIZE TABLE`, mssql uses `ALTER INDEX`.
- PostgreSQL `VACUUM` helpers cannot run inside a transaction block.

## Install

```bash
npm i querymesh

# install only the driver(s) you need
npm i pg
npm i mysql
npm i mssql
npm i oracledb
npm i mongodb
```

## Connect

```js
import QueryMesh from "querymesh";

// PostgreSQL
const db = await QueryMesh.connect({
  dialect: "pg",
  config: { connectionString: process.env.DATABASE_URL },
  // also accepts { server, user, password, database } (server -> host alias)
  // and supports config.options to pass Pool options:
  // config: { server: "localhost", user: "u", password: "p", database: "app", options: { ssl: true, max: 20 } }
});

// MySQL
// const db = await QueryMesh.connect({
//   dialect: "mysql",
//   config: {
//     server: "127.0.0.1", // preferred (alias to host)
//     user: "root",
//     password: "",
//     database: "app",
//     port: 3306,
//     options: { connectTimeout: 10000, multipleStatements: true },
//   },
// });

// SQL Server
// const db = await QueryMesh.connect({
//   dialect: "mssql",
//   config: { server: "localhost", user: "sa", password: "pass", database: "master", options: { trustServerCertificate: true } },
// });

// Oracle
// const db = await QueryMesh.connect({ dialect: "oracle", config: { user, password, connectString } });

// MongoDB
// const db = await QueryMesh.connect({
//   dialect: "mongo", // alias: "mongodb"
//   config: {
//     // any of these forms work:
//     connectionString: "mongodb://localhost:27017/app",
//     // uri: "mongodb://localhost:27017/app",
//     // or host/server + optional port + db:
//     // server: "localhost", port: 27017, database: "app",
//     options: { maxPoolSize: 20, serverSelectionTimeoutMS: 5000 }, // alias of clientOptions
//   },
// });

// Mongoose (reuse existing mongoose connection)
// import mongoose from "mongoose";
// await mongoose.connect("mongodb://localhost:27017/app");
// const db = await QueryMesh.connect({
//   dialect: "mongoose",
//   config: { mongoose }, // or { connection: mongoose.connection }
// });
```

### Pool and connection behavior

- Create one shared `DB` instance with `QueryMesh.connect(...)` and reuse it. Do not open a new connection per request.
- `pg`, `mysql`, `mssql`, and `oracle` create and manage a driver pool internally.
- `mongo` creates a `MongoClient`; MongoDB pooling is handled by the client internally.
- `mongoose` reuses the existing Mongoose/Mongo connection instead of creating a second Mongo client.
- Query builder calls (`db.table(...).get()/run()`) and raw SQL calls (`db.query(...)`) use the same underlying pool/client.
- `db.transaction(...)` checks out a dedicated connection/session for the transaction, then releases it after commit or rollback.
- `db.close()` closes the underlying pool/client.
- Existing SQL pools are not accepted directly by `connect(...)` yet. Existing Mongo `db` / Mongoose connections are supported.

### Switch database

```js
// switch existing DB handle from A -> B
await db.switchDatabase("app_b");

// alias
await db.useDatabase("app_c");
```

Notes:
- `pg`, `mysql`, `mssql`: opens a new pool using the same config with updated database name.
- `mongo`: reuses the same MongoClient and switches `db` handle.
- `oracle`: reconnects with the provided target as `connectString`.

### Switch dialect

```js
// switch this same DB instance from one dialect to another
await db.switchDialect("mongo", {
  connectionString: "mongodb://localhost:27017/app_b",
});

// alias + explicit options
await db.useDialect(
  "pg",
  { server: "localhost", user: "u", password: "p", database: "app_b" },
  {
    closeCurrent: true, // default true
    // features/importer are optional overrides
  }
);
```

Notes:
- Reuses the same `DB` object and swaps its adapter.
- By default the previous adapter is closed; set `closeCurrent: false` to skip that.
- `importer` and `features` can be passed to override runtime driver loading.

## Mongo ObjectId helper

Use `toObjectId(...)` to safely convert values for Mongo `_id`/reference filters.

```js
import QueryMesh, { toObjectId } from "querymesh";

const id = await toObjectId("507f1f77bcf86cd799439011");
const row = await db.table("users").where("_id", id).first();
```

Sync variant (when you already have `ObjectId` constructor):

```js
import { ObjectId } from "mongodb";
import { toObjectIdSync } from "querymesh";

const id = toObjectIdSync("507f1f77bcf86cd799439011", ObjectId);
```

`whereIn` example:

```js
const ids = await Promise.all(rawIds.map((v) => toObjectId(v)));
const rows = await db.table("users").whereIn("_id", ids).get();
```

Extended JSON is supported:

```js
const id = await toObjectId({ $oid: "507f1f77bcf86cd799439011" });
```

Notes:
- If value is already an ObjectId-like instance, `toObjectId` returns it unchanged.
- If `mongodb` driver is missing, the helper throws install guidance (`npm install mongodb`).
- `toObjectIdSync` does not import anything; you must pass `ObjectId` constructor explicitly.

## Query builder

### Select

```js
const users = await db
  .table("users")
  .select(["id", "email"])
  .where("is_active", true)
  .orderBy("id", "desc")
  .limit(20)
  .get();
```

For SQL joins, qualified wildcards like `table.*` also work:

```js
const rows = await db
  .table("posts")
  .leftJoinOn("users", "posts.user_id", "users.id")
  .select(["posts.*", "users.email"])
  .get();
```

### Where + OR

```js
const rows = await db
  .table("users")
  .where("country", "GN")
  .orWhere("role", "admin")
  .get();
```

### Raw SQL fragments

Use `raw(...)` when you need a SQL expression that QueryMesh should not quote as a column or bind as a normal value.

```js
import { raw } from "querymesh";

const user = await db
  .table("users")
  .where(raw("LOWER(email)"), "alice@example.com")
  .first();

await db
  .table("posts")
  .update({ updated_at: raw("CURRENT_TIMESTAMP") })
  .where("id", postId)
  .run();
```

Notes:
- Use `raw(...)` sparingly and only with trusted SQL fragments.
- Keep user input as normal values (`where("email", value)`) so QueryMesh can bind it safely.
- If you pass `raw(sql, params)`, QueryMesh rewrites portable `?` placeholders for the current SQL dialect inside the generated query.
- `raw(...)` is for SQL dialects; it is not a Mongo query expression helper.

### Raw select expressions

Use `selectExpr(...)` when you want a trusted SQL expression in the `SELECT` list with a safe alias. Use `selectRaw(...)` when you already want to write the full select fragment yourself.

```js
const rows = await db
  .table("users")
  .select(["id"])
  .selectExpr("LOWER(email)", "email_lower")
  .selectRaw("CURRENT_TIMESTAMP AS loaded_at")
  .get();
```

Portable placeholders also work inside builder expressions:

```js
const rows = await db
  .table("users")
  .select("id")
  .selectExpr("COALESCE(?, email)", "effective_email", ["fallback@example.com"])
  .get();
```

Notes:
- `selectRaw(...)` and `selectExpr(...)` are SQL-only.
- `selectExpr(sql, alias, params?)` safely quotes the alias for the current dialect.
- `selectRaw(sql, params?)` inserts the fragment as-is, so the SQL itself must already be trusted.

### Full SQL query

Use `db.query(...)` when you want to run a full SQL statement directly. It supports `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and DDL statements for SQL dialects.

```js
// Portable placeholders use ? and QueryMesh rewrites them for the current dialect.
const result = await db.query(
  "SELECT id, email FROM users WHERE id = ?",
  [userId]
);

console.log(result.rows);     // [{ id, email }]
console.log(result.rowCount); // number of rows

const updateResult = await db.query(
  "UPDATE users SET active = ? WHERE id = ?",
  [false, userId]
);

console.log(updateResult.rowCount);
```

Placeholder support:
- Recommended portable style: `?`
- QueryMesh rewrites `?` to the connected dialect (`$1`, `?`, `@p1`, or `:p1`).
- Native placeholder styles are also accepted and normalized: `$1`, `@p1`, `:p1`.

Notes:
- `db.query(...)` returns a rows array that also exposes `.rows` and `.rowCount`.
- `db.exec(...)` no longer executes raw SQL. Use `db.query(...)` for raw SQL commands.
- `schema().exec()` still exists for schema-builder statements.
- MongoDB rejects raw SQL; use the QueryMesh builder or native Mongo collection APIs.

### `raw(...)` vs `db.query(...)`

Use `raw(...)` inside a QueryMesh builder. Use `db.query(...)` when you want to execute a full SQL string directly.

| Need | Use | Example |
|---|---|---|
| SQL expression inside a builder | `raw(...)` | `.where(raw("LOWER(email)"), value)` |
| SQL value/function inside mutation data | `raw(...)` | `.update({ updated_at: raw("CURRENT_TIMESTAMP") })` |
| Full SELECT written manually | `db.query(...)` | `db.query("SELECT * FROM users WHERE id = ?", [id])` |
| Full UPDATE/DELETE/DDL written manually | `db.query(...)` | `db.query("UPDATE users SET active = ? WHERE id = ?", [false, id])` |

```js
// raw(...) is a fragment inside the builder
await db
  .table("users")
  .where(raw("LOWER(email)"), email.toLowerCase())
  .get();

// query(...) executes the whole SQL statement
const result = await db.query(
  "SELECT * FROM users WHERE LOWER(email) = ?",
  [email.toLowerCase()]
);

console.log(result.rows);
```

### Grouped WHERE (explicit parentheses)

Use callback groups when you need exact boolean grouping.

```js
// (a = x AND b = y) OR (c = z OR d = xx)
const rows = await db
  .table("users")
  .where((q) => q.where("a", x).where("b", y))
  .orWhere((q) => q.where("c", z).orWhere("d", xx))
  .get();
```

Nested groups are supported:

```js
// (a=x AND b=y) OR ((c=z OR d=xx) OR (a1=x1 AND b1=y1))
await db.table("users")
  .where((q) => q.where("a", x).where("b", y))
  .orWhere((q) =>
    q.where((g) => g.where("c", z).orWhere("d", xx))
      .orWhere((g) => g.where("a1", x1).where("b1", y1))
  )
  .get();
```

You can negate grouped logic:

```js
// NOT (a = 1 OR b = 2)
await db.table("users")
  .whereNot((q) => q.where("a", 1).orWhere("b", 2))
  .get();
```

### IS / NULL checks

```js
await db.table("users")
  .whereIs("verified", true)
  .whereIsNotNull("deleted_at")
  .get();
```

Aliases:
- `whereNull()` / `whereNotNull()`
- `whereIsNull()` / `whereIsNotNull()`

### ANY / ALL

```js
// Postgres array example
await db.table("users")
  .whereAny("score", ">", [10, 20, 30])
  .get();

// Subquery example (SQL dialects)
const limits = db.table("limits").select("max_score");
await db.table("users")
  .whereAll("score", "<=", limits)
  .get();
```

Notes:
- SQL dialects: `ANY/ALL` support both subquery/raw sources and literal arrays.
- MongoDB: `ANY/ALL` supports literal arrays (not subqueries/raw SQL).

### Joins

Equality joins are the common case:

```js
await db
  .table("orders")
  .leftJoinOn("users", "orders.user_id", "users.id")
  .select(["orders.id", "users.email"])
  .get();
```

The long form still works:

```js
await db
  .table("orders")
  .join("left", "users", "orders.user_id", "=", "users.id")
  .get();
```

Multiple `ON` conditions use `andOn(...)` or `orOn(...)` after the join:

```js
// SQL shape:
// LEFT JOIN A ON B.X = A.X AND B.Y = A.Y
await db
  .table("B")
  .leftJoinOn("A", "B.X", "A.X")
  .andOn("B.Y", "A.Y")
  .get();
```

Use the explicit operator form when needed:

```js
await db
  .table("orders")
  .leftJoin("shipments", "orders.id", "=", "shipments.order_id")
  .andOn("orders.created_at", "<=", "shipments.created_at")
  .get();
```

Rules for join `ON` fields:
- For MongoDB, the first field should be from the current table/collection and the second field should be from the joined table/collection.
- Keep predicates that belong to the join inside `andOn(...)`; moving them to `where(...)` can change `LEFT JOIN` behavior.

Aliased joins are supported (useful when joining the same table multiple times):

```js
await db
  .table("shops")
  .leftJoinOnAs("discounts", "discount", "shops.discountId", "discount.id")
  .leftJoinOnAs("discounts", "adminDiscount", "shops.adminDiscountId", "adminDiscount.id")
  .select(["shops.id", "discount.name", "adminDiscount.name"])
  .get();
```

`join...As` quick reference:
- `joinAs(type, table, alias, left, opOrRight, right?)`
- `joinOnAs(type, table, alias, left, right)` (equality shortcut)
- `innerJoinOn / leftJoinOn / rightJoinOn`
- `innerJoinAs / leftJoinAs / rightJoinAs`
- `innerJoinOnAs / leftJoinOnAs / rightJoinOnAs`
- `andOn(left, opOrRight, right?) / orOn(left, opOrRight, right?)`

Rules:
- `alias` must be non-empty.
- When you use an alias, reference that alias in selected fields and join conditions (`discount.id`, not `discounts.id`).
- Use this pattern when the same table/collection is joined more than once.

SQL compile example for aliased joins:

```sql
SELECT "shops"."id", "discount"."name", "adminDiscount"."name"
FROM "shops"
LEFT JOIN "discounts" AS "discount"
  ON "shops"."discountId" = "discount"."id"
LEFT JOIN "discounts" AS "adminDiscount"
  ON "shops"."adminDiscountId" = "adminDiscount"."id"
```

MongoDB uses `$lookup`:
- `=` joins use `localField/foreignField`.
- `!=`, `<>`, `>`, `>=`, `<`, `<=` joins use `$lookup.pipeline + $expr`.
- Alias joins map to `$lookup.as`, so multiple joins to the same collection can use distinct output keys.

Mongo compile example for aliased joins:

```js
[
  { $lookup: { from: "discounts", localField: "discountId", foreignField: "id", as: "discount" } },
  { $unwind: { path: "$discount", preserveNullAndEmptyArrays: true } },
  { $lookup: { from: "discounts", localField: "adminDiscountId", foreignField: "id", as: "adminDiscount" } },
  { $unwind: { path: "$adminDiscount", preserveNullAndEmptyArrays: true } }
]
```

Output shape example (Mongo):

```json
[
  {
    "id": 1,
    "discount": { "name": "Summer 10%" },
    "adminDiscount": { "name": "Staff 20%" }
  }
]
```

### Group by / Having

```js
const stats = await db
  .table("orders")
  .select("status")
  .count("*", "total")
  .min("amount", "min_amount")
  .max("amount", "max_amount")
  .groupBy("status")
  .having("total", ">", 10)
  .orderBy("total", "desc")
  .get();
```

For SQL expression control, use raw grouping / ordering helpers:

```js
const stats = await db
  .table("orders")
  .selectExpr("DATE(created_at)", "created_day")
  .count("*", "total")
  .groupByRaw("DATE(created_at)")
  .orderByRaw("COUNT(*) DESC")
  .get();
```

### UNION / UNION ALL

```js
const q1 = db.table("users").select("id").where("kind", "user");
const q2 = db.table("admins").select("id").where("enabled", true);
const q3 = db.table("guests").select("id").where("active", true);

const rows = await q1.union(q2).unionAll(q3).get();
```

Notes:
- SQL dialects compile native `UNION` / `UNION ALL`.
- MongoDB supports `UNION` / `UNION ALL` with QueryBuilder sources.

### Insert / Update / Delete

```js
await db.table("users").insert({ email: "a@b.com" }).run();
await db.table("users").update({ name: "A" }).where("id", 1).run();
await db.table("users").delete().where("id", 1).run();
```

### INSERT ... SELECT

```js
const src = db.table("users_archive")
  .select(["id", "email"])
  .where("is_active", true);

await db.table("users")
  .insertSelect(["id", "email"], src)
  .run();
```

Mongo note:
- `insertSelect` on Mongo requires a QueryBuilder source and explicit source select columns (no raw/string SQL source).

### Upsert

Postgres:

```js
await db.table("users")
  .insert({ email: "a@b.com", name: "A" })
  .onConflictDoUpdate("email", { name: "A" })
  .returning(["id"])
  .run();
```

SQL Server:

```js
await db.table("users")
  .insert({ id: 1, email: "a@b.com", name: "A" })
  .onConflictDoUpdate("email", { name: "A2" }) // compiled as MERGE
  .returning(["id"])                            // native OUTPUT
  .run();
```

Oracle:

```js
await db.table("users")
  .insert({ id: 1, email: "a@b.com", name: "A" })
  .onConflictDoUpdate("email", { name: "A2" }) // compiled as MERGE
  .run();
```

MySQL:

```js
await db.table("users")
  .insert({ email: "a@b.com", name: "A" })
  .onDuplicateKeyUpdate({ name: "A" })
  .run();
```

MongoDB:

```js
await db.table("users")
  .insert({ email: "a@b.com", name: "A" })
  .onConflictDoUpdate("email", { name: "A2" }) // mapped to Mongo upsert
  .returning(["email", "name"])                // best-effort returned docs
  .run();
```

Notes:
- `onConflictDoUpdate` is native on pg, and mapped through `MERGE` on mssql/oracle.
- Current mssql/oracle/mongo implementation supports single-row `insert(...)` for `onConflictDoUpdate` (not `insertSelect`).

## Transactions

```js
await db.transaction(async (trx) => {
  await trx.table("accounts").update({ balance: 90 }).where("id", 1).run();
  await trx.table("accounts").update({ balance: 110 }).where("id", 2).run();
});
```

Transaction behavior:
- SQL dialects borrow one dedicated connection from the pool for the duration of the transaction.
- MongoDB starts a session when the underlying `MongoClient` supports it.
- After commit or rollback, QueryMesh releases the borrowed connection/session back to the pool/client.

MongoDB uses sessions when available.

MongoDB transaction notes:
- QueryMesh needs a MongoClient-backed DB handle with `startSession()` (official `mongodb` driver client, or Mongoose connection exposing the underlying client).
- Multi-document transactions require:
  - replica set (MongoDB 4.0+), or
  - sharded cluster (MongoDB 4.2+).
- Standalone MongoDB deployments generally do not support multi-document transactions.

## Model layer

```js
import { BaseModel } from "querymesh";

class User extends BaseModel {
  static table = "users";
  static primaryKey = "id";
}

const UserModel = db.model(User);

const u = await UserModel.find(1);
u.name = "New Name";
await u.save();

// Create (insert) using static create
const created = await UserModel.create({
  email: "new@querymesh.dev",
  name: "New User",
});

// Create (insert) using instance save
const draft = new UserModel({
  email: "draft@querymesh.dev",
  name: "Draft User",
});
await draft.save();
```

`ModelClass` vs `ModelClass.query()`:
- Use model helpers directly on the bound model class: `UserModel.create(...)`, `UserModel.find(id)`, `UserModel.all()`, `instance.save()`.
- Use `UserModel.query()` when you need QueryBuilder features (joins, select lists, aggregates, offset/limit, custom where groups).

```js
// custom query from model
const rows = await UserModel
  .query()
  .select(["users.id", "users.email"])
  .leftJoinOn("departments", "users.id_departments", "departments.id")
  .offset(0)
  .limit(50)
  .get();
```

## Schema builder

### Create table

```js
await db.schema().createTable("companies", (t) => {
  t.increments("id");
  t.string("name", 255).notNull();
  t.unique(["name"]);
  t.timestamps();
}).exec();
```

Type mapping examples:

- `t.string(name, 80)` becomes `VARCHAR(80)` (mysql), `CHARACTER VARYING(80)` (pg), `NVARCHAR(80)` (mssql), `VARCHAR2(80)` (oracle)
- `t.json(name)` becomes `JSONB` (pg), `JSON` (mysql), `NVARCHAR(MAX)` (mssql), `CLOB` (oracle)

### Foreign keys (named constraints)

```js
await db.schema().createTable("orders", (t) => {
  t.increments("id");
  t.int("user_id").notNull();

  t.constraint("fk_orders_user")
    .foreign("user_id")
    .references("users", "id")
    .onDelete("CASCADE");
}).exec();
```

Shorthand:

```js
t.int("user_id").notNull().references("users.id").onDelete("CASCADE");
```

### Alter table

```js
await db.schema().alterTable("users", (t) => {
  t.string("display_name", 120).nullable();
  t.renameColumn("display_name", "name");
  t.dropColumnIfExists("old_field");
  t.dropConstraint("fk_users_company");
}).exec();
```

### Rename and drop tables

```js
await db.schema().renameTable("users", "app_users").exec();
await db.schema().dropTable("app_users", { ifExists: true, cascade: true }).exec();
```

### Maintenance utilities

```js
await db.schema().truncateTable("logs").exec();

await db.schema().analyzeTable("users").exec();

await db.schema().optimizeTable("users").exec();

await db.schema().vacuumTable("users", { analyze: true }).exec();

await db.schema().reindexTable("users").exec();

// mysql only
await db.schema().repairTable("users").exec();
```

If you prefer immediate execution without chaining `.exec()`, use `db.maintenance()`:

```js
await db.maintenance().vacuumDatabase({ verbose: true });
await db.maintenance().reindexTable("users");
```

Notes:
- `truncateTable`:
  - SQL dialects compile to `TRUNCATE TABLE`
  - MongoDB deletes all documents in the collection with `deleteMany({})`
- `analyzeTable`:
  - pg: `ANALYZE`
  - mysql: `ANALYZE TABLE`
  - mssql: `UPDATE STATISTICS`
  - oracle: `DBMS_STATS.GATHER_TABLE_STATS`
- `optimizeTable` is best-effort and not identical across dialects:
  - pg: `VACUUM (ANALYZE)` by default, or `VACUUM` with `{ analyze: false }`
  - mysql: `OPTIMIZE TABLE`
  - mssql: `ALTER INDEX ALL ... REORGANIZE` or `REBUILD` with `{ rebuild: true }`
  - oracle / mongo: not supported
- `vacuumTable` / `vacuumDatabase`:
  - PostgreSQL only
  - useful when you want explicit `VACUUM` instead of the broader `optimizeTable` mapping
  - cannot run inside a transaction block
- `reindexTable`:
  - pg: `REINDEX TABLE` with optional `{ concurrently: true }`
  - mssql: `ALTER INDEX ALL ... REBUILD`
- `repairTable`:
  - mysql only
  - maps to `REPAIR TABLE`
  - mostly useful for storage engines that support table repair directly

### Views

```js
await db.schema()
  .createView("active_users", `
    SELECT id, email
    FROM users
    WHERE is_active = true
  `)
  .exec();

await db.schema().dropView("active_users", { ifExists: true }).exec();
```

### Functions

```js
await db.schema()
  .createFunction({
    name: "public.greet_user",
    args: ["name text"],
    returns: "text",
    language: "plpgsql",
    orReplace: true,
    body: `
BEGIN
  RETURN 'Hello ' || name;
END;
    `.trim(),
  })
  .exec();

await db.schema()
  .dropFunction("public.greet_user", {
    args: ["text"], // pg: use argument types for overloaded functions
    ifExists: true,
  })
  .exec();
```

Notes:

- Supported on `pg`, `mysql`, `mssql`, and `oracle`.
- Not supported on MongoDB in QueryMesh.
- Function bodies are dialect-specific and are passed through mostly as-is.
- `createFunction({ orReplace: true })` is supported on `pg`, `mssql`, and `oracle`. MySQL requires dropping the function first.

### Procedures

```js
await db.schema()
  .createProcedure({
    name: "public.refresh_rollup",
    args: ["target_date date"],
    language: "plpgsql",
    orReplace: true,
    body: `
BEGIN
  PERFORM refresh_rollup_for(target_date);
END;
    `.trim(),
  })
  .exec();

await db.schema()
  .dropProcedure("public.refresh_rollup", {
    args: ["date"], // pg: use argument types for overloaded procedures
    ifExists: true,
  })
  .exec();
```

Notes:

- Supported on `pg`, `mysql`, `mssql`, and `oracle`.
- Not supported on MongoDB in QueryMesh.
- Procedure bodies are dialect-specific and are passed through mostly as-is.
- `createProcedure({ orReplace: true })` is supported on `pg`, `mssql`, and `oracle`. MySQL requires dropping the procedure first.

### Routine Calls

Use `db.call(...)` for simple stored procedure calls and scalar function calls.

```js
await db.call("public.refresh_rollup", ["2026-01-01"]);

const rows = await db.call("public.add_one", [41], {
  kind: "function",
  as: "value",
});
```

Notes:

- `db.call(...)` is SQL-only.
- `OUT` / `INOUT` parameters are supported for procedure calls.
- PostgreSQL maps procedure outputs from the returned row.
- MySQL procedure outputs use session variables and require `config.multipleStatements = true`.
- SQL Server and Oracle output params should provide a driver `type`.
- For complex vendor-specific routine behavior, use `db.query(...)`.

`OUT` / `INOUT` example:

```js
const result = await db.call("dbo.refresh_rollup", [
  { mode: "in", name: "target_date", value: "2026-01-01" },
  { mode: "out", name: "total", type: sql.Int },
  { mode: "inout", name: "status", type: sql.VarChar, value: "queued" },
]);

console.log(result.out); // { total: ..., status: ... }
```

### Table-Valued / Set-Returning Functions

Use `db.callTable(...)` when the routine returns rows.

```js
const rows = await db.callTable("public.list_active_users", [true]);
```

Notes:

- Supported on PostgreSQL, SQL Server, and Oracle.
- Not supported on MySQL or MongoDB.
- `callTable(...)` does not accept `OUT` / `INOUT` params.

### PostgreSQL Types

Use `createType(...)` / `dropType(...)` for PostgreSQL enum or composite types.

```js
await db.schema()
  .createType("public.order_status", {
    kind: "enum",
    values: ["pending", "paid", "cancelled"],
  })
  .exec();

await db.schema()
  .createType("public.address_t", {
    kind: "composite",
    fields: {
      street: "text",
      zip_code: "text",
    },
  }, { ifNotExists: true })
  .exec();

await db.schema()
  .dropType("public.order_status", {
    ifExists: true,
    cascade: true,
  })
  .exec();
```

Notes:

- `createType/dropType` are PostgreSQL-only.
- `createType(name, ["a", "b"])` is shorthand for `AS ENUM ('a', 'b')`.
- You can also pass a raw definition string like `AS RANGE (SUBTYPE = text)` when you need a lower-level PostgreSQL type definition.

### Advanced PostgreSQL Aggregate Objects

```js
await db.schema()
  .createAggregate({
    name: "public.array_concat_agg",
    args: ["anycompatiblearray"],
    definition: {
      SFUNC: "array_cat",
      STYPE: "anycompatiblearray",
      INITCOND: "{}",
      PARALLEL: "SAFE",
    },
  })
  .exec();

await db.schema()
  .dropAggregate("public.array_concat_agg", {
    args: ["anycompatiblearray"],
    ifExists: true,
  })
  .exec();
```

Notes:

- `createAggregate/dropAggregate` are advanced PostgreSQL-only schema APIs.
- They are not required for normal QueryMesh aggregate queries.
- This API is intentionally low-level because custom aggregate objects are not portable across dialects.
- For normal query aggregation, use `count`, `sum`, `avg`, `min`, and `max` on `QueryBuilder`.

### Show Routines

```js
const functions = await db.schema().showFunctions();
const procedures = await db.schema().showProcedures();
const aggregates = await db.schema().showAggregates(); // PostgreSQL only
```

Optional schema filter:

```js
const pgPublicFunctions = await db.schema().showFunctions({ schema: "public" });
```

### Show Views, Triggers, and Indexes

```js
const views = await db.schema().showViews();
const triggers = await db.schema().showTriggers({ table: "public.users" });
const indexes = await db.schema().showIndexes("public.users");
```

Notes:

- `showViews()` lists SQL views and Mongo view collections when the driver exposes them.
- `showTriggers(...)` is SQL-only.
- `showIndexes(name)` works for SQL tables and Mongo collections.

### Show Schemas, Sequences, and Constraints

```js
const schemas = await db.schema().showSchemas();
const sequences = await db.schema().showSequences({ schema: "public" });
const constraints = await db.schema().showConstraints({ table: "public.users" });
```

Notes:

- `showSchemas()` maps to schemas/namespaces for SQL dialects. On MySQL, schemas are databases.
- `showSequences()` is supported on PostgreSQL, SQL Server, and Oracle.
- `showConstraints()` is SQL-only.

### Show tables and databases

```js
const tables = await db.schema().showTables(); // SQL tables / Mongo collections
const dbs = await db.schema().showDatabases();
```

Optional schema filter for SQL:

```js
const pgPublicTables = await db.schema().showTables({ schema: "public" });
```

### Describe table/database structure

```js
// current database/connection structure
const dbDesc = await db.schema().getDesc();

// table/collection structure
const usersDesc = await db.schema().getDesc("users");
// equivalent:
// await db.schema().getDesc("table", "users");

// optional filtering/options
const publicUsers = await db.schema().getDesc("table", { name: "users", schema: "public" });
const deepDb = await db.schema().getDesc("database", { deep: true });
const withScript = await db.schema().getDesc("users", { includeCreateSql: true });
const dbScript = await db.schema().getDesc("database", { includeCreateSql: true });
const richUsers = await db.schema().getDesc("users", { includeIndexes: true, includeTriggers: true });
const richDb = await db.schema().getDesc("database", { includeIndexes: true, includeTriggers: true });

// strict cross-dialect shape (same top-level keys for SQL + Mongo)
const strictDb = await db.schema().getDesc("database", { strict: true });
const strictUsers = await db.schema().getDesc("users", { strict: true });
```

`includeCreateSql: true` adds a `createSql` string to the response.

PostgreSQL `createSql` now includes `CREATE TABLE IF NOT EXISTS ...` with column definitions and primary key constraints (best-effort from catalog metadata).

### Create database (uniform options)

`createDatabase(name, opts)` uses a single option object across dialects. Unsupported fields are ignored by that dialect.

```js
await db.schema().createDatabase("my_app_db", {
  ifNotExists: true,
  // common
  collation: "utf8mb4_unicode_ci", // mysql, mssql
  charset: "utf8mb4",              // mysql
  // postgres
  encoding: "UTF8",
  locale: "en_US.UTF-8",
  owner: "postgres",
  template: "template0",
  tablespace: "pg_default",
}).exec();
```

Notes:

- **Postgres** does not support `CREATE DATABASE IF NOT EXISTS` in a single SQL statement. QueryMesh performs a pre-check and skips creation when it already exists.
- **SQL Server** wraps creation with `IF DB_ID(...) IS NULL` when `ifNotExists` is true.
- **Oracle** database creation is a DBA operation and is not implemented.

### Drop database (uniform options)

```js
await db.schema().dropDatabase("my_app_db", {
  ifExists: true,
  force: true, // best-effort disconnects (pg: terminate sessions, mssql: SINGLE_USER ROLLBACK)
}).exec();
```

Notes:

- **Postgres** `force` attempts to terminate other sessions connected to the DB (requires appropriate privileges). If it can’t, the `DROP DATABASE` may still fail.
- **SQL Server** `force` switches the DB to `SINGLE_USER WITH ROLLBACK IMMEDIATE` before dropping.
- **MySQL** ignores `force` (dropping disconnects sessions automatically).

### PostgreSQL schemas

```js
await db.schema().createSchema("audit").exec();
await db.schema().dropSchema("audit", { ifExists: true, cascade: true }).exec();
```

## Triggers

```js
await db.schema().createTrigger({
  name: "trg_users_touch",
  table: "users",
  timing: "BEFORE",
  events: ["UPDATE"],
  body: "NEW.updated_at = CURRENT_TIMESTAMP;",
}).exec();

await db.schema().dropTrigger("trg_users_touch", { table: "users", ifExists: true }).exec();
```

Notes:
- Supported on `pg`, `mysql`, `mssql`, and `oracle`.
- Not supported on MongoDB in QueryMesh.

## Import/Export (backup)

QueryMesh wraps native CLI tools and provides progress events.

```js
const job = db.backup().export({
  file: "dump.sql.gz",
  format: "plain",
  gzip: true,
  useStdout: true,
  extraArgs: ["--clean"],
});

job.on("progress", (p) => console.log(p));
await job.done;
```

Import progress notes:
- When the tool can read from stdin (for example `mysql`, `psql`, `pg_restore` custom/tar, `mongorestore --archive`), QueryMesh emits byte-based progress.
- For non-stdin restore paths (for example directory restores or engine-native restore commands), QueryMesh emits best-effort progress by parsing `%` from tool logs plus start/end milestones.

Export progress notes:
- `useStdout: true` emits byte progress from streamed stdout.
- File/directory exports emit byte progress by watching output size growth.
- Tool-reported `%` messages are forwarded via `log` events.

The machine running this must have the relevant CLIs installed:

- PG: `pg_dump`, `pg_restore`, `psql`
- MySQL: `mysqldump`, `mysql`
- MongoDB: `mongodump`, `mongorestore`
- SQL Server: `sqlpackage` and/or `sqlcmd`
- Oracle: `expdp`, `impdp`

## Tools / Diagnostics

Use `db.tools()` for runtime checks and version helpers.

```js
const tools = db.tools();

// CLI checks
tools.isPostgresInstalled();
tools.isMongoInstalled();
tools.getToolingStatus(); // current dialect required tools

// DB health/version
await tools.ping();
await tools.getVersion();      // version of current dialect server
await tools.getDiagnostics();  // ping + serverVersion + cli report
```

## TypeScript

QueryMesh ships TypeScript typings.

```ts
import QueryMesh from "querymesh";

const db = await QueryMesh.connect({
  dialect: "pg",
  config: { connectionString: process.env.DATABASE_URL as string },
});
```

## API Reference (by module)

### Module: `QueryMesh` (root)

- `connect({ dialect, config, features?, importer? })`
- `raw(sql, params?)`
- `id(name)`
- `toObjectId(value, opts?)`
- `toObjectIdSync(value, ObjectId)`

### Module: `DB`

- `table(name)`
- `query(sql, params?)`
- `call(name, args?, opts?)`
- `callTable(name, args?)`
- `schema()`
- `backup()`
- `tools()`
- `maintenance()`
- `switchDatabase(name, opts?)`
- `useDatabase(name, opts?)`
- `switchDialect(dialect, config, opts?)`
- `useDialect(dialect, config, opts?)`
- `transaction(async (trx) => ...)`
- `model(ModelClass)`
- `quote(name)`
- `close()`

### Module: `QueryBuilder`

- Selection: `select`, `selectRaw`, `selectExpr`, `distinct`, `aggregate`, `count`, `sum`, `avg`, `min`, `max`
- Filtering: `where`, `orWhere`, `whereGroup`, `orWhereGroup`, `whereNot`, `orWhereNot`
- Predicates: `whereIn`, `whereNotIn`, `whereBetween`, `whereNotBetween`, `whereNull`, `whereNotNull`, `whereIs`, `whereIsNot`
- Quantified: `whereAny`, `whereAll`, `orWhereAny`, `orWhereAll`
- Join/shape: `join`, `joinAs`, `joinOn`, `joinOnAs`, `innerJoin`, `leftJoin`, `rightJoin`, `innerJoinOn`, `leftJoinOn`, `rightJoinOn`, `innerJoinAs`, `leftJoinAs`, `rightJoinAs`, `innerJoinOnAs`, `leftJoinOnAs`, `rightJoinOnAs`, `andOn`, `orOn`
- Set operations: `union`, `unionAll`, `clearUnions`
- Mutation: `insert`, `insertSelect`, `update`, `delete`
- Upsert: `onConflictDoUpdate`, `onDuplicateKeyUpdate`
- Result controls: `groupBy`, `groupByRaw`, `having`, `orderBy`, `orderByRaw`, `limit`, `offset`, `returning`
- Execution: `compile`, `run`, `get`, `first`

### Module: `SchemaBuilder`

- DDL: `createTable`, `alterTable`, `dropTable`, `renameTable`
- Maintenance: `truncateTable`, `analyzeTable`, `optimizeTable`, `vacuumTable`, `vacuumDatabase`, `reindexTable`, `repairTable`
- Databases: `createDatabase`, `dropDatabase`, `showDatabases`, `showSchemas`, `showSequences`
- PostgreSQL schema: `createSchema`, `dropSchema`
- PostgreSQL types: `createType`, `dropType`
- Functions: `createFunction`, `dropFunction`
- Procedures: `createProcedure`, `dropProcedure`
- Advanced PostgreSQL aggregate objects: `createAggregate`, `dropAggregate`
- Views: `createView`, `dropView`
- Triggers: `createTrigger`, `dropTrigger`
- Introspection: `showTables`, `showDatabases`, `showSchemas`, `showSequences`, `showViews`, `showTriggers`, `showIndexes`, `showConstraints`, `showFunctions`, `showProcedures`, `showAggregates`, `getDesc(target?, opts?)`
- `getDesc` opts: `schema`, `deep`, `strict`, `includeViews`, `includeDatabases`, `includeIndexes`, `includeTriggers`, `sampleSize`, `includeCreateSql`
- Execute: `exec`

### Module: `MaintenanceManager`

- `truncateTable(name, opts?)`
- `analyzeTable(name, opts?)`
- `optimizeTable(name, opts?)`
- `vacuumTable(name, opts?)`
- `vacuumDatabase(opts?)`
- `reindexTable(name, opts?)`
- `repairTable(name, opts?)`

### Module: `BaseModel`

- Static: `bind`, `query`, `find`, `all`, `create`, `where`, `hydrate`
- Instance: `save`, `delete`

### Module: `BackupManager`

- `export(options?)`
- `import(options?)`
- Job events: `start`, `progress`, `log`, `error`, `done`

### Module: `ToolsManager`

- Tool checks: `isPostgresInstalled`, `isPostgreInstalled`, `isMySqlInstalled`, `isMsSqlInstalled`, `isOracleInstalled`, `isMongoInstalled`
- CLI metadata: `isCommandAvailable`, `getCliVersion`, `getToolingStatus`, `getAllToolingStatus`, `getCurrentDialectCliVersion`
- Runtime metadata: `ping`, `getVersion`, `getDiagnostics`

## Tests

```bash
npm test
```

## License

MIT
