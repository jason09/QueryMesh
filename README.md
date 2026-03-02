# QueryMesh

![QueryMesh logo](./assets/querymesh-logo.svg)

QueryMesh is a compact **query builder + light ORM** for Node.js with multi-database support.

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
| `schema().createView()/dropView()` | ✅ | ✅ | ✅ | ✅ | ❌ |
| Schema DDL (`createTable/alterTable/dropTable`) | ✅ | ✅ | ✅ | ✅ | ⚠️ (`dropTable` only) |

Mongo notes:
- Mongo joins use `$lookup`; comparison join operators are supported (`=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`).
- Mongo `UNION/UNION ALL` supports QueryBuilder sources (raw/string SQL sources are not supported).
- Mongo `insertSelect` supports QueryBuilder source with explicit selected columns.
- Mongo `schema().dropTable(name)` drops a collection; `createTable/alterTable` remain SQL-only.
- Right/full join semantics are not fully equivalent to SQL joins in Mongo pipelines.
- For Mongo schema APIs, use `showTables`, `showDatabases`, and `getDesc` for introspection.

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

### Where + OR

```js
const rows = await db
  .table("users")
  .where("country", "GN")
  .orWhere("role", "admin")
  .get();
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

MongoDB uses `$lookup`:
- `=` joins use `localField/foreignField`.
- `!=`, `<>`, `>`, `>=`, `<`, `<=` joins use `$lookup.pipeline + $expr`.

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

MongoDB uses sessions when available.

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

### Module: `DB`

- `table(name)`
- `schema()`
- `backup()`
- `tools()`
- `switchDatabase(name, opts?)`
- `useDatabase(name, opts?)`
- `switchDialect(dialect, config, opts?)`
- `useDialect(dialect, config, opts?)`
- `transaction(async (trx) => ...)`
- `model(ModelClass)`
- `quote(name)`
- `close()`

### Module: `QueryBuilder`

- Selection: `select`, `distinct`, `aggregate`, `count`, `sum`, `avg`, `min`, `max`
- Filtering: `where`, `orWhere`, `whereGroup`, `orWhereGroup`, `whereNot`, `orWhereNot`
- Predicates: `whereIn`, `whereNotIn`, `whereBetween`, `whereNotBetween`, `whereNull`, `whereNotNull`, `whereIs`, `whereIsNot`
- Quantified: `whereAny`, `whereAll`, `orWhereAny`, `orWhereAll`
- Join/shape: `join`, `joinOn`, `innerJoin`, `leftJoin`, `rightJoin`
- Set operations: `union`, `unionAll`, `clearUnions`
- Mutation: `insert`, `insertSelect`, `update`, `delete`
- Upsert: `onConflictDoUpdate`, `onDuplicateKeyUpdate`
- Result controls: `groupBy`, `having`, `orderBy`, `limit`, `offset`, `returning`
- Execution: `compile`, `run`, `get`, `first`

### Module: `SchemaBuilder`

- DDL: `createTable`, `alterTable`, `dropTable`, `renameTable`
- Databases: `createDatabase`, `dropDatabase`, `showDatabases`
- PostgreSQL schema: `createSchema`, `dropSchema`
- Views: `createView`, `dropView`
- Triggers: `createTrigger`, `dropTrigger`
- Introspection: `showTables`, `showDatabases`, `getDesc(target?, opts?)`
- `getDesc` opts: `schema`, `deep`, `strict`, `includeViews`, `includeDatabases`, `sampleSize`, `includeCreateSql`
- Execute: `exec`

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
