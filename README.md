# SQuery

SQuery is a compact **query builder + light ORM** for Node.js with multi-database support.

## Supported dialects

- **PostgreSQL** (`pg`)
- **MySQL** (`mysql`)
- **SQL Server** (`mssql`)
- **Oracle** (`oracledb`)
- **MongoDB** (`mongodb`) — same fluent builder, but executed as real MongoDB operations.

## Install

```bash
npm i squery

# install only the driver(s) you need
npm i pg
npm i mysql
npm i mssql
npm i oracledb
npm i mongodb
```

## Connect

```js
import SQuery from "squery";

// PostgreSQL
const db = await SQuery.connect({
  dialect: "pg",
  config: { connectionString: process.env.DATABASE_URL },
  // also accepts { server, user, password, database } (server -> host alias)
  // and supports config.options to pass Pool options:
  // config: { server: "localhost", user: "u", password: "p", database: "app", options: { ssl: true, max: 20 } }
});

// MySQL
// const db = await SQuery.connect({
//   dialect: "mysql",
//   config: { host: "127.0.0.1", user: "root", password: "", database: "app" },
// });

// SQL Server
// const db = await SQuery.connect({
//   dialect: "mssql",
//   config: { server: "localhost", user: "sa", password: "pass", database: "master", options: { trustServerCertificate: true } },
// });

// Oracle
// const db = await SQuery.connect({ dialect: "oracle", config: { user, password, connectString } });

// MongoDB
// const db = await SQuery.connect({
//   dialect: "mongo", // alias: "mongodb"
//   config: { connectionString: "mongodb://localhost:27017/app" }, // or { uri, dbName }
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

### ANY / ALL (SQL)

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
- `ANY/ALL` are SQL-only (not MongoDB).
- `ANY(array)` is currently supported only on PostgreSQL; other dialects should pass a subquery/raw SQL source.

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

MongoDB uses `$lookup` for joins and requires equality.

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

### UNION / UNION ALL (SQL)

```js
const q1 = db.table("users").select("id").where("kind", "user");
const q2 = db.table("admins").select("id").where("enabled", true);
const q3 = db.table("guests").select("id").where("active", true);

const rows = await q1.union(q2).unionAll(q3).get();
```

Notes:
- `UNION` is SQL-only (not MongoDB).

### Insert / Update / Delete

```js
await db.table("users").insert({ email: "a@b.com" }).run();
await db.table("users").update({ name: "A" }).where("id", 1).run();
await db.table("users").delete().where("id", 1).run();
```

### INSERT ... SELECT (SQL)

```js
const src = db.table("users_archive")
  .select(["id", "email"])
  .where("is_active", true);

await db.table("users")
  .insertSelect(["id", "email"], src)
  .run();
```

### Upsert

Postgres:

```js
await db.table("users")
  .insert({ email: "a@b.com", name: "A" })
  .onConflictDoUpdate("email", { name: "A" })
  .returning(["id"]) // pg only
  .run();
```

MySQL:

```js
await db.table("users")
  .insert({ email: "a@b.com", name: "A" })
  .onDuplicateKeyUpdate({ name: "A" })
  .run();
```

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
import { BaseModel } from "squery";

class User extends BaseModel {
  static table = "users";
  static primaryKey = "id";
}

const UserModel = db.model(User);

const u = await UserModel.find(1);
u.name = "New Name";
await u.save();
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

- **Postgres** does not support `CREATE DATABASE IF NOT EXISTS` in a single SQL statement. SQuery performs a pre-check and skips creation when it already exists.
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

SQuery wraps native CLI tools and provides progress events.

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
- When the tool can read from stdin (for example `mysql`, `psql`, `pg_restore` custom/tar, `mongorestore --archive`), SQuery emits byte-based progress.
- For non-stdin restore paths (for example directory restores or engine-native restore commands), SQuery emits best-effort progress by parsing `%` from tool logs plus start/end milestones.

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

SQuery ships TypeScript typings.

```ts
import SQuery from "squery";

const db = await SQuery.connect({
  dialect: "pg",
  config: { connectionString: process.env.DATABASE_URL as string },
});
```

## API Reference (by module)

### Module: `SQuery` (root)

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
