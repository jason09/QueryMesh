import { assertIdent, quoteIdent, splitIdent } from '../utils/identifiers.js';

class ColumnDef {
  constructor(dialect, name, type, table) {
    this.dialect = dialect;
    this.name = name;
    this.type = type;
    /** @type {TableBuilder|null} */
    this._table = table || null;
    this.constraints = [];
    /** @type {ForeignKeyDef|null} */
    this._fk = null;
    /** @type {{values: string[]}|null} */
    this._enum = null;
  }
  notNull() { this.constraints.push('NOT NULL'); return this; }
  nullable() { return this; }
  unique() { this.constraints.push('UNIQUE'); return this; }
  default(val) {
    if (val === null) this.constraints.push('DEFAULT NULL');
    else if (typeof val === 'number') this.constraints.push(`DEFAULT ${val}`);
    else this.constraints.push(`DEFAULT '${String(val).replace(/'/g, "''")}'`);
    return this;
  }

  /**
   * Shorthand: define a foreign key referencing "table.column".
   * Example: t.int('user_id').references('users.id')
   * @param {string} ref
   */
  references(ref) {
    const m = String(ref).split('.');
    if (m.length !== 2) throw new Error(`references() expects "table.column", got: ${ref}`);
    const fk = new ForeignKeyDef([this.name]).references(m[0], [m[1]]);
    this._fk = fk;
    if (this._table) this._table.foreignKeys.push(fk);
    return this._fk;
  }

  /**
   * Internal: register enum values.
   * @param {string[]} values
   */
  _setEnum(values) {
    this._enum = { values: values.map(String) };
  }

  toSQL() {
    const base = `${quoteIdent(this.dialect, this.name)} ${this.type}`;
    const cs = this.constraints.join(' ');
    return `${base} ${cs}`.trim();
  }
}

class ForeignKeyDef {
  /**
   * @param {string[]} columns
   */
  constructor(columns) {
    this.columns = columns;
    /** @type {string|null} */
    this.refTable = null;
    /** @type {string[]|null} */
    this.refColumns = null;
    /** @type {string|null} */
    this.onDeleteAction = null;
    /** @type {string|null} */
    this.onUpdateAction = null;
    /** @type {string|null} */
    this.constraintName = null;
  }

  /**
   * @param {string} table
   * @param {string|string[]} column
   */
  references(table, column) {
    this.refTable = table;
    this.refColumns = Array.isArray(column) ? column : [column];
    return this;
  }

  /** @param {string} name */
  name(name) { this.constraintName = name; return this; }
  /** @param {string} action */
  onDelete(action) { this.onDeleteAction = String(action).toUpperCase(); return this; }
  /** @param {string} action */
  onUpdate(action) { this.onUpdateAction = String(action).toUpperCase(); return this; }

  /**
   * @param {string} dialect
   */
  toSQL(dialect) {
    if (!this.refTable || !this.refColumns) throw new Error('Foreign key missing references()');
    const cols = this.columns.map(c => quoteIdent(dialect, c)).join(', ');
    const rcols = this.refColumns.map(c => quoteIdent(dialect, c)).join(', ');
    const parts = [];
    if (this.constraintName) parts.push(`CONSTRAINT ${quoteIdent(dialect, this.constraintName)}`);
    parts.push(`FOREIGN KEY (${cols}) REFERENCES ${quoteIdent(dialect, this.refTable)} (${rcols})`);
    if (this.onDeleteAction) parts.push(`ON DELETE ${this.onDeleteAction}`);
    if (this.onUpdateAction) parts.push(`ON UPDATE ${this.onUpdateAction}`);
    return parts.join(' ');
  }
}

class TableBuilder {
  constructor(adapter) {
    this.adapter = adapter;
    this.dialect = adapter.dialect;
    this.columns = [];
    /** @type {string[]} */
    this.tableConstraints = [];
    /** @type {ForeignKeyDef[]} */
    this.foreignKeys = [];
    /** @type {{name?: string, columns: string[], unique?: boolean}[]} */
    this.indexes = [];
    /** @type {string[]} */
    this.primaryKeys = [];
    /** @type {boolean} */
    this._timestamps = false;
  }
  increments(name) {
    if (this.dialect === 'pg') {
      this.columns.push(new ColumnDef(this.dialect, name, 'SERIAL PRIMARY KEY', this));
      this.primaryKeys = [name];
    } else if (this.dialect === 'mysql') {
      this.columns.push(new ColumnDef(this.dialect, name, 'INT AUTO_INCREMENT PRIMARY KEY', this));
      this.primaryKeys = [name];
    } else if (this.dialect === 'mssql') {
      this.columns.push(new ColumnDef(this.dialect, name, 'INT IDENTITY(1,1) PRIMARY KEY', this));
      this.primaryKeys = [name];
    } else if (this.dialect === 'oracle') {
      // simplest: user provides sequence/trigger; we just use NUMBER
      this.columns.push(new ColumnDef(this.dialect, name, 'NUMBER PRIMARY KEY', this));
    } else {
      this.columns.push(new ColumnDef(this.dialect, name, 'INTEGER PRIMARY KEY', this));
    }
    return this;
  }
  string(name, len = 255) {
    let type;
    if (this.dialect === 'oracle') type = `VARCHAR2(${len})`;
    else if (this.dialect === 'mssql') type = `NVARCHAR(${len})`;
    else type = `VARCHAR(${len})`;
    const c = new ColumnDef(this.dialect, name, type, this);
    this.columns.push(c);
    return c;
  }

  /** Alias */
  int(name) { return this.integer(name); }
  integer(name) {
    let type;
    if (this.dialect === 'oracle') type = 'NUMBER';
    else if (this.dialect === 'mssql') type = 'INT';
    else if (this.dialect === 'mysql') type = 'INT';
    else type = 'INTEGER';
    const c = new ColumnDef(this.dialect, name, type, this);
    this.columns.push(c);
    return c;
  }
  boolean(name) {
    let type;
    if (this.dialect === 'mysql') type = 'TINYINT(1)';
    else if (this.dialect === 'oracle') type = 'NUMBER(1)';
    else if (this.dialect === 'mssql') type = 'BIT';
    else type = 'BOOLEAN';
    const c = new ColumnDef(this.dialect, name, type, this);
    this.columns.push(c);
    return c;
  }
  timestamp(name) {
    let type;
    if (this.dialect === 'mssql') type = 'DATETIME2';
    else if (this.dialect === 'pg') type = 'TIMESTAMPTZ';
    else type = 'TIMESTAMP';
    const c = new ColumnDef(this.dialect, name, type, this);
    this.columns.push(c);
    return c;
  }

  /**
   * Create created_at/updated_at with defaults.
   * - Always sets DEFAULT per dialect.
   * - Ensures updated_at is refreshed on UPDATE (best-effort):
   *   - MySQL: ON UPDATE CURRENT_TIMESTAMP
   *   - PG/SQLServer/Oracle: a trigger is generated by SchemaBuilder after CREATE TABLE.
   */
  timestamps() {
    this._timestamps = true;
    const nowExpr = this._nowDefaultExpr();
    const created = this.timestamp('created_at').notNull();
    const updated = this.timestamp('updated_at').notNull();
    created.constraints.push(`DEFAULT ${nowExpr}`);
    if (this.dialect === 'mysql') {
      updated.constraints.push('DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP');
    } else {
      updated.constraints.push(`DEFAULT ${nowExpr}`);
    }
    return this;
  }

  /**
   * Enum column.
   * MySQL: ENUM('a','b'); Others: VARCHAR + CHECK(col IN (...))
   * @param {string} name
   * @param {string[]} values
   */
  enum(name, values) {
    const vals = values.map(v => String(v));
    if (this.dialect === 'mysql') {
      const escaped = vals.map(v => `'${v.replace(/'/g, "''")}'`).join(',');
      const c = new ColumnDef(this.dialect, name, `ENUM(${escaped})`, this);
      this.columns.push(c);
      return c;
    }
    // Cross-dialect fallback with CHECK
    const c = this.string(name, 255);
    c._setEnum(vals);
    const list = vals.map(v => `'${v.replace(/'/g, "''")}'`).join(', ');
    this.check(`${quoteIdent(this.dialect, name)} IN (${list})`);
    return c;
  }

  /**
   * Define primary key constraint.
   * @param {string|string[]} columns
   * @param {string} [name]
   */
  primary(columns, name) {
    const cols = (Array.isArray(columns) ? columns : [columns]).map(c => quoteIdent(this.dialect, c)).join(', ');
    const prefix = name ? `CONSTRAINT ${quoteIdent(this.dialect, name)} ` : '';
    this.tableConstraints.push(`${prefix}PRIMARY KEY (${cols})`);
    return this;
  }

  /**
   * Define unique constraint.
   * @param {string|string[]} columns
   * @param {string} [name]
   */
  unique(columns, name) {
    const cols = (Array.isArray(columns) ? columns : [columns]).map(c => quoteIdent(this.dialect, c)).join(', ');
    const prefix = name ? `CONSTRAINT ${quoteIdent(this.dialect, name)} ` : '';
    this.tableConstraints.push(`${prefix}UNIQUE (${cols})`);
    return this;
  }

  /**
   * Define check constraint.
   * @param {string} expression
   * @param {string} [name]
   */
  check(expression, name) {
    const prefix = name ? `CONSTRAINT ${quoteIdent(this.dialect, name)} ` : '';
    this.tableConstraints.push(`${prefix}CHECK (${expression})`);
    return this;
  }

  /**
   * Define foreign key constraint.
   * @param {string|string[]} columns
   */
  foreign(columns) {
    const fk = new ForeignKeyDef(Array.isArray(columns) ? columns : [columns]);
    this.foreignKeys.push(fk);
    return fk;
  }

  /**
   * Constraint naming wrapper.
   * Example: t.constraint('fk_orders_user').foreign('user_id').references('users','id')...
   * @param {string} name
   */
  constraint(name) {
    const self = this;
    const cname = String(name);
    return {
      primary(columns) { self.primary(columns, cname); return self; },
      unique(columns) { self.unique(columns, cname); return self; },
      check(expression) { self.check(expression, cname); return self; },
      foreign(columns) { return self.foreign(columns).name(cname); },
    };
  }

  /**
   * Simple index declaration (built as separate statements later).
   * @param {string|string[]} columns
   * @param {{name?: string, unique?: boolean}} [opts]
   */
  index(columns, opts = {}) {
    const cols = Array.isArray(columns) ? columns : [columns];
    this.indexes.push({ name: opts.name, columns: cols, unique: !!opts.unique });
    return this;
  }

  _nowDefaultExpr() {
    if (this.dialect === 'pg') return 'CURRENT_TIMESTAMP';
    if (this.dialect === 'mysql') return 'CURRENT_TIMESTAMP';
    if (this.dialect === 'mssql') return 'GETDATE()';
    if (this.dialect === 'oracle') return 'CURRENT_TIMESTAMP';
    return 'CURRENT_TIMESTAMP';
  }
}


class AlterTableBuilder {
  constructor(adapter, tableName) {
    this.adapter = adapter;
    this.dialect = adapter.dialect;
    this.tableName = tableName;
    /** @type {string[]} */
    this._sql = [];
    /** @type {string[]} */
    this._post = [];
    /** @type {boolean} */
    this._timestamps = false;
  }

  /**
   * Add column using a ColumnDef.
   * @param {ColumnDef} col
   */
  addColumn(col) {
    const qt = quoteIdent(this.dialect, this.tableName);
    // PG/Oracle: ADD COLUMN, MySQL/MSSQL: ADD
    const kw = (this.dialect === 'pg' || this.dialect === 'oracle') ? 'ADD COLUMN' : 'ADD';
    this._sql.push(`ALTER TABLE ${qt} ${kw} ${col.toSQL()}`);
    return col;
  }

  /** @param {string} name @param {string} type */
  addColumnRaw(name, type) {
    return this.addColumn(new ColumnDef(this.dialect, name, type, null));
  }

  /** @param {string} name */
  dropColumn(name) {
    const qt = quoteIdent(this.dialect, this.tableName);
    const kw = (this.dialect === 'pg' || this.dialect === 'oracle') ? 'DROP COLUMN' : 'DROP COLUMN';
    this._sql.push(`ALTER TABLE ${qt} ${kw} ${quoteIdent(this.dialect, name)}`);
    return this;
  }

  /**
   * Drop a column if it exists (best-effort per dialect).
   * @param {string} name
   */
  dropColumnIfExists(name) {
    const d = this.dialect;
    const qt = quoteIdent(d, this.tableName);
    if (d === 'pg') {
      this._sql.push(`ALTER TABLE ${qt} DROP COLUMN IF EXISTS ${quoteIdent(d, name)}`);
      return this;
    }
    if (d === 'mysql') {
      // MySQL 8 supports IF EXISTS; older versions may not.
      this._sql.push(`ALTER TABLE ${qt} DROP COLUMN IF EXISTS ${quoteIdent(d, name)}`);
      return this;
    }
    if (d === 'mssql') {
      const tbl = String(this.tableName).replace(/'/g, "''");
      const col = String(name).replace(/'/g, "''");
      this._sql.push(`IF COL_LENGTH(N'${tbl}', N'${col}') IS NOT NULL ALTER TABLE ${qt} DROP COLUMN ${quoteIdent(d, name)}`);
      return this;
    }
    // Oracle: no simple IF EXISTS; best-effort.
    this._sql.push(`ALTER TABLE ${qt} DROP COLUMN ${quoteIdent(d, name)}`);
    return this;
  }

  /**
   * Drop constraint by name (best-effort).
   * - PG/MSSQL/Oracle: DROP CONSTRAINT
   * - MySQL: tries DROP FOREIGN KEY then DROP INDEX in post statements
   * @param {string} name
   */
  dropConstraint(name) {
    const qt = quoteIdent(this.dialect, this.tableName);
    if (this.dialect === 'mysql') {
      // MySQL has different verbs depending on constraint type.
      // Best-effort: attempt both. We run them sequentially; some may fail depending on what the name refers to.
      this._post.push(`ALTER TABLE ${qt} DROP FOREIGN KEY ${quoteIdent(this.dialect, name)}`);
      this._post.push(`ALTER TABLE ${qt} DROP INDEX ${quoteIdent(this.dialect, name)}`);
    } else {
      this._sql.push(`ALTER TABLE ${qt} DROP CONSTRAINT ${quoteIdent(this.dialect, name)}`);
    }
    return this;
  }

  /**
   * Rename column (best-effort).
   * - PG/Oracle/MSSQL/MySQL 8+: RENAME COLUMN
   * - Older MySQL may require CHANGE with type; use schema.raw(...) in that case.
   * @param {string} from
   * @param {string} to
   */
  renameColumn(from, to) {
    const qt = quoteIdent(this.dialect, this.tableName);
    if (this.dialect === 'mssql') {
      // sp_rename 'table.old', 'new', 'COLUMN'
      this._sql.push(`EXEC sp_rename '${this.tableName}.${from}', '${to}', 'COLUMN'`);
      return this;
    }
    // PG/MySQL8/Oracle
    this._sql.push(`ALTER TABLE ${qt} RENAME COLUMN ${quoteIdent(this.dialect, from)} TO ${quoteIdent(this.dialect, to)}`);
    return this;
  }

  // Column helpers (so you can do: alterTable('x', t => t.string('a',50).notNull(); ...))
  string(name, len = 255) { return this.addColumn(new ColumnDef(this.dialect, name, `VARCHAR(${len})`, null)); }
  text(name) { return this.addColumn(new ColumnDef(this.dialect, name, 'TEXT', null)); }
  int(name) { return this.addColumn(new ColumnDef(this.dialect, name, (this.dialect === 'oracle') ? 'NUMBER(10)' : 'INT', null)); }
  bigInt(name) { return this.addColumn(new ColumnDef(this.dialect, name, (this.dialect === 'pg') ? 'BIGINT' : (this.dialect === 'oracle' ? 'NUMBER(19)' : 'BIGINT'), null)); }
  bool(name) { return this.addColumn(new ColumnDef(this.dialect, name, (this.dialect === 'mysql') ? 'TINYINT(1)' : (this.dialect === 'mssql' ? 'BIT' : 'BOOLEAN'), null)); }
  decimal(name, precision = 12, scale = 2) { return this.addColumn(new ColumnDef(this.dialect, name, `DECIMAL(${precision},${scale})`, null)); }
  timestamp(name) { return this.addColumn(new ColumnDef(this.dialect, name, 'TIMESTAMP', null)); }

  /**
   * Create created_at/updated_at with defaults and ensure updated_at auto refresh.
   */
  timestamps() {
    const nowExpr = (this.dialect === 'mssql') ? 'GETDATE()' : 'CURRENT_TIMESTAMP';
    const c1 = this.timestamp('created_at').notNull(); c1.constraints.push(`DEFAULT ${nowExpr}`);
    const c2 = this.timestamp('updated_at').notNull();
    if (this.dialect === 'mysql') c2.constraints.push('DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP');
    else c2.constraints.push(`DEFAULT ${nowExpr}`);
    // note: triggers for alter table timestamps are not auto-created here (needs PK for mssql).
    return this;
  }

  toSQL() {
    // For mysql dropConstraint auto statements placed in _post; main statements in _sql
    // Use ';' separated statements handled by SchemaBuilder.exec()
    return this._sql.join('; ');
  }
  postSQL() { return this._post; }
}

/**
 * Minimal schema builder for common DDL.
 * Use db.schema().createTable(...).exec()
 */
export class SchemaBuilder {
  constructor(adapter) {
    this.adapter = adapter;
    this.dialect = adapter.dialect;
    this._sql = null;
    this._postSql = [];
    this._meta = null;
  }

  /**
   * Create table.
   * @param {string} name
   * @param {(t: TableBuilder)=>void} fn
   */
  createTable(name, fn) {
    if (this.dialect === 'mongo') throw new Error('Schema builder is SQL-only');
    const t = new TableBuilder(this.adapter);
    fn(t);
    const colSql = t.columns.map(c => c.toSQL());
    const fkSql = t.foreignKeys.map(fk => fk.toSQL(this.dialect));
    const consSql = t.tableConstraints;
    const all = [...colSql, ...consSql, ...fkSql].join(', ');
    this._sql = `CREATE TABLE ${quoteIdent(this.dialect, name)} (${all})`;

    // store index statements to run after create
    this._postSql = t.indexes.map(ix => {
      const cols = ix.columns.map(c => quoteIdent(this.dialect, c)).join(', ');
      const iname = ix.name || `${name}_${ix.columns.join('_')}_${ix.unique ? 'ux' : 'ix'}`;
      const uniq = ix.unique ? 'UNIQUE ' : '';
      return `CREATE ${uniq}INDEX ${quoteIdent(this.dialect, iname)} ON ${quoteIdent(this.dialect, name)} (${cols})`;
    });

    // timestamps trigger (best-effort) for dialects that need it
    if (t._timestamps && this.dialect !== 'mysql') {
      this._postSql.push(...this._timestampTriggerSql(name, t.primaryKeys));
    }

    return this;
  }


/**
 * Alter table.
 * Supports: addColumn, dropColumn, renameColumn, dropConstraint (best-effort per dialect).
 * @param {string} name
 * @param {(t: AlterTableBuilder)=>void} fn
 */

alterTable(name, fn) {
  if (this.dialect === 'mongo') throw new Error('Schema builder is SQL-only');
  const t = new AlterTableBuilder(this.adapter, name);
  fn(t);
  const main = Array.isArray(t._sql) ? t._sql.slice() : [];
  this._sql = main.shift() ?? null;
  this._postSql = [...main, ...t.postSQL()];

  if (t._timestamps && this.dialect !== 'mysql') {
    this._postSql.push(...this._timestampTriggerSql(name, []));
  }
  return this;
}


/**
 * Generate best-effort SQL to keep updated_at refreshed.
 * @param {string} table
 * @param {string[]} pkCols
 * @returns {string[]}
 */
_timestampTriggerSql(table, pkCols) {
  const d = this.dialect;
  const qt = quoteIdent(d, table);
  if (d === 'pg') {
    const fnName = `${table}_set_updated_at`;
    const trgName = `${table}_updated_at_trg`;
    return [
      `CREATE OR REPLACE FUNCTION ${quoteIdent(d, fnName)}() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = CURRENT_TIMESTAMP; RETURN NEW; END; $$ LANGUAGE plpgsql`,
      `DROP TRIGGER IF EXISTS ${quoteIdent(d, trgName)} ON ${qt}`,
      `CREATE TRIGGER ${quoteIdent(d, trgName)} BEFORE UPDATE ON ${qt} FOR EACH ROW EXECUTE FUNCTION ${quoteIdent(d, fnName)}()`,
    ];
  }
  if (d === 'mssql') {
    const trgName = `trg_${table}_updated_at`;
    const pks = (pkCols && pkCols.length) ? pkCols : [];
    if (!pks.length) {
      // Without PK we can't safely join inserted -> table. Skip trigger.
      return [];
    }
    const join = pks.map(c => `t.${quoteIdent(d, c)} = i.${quoteIdent(d, c)}`).join(' AND ');
    return [
      `IF OBJECT_ID('${trgName}', 'TR') IS NOT NULL DROP TRIGGER ${quoteIdent(d, trgName)}`,
      `CREATE TRIGGER ${quoteIdent(d, trgName)} ON ${qt} AFTER UPDATE AS BEGIN IF (TRIGGER_NESTLEVEL() > 1) RETURN; SET NOCOUNT ON; UPDATE t SET updated_at = GETDATE() FROM ${qt} t JOIN inserted i ON ${join}; END`,
    ];
  }
  if (d === 'oracle') {
    const trgName = `TRG_${table}_UPDATED_AT`;
    return [
      `BEGIN EXECUTE IMMEDIATE 'DROP TRIGGER ${trgName}'; EXCEPTION WHEN OTHERS THEN NULL; END;`,
      `CREATE OR REPLACE TRIGGER ${trgName} BEFORE UPDATE ON ${table} FOR EACH ROW BEGIN :NEW.updated_at := CURRENT_TIMESTAMP; END;`,
    ];
  }
  return [];
}
  /**
   * Drop table.
   */
  
  /**
   * Create database (best-effort).
   * - PG: supports { owner, encoding, lcCollate, lcCtype, template }
   * - MySQL: supports { charset, collation }
   * - MSSQL: supports { collation }
   * - Oracle: not supported (requires DBA provisioning)
   * @param {string} name
   * @param {object} [opts]
   */
  createDatabase(name, opts = {}) {
    const d = this.dialect;
    const dbName = String(name);
    if (d === 'oracle') throw new Error('oracle: createDatabase is not supported (create a user/tablespace via DBA tools).');

    // Keep a meta copy so exec() can implement IF NOT EXISTS where the dialect doesn't support it natively.
    this._meta = { op: 'createDatabase', name: dbName, opts: { ...opts } };

    const ifNotExists = opts.ifNotExists === true;
    const charset = opts.charset;
    const collation = opts.collation;
    const encoding = opts.encoding;
    const locale = opts.locale;
    const owner = opts.owner;
    const template = opts.template;
    const lcCollate = opts.lcCollate ?? locale;
    const lcCtype = opts.lcCtype ?? locale;
    const tablespace = opts.tablespace;

    if (d === 'pg') {
      const parts = [];
      if (owner) parts.push(`OWNER ${quoteIdent(d, owner)}`);
      if (template) parts.push(`TEMPLATE ${quoteIdent(d, template)}`);
      if (encoding) parts.push(`ENCODING '${String(encoding).replace(/'/g, "''")}'`);
      if (lcCollate) parts.push(`LC_COLLATE '${String(lcCollate).replace(/'/g, "''")}'`);
      if (lcCtype) parts.push(`LC_CTYPE '${String(lcCtype).replace(/'/g, "''")}'`);
      if (tablespace) parts.push(`TABLESPACE ${quoteIdent(d, tablespace)}`);
      const withClause = parts.length ? ` WITH ${parts.join(' ')}` : '';
      // PG doesn't support CREATE DATABASE IF NOT EXISTS; exec() will precheck when ifNotExists=true.
      this._sql = `CREATE DATABASE ${quoteIdent(d, dbName)}${withClause}`;
      return this;
    }

    if (d === 'mysql') {
      const ifne = ifNotExists ? ' IF NOT EXISTS' : '';
      const cs = charset ? ` CHARACTER SET ${String(charset)}` : '';
      const coll = collation ? ` COLLATE ${String(collation)}` : '';
      this._sql = `CREATE DATABASE${ifne} ${quoteIdent(d, dbName)}${cs}${coll}`;
      return this;
    }

    if (d === 'mssql') {
      // SQL Server doesn't have CREATE DATABASE IF NOT EXISTS in one statement.
      // exec() will wrap ifNotExists via IF DB_ID(...) IS NULL.
      this._sql = `CREATE DATABASE ${quoteIdent(d, dbName)}`;
      this._postSql = [];
      if (collation) {
        this._postSql.push(`ALTER DATABASE ${quoteIdent(d, dbName)} COLLATE ${String(collation)}`);
      }
      return this;
    }

    this._sql = `CREATE DATABASE ${quoteIdent(d, dbName)}`;
    return this;
  }

  /**
   * Drop database (best-effort).
   * @param {string} name
   * Uniform options across dialects:
   * - ifExists: default true
   * - force: default false (best-effort: terminate active connections / rollback immediate when supported)
   * @param {{ifExists?: boolean, force?: boolean}} [opts]
   */
  dropDatabase(name, opts = {}) {
    const d = this.dialect;
    const dbName = String(name);
    if (d === 'oracle') throw new Error('oracle: dropDatabase is not supported (drop user/tablespace via DBA tools).');
    const ifExists = opts.ifExists !== false;
    const force = opts.force === true;

    // Store meta so exec() can perform best-effort pre-statements (terminate connections, single-user, etc.).
    this._meta = { op: 'dropDatabase', name: dbName, opts: { ifExists, force } };

    if (d === 'pg') {
      this._sql = ifExists ? `DROP DATABASE IF EXISTS ${quoteIdent(d, dbName)}` : `DROP DATABASE ${quoteIdent(d, dbName)}`;
      return this;
    }

    if (d === 'mysql') {
      this._sql = ifExists ? `DROP DATABASE IF EXISTS ${quoteIdent(d, dbName)}` : `DROP DATABASE ${quoteIdent(d, dbName)}`;
      return this;
    }

    if (d === 'mssql') {
      // exec() will add SINGLE_USER WITH ROLLBACK IMMEDIATE when force=true
      this._sql = `IF DB_ID(N'${dbName.replace(/'/g,"''")}') IS NOT NULL DROP DATABASE ${quoteIdent(d, dbName)}`;
      return this;
    }

    this._sql = `DROP DATABASE ${quoteIdent(d, dbName)}`;
    return this;
  }

  /**
   * Drop index (best-effort).
   * - PG/Oracle: DROP INDEX [IF EXISTS] idx
   * - MySQL/MSSQL: DROP INDEX idx ON table (table required)
   * @param {string} indexName
   * @param {{table?: string, ifExists?: boolean}} [opts]
   */
  dropIndex(indexName, opts = {}) {
    const d = this.dialect;
    const idx = String(indexName);
    const ifExists = opts.ifExists !== false;

    if (d === 'mysql' || d === 'mssql') {
      if (!opts.table) throw new Error(`${d}: dropIndex requires opts.table`);
      const qt = quoteIdent(d, opts.table);
      this._sql = `DROP INDEX ${quoteIdent(d, idx)} ON ${qt}`;
      return this;
    }

    if (d === 'pg') {
      this._sql = ifExists ? `DROP INDEX IF EXISTS ${quoteIdent(d, idx)}` : `DROP INDEX ${quoteIdent(d, idx)}`;
      return this;
    }

    if (d === 'oracle') {
      this._sql = `DROP INDEX ${quoteIdent(d, idx)}`;
      return this;
    }

    this._sql = `DROP INDEX ${quoteIdent(d, idx)}`;
    return this;
  }

  /**
   * Drop a table.
   * @param {string} name
   * @param {{ifExists?: boolean, cascade?: boolean}} [opts]
   */
  dropTable(name, opts = {}) {
    const d = this.dialect;
    const ifExists = opts.ifExists === true;
    const cascade = opts.cascade === true;

    if (d === 'mongo') {
      this._sql = null;
      this._postSql = [];
      this._meta = { op: 'dropCollection', name: String(name), opts: { ifExists } };
      return this;
    }

    if (d === 'pg') {
      this._sql = `DROP TABLE${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, name)}${cascade ? ' CASCADE' : ''}`;
      return this;
    }

    if (d === 'mysql') {
      // MySQL has no CASCADE for DROP TABLE (it drops dependent FKs automatically).
      this._sql = `DROP TABLE${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, name)}`;
      return this;
    }

    if (d === 'mssql') {
      if (ifExists) {
        const esc = String(name).replace(/'/g, "''");
        this._sql = `IF OBJECT_ID(N'${esc}', N'U') IS NOT NULL DROP TABLE ${quoteIdent(d, name)}`;
        return this;
      }
      this._sql = `DROP TABLE ${quoteIdent(d, name)}`;
      return this;
    }

    if (d === 'oracle') {
      // Oracle: best-effort. CASCADE CONSTRAINTS is common.
      this._sql = `DROP TABLE ${quoteIdent(d, name)}${cascade ? ' CASCADE CONSTRAINTS' : ''}`;
      return this;
    }

    this._sql = `DROP TABLE ${quoteIdent(d, name)}`;
    return this;
  }

  /**
   * Truncate a table or collection.
   * - SQL: TRUNCATE TABLE
   * - Mongo: deleteMany({})
   * @param {string} name
   * @param {{restartIdentity?: boolean, cascade?: boolean}} [opts]
   */
  truncateTable(name, opts = {}) {
    const d = this.dialect;
    const tableName = String(name);
    const restartIdentity = opts.restartIdentity === true;
    const cascade = opts.cascade === true;

    if (d === 'mongo') {
      this._sql = null;
      this._postSql = [];
      this._meta = { op: 'truncateCollection', name: tableName };
      return this;
    }

    if (d === 'pg') {
      this._sql = `TRUNCATE TABLE ${quoteIdent(d, tableName)}${restartIdentity ? ' RESTART IDENTITY' : ''}${cascade ? ' CASCADE' : ''}`;
      return this;
    }

    this._sql = `TRUNCATE TABLE ${quoteIdent(d, tableName)}`;
    return this;
  }

  /**
   * Update table statistics / analysis metadata.
   * @param {string} name
   * @param {{verbose?: boolean, fullscan?: boolean, schema?: string, cascade?: boolean}} [opts]
   */
  analyzeTable(name, opts = {}) {
    const d = this.dialect;
    const tableName = String(name);

    if (d === 'mongo') throw new Error('mongo: analyzeTable is not supported');

    if (d === 'pg') {
      this._sql = `ANALYZE${opts.verbose === true ? ' VERBOSE' : ''} ${quoteIdent(d, tableName)}`;
      return this;
    }

    if (d === 'mysql') {
      this._sql = `ANALYZE TABLE ${quoteIdent(d, tableName)}`;
      return this;
    }

    if (d === 'mssql') {
      this._sql = `UPDATE STATISTICS ${quoteIdent(d, tableName)}${opts.fullscan === true ? ' WITH FULLSCAN' : ''}`;
      return this;
    }

    if (d === 'oracle') {
      const { schema, table } = splitObjectName(tableName, opts.schema);
      const ownerExpr = schema ? `'${schema.replace(/'/g, "''")}'` : 'USER';
      const tableExpr = `'${table.replace(/'/g, "''")}'`;
      const cascadeExpr = opts.cascade === false ? 'FALSE' : 'TRUE';
      this._sql = `BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname => ${ownerExpr}, tabname => ${tableExpr}, cascade => ${cascadeExpr}); END;`;
      return this;
    }

    throw new Error(`${d}: analyzeTable is not supported`);
  }

  /**
   * Best-effort table maintenance / optimization.
   * - pg: VACUUM / VACUUM (ANALYZE)
   * - mysql: OPTIMIZE TABLE
   * - mssql: ALTER INDEX ALL ... REORGANIZE/REBUILD
   * @param {string} name
   * @param {{analyze?: boolean, rebuild?: boolean}} [opts]
   */
  optimizeTable(name, opts = {}) {
    const d = this.dialect;
    const tableName = String(name);

    if (d === 'pg') {
      this._sql = opts.analyze === false
        ? `VACUUM ${quoteIdent(d, tableName)}`
        : `VACUUM (ANALYZE) ${quoteIdent(d, tableName)}`;
      return this;
    }

    if (d === 'mysql') {
      this._sql = `OPTIMIZE TABLE ${quoteIdent(d, tableName)}`;
      return this;
    }

    if (d === 'mssql') {
      this._sql = `ALTER INDEX ALL ON ${quoteIdent(d, tableName)} ${opts.rebuild === true ? 'REBUILD' : 'REORGANIZE'}`;
      return this;
    }

    if (d === 'oracle') {
      throw new Error('oracle: optimizeTable is not supported; use dialect-specific DBA tools');
    }

    if (d === 'mongo') {
      throw new Error('mongo: optimizeTable is not supported');
    }

    throw new Error(`${d}: optimizeTable is not supported`);
  }

  /**
   * PostgreSQL-only VACUUM for a specific table.
   * @param {string} name
   * @param {{analyze?: boolean, full?: boolean, freeze?: boolean, verbose?: boolean}} [opts]
   */
  vacuumTable(name, opts = {}) {
    if (this.dialect !== 'pg') {
      throw new Error(`${this.dialect}: vacuumTable is only supported on PostgreSQL`);
    }
    const tableName = String(name);
    const options = buildVacuumOptions(opts);
    this._sql = options.length
      ? `VACUUM (${options.join(', ')}) ${quoteIdent('pg', tableName)}`
      : `VACUUM ${quoteIdent('pg', tableName)}`;
    return this;
  }

  /**
   * PostgreSQL-only VACUUM for the current database.
   * @param {{analyze?: boolean, full?: boolean, freeze?: boolean, verbose?: boolean}} [opts]
   */
  vacuumDatabase(opts = {}) {
    if (this.dialect !== 'pg') {
      throw new Error(`${this.dialect}: vacuumDatabase is only supported on PostgreSQL`);
    }
    const options = buildVacuumOptions(opts);
    this._sql = options.length
      ? `VACUUM (${options.join(', ')})`
      : 'VACUUM';
    return this;
  }

  /**
   * Reindex a table.
   * - PostgreSQL: REINDEX TABLE [CONCURRENTLY]
   * - SQL Server: ALTER INDEX ALL ... REBUILD
   * @param {string} name
   * @param {{concurrently?: boolean, online?: boolean, fillfactor?: number}} [opts]
   */
  reindexTable(name, opts = {}) {
    const d = this.dialect;
    const tableName = String(name);

    if (d === 'pg') {
      this._sql = `REINDEX TABLE${opts.concurrently === true ? ' CONCURRENTLY' : ''} ${quoteIdent(d, tableName)}`;
      return this;
    }

    if (d === 'mssql') {
      const withParts = [];
      if (opts.online === true) withParts.push('ONLINE = ON');
      if (Number.isInteger(opts.fillfactor) && opts.fillfactor >= 1 && opts.fillfactor <= 100) {
        withParts.push(`FILLFACTOR = ${opts.fillfactor}`);
      }
      const withClause = withParts.length ? ` WITH (${withParts.join(', ')})` : '';
      this._sql = `ALTER INDEX ALL ON ${quoteIdent(d, tableName)} REBUILD${withClause}`;
      return this;
    }

    throw new Error(`${d}: reindexTable is not supported`);
  }

  /**
   * MySQL-only REPAIR TABLE helper.
   * @param {string} name
   * @param {{quick?: boolean, extended?: boolean, useFrm?: boolean, local?: boolean}} [opts]
   */
  repairTable(name, opts = {}) {
    if (this.dialect !== 'mysql') {
      throw new Error(`${this.dialect}: repairTable is only supported on MySQL`);
    }
    const tableName = String(name);
    const parts = ['REPAIR'];
    if (opts.local === true) parts.push('LOCAL');
    parts.push('TABLE', quoteIdent('mysql', tableName));
    if (opts.quick === true) parts.push('QUICK');
    if (opts.extended === true) parts.push('EXTENDED');
    if (opts.useFrm === true) parts.push('USE_FRM');
    this._sql = parts.join(' ');
    return this;
  }

  /**
   * Rename a table (best-effort per dialect).
   * @param {string} oldName
   * @param {string} newName
   */
  renameTable(oldName, newName) {
    const d = this.dialect;
    if (d === 'pg') {
      this._sql = `ALTER TABLE ${quoteIdent(d, oldName)} RENAME TO ${quoteIdent(d, newName)}`;
      return this;
    }
    if (d === 'mysql') {
      this._sql = `RENAME TABLE ${quoteIdent(d, oldName)} TO ${quoteIdent(d, newName)}`;
      return this;
    }
    if (d === 'mssql') {
      // sp_rename expects 'schema.old', 'new'
      this._sql = `EXEC sp_rename N'${String(oldName).replace(/'/g, "''")}', N'${String(newName).replace(/'/g, "''")}'`;
      return this;
    }
    if (d === 'oracle') {
      this._sql = `ALTER TABLE ${quoteIdent(d, oldName)} RENAME TO ${quoteIdent(d, newName)}`;
      return this;
    }
    this._sql = `ALTER TABLE ${quoteIdent(d, oldName)} RENAME TO ${quoteIdent(d, newName)}`;
    return this;
  }

  /**
   * Create PostgreSQL schema.
   * @param {string} name
   */
  createSchema(name) {
    if (this.dialect !== 'pg') throw new Error(`${this.dialect}: createSchema is only supported on PostgreSQL`);
    this._sql = `CREATE SCHEMA ${quoteIdent('pg', name)}`;
    return this;
  }

  /**
   * Drop PostgreSQL schema.
   * @param {string} name
   * @param {{ifExists?: boolean, cascade?: boolean}} [opts]
   */
  dropSchema(name, opts = {}) {
    if (this.dialect !== 'pg') throw new Error(`${this.dialect}: dropSchema is only supported on PostgreSQL`);
    const ifExists = opts.ifExists === true;
    const cascade = opts.cascade === true;
    this._sql = `DROP SCHEMA${ifExists ? ' IF EXISTS' : ''} ${quoteIdent('pg', name)}${cascade ? ' CASCADE' : ''}`;
    return this;
  }

  /**
   * Create a PostgreSQL type.
   *
   * Supported definition shapes:
   * - string: "AS ENUM ('a', 'b')" or "ENUM ('a', 'b')"
   * - string[]: enum values
   * - { kind: 'enum', values: [...] }
   * - { kind: 'composite', fields: { city: 'text', zip: 'text' } }
   * - { composite: ['city text', 'zip text'] }
   *
   * @param {string} name
   * @param {any} definition
   * @param {{ifNotExists?: boolean}} [opts]
   */
  createType(name, definition, opts = {}) {
    if (this.dialect !== 'pg') throw new Error(`${this.dialect}: createType is only supported on PostgreSQL`);
    const typeName = String(name ?? '').trim();
    if (!typeName) throw new Error('createType requires a type name');

    const defSql = formatTypeDefinition(definition);
    if (!defSql) throw new Error('createType requires a valid type definition');

    const stmt = `CREATE TYPE ${quoteIdent('pg', typeName)} ${defSql}`;
    this._sql = opts.ifNotExists === true
      ? `DO $$ BEGIN ${stmt}; EXCEPTION WHEN duplicate_object THEN NULL; END $$;`
      : stmt;
    return this;
  }

  /**
   * Drop a PostgreSQL type.
   * @param {string} name
   * @param {{ifExists?: boolean, cascade?: boolean}} [opts]
   */
  dropType(name, opts = {}) {
    if (this.dialect !== 'pg') throw new Error(`${this.dialect}: dropType is only supported on PostgreSQL`);
    const typeName = String(name ?? '').trim();
    if (!typeName) throw new Error('dropType requires a type name');
    const ifExists = opts.ifExists === true;
    const cascade = opts.cascade === true;
    this._sql = `DROP TYPE${ifExists ? ' IF EXISTS' : ''} ${quoteIdent('pg', typeName)}${cascade ? ' CASCADE' : ''}`;
    return this;
  }

  /**
   * List tables/collections for the current connection.
   * @param {{schema?: string}} [opts]
   * @returns {Promise<string[]>}
   */
  async showTables(opts = {}) {
    const d = this.dialect;

    if (d === 'mongo') {
      const cursor = this.adapter?.db?.listCollections?.({}, { nameOnly: true });
      if (!cursor?.toArray) throw new Error('mongo: listCollections() is not available on this adapter');
      const rows = await cursor.toArray();
      return rowsToNames(rows, 'name').sort();
    }

    if (d === 'pg') {
      const schema = opts?.schema;
      if (schema) {
        const p = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT tablename AS name FROM pg_catalog.pg_tables WHERE schemaname = ${p} ORDER BY tablename`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT tablename AS name FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY tablename",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      const schema = opts?.schema;
      if (schema) {
        const p = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT table_name AS name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = ${p} ORDER BY table_name`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT table_name AS name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = DATABASE() ORDER BY table_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const schema = opts?.schema;
      if (schema) {
        const p = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ${p} ORDER BY TABLE_NAME`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      const schema = opts?.schema;
      if (schema) {
        const p = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT table_name AS name FROM all_tables WHERE owner = ${p} ORDER BY table_name`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        'SELECT table_name AS name FROM user_tables ORDER BY table_name',
        [],
      );
      return rowsToNames(rows, 'name');
    }

    throw new Error(`${d}: showTables not supported`);
  }

  /**
   * List databases visible to the current connection.
   * @returns {Promise<string[]>}
   */
  async showDatabases() {
    const d = this.dialect;

    if (d === 'mongo') {
      const admin = this.adapter?.db?.admin?.();
      if (!admin?.listDatabases) throw new Error('mongo: admin().listDatabases() is not available on this adapter');
      const out = await admin.listDatabases();
      const names = rowsToNames(out?.databases ?? [], 'name');
      return names.sort();
    }

    if (d === 'pg') {
      const rows = await this._runSelect(
        "SELECT datname AS name FROM pg_database WHERE datistemplate = false ORDER BY datname",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      const rows = await this._runSelect(
        'SELECT schema_name AS name FROM information_schema.schemata ORDER BY schema_name',
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const rows = await this._runSelect(
        'SELECT name FROM sys.databases ORDER BY name',
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      try {
        const rows = await this._runSelect('SELECT name FROM v$database', []);
        return rowsToNames(rows, 'name');
      } catch {
        const rows = await this._runSelect('SELECT username AS name FROM all_users ORDER BY username', []);
        return rowsToNames(rows, 'name');
      }
    }

    throw new Error(`${d}: showDatabases not supported`);
  }

  /**
   * List schemas / namespaces visible to the current connection.
   * MySQL maps schemas to databases.
   *
   * @param {{includeSystem?: boolean}} [opts]
   * @returns {Promise<string[]>}
   */
  async showSchemas(opts = {}) {
    const d = this.dialect;
    const includeSystem = opts?.includeSystem === true;

    if (d === 'mongo') {
      throw new Error('mongo: showSchemas is not supported');
    }

    if (d === 'pg') {
      const rows = await this._runSelect(
        includeSystem
          ? 'SELECT schema_name AS name FROM information_schema.schemata ORDER BY schema_name'
          : "SELECT schema_name AS name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema') ORDER BY schema_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      const rows = await this._runSelect(
        'SELECT schema_name AS name FROM information_schema.schemata ORDER BY schema_name',
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const rows = await this._runSelect(
        includeSystem
          ? 'SELECT name FROM sys.schemas ORDER BY name'
          : "SELECT name FROM sys.schemas WHERE name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys') ORDER BY name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      const rows = await this._runSelect(
        includeSystem
          ? 'SELECT username AS name FROM all_users ORDER BY username'
          : "SELECT username AS name FROM all_users WHERE username NOT IN ('SYS', 'SYSTEM') ORDER BY username",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    throw new Error(`${d}: showSchemas not supported`);
  }

  /**
   * List sequences visible to the current connection.
   *
   * @param {{schema?: string}} [opts]
   * @returns {Promise<string[]>}
   */
  async showSequences(opts = {}) {
    const d = this.dialect;

    if (d === 'mongo') {
      throw new Error('mongo: showSequences is not supported');
    }

    if (d === 'pg') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT sequence_name AS name FROM information_schema.sequences WHERE sequence_schema = ${p1} ORDER BY sequence_name`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT sequence_name AS name FROM information_schema.sequences WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY sequence_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const schema = opts?.schema;
      const params = [];
      const where = [];
      if (schema) {
        params.push(schema);
        where.push(`s.name = ${this.adapter.placeholder(params.length)}`);
      }
      const rows = await this._runSelect(
        `SELECT seq.name AS name FROM sys.sequences seq JOIN sys.schemas s ON s.schema_id = seq.schema_id${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY seq.name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT sequence_name AS name FROM all_sequences WHERE sequence_owner = ${p1} ORDER BY sequence_name`,
          [String(schema).toUpperCase()],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        'SELECT sequence_name AS name FROM user_sequences ORDER BY sequence_name',
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      throw new Error('mysql: showSequences is not supported');
    }

    throw new Error(`${d}: showSequences not supported`);
  }

  /**
   * List SQL views / Mongo view collections visible to the current connection.
   * @param {{schema?: string}} [opts]
   * @returns {Promise<string[]>}
   */
  async showViews(opts = {}) {
    return this._listViews(opts);
  }

  /**
   * List triggers visible to the current connection.
   * Use opts.table to scope results to a table when needed.
   *
   * @param {{schema?: string, table?: string}} [opts]
   * @returns {Promise<string[]>}
   */
  async showTriggers(opts = {}) {
    const d = this.dialect;

    if (d === 'mongo') {
      throw new Error('mongo: showTriggers is not supported');
    }

    const scope = normalizeScopedObjectFilter(opts?.table, opts?.schema);
    const schema = scope.schema;
    const table = scope.name;

    if (d === 'pg') {
      const where = [];
      const params = [];
      if (schema) {
        params.push(schema);
        where.push(`trigger_schema = ${this.adapter.placeholder(params.length)}`);
      } else {
        where.push("trigger_schema NOT IN ('pg_catalog', 'information_schema')");
      }
      if (table) {
        params.push(table);
        where.push(`event_object_table = ${this.adapter.placeholder(params.length)}`);
      }
      const rows = await this._runSelect(
        `SELECT DISTINCT trigger_name AS name FROM information_schema.triggers${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY trigger_name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      const where = [];
      const params = [];
      if (schema) {
        params.push(schema);
        where.push(`trigger_schema = ${this.adapter.placeholder(params.length)}`);
      } else {
        where.push('trigger_schema = DATABASE()');
      }
      if (table) {
        params.push(table);
        where.push(`event_object_table = ${this.adapter.placeholder(params.length)}`);
      }
      const rows = await this._runSelect(
        `SELECT trigger_name AS name FROM information_schema.triggers${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY trigger_name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const where = ['t.parent_class_desc = \'OBJECT_OR_COLUMN\''];
      const params = [];
      if (schema) {
        params.push(schema);
        where.push(`s.name = ${this.adapter.placeholder(params.length)}`);
      }
      if (table) {
        params.push(table);
        where.push(`tb.name = ${this.adapter.placeholder(params.length)}`);
      }
      const rows = await this._runSelect(
        `SELECT t.name AS name FROM sys.triggers t JOIN sys.tables tb ON tb.object_id = t.parent_id JOIN sys.schemas s ON s.schema_id = tb.schema_id WHERE ${where.join(' AND ')} ORDER BY t.name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      const upperSchema = schema ? String(schema).toUpperCase() : null;
      const upperTable = table ? String(table).toUpperCase() : null;
      const where = [];
      const params = [];
      if (upperSchema) {
        params.push(upperSchema);
        where.push(`owner = ${this.adapter.placeholder(params.length)}`);
      }
      if (upperTable) {
        params.push(upperTable);
        where.push(`table_name = ${this.adapter.placeholder(params.length)}`);
      }
      const source = upperSchema ? 'all_triggers' : 'user_triggers';
      const rows = await this._runSelect(
        `SELECT trigger_name AS name FROM ${source}${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY trigger_name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    throw new Error(`${d}: showTriggers not supported`);
  }

  /**
   * List indexes for a table / collection.
   * @param {string} name
   * @param {{schema?: string}} [opts]
   * @returns {Promise<string[]>}
   */
  async showIndexes(name, opts = {}) {
    const d = this.dialect;
    const scope = normalizeScopedObjectFilter(name, opts?.schema);
    const tableName = scope.name;
    const schema = scope.schema;
    if (!tableName) throw new Error('showIndexes(name) requires a table/collection name');

    if (d === 'mongo') {
      const col = this.adapter?.db?.collection?.(tableName);
      if (!col?.listIndexes) throw new Error('mongo: collection().listIndexes() is not available on this adapter');
      const rows = await col.listIndexes().toArray();
      return rowsToNames(rows, 'name').sort();
    }

    if (d === 'pg') {
      const params = [];
      const where = [];
      if (schema) {
        params.push(schema);
        where.push(`schemaname = ${this.adapter.placeholder(params.length)}`);
      } else {
        where.push("schemaname NOT IN ('pg_catalog', 'information_schema')");
      }
      params.push(tableName);
      where.push(`tablename = ${this.adapter.placeholder(params.length)}`);
      const rows = await this._runSelect(
        `SELECT indexname AS name FROM pg_indexes WHERE ${where.join(' AND ')} ORDER BY indexname`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      const params = [];
      const where = [];
      if (schema) {
        params.push(schema);
        where.push(`table_schema = ${this.adapter.placeholder(params.length)}`);
      } else {
        where.push('table_schema = DATABASE()');
      }
      params.push(tableName);
      where.push(`table_name = ${this.adapter.placeholder(params.length)}`);
      const rows = await this._runSelect(
        `SELECT DISTINCT index_name AS name FROM information_schema.statistics WHERE ${where.join(' AND ')} ORDER BY index_name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const params = [];
      const where = ['i.name IS NOT NULL', 'i.is_hypothetical = 0'];
      params.push(tableName);
      where.push(`tb.name = ${this.adapter.placeholder(params.length)}`);
      if (schema) {
        params.push(schema);
        where.push(`s.name = ${this.adapter.placeholder(params.length)}`);
      }
      const rows = await this._runSelect(
        `SELECT i.name AS name FROM sys.indexes i JOIN sys.tables tb ON tb.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = tb.schema_id WHERE ${where.join(' AND ')} ORDER BY i.name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      const upperSchema = schema ? String(schema).toUpperCase() : null;
      const upperTable = String(tableName).toUpperCase();
      const params = [];
      const where = [];
      if (upperSchema) {
        params.push(upperSchema);
        where.push(`owner = ${this.adapter.placeholder(params.length)}`);
      }
      params.push(upperTable);
      where.push(`table_name = ${this.adapter.placeholder(params.length)}`);
      const source = upperSchema ? 'all_indexes' : 'user_indexes';
      const rows = await this._runSelect(
        `SELECT index_name AS name FROM ${source} WHERE ${where.join(' AND ')} ORDER BY index_name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    throw new Error(`${d}: showIndexes not supported`);
  }

  /**
   * List table constraints visible to the current connection.
   * Use opts.table / opts.schema to scope results.
   *
   * @param {{schema?: string, table?: string, type?: string}} [opts]
   * @returns {Promise<string[]>}
   */
  async showConstraints(opts = {}) {
    const d = this.dialect;

    if (d === 'mongo') {
      throw new Error('mongo: showConstraints is not supported');
    }

    const scope = normalizeScopedObjectFilter(opts?.table, opts?.schema);
    const schema = scope.schema;
    const table = scope.name;
    const type = normalizeConstraintTypeFilter(d, opts?.type);

    if (d === 'pg') {
      const where = [];
      const params = [];
      if (schema) {
        params.push(schema);
        where.push(`table_schema = ${this.adapter.placeholder(params.length)}`);
      } else {
        where.push("table_schema NOT IN ('pg_catalog', 'information_schema')");
      }
      if (table) {
        params.push(table);
        where.push(`table_name = ${this.adapter.placeholder(params.length)}`);
      }
      if (type) {
        params.push(type);
        where.push(`constraint_type = ${this.adapter.placeholder(params.length)}`);
      }
      const rows = await this._runSelect(
        `SELECT constraint_name AS name FROM information_schema.table_constraints${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY constraint_name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      const where = [];
      const params = [];
      if (schema) {
        params.push(schema);
        where.push(`table_schema = ${this.adapter.placeholder(params.length)}`);
      } else {
        where.push('table_schema = DATABASE()');
      }
      if (table) {
        params.push(table);
        where.push(`table_name = ${this.adapter.placeholder(params.length)}`);
      }
      if (type) {
        params.push(type);
        where.push(`constraint_type = ${this.adapter.placeholder(params.length)}`);
      }
      const rows = await this._runSelect(
        `SELECT constraint_name AS name FROM information_schema.table_constraints${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY constraint_name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const where = [];
      const params = [];
      if (schema) {
        params.push(schema);
        where.push(`TABLE_SCHEMA = ${this.adapter.placeholder(params.length)}`);
      }
      if (table) {
        params.push(table);
        where.push(`TABLE_NAME = ${this.adapter.placeholder(params.length)}`);
      }
      if (type) {
        params.push(type);
        where.push(`CONSTRAINT_TYPE = ${this.adapter.placeholder(params.length)}`);
      }
      const rows = await this._runSelect(
        `SELECT CONSTRAINT_NAME AS name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY CONSTRAINT_NAME`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      const upperSchema = schema ? String(schema).toUpperCase() : null;
      const upperTable = table ? String(table).toUpperCase() : null;
      const where = [];
      const params = [];
      if (upperSchema) {
        params.push(upperSchema);
        where.push(`owner = ${this.adapter.placeholder(params.length)}`);
      }
      if (upperTable) {
        params.push(upperTable);
        where.push(`table_name = ${this.adapter.placeholder(params.length)}`);
      }
      if (type) {
        params.push(type);
        where.push(`constraint_type = ${this.adapter.placeholder(params.length)}`);
      }
      const source = upperSchema ? 'all_constraints' : 'user_constraints';
      const rows = await this._runSelect(
        `SELECT constraint_name AS name FROM ${source}${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY constraint_name`,
        params,
      );
      return rowsToNames(rows, 'name');
    }

    throw new Error(`${d}: showConstraints not supported`);
  }

  /**
   * List functions visible to the current connection.
   * @param {{schema?: string}} [opts]
   * @returns {Promise<string[]>}
   */
  async showFunctions(opts = {}) {
    const d = this.dialect;

    if (d === 'mongo') {
      throw new Error('mongo: showFunctions is not supported');
    }

    if (d === 'pg') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT routine_name AS name FROM information_schema.routines WHERE routine_schema = ${p1} AND routine_type = 'FUNCTION' ORDER BY routine_name`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT routine_name AS name FROM information_schema.routines WHERE routine_type = 'FUNCTION' AND routine_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY routine_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT routine_name AS name FROM information_schema.routines WHERE routine_schema = ${p1} AND routine_type = 'FUNCTION' ORDER BY routine_name`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT routine_name AS name FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_type = 'FUNCTION' ORDER BY routine_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT ROUTINE_NAME AS name FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = ${p1} AND ROUTINE_TYPE = 'FUNCTION' ORDER BY ROUTINE_NAME`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT ROUTINE_NAME AS name FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION' ORDER BY ROUTINE_NAME",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT object_name AS name FROM all_procedures WHERE owner = ${p1} AND object_type = 'FUNCTION' AND procedure_name IS NULL ORDER BY object_name`,
          [String(schema).toUpperCase()],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT object_name AS name FROM user_procedures WHERE object_type = 'FUNCTION' AND procedure_name IS NULL ORDER BY object_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    throw new Error(`${d}: showFunctions not supported`);
  }

  /**
   * List procedures visible to the current connection.
   * @param {{schema?: string}} [opts]
   * @returns {Promise<string[]>}
   */
  async showProcedures(opts = {}) {
    const d = this.dialect;

    if (d === 'mongo') {
      throw new Error('mongo: showProcedures is not supported');
    }

    if (d === 'pg') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT routine_name AS name FROM information_schema.routines WHERE routine_schema = ${p1} AND routine_type = 'PROCEDURE' ORDER BY routine_name`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT routine_name AS name FROM information_schema.routines WHERE routine_type = 'PROCEDURE' AND routine_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY routine_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT routine_name AS name FROM information_schema.routines WHERE routine_schema = ${p1} AND routine_type = 'PROCEDURE' ORDER BY routine_name`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT routine_name AS name FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_type = 'PROCEDURE' ORDER BY routine_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT ROUTINE_NAME AS name FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = ${p1} AND ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_NAME`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT ROUTINE_NAME AS name FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_NAME",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT object_name AS name FROM all_procedures WHERE owner = ${p1} AND object_type = 'PROCEDURE' AND procedure_name IS NULL ORDER BY object_name`,
          [String(schema).toUpperCase()],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT object_name AS name FROM user_procedures WHERE object_type = 'PROCEDURE' AND procedure_name IS NULL ORDER BY object_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    throw new Error(`${d}: showProcedures not supported`);
  }

  /**
   * List PostgreSQL aggregate objects visible to the current connection.
   * @param {{schema?: string}} [opts]
   * @returns {Promise<string[]>}
   */
  async showAggregates(opts = {}) {
    const d = this.dialect;
    if (d !== 'pg') throw new Error(`${d}: showAggregates is only supported on PostgreSQL`);

    const schema = opts?.schema;
    if (schema) {
      const p1 = this.adapter.placeholder(1);
      const rows = await this._runSelect(
        `SELECT p.proname AS name FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_aggregate a ON a.aggfnoid = p.oid WHERE n.nspname = ${p1} ORDER BY p.proname`,
        [schema],
      );
      return rowsToNames(rows, 'name');
    }

    const rows = await this._runSelect(
      "SELECT p.proname AS name FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_aggregate a ON a.aggfnoid = p.oid WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') ORDER BY p.proname",
      [],
    );
    return rowsToNames(rows, 'name');
  }

  /**
   * Describe table/collection or database structure.
   *
   * Usage:
   * - getDesc() -> current database structure
   * - getDesc('database')
   * - getDesc('table', 'users')
   * - getDesc('users') // shorthand for table/collection
   * - getDesc({ kind: 'table', name: 'users', schema: 'public' })
   *
   * @param {any} [target='database']
   * @param {any} [opts={}]
   * @returns {Promise<any>}
   */
  async getDesc(target = 'database', opts = {}) {
    const parsed = normalizeGetDescArgs(target, opts);
    if (parsed.kind === 'database') {
      const out = await this._describeDatabase(parsed.opts);
      return parsed.opts?.strict === true ? toStrictDatabaseShape(out) : out;
    }
    const out = await this._describeTable(parsed.name, parsed.opts);
    return parsed.opts?.strict === true ? toStrictTableShape(out) : out;
  }

  async _describeDatabase(opts = {}) {
    const d = this.dialect;
    const includeCreateSql = opts?.includeCreateSql === true;
    const includeIndexes = opts?.includeIndexes === true;
    const includeTriggers = opts?.includeTriggers === true;

    if (d === 'mongo') {
      const name = this.adapter?.db?.databaseName ?? null;
      const collections = await this.showTables();
      const out = {
        kind: 'database',
        dialect: d,
        name,
        collections,
      };

      if (opts.includeDatabases === true) {
        out.databases = await this.showDatabases();
      }

      if (opts.deep === true || includeIndexes || includeTriggers) {
        const collectionDescriptions = {};
        for (const c of collections) {
          collectionDescriptions[c] = await this._describeTable(c, { ...opts, strict: false, includeCreateSql });
        }
        out.collectionDescriptions = collectionDescriptions;
      }
      if (includeCreateSql) {
        out.createSql = buildCreateDatabaseScript(out);
      }
      return out;
    }

    const meta = await this._getCurrentDatabaseMeta();
    const schema = opts?.schema ?? meta.schema ?? undefined;
    const tables = await this.showTables(schema ? { schema } : {});
    const views = (opts?.includeViews === false) ? [] : await this._listViews({ schema });

    const out = {
      kind: 'database',
      dialect: d,
      name: meta.name ?? null,
      schema: schema ?? null,
      tables,
      views,
    };

    if (opts.deep === true || includeIndexes || includeTriggers) {
      const tableDescriptions = {};
      for (const t of tables) {
        tableDescriptions[t] = await this._describeTable(t, { ...opts, schema, strict: false, includeCreateSql });
      }
      out.tableDescriptions = tableDescriptions;
    }

    if (includeCreateSql) {
      /** @type {Record<string, any>|null} */
      let tableDescriptionsForScript = out.tableDescriptions ?? null;
      if (!tableDescriptionsForScript) {
        tableDescriptionsForScript = {};
        for (const t of tables) {
          tableDescriptionsForScript[t] = await this._describeTable(t, { ...opts, schema, strict: false, includeCreateSql: true });
        }
      }
      out.createSql = buildCreateDatabaseScript(out, {
        tableDescriptions: tableDescriptionsForScript,
      });
    }

    return out;
  }

  async _describeTable(name, opts = {}) {
    const d = this.dialect;
    const tableName = String(name ?? '').trim();
    if (!tableName) throw new Error('getDesc("table", name) requires a table/collection name');
    const includeIndexes = opts?.includeIndexes === true;
    const includeTriggers = opts?.includeTriggers === true;

    if (d === 'mongo') {
      return this._describeMongoCollection(tableName, opts);
    }

    if (d === 'pg') {
      const schema = String(opts?.schema ?? 'public');
      const p1 = this.adapter.placeholder(1);
      const p2 = this.adapter.placeholder(2);
      const rows = await this._runSelect(
        `SELECT column_name, data_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, ordinal_position FROM information_schema.columns WHERE table_schema = ${p1} AND table_name = ${p2} ORDER BY ordinal_position`,
        [schema, tableName],
      );
      const out = {
        kind: 'table',
        dialect: d,
        name: tableName,
        schema,
        columns: normalizeColumnDescriptions(rows),
      };
      if (includeIndexes) out.indexes = await this.showIndexes(`${schema}.${tableName}`);
      if (includeTriggers) out.triggers = await this.showTriggers({ schema, table: tableName });
      if (opts?.includeCreateSql === true) {
        out.createSql = await this._buildPgCreateTableSql(schema, tableName);
      }
      return out;
    }

    if (d === 'mysql') {
      const schema = opts?.schema ? String(opts.schema) : null;
      let rows;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const p2 = this.adapter.placeholder(2);
        rows = await this._runSelect(
          `SELECT column_name, column_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, ordinal_position, extra FROM information_schema.columns WHERE table_schema = ${p1} AND table_name = ${p2} ORDER BY ordinal_position`,
          [schema, tableName],
        );
      } else {
        const p1 = this.adapter.placeholder(1);
        rows = await this._runSelect(
          `SELECT column_name, column_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, ordinal_position, extra FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ${p1} ORDER BY ordinal_position`,
          [tableName],
        );
      }

      const out = {
        kind: 'table',
        dialect: d,
        name: tableName,
        schema,
        columns: normalizeColumnDescriptions(rows),
      };
      if (includeIndexes) out.indexes = await this.showIndexes(schema ? `${schema}.${tableName}` : tableName);
      if (includeTriggers) out.triggers = await this.showTriggers({ schema, table: tableName });
      if (opts?.includeCreateSql === true) out.createSql = buildCreateTableScript(out);
      return out;
    }

    if (d === 'mssql') {
      const schema = String(opts?.schema ?? 'dbo');
      const p1 = this.adapter.placeholder(1);
      const p2 = this.adapter.placeholder(2);
      const rows = await this._runSelect(
        `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ${p1} AND TABLE_NAME = ${p2} ORDER BY ORDINAL_POSITION`,
        [schema, tableName],
      );
      const out = {
        kind: 'table',
        dialect: d,
        name: tableName,
        schema,
        columns: normalizeColumnDescriptions(rows),
      };
      if (includeIndexes) out.indexes = await this.showIndexes(`${schema}.${tableName}`);
      if (includeTriggers) out.triggers = await this.showTriggers({ schema, table: tableName });
      if (opts?.includeCreateSql === true) out.createSql = buildCreateTableScript(out);
      return out;
    }

    if (d === 'oracle') {
      const schema = opts?.schema ? String(opts.schema).toUpperCase() : null;
      const upperName = tableName.toUpperCase();
      let rows;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const p2 = this.adapter.placeholder(2);
        rows = await this._runSelect(
          `SELECT column_name, data_type, nullable, data_default, data_length, data_precision, data_scale, column_id FROM all_tab_columns WHERE owner = ${p1} AND table_name = ${p2} ORDER BY column_id`,
          [schema, upperName],
        );
      } else {
        const p1 = this.adapter.placeholder(1);
        rows = await this._runSelect(
          `SELECT column_name, data_type, nullable, data_default, data_length, data_precision, data_scale, column_id FROM user_tab_columns WHERE table_name = ${p1} ORDER BY column_id`,
          [upperName],
        );
      }

      const out = {
        kind: 'table',
        dialect: d,
        name: tableName,
        schema,
        columns: normalizeColumnDescriptions(rows),
      };
      if (includeIndexes) out.indexes = await this.showIndexes(schema ? `${schema}.${tableName}` : tableName);
      if (includeTriggers) out.triggers = await this.showTriggers({ schema, table: tableName });
      if (opts?.includeCreateSql === true) out.createSql = buildCreateTableScript(out);
      return out;
    }

    throw new Error(`${d}: getDesc(table) not supported`);
  }

  async _describeMongoCollection(collectionName, opts = {}) {
    const col = this.adapter?.db?.collection?.(collectionName);
    if (!col?.find) {
      throw new Error('mongo: collection().find() is not available on this adapter');
    }

    const sampleSize = normalizePositiveInt(opts?.sampleSize, 20, 200);
    let cursor = col.find({});
    if (cursor?.limit) cursor = cursor.limit(sampleSize);
    const docs = cursor?.toArray ? await cursor.toArray() : [];

    const out = {
      kind: 'table',
      dialect: this.dialect,
      name: collectionName,
      schema: null,
      sampledDocuments: docs.length,
      sampleSize,
      columns: inferMongoColumns(docs),
    };
    if (opts?.includeIndexes === true) out.indexes = await this.showIndexes(collectionName);
    if (opts?.includeTriggers === true) out.triggers = [];
    if (opts?.includeCreateSql === true) out.createSql = buildCreateTableScript(out);
    return out;
  }

  async _buildPgCreateTableSql(schema, tableName) {
    const p1 = this.adapter.placeholder(1);
    const p2 = this.adapter.placeholder(2);

    const cols = await this._runSelect(
      `SELECT a.attname AS column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type, a.attnotnull AS is_not_null, pg_get_expr(ad.adbin, ad.adrelid) AS column_default, a.attidentity AS identity_kind, a.attnum AS ordinal_position FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum WHERE n.nspname = ${p1} AND c.relname = ${p2} AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`,
      [schema, tableName],
    );

    const constraints = await this._runSelect(
      `SELECT con.conname AS constraint_name, pg_get_constraintdef(con.oid, true) AS constraint_def FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid = con.conrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ${p1} AND c.relname = ${p2} AND con.contype IN ('p') ORDER BY con.conname`,
      [schema, tableName],
    );

    const qTable = `${quoteIdent('pg', schema)}.${quoteIdent('pg', tableName)}`;
    const lines = [];

    for (const r of cols ?? []) {
      const name = pickRowField(r, ['column_name']);
      if (!name) continue;
      const colType = String(pickRowField(r, ['column_type']) ?? 'text');
      const isNotNull = parseBooleanLike(pickRowField(r, ['is_not_null'])) === true;
      const columnDefault = pickRowField(r, ['column_default']);
      const identityKind = String(pickRowField(r, ['identity_kind']) ?? '');

      const parts = [`${quoteIdent('pg', String(name))} ${colType}`];
      if (identityKind === 'a') parts.push('GENERATED ALWAYS AS IDENTITY');
      else if (identityKind === 'd') parts.push('GENERATED BY DEFAULT AS IDENTITY');
      else if (columnDefault != null) parts.push(`DEFAULT ${String(columnDefault)}`);
      if (isNotNull) parts.push('NOT NULL');
      lines.push(parts.join(' '));
    }

    for (const c of constraints ?? []) {
      const cname = pickRowField(c, ['constraint_name']);
      const cdef = pickRowField(c, ['constraint_def']);
      if (!cname || !cdef) continue;
      lines.push(`CONSTRAINT ${quoteIdent('pg', String(cname))} ${String(cdef)}`);
    }

    if (!lines.length) return `CREATE TABLE IF NOT EXISTS ${qTable} ();`;
    return `CREATE TABLE IF NOT EXISTS ${qTable}\n(\n\t${lines.join(',\n\t')}\n);`;
  }

  async _getCurrentDatabaseMeta() {
    const d = this.dialect;
    if (d === 'pg') {
      const rows = await this._runSelect('SELECT current_database() AS name, current_schema() AS schema', []);
      return {
        name: pickRowField(rows[0], ['name']),
        schema: pickRowField(rows[0], ['schema']),
      };
    }
    if (d === 'mysql') {
      const rows = await this._runSelect('SELECT DATABASE() AS name', []);
      return { name: pickRowField(rows[0], ['name']), schema: null };
    }
    if (d === 'mssql') {
      const rows = await this._runSelect('SELECT DB_NAME() AS name, SCHEMA_NAME() AS schema_name', []);
      return {
        name: pickRowField(rows[0], ['name']),
        schema: pickRowField(rows[0], ['schema_name']),
      };
    }
    if (d === 'oracle') {
      try {
        const rows = await this._runSelect("SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS name, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS schema_name FROM dual", []);
        return {
          name: pickRowField(rows[0], ['name']),
          schema: pickRowField(rows[0], ['schema_name']),
        };
      } catch {
        const rows = await this._runSelect('SELECT USER AS schema_name FROM dual', []);
        return {
          name: null,
          schema: pickRowField(rows[0], ['schema_name']),
        };
      }
    }
    return { name: null, schema: null };
  }

  async _listViews(opts = {}) {
    const d = this.dialect;
    if (d === 'mongo') {
      const cursor = this.adapter?.db?.listCollections?.({ type: 'view' }, { nameOnly: true });
      if (!cursor?.toArray) return [];
      const rows = await cursor.toArray();
      return rowsToNames(rows, 'name').sort();
    }

    if (d === 'pg') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT table_name AS name FROM information_schema.views WHERE table_schema = ${p1} ORDER BY table_name`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        "SELECT table_name AS name FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_name",
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mysql') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT table_name AS name FROM information_schema.views WHERE table_schema = ${p1} ORDER BY table_name`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        'SELECT table_name AS name FROM information_schema.views WHERE table_schema = DATABASE() ORDER BY table_name',
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'mssql') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT TABLE_NAME AS name FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = ${p1} ORDER BY TABLE_NAME`,
          [schema],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        'SELECT TABLE_NAME AS name FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_NAME',
        [],
      );
      return rowsToNames(rows, 'name');
    }

    if (d === 'oracle') {
      const schema = opts?.schema;
      if (schema) {
        const p1 = this.adapter.placeholder(1);
        const rows = await this._runSelect(
          `SELECT view_name AS name FROM all_views WHERE owner = ${p1} ORDER BY view_name`,
          [String(schema).toUpperCase()],
        );
        return rowsToNames(rows, 'name');
      }
      const rows = await this._runSelect(
        'SELECT view_name AS name FROM user_views ORDER BY view_name',
        [],
      );
      return rowsToNames(rows, 'name');
    }

    return [];
  }

  /**
   * Create a SQL view.
   * @param {string} name
   * @param {string|import('./Raw.js').Raw|import('./QueryBuilder.js').QueryBuilder} source
   * @param {{orReplace?: boolean, ifNotExists?: boolean, temporary?: boolean}} [opts]
   */
  createView(name, source, opts = {}) {
    if (this.dialect === 'mongo') throw new Error('Schema builder is SQL-only');
    const d = this.dialect;
    const qn = quoteIdent(d, name);
    const src = this._viewSourceSql(source);

    const orReplace = opts.orReplace === true;
    const ifNotExists = opts.ifNotExists === true;
    const temporary = opts.temporary === true;

    if (d === 'pg') {
      if (orReplace && ifNotExists) {
        throw new Error('pg: createView does not support combining orReplace with ifNotExists');
      }
      this._sql = `CREATE${orReplace ? ' OR REPLACE' : ''}${temporary ? ' TEMP' : ''} VIEW${ifNotExists ? ' IF NOT EXISTS' : ''} ${qn} AS ${src}`;
      return this;
    }

    if (d === 'mysql') {
      if (ifNotExists) throw new Error('mysql: createView does not support ifNotExists');
      if (temporary) throw new Error('mysql: temporary views are not supported');
      this._sql = `CREATE${orReplace ? ' OR REPLACE' : ''} VIEW ${qn} AS ${src}`;
      return this;
    }

    if (d === 'mssql') {
      if (temporary) throw new Error('mssql: temporary views are not supported');
      if (ifNotExists) {
        const esc = String(name).replace(/'/g, "''");
        this._sql = `IF OBJECT_ID(N'${esc}', N'V') IS NULL CREATE VIEW ${qn} AS ${src}`;
        return this;
      }
      this._sql = `${orReplace ? 'CREATE OR ALTER' : 'CREATE'} VIEW ${qn} AS ${src}`;
      return this;
    }

    if (d === 'oracle') {
      if (ifNotExists) throw new Error('oracle: createView does not support ifNotExists');
      if (temporary) throw new Error('oracle: temporary views are not supported');
      this._sql = `CREATE${orReplace ? ' OR REPLACE' : ''} VIEW ${qn} AS ${src}`;
      return this;
    }

    this._sql = `CREATE VIEW ${qn} AS ${src}`;
    return this;
  }

  /**
   * Create a SQL function (best-effort, dialect-specific).
   *
   * The function body is passed through as a dialect-specific routine body.
   * Keep the body syntax valid for the target database.
   *
   * @param {{
   *   name: string,
   *   args?: string|string[],
   *   returns: string,
   *   body: string,
   *   language?: string,
   *   orReplace?: boolean,
   *   deterministic?: boolean
   * }} def
   */
  createFunction(def = {}) {
    if (this.dialect === 'mongo') throw new Error('Schema builder is SQL-only');

    const d = this.dialect;
    const name = String(def.name ?? '').trim();
    const returns = String(def.returns ?? '').trim();
    const body = String(def.body ?? '').trim();
    const args = formatRoutineArgList(def.args);
    const orReplace = def.orReplace === true;
    const deterministic = def.deterministic === true;

    if (!name || !returns || !body) {
      throw new Error('createFunction requires { name, returns, body }');
    }

    if (d === 'pg') {
      const language = String(def.language ?? 'plpgsql').trim();
      if (!language) throw new Error('pg: createFunction requires a non-empty language');
      this._sql = `CREATE${orReplace ? ' OR REPLACE' : ''} FUNCTION ${quoteIdent(d, name)}(${args}) RETURNS ${returns} AS $$\n${body}\n$$ LANGUAGE ${language}`;
      return this;
    }

    if (d === 'mysql') {
      if (orReplace) throw new Error('mysql: createFunction does not support orReplace; drop the function first');
      const attrs = [];
      if (deterministic) attrs.push('DETERMINISTIC');
      const attrSql = attrs.length ? ` ${attrs.join(' ')}` : '';
      this._sql = `CREATE FUNCTION ${quoteIdent(d, name)}(${args}) RETURNS ${returns}${attrSql}\n${body}`;
      return this;
    }

    if (d === 'mssql') {
      const keyword = orReplace ? 'CREATE OR ALTER' : 'CREATE';
      this._sql = `${keyword} FUNCTION ${quoteIdent(d, name)}(${args}) RETURNS ${returns} AS\n${body}`;
      return this;
    }

    if (d === 'oracle') {
      this._sql = `CREATE${orReplace ? ' OR REPLACE' : ''} FUNCTION ${quoteIdent(d, name)}(${args}) RETURN ${returns}\n${body}`;
      return this;
    }

    throw new Error(`${d}: createFunction not supported`);
  }

  /**
   * Create a SQL procedure (best-effort, dialect-specific).
   *
   * The procedure body is passed through as a dialect-specific routine body.
   * Keep the body syntax valid for the target database.
   *
   * @param {{
   *   name: string,
   *   args?: string|string[],
   *   body: string,
   *   language?: string,
   *   orReplace?: boolean
   * }} def
   */
  createProcedure(def = {}) {
    if (this.dialect === 'mongo') throw new Error('Schema builder is SQL-only');

    const d = this.dialect;
    const name = String(def.name ?? '').trim();
    const body = String(def.body ?? '').trim();
    const args = formatRoutineArgList(def.args);
    const orReplace = def.orReplace === true;

    if (!name || !body) {
      throw new Error('createProcedure requires { name, body }');
    }

    if (d === 'pg') {
      const language = String(def.language ?? 'plpgsql').trim();
      if (!language) throw new Error('pg: createProcedure requires a non-empty language');
      this._sql = `CREATE${orReplace ? ' OR REPLACE' : ''} PROCEDURE ${quoteIdent(d, name)}(${args}) LANGUAGE ${language} AS $$\n${body}\n$$`;
      return this;
    }

    if (d === 'mysql') {
      if (orReplace) throw new Error('mysql: createProcedure does not support orReplace; drop the procedure first');
      this._sql = `CREATE PROCEDURE ${quoteIdent(d, name)}(${args})\n${body}`;
      return this;
    }

    if (d === 'mssql') {
      const keyword = orReplace ? 'CREATE OR ALTER' : 'CREATE';
      const argSql = args ? ` ${args}` : '';
      this._sql = `${keyword} PROCEDURE ${quoteIdent(d, name)}${argSql} AS\n${body}`;
      return this;
    }

    if (d === 'oracle') {
      this._sql = `CREATE${orReplace ? ' OR REPLACE' : ''} PROCEDURE ${quoteIdent(d, name)}(${args})\n${body}`;
      return this;
    }

    throw new Error(`${d}: createProcedure not supported`);
  }

  /**
   * Create a PostgreSQL aggregate object.
   *
   * This is intentionally PostgreSQL-only because user-defined aggregate
   * objects do not have a portable cross-dialect shape.
   *
   * @param {{
   *   name: string,
   *   args?: string|string[],
   *   definition: string|Record<string, any>
   * }} def
   */
  createAggregate(def = {}) {
    const d = this.dialect;
    if (d !== 'pg') throw new Error(`${d}: createAggregate is only supported on PostgreSQL`);

    const name = String(def.name ?? '').trim();
    const args = formatRoutineArgList(def.args);
    const definition = formatAggregateDefinition(def.definition);

    if (!name || !definition) {
      throw new Error('createAggregate requires { name, definition }');
    }

    this._sql = `CREATE AGGREGATE ${quoteIdent(d, name)}(${args}) (\n  ${definition}\n)`;
    return this;
  }

  /**
   * Drop a SQL view.
   * @param {string} name
   * @param {{ifExists?: boolean, cascade?: boolean}} [opts]
   */
  dropView(name, opts = {}) {
    if (this.dialect === 'mongo') throw new Error('Schema builder is SQL-only');
    const d = this.dialect;
    const qn = quoteIdent(d, name);
    const ifExists = opts.ifExists === true;
    const cascade = opts.cascade === true;

    if (d === 'pg') {
      this._sql = `DROP VIEW${ifExists ? ' IF EXISTS' : ''} ${qn}${cascade ? ' CASCADE' : ''}`;
      return this;
    }

    if (d === 'mysql') {
      this._sql = `DROP VIEW${ifExists ? ' IF EXISTS' : ''} ${qn}`;
      return this;
    }

    if (d === 'mssql') {
      if (ifExists) {
        const esc = String(name).replace(/'/g, "''");
        this._sql = `IF OBJECT_ID(N'${esc}', N'V') IS NOT NULL DROP VIEW ${qn}`;
        return this;
      }
      this._sql = `DROP VIEW ${qn}`;
      return this;
    }

    if (d === 'oracle') {
      this._sql = ifExists
        ? `BEGIN EXECUTE IMMEDIATE 'DROP VIEW ${String(name).replace(/'/g, "''")}'; EXCEPTION WHEN OTHERS THEN NULL; END;`
        : `DROP VIEW ${qn}`;
      return this;
    }

    this._sql = `DROP VIEW ${qn}`;
    return this;
  }

  /**
   * Drop a SQL function (best-effort, dialect-specific).
   * For PostgreSQL, pass `opts.args` using argument types for overloaded functions.
   *
   * @param {string} name
   * @param {{args?: string|string[], ifExists?: boolean, cascade?: boolean}} [opts]
   */
  dropFunction(name, opts = {}) {
    if (this.dialect === 'mongo') throw new Error('Schema builder is SQL-only');

    const d = this.dialect;
    const fn = String(name).trim();
    const ifExists = opts.ifExists === true;
    const cascade = opts.cascade === true;
    const args = formatRoutineArgList(opts.args);

    if (!fn) throw new Error('dropFunction requires a function name');

    if (d === 'pg') {
      this._sql = `DROP FUNCTION${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, fn)}(${args})${cascade ? ' CASCADE' : ''}`;
      return this;
    }

    if (d === 'mysql') {
      this._sql = `DROP FUNCTION${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, fn)}`;
      return this;
    }

    if (d === 'mssql') {
      if (ifExists) {
        const esc = fn.replace(/'/g, "''");
        this._sql = `IF OBJECT_ID(N'${esc}') IS NOT NULL DROP FUNCTION ${quoteIdent(d, fn)}`;
        return this;
      }
      this._sql = `DROP FUNCTION ${quoteIdent(d, fn)}`;
      return this;
    }

    if (d === 'oracle') {
      this._sql = ifExists
        ? `BEGIN EXECUTE IMMEDIATE 'DROP FUNCTION ${String(fn).replace(/'/g, "''")}'; EXCEPTION WHEN OTHERS THEN NULL; END;`
        : `DROP FUNCTION ${quoteIdent(d, fn)}`;
      return this;
    }

    throw new Error(`${d}: dropFunction not supported`);
  }

  /**
   * Drop a SQL procedure (best-effort, dialect-specific).
   * For PostgreSQL, pass `opts.args` using argument types for overloaded procedures.
   *
   * @param {string} name
   * @param {{args?: string|string[], ifExists?: boolean, cascade?: boolean}} [opts]
   */
  dropProcedure(name, opts = {}) {
    if (this.dialect === 'mongo') throw new Error('Schema builder is SQL-only');

    const d = this.dialect;
    const proc = String(name).trim();
    const ifExists = opts.ifExists === true;
    const cascade = opts.cascade === true;
    const args = formatRoutineArgList(opts.args);

    if (!proc) throw new Error('dropProcedure requires a procedure name');

    if (d === 'pg') {
      this._sql = `DROP PROCEDURE${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, proc)}(${args})${cascade ? ' CASCADE' : ''}`;
      return this;
    }

    if (d === 'mysql') {
      this._sql = `DROP PROCEDURE${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, proc)}`;
      return this;
    }

    if (d === 'mssql') {
      if (ifExists) {
        const esc = proc.replace(/'/g, "''");
        this._sql = `IF OBJECT_ID(N'${esc}', N'P') IS NOT NULL DROP PROCEDURE ${quoteIdent(d, proc)}`;
        return this;
      }
      this._sql = `DROP PROCEDURE ${quoteIdent(d, proc)}`;
      return this;
    }

    if (d === 'oracle') {
      this._sql = ifExists
        ? `BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE ${String(proc).replace(/'/g, "''")}'; EXCEPTION WHEN OTHERS THEN NULL; END;`
        : `DROP PROCEDURE ${quoteIdent(d, proc)}`;
      return this;
    }

    throw new Error(`${d}: dropProcedure not supported`);
  }

  /**
   * Drop a PostgreSQL aggregate object.
   * Pass `opts.args` with the aggregate signature types.
   *
   * @param {string} name
   * @param {{args?: string|string[], ifExists?: boolean, cascade?: boolean}} [opts]
   */
  dropAggregate(name, opts = {}) {
    const d = this.dialect;
    if (d !== 'pg') throw new Error(`${d}: dropAggregate is only supported on PostgreSQL`);

    const agg = String(name).trim();
    const ifExists = opts.ifExists === true;
    const cascade = opts.cascade === true;
    const args = formatRoutineArgList(opts.args);

    if (!agg) throw new Error('dropAggregate requires an aggregate name');

    this._sql = `DROP AGGREGATE${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, agg)}(${args})${cascade ? ' CASCADE' : ''}`;
    return this;
  }

  /**
   * Create a trigger (best-effort, dialect-specific).
   *
   * PG: creates helper function `<name>_fn` and the trigger.
   * MySQL: supports a single event per trigger; if `events` has multiple entries, multiple triggers are created.
   * MSSQL: creates `CREATE TRIGGER ... ON ... AFTER ... AS BEGIN ... END`.
   * Oracle: creates `CREATE OR REPLACE TRIGGER ...`.
   */
  createTrigger({ name, table, timing = 'BEFORE', events = ['UPDATE'], body }) {
    const d = this.dialect;
    const trg = String(name);
    const tbl = String(table);
    const t = String(timing).toUpperCase();
    const evs = (Array.isArray(events) ? events : [events]).map(e => String(e).toUpperCase());
    const b = String(body ?? '').trim();
    if (!trg || !tbl || !b) throw new Error('createTrigger requires { name, table, body }');

    if (d === 'pg') {
      const fnName = `${trg}_fn`;
      const qtFn = quoteIdent(d, fnName);
      const qtTrg = quoteIdent(d, trg);
      const qtTbl = quoteIdent(d, tbl);
      // In PG, you typically write PL/pgSQL. We wrap it.
      const fnSql = `CREATE OR REPLACE FUNCTION ${qtFn}() RETURNS trigger AS $$\nBEGIN\n${b}\nRETURN NEW;\nEND;\n$$ LANGUAGE plpgsql`;
      const evSql = evs.join(' OR ');
      const trgSql = `CREATE TRIGGER ${qtTrg} ${t} ${evSql} ON ${qtTbl} FOR EACH ROW EXECUTE FUNCTION ${qtFn}()`;
      this._sql = fnSql;
      this._postSql = [trgSql];
      return this;
    }

    if (d === 'mysql') {
      // One event per trigger. Create multiple if needed.
      const out = [];
      for (const ev of evs) {
        const trgName = evs.length === 1 ? trg : `${trg}_${ev.toLowerCase()}`;
        out.push(`CREATE TRIGGER ${quoteIdent(d, trgName)} ${t} ${ev} ON ${quoteIdent(d, tbl)} FOR EACH ROW BEGIN\n${b}\nEND`);
      }
      this._sql = out[0] ?? null;
      this._postSql = out.slice(1);
      return this;
    }

    if (d === 'mssql') {
      const evSql = evs.join(', ');
      // In SQL Server you usually use AFTER/INSTEAD OF. We'll map BEFORE -> INSTEAD OF (best-effort).
      const when = t === 'BEFORE' ? 'INSTEAD OF' : 'AFTER';
      this._sql = `CREATE TRIGGER ${quoteIdent(d, trg)} ON ${quoteIdent(d, tbl)} ${when} ${evSql} AS\nBEGIN\n${b}\nEND`;
      return this;
    }

    if (d === 'oracle') {
      const evSql = evs.join(' OR ');
      this._sql = `CREATE OR REPLACE TRIGGER ${quoteIdent(d, trg)} ${t} ${evSql} ON ${quoteIdent(d, tbl)} FOR EACH ROW\nBEGIN\n${b}\nEND;`;
      return this;
    }

    throw new Error(`${d}: createTrigger not supported`);
  }

  /**
   * Drop a trigger.
   * @param {string} name
   * @param {{table?: string, ifExists?: boolean}} [opts]
   */
  dropTrigger(name, opts = {}) {
    const d = this.dialect;
    const trg = String(name);
    const ifExists = opts.ifExists === true;
    if (d === 'pg') {
      if (!opts.table) throw new Error('pg: dropTrigger requires opts.table');
      const fnName = `${trg}_fn`;
      this._sql = `DROP TRIGGER${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, trg)} ON ${quoteIdent(d, opts.table)}`;
      this._postSql = [`DROP FUNCTION${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, fnName)}()`];
      return this;
    }
    if (d === 'mysql') {
      this._sql = `DROP TRIGGER${ifExists ? ' IF EXISTS' : ''} ${quoteIdent(d, trg)}`;
      return this;
    }
    if (d === 'mssql') {
      if (ifExists) {
        const esc = trg.replace(/'/g, "''");
        this._sql = `IF OBJECT_ID(N'${esc}', N'TR') IS NOT NULL DROP TRIGGER ${quoteIdent(d, trg)}`;
        return this;
      }
      this._sql = `DROP TRIGGER ${quoteIdent(d, trg)}`;
      return this;
    }
    if (d === 'oracle') {
      this._sql = `DROP TRIGGER ${quoteIdent(d, trg)}`;
      return this;
    }
    throw new Error(`${d}: dropTrigger not supported`);
  }

  async _runSelect(sql, params = []) {
    const qb = {
      dialect: this.dialect,
      compile: () => ({ sql, params }),
      _type: 'select',
      _returning: null,
    };
    return this.adapter.execute(/** @type {any} */(qb));
  }

  _viewSourceSql(source) {
    if (typeof source === 'string') {
      const s = source.trim();
      if (!s) throw new Error('createView source SQL must not be empty');
      return s;
    }
    if (source && typeof source === 'object' && typeof source.sql === 'string') {
      const params = Array.isArray(source.params) ? source.params : [];
      if (params.length) throw new Error('createView(raw) does not support parameterized SQL; pass a plain SQL string');
      return String(source.sql);
    }
    if (source && typeof source === 'object' && typeof source.compile === 'function') {
      const out = source.compile();
      const sql = String(out?.sql ?? '').trim();
      const params = Array.isArray(out?.params) ? out.params : [];
      if (!sql) throw new Error('createView(query) compiled to empty SQL');
      if (params.length) {
        throw new Error('createView(query) does not support parameterized queries; use literal SQL in the view definition');
      }
      return sql;
    }
    throw new Error('createView(name, source) expects SQL string, raw(sql), or QueryBuilder');
  }

  /**
   * Execute schema statement.
   */
  async exec() {
    // Native mongo schema operations that do not map to SQL.
    if (this._meta?.op === 'dropCollection' && this.dialect === 'mongo') {
      const collectionName = String(this._meta.name ?? '').trim();
      if (!collectionName) throw new Error('mongo: dropTable(name) requires a collection name');

      const col = this.adapter?.db?.collection?.(collectionName);
      if (!col || typeof col.drop !== 'function') {
        throw new Error('mongo: collection().drop() is not available on this adapter');
      }

      try {
        const out = await col.drop();
        return { dropped: !!out, collection: collectionName };
      } catch (e) {
        if (this._meta?.opts?.ifExists === true && isMongoNamespaceNotFoundError(e)) {
          return { dropped: false, collection: collectionName, skipped: true };
        }
        throw e;
      }
    }

    if (this._meta?.op === 'truncateCollection' && this.dialect === 'mongo') {
      const collectionName = String(this._meta.name ?? '').trim();
      if (!collectionName) throw new Error('mongo: truncateTable(name) requires a collection name');

      const col = this.adapter?.db?.collection?.(collectionName);
      if (!col || typeof col.deleteMany !== 'function') {
        throw new Error('mongo: collection().deleteMany() is not available on this adapter');
      }

      const out = await col.deleteMany({});
      return {
        truncated: true,
        collection: collectionName,
        deletedCount: Number(out?.deletedCount ?? 0),
      };
    }

    const hasMain = typeof this._sql === 'string' && this._sql.trim().length > 0;
    const hasPost = Array.isArray(this._postSql) && this._postSql.length > 0;
    if (!hasMain && !hasPost) throw new Error('No schema statement');
    // run raw execution using adapter
    const runOne = async (sql) => {
      const qb = { dialect: this.dialect, compile: () => ({ sql, params: [] }), _type: 'schema', _returning: null };
      return this.adapter.execute(/** @type {any} */(qb));
    };

    // Special handling for CREATE DATABASE IF NOT EXISTS on dialects that don't support it natively.
    if (this._meta?.op === 'createDatabase' && this._meta.opts?.ifNotExists === true) {
      const name = String(this._meta.name);
      if (this.dialect === 'pg') {
        const qb = {
          dialect: this.dialect,
          compile: () => ({ sql: `SELECT 1 FROM pg_database WHERE datname = ${this.adapter.placeholder(1)}`, params: [name] }),
          _type: 'select',
        };
        const rows = await this.adapter.execute(/** @type {any} */(qb));
        if (Array.isArray(rows) && rows.length) return { skipped: true };
      }

      if (this.dialect === 'mssql') {
        // Wrap create statement in IF DB_ID(...) IS NULL
        const dbNameEsc = name.replace(/'/g, "''");
        this._sql = `IF DB_ID(N'${dbNameEsc}') IS NULL ${String(this._sql)}`;
      }
    }

    // Best-effort handling for DROP DATABASE with force=true.
    if (this._meta?.op === 'dropDatabase' && this._meta.opts?.force === true) {
      const name = String(this._meta.name);
      if (this.dialect === 'pg') {
        // Terminate other sessions connected to the database.
        // Requires privileges (often superuser or role with pg_signal_backend).
        const qb = {
          dialect: this.dialect,
          compile: () => ({
            sql: `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = ${this.adapter.placeholder(1)} AND pid <> pg_backend_pid()`,
            params: [name],
          }),
          _type: 'select',
        };
        try { await this.adapter.execute(/** @type {any} */(qb)); } catch (_) {
          // Best-effort: if we cannot terminate connections, the DROP may still fail.
        }
      }

      if (this.dialect === 'mssql') {
        // Put DB into SINGLE_USER with rollback immediate to force disconnects.
        const esc = name.replace(/'/g, "''");
        try {
          await runOne(`IF DB_ID(N'${esc}') IS NOT NULL ALTER DATABASE ${quoteIdent(this.dialect, name)} SET SINGLE_USER WITH ROLLBACK IMMEDIATE`);
        } catch (_) {
          // Best-effort.
        }
      }
    }

    let res = null;
    if (hasMain) {
      res = await runOne(String(this._sql));
    }

    if (hasPost) {
      for (const s of this._postSql) {
        try { await runOne(s); } catch (e) {
          // Best-effort post statements (e.g., MySQL dropConstraint auto tries)
          // Re-throw only for non-MySQL or when explicitly needed.
          if (this.dialect !== 'mysql') throw e;
        }
      }
    }
    return res;
  }
}

function rowsToNames(rows, preferredKey = 'name') {
  if (!Array.isArray(rows)) return [];
  const out = [];
  for (const row of rows) {
    const v = pickRowValue(row, preferredKey);
    if (v == null) continue;
    const s = String(v).trim();
    if (s) out.push(s);
  }
  return out;
}

function isMongoNamespaceNotFoundError(err) {
  if (!err || typeof err !== 'object') return false;
  const code = Number(err.code);
  if (code === 26) return true;
  const codeName = String(err.codeName ?? '');
  if (codeName === 'NamespaceNotFound') return true;
  const msg = String(err.message ?? '').toLowerCase();
  return msg.includes('ns not found') || msg.includes('namespace not found');
}

function pickRowValue(row, preferredKey) {
  if (row == null) return null;
  if (Array.isArray(row)) return row[0] ?? null;
  if (typeof row !== 'object') return row;

  const keys = Object.keys(row);
  if (!keys.length) return null;

  const exact = keys.find(k => k === preferredKey);
  if (exact) return row[exact];

  const folded = String(preferredKey).toLowerCase();
  const ci = keys.find(k => String(k).toLowerCase() === folded);
  if (ci) return row[ci];

  return row[keys[0]];
}

function normalizeGetDescArgs(target, opts) {
  if (target && typeof target === 'object' && !Array.isArray(target)) {
    const cfg = target;
    const kindRaw = String(cfg.kind ?? cfg.type ?? 'database').toLowerCase();
    if (kindRaw === 'database' || kindRaw === 'db') {
      return { kind: 'database', opts: cfg };
    }
    if (kindRaw === 'table') {
      const name = cfg.name ?? cfg.table;
      if (!name) throw new Error("getDesc({ kind: 'table', name }) requires a name");
      return { kind: 'table', name: String(name), opts: cfg };
    }
    throw new Error(`Unsupported getDesc kind: ${kindRaw}`);
  }

  const text = String(target ?? '').trim();
  const lowered = text.toLowerCase();

  if (!text || lowered === 'database' || lowered === 'db') {
    return { kind: 'database', opts: asObject(opts) };
  }

  if (lowered === 'table') {
    if (typeof opts === 'string') {
      return { kind: 'table', name: opts, opts: {} };
    }
    const cfg = asObject(opts);
    const name = cfg.name ?? cfg.table;
    if (!name) throw new Error("getDesc('table', name) requires a table/collection name");
    return { kind: 'table', name: String(name), opts: cfg };
  }

  // shorthand: getDesc('users')
  return { kind: 'table', name: text, opts: asObject(opts) };
}

function asObject(v) {
  return (v && typeof v === 'object' && !Array.isArray(v)) ? v : {};
}

function normalizeConstraintTypeFilter(dialect, type) {
  if (type == null) return null;
  const raw = String(type).trim();
  if (!raw) return null;
  const upper = raw.toUpperCase();

  if (dialect === 'oracle') {
    if (upper === 'PRIMARY KEY' || upper === 'PRIMARY') return 'P';
    if (upper === 'UNIQUE') return 'U';
    if (upper === 'FOREIGN KEY' || upper === 'FOREIGN') return 'R';
    if (upper === 'CHECK') return 'C';
    return upper;
  }

  if (upper === 'PRIMARY') return 'PRIMARY KEY';
  if (upper === 'FOREIGN') return 'FOREIGN KEY';
  return upper;
}

function normalizeColumnDescriptions(rows) {
  if (!Array.isArray(rows)) return [];
  return rows.map((row) => {
    const name = pickRowField(row, ['column_name', 'name', 'field']);
    const type = pickRowField(row, ['column_type', 'data_type', 'type']);
    const nullable = parseNullable(pickRowField(row, ['is_nullable', 'nullable']));
    const defaultValue = pickRowField(row, ['column_default', 'data_default', 'default']);
    const maxLength = parseMaybeNumber(pickRowField(row, ['character_maximum_length', 'data_length', 'max_length']));
    const precision = parseMaybeNumber(pickRowField(row, ['numeric_precision', 'data_precision', 'precision']));
    const scale = parseMaybeNumber(pickRowField(row, ['numeric_scale', 'data_scale', 'scale']));
    const ordinal = parseMaybeNumber(pickRowField(row, ['ordinal_position', 'column_id', 'ordinal']));
    const extra = pickRowField(row, ['extra']);

    return {
      name: name == null ? null : String(name),
      type: type == null ? null : String(type),
      nullable,
      default: defaultValue ?? null,
      maxLength,
      precision,
      scale,
      ordinal,
      extra: extra == null ? null : String(extra),
    };
  }).filter(c => c.name);
}

function pickRowField(row, keys) {
  if (row == null) return null;
  if (Array.isArray(row)) return row[0] ?? null;
  if (typeof row !== 'object') return row;

  const objKeys = Object.keys(row);
  for (const k of keys) {
    const exact = objKeys.find(x => x === k);
    if (exact) return row[exact];
    const folded = String(k).toLowerCase();
    const ci = objKeys.find(x => String(x).toLowerCase() === folded);
    if (ci) return row[ci];
  }
  return null;
}

function parseNullable(v) {
  if (v == null) return null;
  if (typeof v === 'boolean') return v;
  const s = String(v).trim().toUpperCase();
  if (s === 'YES' || s === 'Y' || s === 'TRUE' || s === '1') return true;
  if (s === 'NO' || s === 'N' || s === 'FALSE' || s === '0') return false;
  return null;
}

function parseMaybeNumber(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseBooleanLike(v) {
  if (v == null) return null;
  if (typeof v === 'boolean') return v;
  const s = String(v).trim().toUpperCase();
  if (s === 'TRUE' || s === 'T' || s === 'YES' || s === 'Y' || s === '1') return true;
  if (s === 'FALSE' || s === 'F' || s === 'NO' || s === 'N' || s === '0') return false;
  return null;
}

function normalizePositiveInt(value, fallback, max = 200) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), max);
}

function inferMongoColumns(docs) {
  if (!Array.isArray(docs) || !docs.length) return [];
  const map = new Map();
  for (const doc of docs) {
    if (!doc || typeof doc !== 'object' || Array.isArray(doc)) continue;
    for (const [key, value] of Object.entries(doc)) {
      collectMongoField(map, key, value, 0);
    }
  }

  return Array.from(map.entries())
    .map(([name, typeSet]) => {
      const types = Array.from(typeSet).sort();
      return {
        name,
        type: types.length === 1 ? types[0] : 'mixed',
        types,
        nullable: types.includes('null'),
        default: null,
        maxLength: null,
        precision: null,
        scale: null,
        ordinal: null,
        extra: null,
      };
    })
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));
}

function collectMongoField(map, path, value, depth) {
  if (!path) return;
  const t = detectMongoType(value);
  if (!map.has(path)) map.set(path, new Set());
  map.get(path).add(t);

  if (depth >= 4) return;
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    for (const [k, v] of Object.entries(value)) {
      const child = `${path}.${k}`;
      collectMongoField(map, child, v, depth + 1);
    }
  }
}

function detectMongoType(value) {
  if (value == null) return 'null';
  if (Array.isArray(value)) return 'array';
  if (value instanceof Date) return 'date';
  if (typeof value === 'string') return 'string';
  if (typeof value === 'number') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'bigint') return 'bigint';
  if (value && typeof value === 'object' && value._bsontype) return String(value._bsontype).toLowerCase();
  if (typeof value === 'object') return 'object';
  return typeof value;
}

function toStrictDatabaseShape(desc) {
  const tables = Array.isArray(desc?.tables)
    ? desc.tables.slice()
    : (Array.isArray(desc?.collections) ? desc.collections.slice() : []);

  const views = Array.isArray(desc?.views) ? desc.views.slice() : [];
  const databases = Array.isArray(desc?.databases) ? desc.databases.slice() : null;

  let tableDescriptions = null;
  if (desc?.tableDescriptions && typeof desc.tableDescriptions === 'object') {
    tableDescriptions = Object.fromEntries(
      Object.entries(desc.tableDescriptions).map(([k, v]) => [k, toStrictTableShape(v)]),
    );
  } else if (desc?.collectionDescriptions && typeof desc.collectionDescriptions === 'object') {
    tableDescriptions = Object.fromEntries(
      Object.entries(desc.collectionDescriptions).map(([k, v]) => [k, toStrictTableShape(v)]),
    );
  }

  return {
    kind: 'database',
    dialect: desc?.dialect ?? null,
    name: desc?.name ?? null,
    schema: desc?.schema ?? null,
    tables,
    views,
    databases,
    tableDescriptions,
    createSql: desc?.createSql ?? null,
  };
}

function toStrictTableShape(desc) {
  const cols = Array.isArray(desc?.columns) ? desc.columns : [];
  const columns = cols.map((c) => ({
    name: c?.name ?? null,
    type: c?.type ?? null,
    types: Array.isArray(c?.types) ? c.types.slice() : (c?.type != null ? [String(c.type)] : []),
    nullable: c?.nullable ?? null,
    default: c?.default ?? null,
    maxLength: c?.maxLength ?? null,
    precision: c?.precision ?? null,
    scale: c?.scale ?? null,
    ordinal: c?.ordinal ?? null,
    extra: c?.extra ?? null,
  }));

  return {
    kind: 'table',
    dialect: desc?.dialect ?? null,
    name: desc?.name ?? null,
    schema: desc?.schema ?? null,
    columns,
    indexes: Array.isArray(desc?.indexes) ? desc.indexes.slice() : [],
    triggers: Array.isArray(desc?.triggers) ? desc.triggers.slice() : [],
    sampleSize: desc?.sampleSize ?? null,
    sampledDocuments: desc?.sampledDocuments ?? null,
    createSql: desc?.createSql ?? null,
  };
}

function buildCreateDatabaseScript(dbDesc, opts = {}) {
  const d = dbDesc?.dialect;
  const lines = [];
  const dbName = dbDesc?.name ? String(dbDesc.name) : null;

  if (d === 'mongo') {
    if (dbName) lines.push(`use ${dbName}`);
    const tables = Array.isArray(dbDesc?.collections) ? dbDesc.collections : [];
    for (const t of tables) {
      lines.push(`db.createCollection(${quoteStringLiteral(t)});`);
    }
    return lines.join('\n');
  }

  if (dbName) {
    lines.push(`CREATE DATABASE ${quoteIdent(d, dbName)};`);
  }

  const schema = dbDesc?.schema ? String(dbDesc.schema) : null;
  if (schema && d === 'pg') {
    lines.push(`CREATE SCHEMA IF NOT EXISTS ${quoteIdent(d, schema)};`);
  }

  const tableMap = opts?.tableDescriptions && typeof opts.tableDescriptions === 'object'
    ? opts.tableDescriptions
    : null;

  const tableNames = Array.isArray(dbDesc?.tables) ? dbDesc.tables : [];
  for (const t of tableNames) {
    const tableDesc = tableMap?.[t];
    if (tableDesc?.createSql) lines.push(String(tableDesc.createSql));
  }

  const viewNames = Array.isArray(dbDesc?.views) ? dbDesc.views : [];
  for (const v of viewNames) {
    lines.push(`-- View ${v}: definition not included by getDesc(includeCreateSql)`);
  }

  return lines.join('\n\n');
}

function buildCreateTableScript(tableDesc) {
  const d = tableDesc?.dialect;
  const tableName = tableDesc?.name ? String(tableDesc.name) : '';
  if (!tableName) return '';

  if (d === 'mongo') {
    return `db.createCollection(${quoteStringLiteral(tableName)});`;
  }

  const schema = tableDesc?.schema ? String(tableDesc.schema) : null;
  const qTable = schema ? `${quoteIdent(d, schema)}.${quoteIdent(d, tableName)}` : quoteIdent(d, tableName);
  const cols = Array.isArray(tableDesc?.columns) ? tableDesc.columns : [];
  const colLines = cols.map((c) => buildCreateColumnLine(d, c)).filter(Boolean);
  if (!colLines.length) return `CREATE TABLE ${qTable} ();`;
  return `CREATE TABLE ${qTable} (\n  ${colLines.join(',\n  ')}\n);`;
}

function buildCreateColumnLine(dialect, col) {
  const name = col?.name ? String(col.name) : null;
  if (!name) return '';
  const type = col?.type ? String(col.type) : defaultTypeForDialect(dialect);
  const parts = [`${quoteIdent(dialect, name)} ${type}`];
  if (col?.nullable === false) parts.push('NOT NULL');
  if (col?.default != null) {
    const dflt = formatDefaultSql(col.default, dialect);
    if (dflt) parts.push(`DEFAULT ${dflt}`);
  }
  if (col?.extra) parts.push(String(col.extra));
  return parts.join(' ');
}

function defaultTypeForDialect(dialect) {
  if (dialect === 'oracle') return 'VARCHAR2(255)';
  if (dialect === 'mssql') return 'NVARCHAR(255)';
  return 'VARCHAR(255)';
}

function formatDefaultSql(value, dialect) {
  if (value == null) return 'NULL';
  if (typeof value === 'number') return Number.isFinite(value) ? String(value) : null;
  if (typeof value === 'boolean') {
    if (dialect === 'mysql' || dialect === 'mssql' || dialect === 'oracle') return value ? '1' : '0';
    return value ? 'TRUE' : 'FALSE';
  }

  const raw = String(value).trim();
  if (!raw) return null;
  if (looksLikeSqlExpression(raw)) return raw;
  return quoteStringLiteral(raw);
}

function looksLikeSqlExpression(s) {
  if (s.startsWith("'") || s.startsWith('"') || s.startsWith('(')) return true;
  if (/^-?\d+(\.\d+)?$/.test(s)) return true;
  if (/::/.test(s)) return true;
  if (/^[A-Z_][A-Z0-9_]*$/i.test(s)) return true;
  if (/^[A-Z_][A-Z0-9_]*\s*\(/i.test(s)) return true;
  return false;
}

function splitObjectName(name, schemaOverride = null) {
  assertIdent(name);
  if (schemaOverride != null) assertIdent(schemaOverride);
  const parts = splitIdent(name);
  if (!parts.length) throw new Error(`Invalid identifier: "${name}"`);
  return {
    schema: schemaOverride ?? (parts.length > 1 ? parts[0] : null),
    table: parts[parts.length - 1],
  };
}

function normalizeScopedObjectFilter(name, schemaOverride = null) {
  if (name == null || String(name).trim() === '') {
    const schema = schemaOverride == null ? null : String(schemaOverride);
    if (schema) assertIdent(schema);
    return { schema, name: null };
  }
  const scoped = splitObjectName(String(name), schemaOverride);
  return {
    schema: scoped.schema,
    name: scoped.table,
  };
}

function formatTypeDefinition(definition) {
  if (Array.isArray(definition)) {
    return definition.length ? `AS ENUM (${definition.map(v => quoteStringLiteral(v)).join(', ')})` : '';
  }

  if (typeof definition === 'string') {
    const raw = String(definition).trim();
    if (!raw) return '';
    return /^AS\s+/i.test(raw) ? raw : `AS ${raw}`;
  }

  if (!definition || typeof definition !== 'object') {
    return '';
  }

  const kind = String(definition.kind ?? '').trim().toLowerCase();
  const enumValues = Array.isArray(definition.values) ? definition.values : definition.enum;
  if (kind === 'enum' || Array.isArray(enumValues)) {
    const vals = Array.isArray(enumValues) ? enumValues : [];
    return vals.length ? `AS ENUM (${vals.map(v => quoteStringLiteral(v)).join(', ')})` : '';
  }

  const compositeFields = definition.fields ?? definition.composite;
  if (kind === 'composite' || compositeFields) {
    const attrs = formatCompositeTypeFields(compositeFields);
    return attrs.length ? `AS (${attrs.join(', ')})` : '';
  }

  const raw = String(definition.definition ?? definition.raw ?? '').trim();
  if (!raw) return '';
  return /^AS\s+/i.test(raw) ? raw : `AS ${raw}`;
}

function formatCompositeTypeFields(fields) {
  if (Array.isArray(fields)) {
    return fields
      .map(field => String(field ?? '').trim())
      .filter(Boolean);
  }
  if (!fields || typeof fields !== 'object') return [];
  return Object.entries(fields)
    .map(([name, type]) => {
      const fieldName = String(name ?? '').trim();
      const fieldType = String(type ?? '').trim();
      if (!fieldName || !fieldType) return null;
      return `${quoteIdent('pg', fieldName)} ${fieldType}`;
    })
    .filter(Boolean);
}

function formatRoutineArgList(args) {
  if (args == null) return '';
  const arr = Array.isArray(args) ? args : [args];
  return arr
    .map(arg => String(arg ?? '').trim())
    .filter(Boolean)
    .join(', ');
}

function formatAggregateDefinition(definition) {
  if (typeof definition === 'string') {
    return String(definition).trim();
  }
  if (!definition || typeof definition !== 'object' || Array.isArray(definition)) {
    return '';
  }

  return Object.entries(definition)
    .map(([key, value]) => {
      const k = String(key ?? '').trim().toUpperCase();
      if (!k) return null;
      if (value == null) return `${k} = NULL`;
      if (typeof value === 'number') return `${k} = ${value}`;
      if (typeof value === 'boolean') return `${k} = ${value ? 'TRUE' : 'FALSE'}`;
      const raw = String(value).trim();
      if (!raw) return null;
      return `${k} = ${looksLikeSqlExpression(raw) ? raw : quoteStringLiteral(raw)}`;
    })
    .filter(Boolean)
    .join(',\n  ');
}

function buildVacuumOptions(opts = {}) {
  const options = [];
  if (opts.full === true) options.push('FULL');
  if (opts.freeze === true) options.push('FREEZE');
  if (opts.verbose === true) options.push('VERBOSE');
  if (opts.analyze === true) options.push('ANALYZE');
  return options;
}

function quoteStringLiteral(s) {
  return `'${String(s).replace(/'/g, "''")}'`;
}
