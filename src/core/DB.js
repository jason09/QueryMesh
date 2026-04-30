import { QueryBuilder } from './QueryBuilder.js';
import { raw } from './Raw.js';
import { id } from './Identifier.js';
import { SchemaBuilder } from './SchemaBuilder.js';
import { quoteIdent } from '../utils/identifiers.js';
import { makeRawQueryResult } from '../utils/rawSql.js';
import { BackupManager } from '../backup/BackupManager.js';
import { ToolsManager } from '../tools/ToolsManager.js';
import { MaintenanceManager } from '../maintenance/MaintenanceManager.js';

/**
 * DB is the main entry point.
 */
export class DB {
  /**
   * @param {import('../adapters/BaseAdapter.js').BaseAdapter} adapter
   */
  constructor(adapter) {
    this.adapter = adapter;
  }

  /**
   * Start a new query for a table/collection.
   * @param {string} name
   * @returns {QueryBuilder}
   */
  table(name) {
    return new QueryBuilder(this.adapter, name);
  }

  /**
   * Create a raw SQL fragment.
   * @param {string} sql
   * @param {any[]} [params]
   */
  raw(sql, params = []) { return raw(sql, params); }

  /**
   * Create a safe identifier wrapper.
   * @param {string} name
   */
  id(name) { return id(name); }

  /**
   * Eager-safe identifier quoting.
   * @param {string} name
   */
  quote(name) {
    if (this.adapter.dialect === 'mongo') return name;
    return quoteIdent(/** @type {any} */(this.adapter.dialect), name);
  }

  /**
   * Execute raw SQL and return a rows array with .rows and .rowCount metadata.
   * SQL dialects only.
   * @param {string} sql
   * @param {any[]} [params]
   */
  async query(sql, params = []) {
    return this.adapter.query(sql, params);
  }

  /**
   * Call a stored procedure or scalar function.
   *
   * This is a best-effort helper for common routine execution. For advanced
   * vendor-specific routine behavior, use db.query(...) directly.
   *
   * @param {string} name
   * @param {any[]|any} [args]
   * @param {{ kind?: 'procedure'|'function', as?: string }} [opts]
   */
  async call(name, args = [], opts = {}) {
    if (this.adapter?.dialect === 'mongo') {
      throw new Error('mongo: db.call() is not supported');
    }
    const routineArgs = normalizeRoutineArgs(args);
    const kind = normalizeRoutineKind(opts.kind);
    if (kind !== 'procedure' && routineArgs.some((arg) => arg.mode !== 'in')) {
      throw new Error('OUT/INOUT params are only supported for procedure calls');
    }

    const hasOut = routineArgs.some((arg) => arg.mode !== 'in');
    if (hasOut) {
      return runRoutineWithOutputs(this.adapter, name, routineArgs, opts);
    }

    const params = routineArgs.map((arg) => arg.value);
    const sql = buildRoutineCallSql(this.adapter?.dialect, name, params, { ...opts, kind });
    return this.query(sql, params);
  }

  /**
   * Execute a set-returning or table-valued function and return rows.
   *
   * Supports:
   * - PostgreSQL set-returning functions
   * - SQL Server table-valued functions
   * - Oracle table/pipelined functions
   *
   * MySQL does not support table-valued functions with this helper.
   *
   * @param {string} name
   * @param {any[]|any} [args]
   * @param {{}} [opts]
   */
  async callTable(name, args = [], opts = {}) {
    if (this.adapter?.dialect === 'mongo') {
      throw new Error('mongo: db.callTable() is not supported');
    }
    void opts;
    const routineArgs = normalizeRoutineArgs(args);
    if (routineArgs.some((arg) => arg.mode !== 'in')) {
      throw new Error('callTable() does not support OUT/INOUT params');
    }
    const params = routineArgs.map((arg) => arg.value);
    const sql = buildRoutineTableSql(this.adapter?.dialect, name, params);
    return this.query(sql, params);
  }

  /**
   * db.exec() no longer executes raw SQL. Use db.query(sql, params) for
   * SELECT, INSERT, UPDATE, DELETE, and DDL statements.
   * @param {string} sql
   * @param {any[]} [params]
   */
  async exec(sql, params = []) {
    void sql;
    void params;
    throw new Error('db.exec() no longer executes raw SQL. Use db.query(sql, params) instead.');
  }

  /**
   * Schema/DDL builder (SQL dialects only).
   */
  schema() { return new SchemaBuilder(this.adapter); }

  /**
   * Backup/restore manager (uses native DB CLI tools).
   */
  backup() { return new BackupManager(this); }

  /**
   * Runtime diagnostics/tooling manager.
   */
  tools() { return new ToolsManager(this); }

  /**
   * Maintenance/admin helper for VACUUM, REINDEX, REPAIR, etc.
   */
  maintenance() { return new MaintenanceManager(this); }

  /**
   * Run a transaction.
   * @param {(trxDb: DB) => Promise<any>} fn
   */
  transaction(fn) { return this.adapter.transaction(fn); }

  /**
   * Close underlying pools/clients (best-effort).
   */
  async close() { return this.adapter.close?.(); }

  /**
   * Switch current DB instance to another database and reuse the same DB object.
   * Dialect support:
   * - pg/mysql/mssql: reconnect to target database
   * - mongo: switch Db handle on same client
   * - oracle: reconnect using target connectString/service
   *
   * @param {string} name
   * @param {Record<string, any>} [opts]
   * @returns {Promise<DB>}
   */
  async switchDatabase(name, opts = {}) {
    if (!this.adapter || typeof this.adapter.switchDatabase !== 'function') {
      throw new Error(`${this.adapter?.dialect ?? 'unknown'}: switchDatabase is not supported by this adapter`);
    }
    const nextAdapter = await this.adapter.switchDatabase(name, opts);
    if (!nextAdapter) throw new Error('switchDatabase() did not return an adapter');
    this.adapter = nextAdapter;
    return this;
  }

  /**
   * Alias of switchDatabase(name, opts).
   * @param {string} name
   * @param {Record<string, any>} [opts]
   */
  async useDatabase(name, opts = {}) {
    return this.switchDatabase(name, opts);
  }

  /**
   * Switch this DB instance to another dialect+config.
   * Creates a new underlying adapter via SQuery.connect and rebinds this DB.
   *
   * @param {'pg'|'mysql'|'mssql'|'oracle'|'mongo'|'mongodb'|'mongoose'} dialect
   * @param {Record<string, any>} config
   * @param {{closeCurrent?: boolean, features?: Record<string, any>, importer?: (name:string)=>Promise<any>|any}} [opts]
   * @returns {Promise<DB>}
   */
  async switchDialect(dialect, config, opts = {}) {
    const d = String(dialect ?? '').trim();
    if (!d) throw new Error('switchDialect(dialect, config) requires a dialect');
    if (!config || typeof config !== 'object') {
      throw new Error('switchDialect(dialect, config) requires a config object');
    }

    const currentFeatures = (this.adapter && typeof this.adapter.features === 'object' && this.adapter.features)
      ? this.adapter.features
      : {};
    const providedFeatures = (opts.features && typeof opts.features === 'object')
      ? opts.features
      : {};
    const features = { ...currentFeatures, ...providedFeatures };
    const importer = opts.importer ?? providedFeatures.driverImporter ?? currentFeatures.driverImporter;
    const { connect } = await import('../index.js');
    const nextDb = await connect({ dialect: /** @type {any} */(d), config, features, importer });

    if (opts.closeCurrent !== false) {
      try { await this.close(); } catch {}
    }
    this.adapter = nextDb.adapter;
    return this;
  }

  /**
   * Alias of switchDialect(dialect, config, opts).
   * @param {'pg'|'mysql'|'mssql'|'oracle'|'mongo'|'mongodb'|'mongoose'} dialect
   * @param {Record<string, any>} config
   * @param {{closeCurrent?: boolean, features?: Record<string, any>, importer?: (name:string)=>Promise<any>|any}} [opts]
   */
  async useDialect(dialect, config, opts = {}) {
    return this.switchDialect(dialect, config, opts);
  }

  /**
   * Bind a Model class (extends BaseModel) to this DB.
   *
   * @template T
   * @param {T} ModelClass
   * @returns {T}
   */
  model(ModelClass) {
    if (!ModelClass || typeof ModelClass.bind !== 'function') {
      throw new Error('ModelClass must extend BaseModel');
    }
    return ModelClass.bind(this);
  }
}

function normalizeCallArgs(args) {
  if (args == null) return [];
  return Array.isArray(args) ? args : [args];
}

function normalizeRoutineArgs(args) {
  return normalizeCallArgs(args).map((arg, index) => {
    if (isRoutineArgDescriptor(arg)) {
      const mode = normalizeRoutineMode(arg.mode);
      return {
        mode,
        value: arg.value,
        name: normalizeRoutineParamName(arg.name, index),
        type: arg.type,
        size: arg.size,
      };
    }
    return {
      mode: 'in',
      value: arg,
      name: normalizeRoutineParamName(null, index),
      type: undefined,
      size: undefined,
    };
  });
}

function buildRoutineCallSql(dialect, name, params, opts = {}) {
  const d = String(dialect ?? '').trim();
  const qn = quoteIdent(/** @type {any} */(d), String(name));
  const kind = normalizeRoutineKind(opts.kind);
  const placeholders = params.map(() => '?').join(', ');

  if (kind === 'function') {
    const alias = quoteIdent(/** @type {any} */(d), String(opts.as ?? 'value'));
    const expr = `${qn}(${placeholders})`;
    if (d === 'oracle') return `SELECT ${expr} AS ${alias} FROM DUAL`;
    return `SELECT ${expr} AS ${alias}`;
  }

  if (kind === 'procedure') {
    if (d === 'pg' || d === 'mysql') return `CALL ${qn}(${placeholders})`;
    if (d === 'mssql') return placeholders ? `EXEC ${qn} ${placeholders}` : `EXEC ${qn}`;
    if (d === 'oracle') return placeholders ? `BEGIN ${qn}(${placeholders}); END;` : `BEGIN ${qn}; END;`;
  }

  throw new Error(`Unsupported routine kind: ${opts.kind}`);
}

function buildRoutineTableSql(dialect, name, params) {
  const d = String(dialect ?? '').trim();
  const qn = quoteIdent(/** @type {any} */(d), String(name));
  const placeholders = params.map(() => '?').join(', ');
  if (d === 'pg' || d === 'mssql') return `SELECT * FROM ${qn}(${placeholders})`;
  if (d === 'oracle') return `SELECT * FROM TABLE(${qn}(${placeholders}))`;
  if (d === 'mysql') throw new Error('mysql: db.callTable() is not supported');
  throw new Error(`${d}: db.callTable() is not supported`);
}

async function runRoutineWithOutputs(adapter, name, routineArgs, opts = {}) {
  const d = String(adapter?.dialect ?? '').trim();
  if (normalizeRoutineKind(opts.kind) !== 'procedure') {
    throw new Error('OUT/INOUT params are only supported for procedure calls');
  }

  if (d === 'pg') return runPgRoutineWithOutputs(adapter, name, routineArgs);
  if (d === 'mysql') return runMySqlRoutineWithOutputs(adapter, name, routineArgs);
  if (d === 'mssql') return runMsSqlRoutineWithOutputs(adapter, name, routineArgs);
  if (d === 'oracle') return runOracleRoutineWithOutputs(adapter, name, routineArgs);
  throw new Error(`${d}: routine OUT/INOUT params are not supported`);
}

async function runPgRoutineWithOutputs(adapter, name, routineArgs) {
  const inputArgs = routineArgs.filter((arg) => arg.mode !== 'out').map((arg) => arg.value);
  const sql = buildRoutineCallSql(adapter?.dialect, name, inputArgs, { kind: 'procedure' });
  const result = await adapter.query(sql, inputArgs);
  const out = mapPgOutValues(result?.rows?.[0] ?? result?.[0], routineArgs);
  return attachOut(result, out);
}

async function runMySqlRoutineWithOutputs(adapter, name, routineArgs) {
  if (adapter?.config?.multipleStatements !== true) {
    throw new Error('mysql: OUT/INOUT routine params require config.multipleStatements = true');
  }
  if (typeof adapter?.queryAsync !== 'function') {
    throw new Error('mysql: OUT/INOUT routine params require a mysql pool/connection with query() support');
  }

  const qn = quoteIdent('mysql', String(name));
  const setClauses = [];
  const callParts = [];
  const selectParts = [];
  const setParams = [];
  const callParams = [];

  for (let i = 0; i < routineArgs.length; i++) {
    const arg = routineArgs[i];
    const varName = `@qm_out_${i + 1}`;
    const alias = quoteIdent('mysql', arg.name);
    if (arg.mode === 'in') {
      callParts.push('?');
      callParams.push(arg.value);
      continue;
    }
    if (arg.mode === 'inout') {
      setClauses.push(`${varName} = ?`);
      setParams.push(arg.value);
    }
    callParts.push(varName);
    selectParts.push(`${varName} AS ${alias}`);
  }

  const statements = [];
  if (setClauses.length) statements.push(`SET ${setClauses.join(', ')}`);
  statements.push(`CALL ${qn}(${callParts.join(', ')})`);
  statements.push(`SELECT ${selectParts.join(', ')}`);

  const raw = await adapter.queryAsync(statements.join('; '), [...setParams, ...callParams]);
  const outRows = findLastMySqlRowset(raw);
  const out = { ...(outRows?.[0] ?? {}) };
  return makeRawQueryResult([], {
    rowCount: 0,
    out,
    raw,
  });
}

async function runMsSqlRoutineWithOutputs(adapter, name, routineArgs) {
  const req = adapter?.pool?.request?.();
  if (!req || typeof req.query !== 'function') {
    throw new Error('mssql: OUT/INOUT routine params require a request-capable pool');
  }

  const qn = quoteIdent('mssql', String(name));
  const parts = [];

  for (const arg of routineArgs) {
    const bindName = arg.name;
    const sqlRef = `@${bindName}`;
    if (arg.mode === 'in') {
      req.input(bindName, arg.value);
      parts.push(`${sqlRef} = ${sqlRef}`);
      continue;
    }
    if (arg.type == null) {
      throw new Error(`mssql: OUT/INOUT param "${bindName}" requires a type`);
    }
    if (arg.mode === 'out') {
      req.output(bindName, arg.type);
    } else {
      req.output(bindName, arg.type, arg.value);
    }
    parts.push(`${sqlRef} = ${sqlRef} OUTPUT`);
  }

  const sql = parts.length ? `EXEC ${qn} ${parts.join(', ')}` : `EXEC ${qn}`;
  const res = await req.query(sql);
  return makeRawQueryResult(res?.recordset ?? [], {
    rowCount: res?.rowsAffected,
    rowsAffected: res?.rowsAffected ?? [],
    out: res?.output ?? {},
    raw: res,
  });
}

async function runOracleRoutineWithOutputs(adapter, name, routineArgs) {
  const pool = adapter?.pool;
  if (!pool || typeof pool.getConnection !== 'function') {
    throw new Error('oracle: OUT/INOUT routine params require a connection-capable pool');
  }
  const qn = quoteIdent('oracle', String(name));
  const driver = (typeof adapter?.importDriver === 'function')
    ? await adapter.importDriver('oracledb')
    : { BIND_OUT: 'BIND_OUT', BIND_INOUT: 'BIND_INOUT', STRING: 'STRING' };

  const binds = {};
  const placeholders = [];

  for (const arg of routineArgs) {
    const bindName = arg.name;
    placeholders.push(`:${bindName}`);
    if (arg.mode === 'in') {
      binds[bindName] = arg.value;
      continue;
    }
    const type = arg.type ?? inferOracleBindType(arg.value, driver);
    const base = { type };
    if (arg.size != null) base.maxSize = arg.size;
    else if (type === driver.STRING) base.maxSize = 4000;

    if (arg.mode === 'out') {
      binds[bindName] = { ...base, dir: driver.BIND_OUT };
    } else {
      binds[bindName] = { ...base, dir: driver.BIND_INOUT, val: arg.value };
    }
  }

  const sql = placeholders.length ? `BEGIN ${qn}(${placeholders.join(', ')}); END;` : `BEGIN ${qn}; END;`;
  const conn = await pool.getConnection();
  try {
    const res = await conn.execute(sql, binds, { autoCommit: true });
    return makeRawQueryResult([], {
      rowCount: 0,
      out: normalizeOracleOutValues(res?.outBinds, routineArgs),
      raw: res,
    });
  } finally {
    try { await conn.close(); } catch {}
  }
}

function isRoutineArgDescriptor(arg) {
  return arg != null
    && typeof arg === 'object'
    && !Array.isArray(arg)
    && (
      Object.prototype.hasOwnProperty.call(arg, 'mode')
      || Object.prototype.hasOwnProperty.call(arg, 'type')
      || Object.prototype.hasOwnProperty.call(arg, 'size')
      || (
        Object.prototype.hasOwnProperty.call(arg, 'value')
        && Object.prototype.hasOwnProperty.call(arg, 'name')
      )
    );
}

function normalizeRoutineMode(mode) {
  const normalized = String(mode ?? 'in').trim().toLowerCase();
  if (normalized === 'in' || normalized === 'out' || normalized === 'inout') return normalized;
  throw new Error(`Unsupported routine param mode: ${mode}`);
}

function normalizeRoutineKind(kind) {
  const normalized = String(kind ?? 'procedure').trim().toLowerCase();
  if (normalized === 'procedure' || normalized === 'function') return normalized;
  throw new Error(`Unsupported routine kind: ${kind}`);
}

function normalizeRoutineParamName(name, index) {
  const raw = String(name ?? `p${index + 1}`).trim();
  if (!raw) return `p${index + 1}`;
  return raw.replace(/[^A-Za-z0-9_]/g, '_');
}

function mapPgOutValues(row, routineArgs) {
  if (!row || typeof row !== 'object') return {};
  const out = {};
  for (const arg of routineArgs) {
    if (arg.mode === 'in') continue;
    const value = pickObjectValue(row, arg.name);
    if (value !== undefined) out[arg.name] = value;
  }
  return out;
}

function attachOut(result, out) {
  if (!result || typeof result !== 'object') return result;
  defineHidden(result, 'out', out ?? {});
  return result;
}

function findLastMySqlRowset(raw) {
  if (!Array.isArray(raw)) return null;
  for (let i = raw.length - 1; i >= 0; i--) {
    if (Array.isArray(raw[i])) return raw[i];
  }
  return null;
}

function inferOracleBindType(value, driver) {
  if (typeof value === 'number') return driver.NUMBER ?? driver.STRING;
  if (value instanceof Date) return driver.DATE ?? driver.STRING;
  return driver.STRING;
}

function normalizeOracleOutValues(outBinds, routineArgs) {
  if (outBinds == null || typeof outBinds !== 'object') return {};
  const out = {};
  for (const arg of routineArgs) {
    if (arg.mode === 'in') continue;
    if (Object.prototype.hasOwnProperty.call(outBinds, arg.name)) {
      out[arg.name] = outBinds[arg.name];
    }
  }
  return out;
}

function pickObjectValue(row, key) {
  if (!row || typeof row !== 'object') return undefined;
  if (Object.prototype.hasOwnProperty.call(row, key)) return row[key];
  const folded = String(key).toLowerCase();
  for (const k of Object.keys(row)) {
    if (String(k).toLowerCase() === folded) return row[k];
  }
  return undefined;
}

function defineHidden(target, key, value) {
  Object.defineProperty(target, key, {
    value,
    enumerable: false,
    configurable: true,
    writable: false,
  });
}
