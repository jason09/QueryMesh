import { QueryBuilder } from './QueryBuilder.js';
import { raw } from './Raw.js';
import { id } from './Identifier.js';
import { SchemaBuilder } from './SchemaBuilder.js';
import { quoteIdent } from '../utils/identifiers.js';
import { BackupManager } from '../backup/BackupManager.js';
import { ToolsManager } from '../tools/ToolsManager.js';

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
   * @param {'pg'|'mysql'|'mssql'|'oracle'|'mongo'|'mongodb'} dialect
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

    const features = opts.features ?? this.adapter?.features ?? {};
    const importer = opts.importer ?? features?.driverImporter;
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
   * @param {'pg'|'mysql'|'mssql'|'oracle'|'mongo'|'mongodb'} dialect
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
