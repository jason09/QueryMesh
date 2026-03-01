import { BaseAdapter } from './BaseAdapter.js';
import { promisify } from 'util';

/**
 * MySQL adapter using `mysql`.
 */
export class MySqlAdapter extends BaseAdapter {
  /**
   * @param {import('mysql').Pool|import('mysql').PoolConnection} driver
   */
  constructor(driver, options = {}) {
    super('mysql', options);
    this.driver = driver;
    this.queryAsync = promisify(driver.query).bind(driver);
    this._hasGetConnection = typeof driver.getConnection === 'function';
  }

  supportsReturning() { return false; }

  async execute(qb) {
    const { sql, params } = qb.compile();
    const res = await this.queryAsync(sql, params);
    if (qb._type === 'select') return res;
    return res;
  }

  _mysqlEnv() {
    const c = this.config ?? {};
    const env = {};
    if (c.password != null) env.MYSQL_PWD = String(c.password);
    return env;
  }

  _mysqlConnArgs() {
    const c = this.config ?? {};
    const args = [];
    if (c.host) args.push('-h', String(c.host));
    if (c.port) args.push('-P', String(c.port));
    if (c.user) args.push('-u', String(c.user));
    return args;
  }

  /**
   * @param {Record<string, any>} options
   */
  getExportCommand(options = {}) {
    const c = this.config ?? {};
    const args = [...this._mysqlConnArgs()];

    const dbName = String(options.database ?? c.database ?? '');
    if (options.schemaOnly) args.push('--no-data');
    if (options.dataOnly) args.push('--no-create-info');
    if (options.clean) args.push('--add-drop-table');
    if (options.inserts) args.push('--skip-extended-insert');
    if (options.columnInserts) args.push('--complete-insert');

    const useStdout = options.useStdout === true;
    const outputFilePath = options.file ? String(options.file) : undefined;
    if (!useStdout && outputFilePath) args.push(`--result-file=${outputFilePath}`);

    const tables = normalizeList(options.tables ?? options.table);
    if (options.create && dbName) {
      args.push('--databases', dbName);
      args.push(...tables);
    } else {
      if (dbName) args.push(dbName);
      args.push(...tables);
    }

    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.mysqlArgs));
    args.push(...normalizeList(options.mysqlDumpArgs));

    return {
      cmd: 'mysqldump',
      args,
      env: this._mysqlEnv(),
      useStdout,
      outputFilePath,
      outputFile: !useStdout && outputFilePath ? outputFilePath : undefined,
    };
  }

  /**
   * @param {Record<string, any>} options
   */
  getImportCommand(options = {}) {
    const c = this.config ?? {};
    const args = [...this._mysqlConnArgs()];
    const dbName = String(options.database ?? c.database ?? '');
    if (dbName) args.push(dbName);

    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.mysqlArgs));
    args.push(...normalizeList(options.mysqlImportArgs));

    return {
      cmd: 'mysql',
      args,
      env: this._mysqlEnv(),
      inputFile: String(options.file ?? ''),
      pipeStdin: true,
    };
  }

  /**
   * Transaction using getConnection/beginTransaction.
   * @param {(trxDb: import('../core/DB.js').DB) => Promise<any>} fn
   */
  async transaction(fn) {
    if (!this._hasGetConnection) {
      throw new Error('MySqlAdapter transaction requires a Pool (not a single connection)');
    }
    const getConn = promisify(this.driver.getConnection).bind(this.driver);
    const conn = await getConn();
    const begin = promisify(conn.beginTransaction).bind(conn);
    const commit = promisify(conn.commit).bind(conn);
    const rollback = promisify(conn.rollback).bind(conn);

    try {
      await begin();
      const { DB } = await import('../core/DB.js');
      const trxAdapter = new MySqlAdapter(conn, { features: this.features, config: this.config });
      const trxDb = new DB(trxAdapter);
      const out = await fn(trxDb);
      await commit();
      return out;
    } catch (e) {
      try { await rollback(); } catch {}
      throw e;
    } finally {
      conn.release();
    }
  }

  async close() {
    if (typeof this.driver.end === 'function') {
      const end = promisify(this.driver.end).bind(this.driver);
      await end();
    }
  }

  /**
   * Switch MySQL database by creating a new pool.
   * @param {string} name
   * @param {{closeCurrent?: boolean}} [opts]
   */
  async switchDatabase(name, opts = {}) {
    const dbName = String(name ?? '').trim();
    if (!dbName) throw new Error('mysql: switchDatabase(name) requires a database name');

    const cfg = { ...(this.config ?? {}), database: dbName };
    const mysql = await this.importDriver('mysql');
    const pool = mysql.createPool(cfg);
    const next = new MySqlAdapter(pool, { features: this.features, config: cfg });

    if (opts.closeCurrent !== false) {
      try { await this.close(); } catch {}
    }
    return next;
  }
}

function normalizeList(v) {
  if (!v) return [];
  return Array.isArray(v) ? v.map(String) : [String(v)];
}
