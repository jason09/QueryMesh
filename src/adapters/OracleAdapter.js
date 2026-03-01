import { BaseAdapter } from './BaseAdapter.js';
import path from 'path';

/**
 * Oracle adapter using `oracledb`.
 *
 * Note: Oracle uses named binds like :p1, :p2.
 */
export class OracleAdapter extends BaseAdapter {
  /**
   * @param {import('oracledb').Pool} pool
   */
  constructor(pool, options = {}) {
    super('oracle', options);
    this.pool = pool;
  }

  placeholder(index) {
    return `:p${index}`;
  }

  supportsReturning() { return false; }

  async execute(qb) {
    const { sql, params } = qb.compile();
    const conn = await this.pool.getConnection();
    try {
      const binds = {};
      for (let i = 0; i < params.length; i++) binds[`p${i + 1}`] = params[i];
      const res = await conn.execute(sql, binds, { autoCommit: qb._type !== 'select' });
      if (qb._type === 'select') {
        // outFormat object (if configured) is recommended, but we keep generic
        return res.rows ?? [];
      }
      return { rowsAffected: res.rowsAffected };
    } finally {
      try { await conn.close(); } catch {}
    }
  }

  _oracleAuth() {
    const c = this.config ?? {};
    const user = c.user ? String(c.user) : '';
    const password = c.password ? String(c.password) : '';
    const connectString = c.connectString ? String(c.connectString) : '';
    if (user && password && connectString) return `${user}/${password}@${connectString}`;
    return '';
  }

  /**
   * @param {Record<string, any>} options
   */
  getExportCommand(options = {}) {
    const file = String(options.file ?? '');
    const dumpFile = file ? path.basename(file) : 'export.dmp';
    const dir = String(options.oracleDirectory ?? this.config?.oracleDirectory ?? 'DATA_PUMP_DIR');
    const logFile = `${dumpFile}.log`;

    const args = [];
    const auth = this._oracleAuth();
    if (auth) args.push(auth);
    args.push(`DIRECTORY=${dir}`);
    args.push(`DUMPFILE=${dumpFile}`);
    args.push(`LOGFILE=${logFile}`);

    const tables = normalizeList(options.tables ?? options.table);
    const schemas = normalizeList(options.schemas ?? options.schema);
    if (tables.length) args.push(`TABLES=${tables.join(',')}`);
    if (schemas.length) args.push(`SCHEMAS=${schemas.join(',')}`);
    if (options.schemaOnly) args.push('CONTENT=METADATA_ONLY');
    if (options.dataOnly) args.push('CONTENT=DATA_ONLY');

    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.oracleArgs));
    args.push(...normalizeList(options.oracleDumpArgs));

    return {
      cmd: 'expdp',
      args,
      outputFile: file || undefined,
    };
  }

  /**
   * @param {Record<string, any>} options
   */
  getImportCommand(options = {}) {
    const file = String(options.file ?? '');
    const dumpFile = file ? path.basename(file) : 'import.dmp';
    const dir = String(options.oracleDirectory ?? this.config?.oracleDirectory ?? 'DATA_PUMP_DIR');
    const logFile = `${dumpFile}.log`;

    const args = [];
    const auth = this._oracleAuth();
    if (auth) args.push(auth);
    args.push(`DIRECTORY=${dir}`);
    args.push(`DUMPFILE=${dumpFile}`);
    args.push(`LOGFILE=${logFile}`);

    if (options.schemaOnly) args.push('CONTENT=METADATA_ONLY');
    if (options.dataOnly) args.push('CONTENT=DATA_ONLY');

    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.oracleArgs));
    args.push(...normalizeList(options.oracleImportArgs));

    return {
      cmd: 'impdp',
      args,
      inputFile: file,
      pipeStdin: false,
    };
  }

  /**
   * Transaction using a dedicated connection.
   * @param {(trxDb: import('../core/DB.js').DB) => Promise<any>} fn
   */
  async transaction(fn) {
    const conn = await this.pool.getConnection();
    try {
      const { DB } = await import('../core/DB.js');
      const trxAdapter = new OracleAdapter({ getConnection: async () => conn }, { features: this.features, config: this.config });
      // Override execute to not autoCommit
      trxAdapter.execute = async (qb) => {
        const { sql, params } = qb.compile();
        const binds = {};
        for (let i = 0; i < params.length; i++) binds[`p${i + 1}`] = params[i];
        const res = await conn.execute(sql, binds, { autoCommit: false });
        if (qb._type === 'select') return res.rows ?? [];
        return { rowsAffected: res.rowsAffected };
      };
      const trxDb = new DB(trxAdapter);
      const out = await fn(trxDb);
      await conn.commit();
      return out;
    } catch (e) {
      try { await conn.rollback(); } catch {}
      throw e;
    } finally {
      try { await conn.close(); } catch {}
    }
  }

  async close() {
    if (typeof this.pool.close === 'function') {
      await this.pool.close(0);
    }
  }

  /**
   * Switch Oracle database/service by creating a new pool.
   * Note: Oracle does not have a simple USE <db>. Pass service/connection target.
   * @param {string} name
   * @param {{closeCurrent?: boolean, connectString?: string}} [opts]
   */
  async switchDatabase(name, opts = {}) {
    const target = String(opts.connectString ?? name ?? '').trim();
    if (!target) throw new Error('oracle: switchDatabase(name) requires a target connectString/service');

    const cfg = { ...(this.config ?? {}), connectString: target };
    const oracledb = await this.importDriver('oracledb');
    const pool = await oracledb.createPool(cfg);
    const next = new OracleAdapter(pool, { features: this.features, config: cfg });

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
