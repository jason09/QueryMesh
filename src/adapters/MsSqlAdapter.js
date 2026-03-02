import { BaseAdapter } from './BaseAdapter.js';

/**
 * SQL Server adapter using `mssql`.
 */
export class MsSqlAdapter extends BaseAdapter {
  /**
   * @param {{request:()=>any}} pool
   */
  constructor(pool, options = {}) {
    super('mssql', options);
    this.pool = pool;
  }

  placeholder(index) {
    return `@p${index}`;
  }

  supportsReturning() { return true; }

  async execute(qb) {
    const { sql, params } = qb.compile();
    const req = this.pool.request();
    for (let i = 0; i < params.length; i++) req.input(`p${i + 1}`, params[i]);
    const res = await req.query(sql);
    if (qb._type === 'select') return res.recordset;
    if (qb._returning) return res.recordset ?? [];
    return { rowsAffected: res.rowsAffected };
  }

  _serverName() {
    const c = this.config ?? {};
    return c.server ? String(c.server) : 'localhost';
  }

  _databaseName(options = {}) {
    const c = this.config ?? {};
    return String(options.database ?? c.database ?? '');
  }

  /**
   * @param {Record<string, any>} options
   */
  getExportCommand(options = {}) {
    const c = this.config ?? {};
    const format = String(options.format ?? 'plain');
    const file = String(options.file ?? '');
    const dbName = this._databaseName(options);

    if (format === 'bacpac' || format === 'binary') {
      const args = [
        '/Action:Export',
        `/SourceServerName:${this._serverName()}`,
      ];
      if (dbName) args.push(`/SourceDatabaseName:${dbName}`);
      if (c.user) args.push(`/SourceUser:${String(c.user)}`);
      if (c.password) args.push(`/SourcePassword:${String(c.password)}`);
      if (file) args.push(`/TargetFile:${file}`);

      args.push(...normalizeList(options.extraArgs ?? options.args ?? options.mssqlArgs));
      args.push(...normalizeList(options.bacpacArgs));

      return {
        cmd: 'sqlpackage',
        args,
        outputFile: file || undefined,
      };
    }

    const query = dbName && file
      ? `BACKUP DATABASE [${dbName.replace(/]/g, ']]')}] TO DISK = N'${file.replace(/'/g, "''")}' WITH INIT`
      : '';
    const args = ['-S', this._serverName()];
    if (c.user) {
      args.push('-U', String(c.user));
      if (c.password) args.push('-P', String(c.password));
    } else {
      args.push('-E');
    }
    if (query) args.push('-Q', query);
    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.mssqlArgs));

    return {
      cmd: 'sqlcmd',
      args,
      outputFile: file || undefined,
    };
  }

  /**
   * @param {Record<string, any>} options
   */
  getImportCommand(options = {}) {
    const c = this.config ?? {};
    const format = String(options.format ?? 'plain');
    const file = String(options.file ?? '');
    const dbName = this._databaseName(options);

    if (format === 'bacpac' || format === 'binary') {
      const args = [
        '/Action:Import',
        `/TargetServerName:${this._serverName()}`,
      ];
      if (dbName) args.push(`/TargetDatabaseName:${dbName}`);
      if (c.user) args.push(`/TargetUser:${String(c.user)}`);
      if (c.password) args.push(`/TargetPassword:${String(c.password)}`);
      if (file) args.push(`/SourceFile:${file}`);

      args.push(...normalizeList(options.extraArgs ?? options.args ?? options.mssqlArgs));
      args.push(...normalizeList(options.bacpacArgs));

      return {
        cmd: 'sqlpackage',
        args,
        inputFile: file,
        pipeStdin: false,
      };
    }

    const query = dbName && file
      ? `RESTORE DATABASE [${dbName.replace(/]/g, ']]')}] FROM DISK = N'${file.replace(/'/g, "''")}' WITH REPLACE`
      : '';
    const args = ['-S', this._serverName()];
    if (c.user) {
      args.push('-U', String(c.user));
      if (c.password) args.push('-P', String(c.password));
    } else {
      args.push('-E');
    }
    if (query) args.push('-Q', query);
    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.mssqlArgs));
    args.push(...normalizeList(options.sqlcmdArgs));

    return {
      cmd: 'sqlcmd',
      args,
      inputFile: file,
      pipeStdin: false,
    };
  }

  /**
   * Transaction using mssql.Transaction.
   * @param {(trxDb: import('../core/DB.js').DB) => Promise<any>} fn
   */
  async transaction(fn) {
    const mssql = await import('mssql');
    const tx = new mssql.Transaction(/** @type {any} */(this.pool));
    await tx.begin();
    try {
      const { DB } = await import('../core/DB.js');
      const trxAdapter = new MsSqlAdapter({ request: () => tx.request() }, { features: this.features, config: this.config });
      const trxDb = new DB(trxAdapter);
      const out = await fn(trxDb);
      await tx.commit();
      return out;
    } catch (e) {
      try { await tx.rollback(); } catch {}
      throw e;
    }
  }

  async close() {
    if (typeof this.pool.close === 'function') {
      await this.pool.close();
    }
  }

  /**
   * Switch SQL Server database by opening a new pool with updated config.
   * @param {string} name
   * @param {{closeCurrent?: boolean}} [opts]
   */
  async switchDatabase(name, opts = {}) {
    const dbName = String(name ?? '').trim();
    if (!dbName) throw new Error('mssql: switchDatabase(name) requires a database name');

    const cfg = { ...(this.config ?? {}), database: dbName };
    const mssql = await this.importDriver('mssql');
    let pool;
    if (typeof mssql.ConnectionPool === 'function') {
      pool = new mssql.ConnectionPool(cfg);
      await pool.connect();
    } else {
      pool = await mssql.connect(cfg);
    }
    const next = new MsSqlAdapter(pool, { features: this.features, config: cfg });

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
