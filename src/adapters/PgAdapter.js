import { BaseAdapter } from './BaseAdapter.js';

/**
 * PostgreSQL adapter using `pg`.
 */
export class PgAdapter extends BaseAdapter {
  /**
   * @param {{query:(sql:string, params?:any[])=>Promise<any>, connect?:()=>Promise<any>, end?:()=>Promise<void>}} pool
   */
  constructor(pool, options = {}) {
    super('pg', options);
    this.pool = pool;
  }

  supportsReturning() { return true; }

  async execute(qb) {
    const { sql, params } = qb.compile();
    const res = await this.pool.query(sql, params);
    if (qb._type === 'select') return res.rows;
    if (qb._returning) return res.rows;
    return { rowCount: res.rowCount };
  }

  /**
   * Build PG CLI environment variables.
   */
  _pgEnv() {
    const c = this.config ?? {};
    const env = {};
    if (c.password != null) env.PGPASSWORD = String(c.password);
    return env;
  }

  /**
   * @param {Record<string, any>} options
   */
  getExportCommand(options = {}) {
    const args = [];

    const format = String(options.format ?? 'plain');
    if (format === 'custom') args.push('-Fc');
    if (format === 'directory') args.push('-Fd');
    if (format === 'tar') args.push('-Ft');

    const tables = normalizeList(options.tables ?? options.table);
    const schemas = normalizeList(options.schemas ?? options.schema);
    const excludeTables = normalizeList(options.excludeTables ?? options.excludeTable);
    for (const t of tables) args.push(`--table=${t}`);
    for (const s of schemas) args.push(`--schema=${s}`);
    for (const t of excludeTables) args.push(`--exclude-table=${t}`);

    if (options.schemaOnly) args.push('--schema-only');
    if (options.dataOnly) args.push('--data-only');
    if (options.create) args.push('--create');
    if (options.clean) args.push('--clean');
    if (options.ifExists) args.push('--if-exists');
    if (options.inserts) args.push('--inserts');
    if (options.columnInserts) args.push('--column-inserts');
    if (Number.isInteger(options.rowsPerInsert) && options.rowsPerInsert > 0) {
      args.push(`--rows-per-insert=${options.rowsPerInsert}`);
    }
    if (options.largeObjects || options.blobs) args.push('--blobs');

    const dbArg = this._pgDbArg();
    if (dbArg) args.push(dbArg);

    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.pgArgs));
    args.push(...normalizeList(options.pgDumpArgs));

    const useStdout = options.useStdout === true;
    const outputFilePath = options.file ? String(options.file) : undefined;
    if (!useStdout && outputFilePath) {
      args.push('-f', outputFilePath);
    }

    return {
      cmd: 'pg_dump',
      args,
      env: this._pgEnv(),
      useStdout,
      outputFilePath,
      outputFile: !useStdout && outputFilePath && format !== 'directory' ? outputFilePath : undefined,
      outputDir: !useStdout && outputFilePath && format === 'directory' ? outputFilePath : undefined,
    };
  }

  /**
   * @param {Record<string, any>} options
   */
  getImportCommand(options = {}) {
    const c = this.config ?? {};
    const format = String(options.format ?? 'plain');
    const file = String(options.file ?? '');

    if (format === 'custom' || format === 'directory' || format === 'tar') {
      const args = [];
      const dbArg = this._pgDbArg();
      if (dbArg) args.push(dbArg);

      if (options.clean) args.push('--clean');
      if (options.ifExists) args.push('--if-exists');
      if (options.create) args.push('--create');

      args.push(...normalizeList(options.extraArgs ?? options.args ?? options.pgArgs));
      args.push(...normalizeList(options.restoreArgs));

      const canStream = (format === 'custom' || format === 'tar') && options.pipeStdin !== false;
      if (canStream) {
        if (format === 'custom') args.push('-Fc');
        if (format === 'tar') args.push('-Ft');
      } else if (file) {
        args.push(file);
      }
      return {
        cmd: 'pg_restore',
        args,
        env: this._pgEnv(),
        inputFile: file,
        pipeStdin: canStream,
      };
    }

    const args = [];
    if (c.host) args.push('-h', String(c.host));
    if (c.port) args.push('-p', String(c.port));
    if (c.user) args.push('-U', String(c.user));
    if (c.database) args.push(String(c.database));

    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.pgArgs));
    args.push(...normalizeList(options.psqlArgs));

    return {
      cmd: 'psql',
      args,
      env: this._pgEnv(),
      inputFile: file,
      pipeStdin: true,
    };
  }

  _pgDbArg() {
    const c = this.config ?? {};
    if (c.connectionString) return `--dbname=${String(c.connectionString)}`;

    const args = [];
    if (c.host) args.push(`host=${String(c.host)}`);
    if (c.port) args.push(`port=${String(c.port)}`);
    if (c.user) args.push(`user=${String(c.user)}`);
    if (c.database) args.push(`dbname=${String(c.database)}`);
    if (!args.length) return null;
    return `--dbname=${args.join(' ')}`;
  }

  /**
   * Transaction using BEGIN/COMMIT/ROLLBACK.
   * @param {(trxDb: import('../core/DB.js').DB) => Promise<any>} fn
   */
  async transaction(fn) {
    if (!this.pool.connect) throw new Error('PgAdapter transaction requires a Pool with connect()');
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { DB } = await import('../core/DB.js');
      const trxAdapter = new PgAdapter({ query: (s, p) => client.query(s, p) }, { features: this.features, config: this.config });
      const trxDb = new DB(trxAdapter);
      const out = await fn(trxDb);
      await client.query('COMMIT');
      return out;
    } catch (e) {
      try { await client.query('ROLLBACK'); } catch {}
      throw e;
    } finally {
      client.release();
    }
  }

  async close() {
    if (typeof this.pool.end === 'function') {
      await this.pool.end();
    }
  }

  /**
   * Switch PostgreSQL database by creating a new Pool.
   * @param {string} name
   * @param {{closeCurrent?: boolean}} [opts]
   */
  async switchDatabase(name, opts = {}) {
    const dbName = String(name ?? '').trim();
    if (!dbName) throw new Error('pg: switchDatabase(name) requires a database name');

    const cfg = { ...(this.config ?? {}) };
    if (cfg.connectionString) {
      try {
        const u = new URL(String(cfg.connectionString));
        u.pathname = `/${encodeURIComponent(dbName)}`;
        cfg.connectionString = u.toString();
      } catch {
        cfg.database = dbName;
      }
    } else {
      cfg.database = dbName;
    }

    const { Pool } = await this.importDriver('pg');
    const pool = new Pool(cfg);
    const next = new PgAdapter(pool, { features: this.features, config: cfg });

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
