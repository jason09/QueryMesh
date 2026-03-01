/**
 * BaseAdapter defines the interface adapters must implement.
 */
export class BaseAdapter {
  /**
   * @param {'pg'|'mysql'|'mssql'|'oracle'|'mongo'} dialect
   * @param {{features?: Record<string, any>, config?: Record<string, any>}} [options]
   */
  constructor(dialect, options = {}) {
    this.dialect = dialect;
    this.features = options.features ?? {};
    this.config = options.config ?? {};
  }

  /**
   * Return placeholder for param index (1-based).
   * @param {number} index
   */
  placeholder(index) {
    // MySQL / MSSQL / Oracle use '?', PG uses '$n'. Mongo not used.
    return this.dialect === 'pg' ? `$${index}` : '?';
  }

  /**
   * Whether adapter supports RETURNING clause.
   */
  supportsReturning() {
    return this.dialect === 'pg';
  }

  /**
   * Execute a query builder.
   * @param {import('../core/QueryBuilder.js').QueryBuilder} qb
   */
  async execute(qb) {
    throw new Error('Not implemented');
  }

  /**
   * Run a transaction.
   * @param {(trx: any) => Promise<any>} fn
   */
  async transaction(fn) {
    throw new Error('Transactions not implemented for this adapter');
  }

  /**
   * Build CLI export command for BackupManager.
   * @param {Record<string, any>} _options
   */
  getExportCommand(_options = {}) {
    return null;
  }

  /**
   * Build CLI import command for BackupManager.
   * @param {Record<string, any>} _options
   */
  getImportCommand(_options = {}) {
    return null;
  }

  /**
   * Close the underlying client/pool where applicable.
   */
  async close() {
    return undefined;
  }

  /**
   * Switch to another database/context.
   * Adapter should return a new adapter instance configured for the target database.
   * @param {string} _name
   * @param {Record<string, any>} [_opts]
   */
  async switchDatabase(_name, _opts = {}) {
    throw new Error(`${this.dialect}: switchDatabase is not implemented`);
  }

  /**
   * Load runtime driver module.
   * @param {string} packageName
   */
  async importDriver(packageName) {
    const importer = (this.features && typeof this.features.driverImporter === 'function')
      ? this.features.driverImporter
      : (name => import(name));
    return importer(packageName);
  }
}
