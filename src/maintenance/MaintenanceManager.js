import { SchemaBuilder } from '../core/SchemaBuilder.js';

/**
 * Maintenance helper for table/database admin operations.
 * Methods execute immediately and return driver results.
 */
export class MaintenanceManager {
  /**
   * @param {import('../core/DB.js').DB} db
   */
  constructor(db) {
    this.db = db;
  }

  _schema() {
    return new SchemaBuilder(this.db.adapter);
  }

  truncateTable(name, opts = {}) {
    return this._schema().truncateTable(name, opts).exec();
  }

  analyzeTable(name, opts = {}) {
    return this._schema().analyzeTable(name, opts).exec();
  }

  optimizeTable(name, opts = {}) {
    return this._schema().optimizeTable(name, opts).exec();
  }

  vacuumTable(name, opts = {}) {
    return this._schema().vacuumTable(name, opts).exec();
  }

  vacuumDatabase(opts = {}) {
    return this._schema().vacuumDatabase(opts).exec();
  }

  reindexTable(name, opts = {}) {
    return this._schema().reindexTable(name, opts).exec();
  }

  repairTable(name, opts = {}) {
    return this._schema().repairTable(name, opts).exec();
  }
}
