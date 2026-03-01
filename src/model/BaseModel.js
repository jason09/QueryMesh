/**
 * BaseModel provides a light ORM-ish layer on top of SQuery.
 *
 * You typically extend it:
 *
 * class User extends BaseModel {
 *   static table = 'users';
 *   static primaryKey = 'id';
 * }
 */
export class BaseModel {
  /** @type {import('../core/DB.js').DB} */
  static db;
  static table = '';
  static primaryKey = 'id';
  static timestamps = false;

  /**
   * @param {Record<string, any>} [attrs]
   */
  constructor(attrs = {}) {
    Object.assign(this, attrs);
    this._isNew = true;
  }

  /**
   * Bind a DB instance to the model class.
   * @param {import('../core/DB.js').DB} db
   */
  static bind(db) {
    this.db = db;
    return this;
  }

  /**
   * Start a query builder for this model.
   */
  static query() {
    if (!this.db) throw new Error('Model is not bound to a DB instance');
    return this.db.table(this.table);
  }

  /**
   * Find by primary key.
   * @param {any} id
   */
  static async find(id) {
    const row = await this.query().where(this.primaryKey, id).first();
    return row ? this.hydrate(row) : null;
  }

  /**
   * Return all rows.
   */
  static async all() {
    const rows = await this.query().get();
    return rows.map(r => this.hydrate(r));
  }

  /**
   * Create a row.
   * @param {Record<string, any>} data
   */
  static async create(data) {
    const toInsert = this._withTimestampsOnCreate(data);
    let q = this.query().insert(toInsert);
    if (this.db.adapter.supportsReturning()) {
      q = q.returning('*');
      const rows = await q.run();
      return this.hydrate(rows[0]);
    }
    const res = await q.run();
    if (this.db.adapter?.dialect === 'mongo') {
      const row = { ...toInsert };
      this._applyInsertMetadata(row, res);
      return this.hydrate(row);
    }
    return { result: res };
  }

  /**
   * Simple where shortcut returning model instances.
   * @param {string} col
   * @param {any} val
   */
  static async where(col, val) {
    const rows = await this.query().where(col, val).get();
    return rows.map(r => this.hydrate(r));
  }

  /**
   * Hydrate a model instance from a DB row.
   * @param {Record<string, any>} row
   */
  static hydrate(row) {
    const m = new this(row);
    m._isNew = false;
    return m;
  }

  /**
   * Persist the instance (insert or update).
   */
  async save() {
    const C = /** @type {typeof BaseModel} */ (this.constructor);
    const pk = C.primaryKey;

    const data = { ...this };
    delete data._isNew;

    if (this._isNew || this[pk] == null) {
      const toInsert = C._withTimestampsOnCreate(data);
      let q = C.query().insert(toInsert);
      if (C.db.adapter.supportsReturning()) {
        q = q.returning('*');
        const rows = await q.run();
        Object.assign(this, rows[0]);
      } else {
        const res = await q.run();
        if (C.db.adapter?.dialect === 'mongo') {
          C._applyInsertMetadata(this, res);
        }
      }
      this._isNew = false;
      return this;
    }

    const toUpdate = C._withTimestampsOnUpdate(data);
    delete toUpdate[pk];
    let q = C.query().update(toUpdate).where(pk, this[pk]);
    if (C.db.adapter.supportsReturning()) {
      q = q.returning('*');
      const rows = await q.run();
      Object.assign(this, rows[0]);
    } else {
      await q.run();
    }
    return this;
  }

  /**
   * Delete this instance.
   */
  async delete() {
    const C = /** @type {typeof BaseModel} */ (this.constructor);
    const pk = C.primaryKey;
    if (this[pk] == null) throw new Error('Cannot delete without primary key');
    await C.query().delete().where(pk, this[pk]).run();
    return true;
  }

  static _withTimestampsOnCreate(data) {
    if (!this.timestamps) return data;
    const now = new Date();
    return { ...data, created_at: data.created_at ?? now, updated_at: data.updated_at ?? now };
  }

  static _withTimestampsOnUpdate(data) {
    if (!this.timestamps) return data;
    const now = new Date();
    return { ...data, updated_at: now };
  }

  /**
   * Apply adapter insert metadata (e.g., Mongo insertedId) onto a model-like object.
   * @param {Record<string, any>} target
   * @param {Record<string, any>} result
   */
  static _applyInsertMetadata(target, result) {
    if (!result || typeof result !== 'object') return;
    const insertedId = result.insertedId;
    if (insertedId == null) return;
    const pk = this.primaryKey || 'id';
    if (target[pk] == null) target[pk] = insertedId;
    if (pk !== '_id' && target._id == null) target._id = insertedId;
  }
}
