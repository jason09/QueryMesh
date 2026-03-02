import { BaseAdapter } from './BaseAdapter.js';

/**
 * MongoDB adapter using `mongodb`.
 *
 * Important: SQuery still uses the same fluent builder, but instead of compiling
 * to SQL it compiles to a Mongo operation descriptor and executes real Mongo commands.
 */
export class MongoAdapter extends BaseAdapter {
  /**
   * @param {import('mongodb').Db} db
   */
  constructor(db, options = {}) {
    super('mongo', options);
    this.db = db;
  }

  placeholder() { return '?'; }
  supportsReturning() { return false; }

  async execute(qb) {
    const { mongo } = qb.compile();
    return this._executeMongoOperation(qb, mongo);
  }

  async _executeMongoOperation(qb, mongo, opts = {}) {
    const col = this.db.collection(mongo.collection);
    const session = opts.session;
    const opOpts = session ? { session } : {};
    const wantsReturning = hasReturning(qb?._returning);
    const returningProjection = wantsReturning
      ? buildReturningProjection(qb._returning, mongo.collection)
      : null;

    switch (mongo.op) {
      case 'find': {
        let cursor = col.find(mongo.filter ?? {}, opOpts);
        if (mongo.projection) cursor = cursor.project(mongo.projection);
        if (mongo.sort) cursor = cursor.sort(mongo.sort);
        if (mongo.skip != null) cursor = cursor.skip(mongo.skip);
        if (mongo.limit != null) cursor = cursor.limit(mongo.limit);
        return await cursor.toArray();
      }
      case 'aggregate': {
        const cursor = col.aggregate(mongo.pipeline ?? [], opOpts);
        return await cursor.toArray();
      }
      case 'union': {
        let out = await this._executeMongoOperation({ _returning: null }, mongo.base, opts);
        if (!Array.isArray(out)) {
          throw new Error('Mongo union base query did not return rows');
        }
        for (const u of mongo.unions ?? []) {
          const rhs = await this._executeMongoOperation({ _returning: null }, u.source, opts);
          if (!Array.isArray(rhs)) {
            throw new Error('Mongo union source query did not return rows');
          }
          out = out.concat(rhs);
          if (!u.all) out = dedupeDocs(out);
        }
        return out;
      }
      case 'insertSelect': {
        const rows = await this._executeMongoOperation({ _returning: null }, mongo.source, opts);
        if (!Array.isArray(rows)) {
          throw new Error('Mongo insertSelect source query did not return rows');
        }

        const docs = rows.map(row => mapInsertSelectRow(row, mongo.mappings ?? []));
        if (!docs.length) {
          return wantsReturning ? [] : { insertedCount: 0 };
        }

        if (docs.length === 1) {
          const res = await col.insertOne(docs[0], opOpts);
          if (wantsReturning) {
            const row = await findOneProjected(col, { _id: res.insertedId }, returningProjection, opOpts);
            return row ? [row] : [];
          }
          return { insertedId: res.insertedId };
        }

        const res = await col.insertMany(docs, opOpts);
        if (wantsReturning) {
          const ids = Object.values(res.insertedIds ?? {});
          if (!ids.length) return [];
          const orderedProjection = projectionWithIdForOrdering(returningProjection);
          const insertedRows = await findManyProjected(col, { _id: { $in: ids } }, orderedProjection, opOpts);
          return stripIdIfExcluded(sortRowsByIdOrder(insertedRows, ids), returningProjection);
        }
        return { insertedIds: res.insertedIds };
      }
      case 'insertOne': {
        const doc = (mongo.docs ?? [])[0];
        const res = await col.insertOne(doc, opOpts);
        if (wantsReturning) {
          const row = await findOneProjected(col, { _id: res.insertedId }, returningProjection, opOpts);
          return row ? [row] : [];
        }
        return { insertedId: res.insertedId };
      }
      case 'insertMany': {
        const res = await col.insertMany(mongo.docs ?? [], opOpts);
        if (wantsReturning) {
          const ids = Object.values(res.insertedIds ?? {});
          if (!ids.length) return [];
          const orderedProjection = projectionWithIdForOrdering(returningProjection);
          const rows = await findManyProjected(col, { _id: { $in: ids } }, orderedProjection, opOpts);
          return stripIdIfExcluded(sortRowsByIdOrder(rows, ids), returningProjection);
        }
        return { insertedIds: res.insertedIds };
      }
      case 'upsertOne': {
        let existingId = null;
        if (wantsReturning) {
          const existing = await findOneProjected(col, mongo.filter ?? {}, { _id: 1 }, opOpts);
          existingId = existing?._id ?? null;
        }

        const res = await col.updateOne(mongo.filter ?? {}, mongo.update ?? {}, { ...opOpts, upsert: true });

        if (wantsReturning) {
          const docId = res.upsertedId ?? existingId;
          if (docId != null) {
            const row = await findOneProjected(col, { _id: docId }, returningProjection, opOpts);
            return row ? [row] : [];
          }
          const row = await findOneProjected(col, mongo.filter ?? {}, returningProjection, opOpts);
          return row ? [row] : [];
        }

        return {
          matchedCount: res.matchedCount,
          modifiedCount: res.modifiedCount,
          upsertedCount: res.upsertedCount ?? (res.upsertedId == null ? 0 : 1),
          upsertedId: res.upsertedId ?? null,
        };
      }
      case 'updateMany': {
        let ids = null;
        if (wantsReturning) {
          ids = await findIds(col, mongo.filter ?? {}, opOpts);
        }

        const res = await col.updateMany(mongo.filter ?? {}, mongo.update ?? {}, opOpts);

        if (wantsReturning) {
          if (!ids?.length) return [];
          const orderedProjection = projectionWithIdForOrdering(returningProjection);
          const rows = await findManyProjected(col, { _id: { $in: ids } }, orderedProjection, opOpts);
          return stripIdIfExcluded(sortRowsByIdOrder(rows, ids), returningProjection);
        }

        return { matchedCount: res.matchedCount, modifiedCount: res.modifiedCount };
      }
      case 'deleteMany': {
        let rows = null;
        if (wantsReturning) {
          rows = await findManyProjected(col, mongo.filter ?? {}, returningProjection, opOpts);
        }

        const res = await col.deleteMany(mongo.filter ?? {}, opOpts);

        if (wantsReturning) {
          return rows ?? [];
        }

        return { deletedCount: res.deletedCount };
      }
      default:
        throw new Error(`Unsupported mongo operation: ${mongo.op}`);
    }
  }

  /**
   * @param {Record<string, any>} options
   */
  getExportCommand(options = {}) {
    const c = this.config ?? {};
    const args = [];
    if (c.uri) args.push(`--uri=${String(c.uri)}`);
    const dbName = options.dbName ?? c.dbName ?? this.db?.databaseName;
    if (dbName) args.push(`--db=${String(dbName)}`);

    const tables = normalizeList(options.tables ?? options.table);
    if (tables.length === 1) args.push(`--collection=${tables[0]}`);

    const file = String(options.file ?? '');
    const format = String(options.format ?? 'archive');
    if (format === 'archive') {
      args.push(file ? `--archive=${file}` : '--archive');
    } else {
      const outDir = file || String(options.out ?? 'dump');
      args.push(`--out=${outDir}`);
    }

    if (options.gzip) args.push('--gzip');
    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.mongoArgs));
    args.push(...normalizeList(options.mongoDumpArgs));

    return {
      cmd: 'mongodump',
      args,
      outputFile: format === 'archive' && file ? file : undefined,
      outputDir: format !== 'archive' && file ? file : undefined,
    };
  }

  /**
   * @param {Record<string, any>} options
   */
  getImportCommand(options = {}) {
    const c = this.config ?? {};
    const args = [];
    if (c.uri) args.push(`--uri=${String(c.uri)}`);
    const dbName = options.dbName ?? c.dbName ?? this.db?.databaseName;
    if (dbName) args.push(`--db=${String(dbName)}`);

    const tables = normalizeList(options.tables ?? options.table);
    if (tables.length === 1) args.push(`--collection=${tables[0]}`);

    const file = String(options.file ?? '');
    const format = String(options.format ?? 'archive');
    let pipeStdin = false;
    if (format === 'archive') {
      pipeStdin = options.pipeStdin !== false;
      args.push(pipeStdin ? '--archive' : (file ? `--archive=${file}` : '--archive'));
    } else if (file) {
      args.push(`--dir=${file}`);
    }

    if (options.gzip) args.push('--gzip');
    args.push(...normalizeList(options.extraArgs ?? options.args ?? options.mongoArgs));
    args.push(...normalizeList(options.mongoImportArgs));

    return {
      cmd: 'mongorestore',
      args,
      inputFile: file,
      pipeStdin,
    };
  }

  /**
   * Transaction using Mongo sessions.
   * @param {(trxDb: import('../core/DB.js').DB) => Promise<any>} fn
   */
  async transaction(fn) {
    const client = this.db.client;
    if (!client?.startSession) throw new Error('MongoAdapter transaction requires Db from MongoClient');

    const session = client.startSession();
    try {
      let out;
      await session.withTransaction(async () => {
        const { DB } = await import('../core/DB.js');
        const trxAdapter = new MongoAdapter(this.db, { features: this.features, config: this.config });
        // patch execute to use session
        trxAdapter.execute = async (qb) => {
          const { mongo } = qb.compile();
          return trxAdapter._executeMongoOperation(qb, mongo, { session });
        };
        const trxDb = new DB(trxAdapter);
        out = await fn(trxDb);
      });
      return out;
    } finally {
      await session.endSession();
    }
  }

  async close() {
    if (typeof this.db?.client?.close === 'function') {
      await this.db.client.close();
    }
  }

  /**
   * Switch Mongo database on same client.
   * @param {string} name
   * @param {{closeCurrent?: boolean}} [_opts]
   */
  async switchDatabase(name, _opts = {}) {
    const dbName = String(name ?? '').trim();
    if (!dbName) throw new Error('mongo: switchDatabase(name) requires a database name');

    const client = this.db?.client;
    if (!client?.db) {
      throw new Error('mongo: switchDatabase requires a MongoClient-backed Db');
    }

    const db = client.db(dbName);
    db.client = client;
    const cfg = { ...(this.config ?? {}), dbName, database: dbName };
    return new MongoAdapter(db, { features: this.features, config: cfg });
  }
}

function normalizeList(v) {
  if (!v) return [];
  return Array.isArray(v) ? v.map(String) : [String(v)];
}

function hasReturning(returning) {
  return Array.isArray(returning) && returning.length > 0;
}

function stripCollectionPrefix(path, collection) {
  const s = String(path ?? '').trim();
  if (!s) return '';
  const prefix = `${String(collection)}.`;
  return s.startsWith(prefix) ? s.slice(prefix.length) : s;
}

function buildReturningProjection(returning, collection) {
  const cols = Array.isArray(returning) ? returning : [];
  if (!cols.length) return null;
  if (cols.some(c => {
    const v = String(c ?? '').trim();
    return v === '*' || v === `${collection}.*`;
  })) {
    return null;
  }

  const projection = {};
  let includesId = false;
  for (const c of cols) {
    const field = stripCollectionPrefix(c, collection);
    if (!field || field === '*') return null;
    projection[field] = 1;
    if (field === '_id') includesId = true;
  }
  if (!includesId) projection._id = 0;
  return projection;
}

async function findOneProjected(col, filter, projection, opOpts = {}) {
  const opts = { ...opOpts };
  if (projection) opts.projection = projection;
  return await col.findOne(filter, opts);
}

async function findManyProjected(col, filter, projection, opOpts = {}) {
  let cursor = col.find(filter, opOpts);
  if (projection) cursor = cursor.project(projection);
  return await cursor.toArray();
}

async function findIds(col, filter, opOpts = {}) {
  let cursor = col.find(filter, opOpts);
  cursor = cursor.project({ _id: 1 });
  const rows = await cursor.toArray();
  return rows.map(r => r?._id).filter(v => v != null);
}

function sortRowsByIdOrder(rows, ids) {
  const byId = new Map();
  for (const row of rows ?? []) {
    byId.set(serializeMongoId(row?._id), row);
  }
  return (ids ?? [])
    .map(id => byId.get(serializeMongoId(id)))
    .filter(Boolean);
}

function serializeMongoId(value) {
  if (value == null) return String(value);
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (typeof value === 'object' && typeof value.toHexString === 'function') {
    return value.toHexString();
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function projectionWithIdForOrdering(projection) {
  if (!projection) return null;
  if (projection._id !== 0) return projection;
  return { ...projection, _id: 1 };
}

function stripIdIfExcluded(rows, projection) {
  if (!projection || projection._id !== 0) return rows;
  return (rows ?? []).map(row => {
    if (!row || typeof row !== 'object') return row;
    const next = { ...row };
    delete next._id;
    return next;
  });
}

function dedupeDocs(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows ?? []) {
    const key = stableDocKey(row);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function stableDocKey(value) {
  return JSON.stringify(normalizeDocValue(value));
}

function normalizeDocValue(value) {
  if (value == null) return value;
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return value;
  if (value instanceof Date) return { __date: value.toISOString() };
  if (Array.isArray(value)) return value.map(normalizeDocValue);
  if (typeof value === 'object') {
    if (typeof value.toHexString === 'function') return { __oid: value.toHexString() };
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = normalizeDocValue(value[key]);
    }
    return out;
  }
  return String(value);
}

function mapInsertSelectRow(row, mappings) {
  const out = {};
  for (const m of mappings ?? []) {
    const target = String(m?.target ?? '').trim();
    if (!target) continue;
    const source = String(m?.source ?? '').trim();
    const value = source ? readPath(row, source) : undefined;
    out[target] = value === undefined ? null : value;
  }
  return out;
}

function readPath(value, path) {
  if (!path) return value;
  const parts = String(path).split('.');
  let cur = value;
  for (const part of parts) {
    if (cur == null || typeof cur !== 'object') return undefined;
    cur = cur[part];
  }
  return cur;
}
