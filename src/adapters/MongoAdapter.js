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
    const col = this.db.collection(mongo.collection);

    switch (mongo.op) {
      case 'find': {
        let cursor = col.find(mongo.filter ?? {});
        if (mongo.projection) cursor = cursor.project(mongo.projection);
        if (mongo.sort) cursor = cursor.sort(mongo.sort);
        if (mongo.skip != null) cursor = cursor.skip(mongo.skip);
        if (mongo.limit != null) cursor = cursor.limit(mongo.limit);
        return await cursor.toArray();
      }
      case 'aggregate': {
        const cursor = col.aggregate(mongo.pipeline ?? []);
        return await cursor.toArray();
      }
      case 'insertOne': {
        const doc = (mongo.docs ?? [])[0];
        const res = await col.insertOne(doc);
        return { insertedId: res.insertedId };
      }
      case 'insertMany': {
        const res = await col.insertMany(mongo.docs ?? []);
        return { insertedIds: res.insertedIds };
      }
      case 'updateMany': {
        const res = await col.updateMany(mongo.filter ?? {}, mongo.update ?? {});
        return { matchedCount: res.matchedCount, modifiedCount: res.modifiedCount };
      }
      case 'deleteMany': {
        const res = await col.deleteMany(mongo.filter ?? {});
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
          const col = this.db.collection(mongo.collection);
          switch (mongo.op) {
            case 'find': {
              let cursor = col.find(mongo.filter ?? {}, { session });
              if (mongo.projection) cursor = cursor.project(mongo.projection);
              if (mongo.sort) cursor = cursor.sort(mongo.sort);
              if (mongo.skip != null) cursor = cursor.skip(mongo.skip);
              if (mongo.limit != null) cursor = cursor.limit(mongo.limit);
              return await cursor.toArray();
            }
            case 'aggregate': {
              const cursor = col.aggregate(mongo.pipeline ?? [], { session });
              return await cursor.toArray();
            }
            case 'insertOne': {
              const doc = (mongo.docs ?? [])[0];
              const res = await col.insertOne(doc, { session });
              return { insertedId: res.insertedId };
            }
            case 'insertMany': {
              const res = await col.insertMany(mongo.docs ?? [], { session });
              return { insertedIds: res.insertedIds };
            }
            case 'updateMany': {
              const res = await col.updateMany(mongo.filter ?? {}, mongo.update ?? {}, { session });
              return { matchedCount: res.matchedCount, modifiedCount: res.modifiedCount };
            }
            case 'deleteMany': {
              const res = await col.deleteMany(mongo.filter ?? {}, { session });
              return { deletedCount: res.deletedCount };
            }
            default:
              throw new Error(`Unsupported mongo operation: ${mongo.op}`);
          }
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
