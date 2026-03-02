import { DB } from './core/DB.js';
import { raw } from './core/Raw.js';
import { id } from './core/Identifier.js';
import { BaseModel } from './model/BaseModel.js';
import { PgAdapter } from './adapters/PgAdapter.js';
import { MySqlAdapter } from './adapters/MySqlAdapter.js';
import { MsSqlAdapter } from './adapters/MsSqlAdapter.js';
import { OracleAdapter } from './adapters/OracleAdapter.js';
import { MongoAdapter } from './adapters/MongoAdapter.js';
import { SQueryError } from './utils/errors.js';
import { ToolsManager } from './tools/ToolsManager.js';

/**
 * @typedef {'pg'|'mysql'|'mssql'|'oracle'|'mongo'|'mongodb'|'mongoose'} Dialect
 */

const DIALECT_PACKAGE = {
  pg: 'pg',
  mysql: 'mysql',
  mssql: 'mssql',
  oracle: 'oracledb',
  mongo: 'mongodb',
};

function isMissingModuleError(err, pkg) {
  const code = String(err?.code ?? '');
  if (code !== 'ERR_MODULE_NOT_FOUND' && code !== 'MODULE_NOT_FOUND') return false;
  const msg = String(err?.message ?? '');
  return msg.includes(`'${pkg}'`) || msg.includes(`"${pkg}"`);
}

function normalizeDialect(dialect) {
  if (dialect === 'mongodb') return 'mongo';
  if (dialect === 'mongoose') return 'mongo';
  return dialect;
}

function inferMongoDbName(uri) {
  const raw = String(uri ?? '').trim();
  if (!raw) return null;
  try {
    const u = new URL(raw);
    const path = String(u.pathname ?? '').replace(/^\/+/, '');
    if (!path) return null;
    const first = path.split('/')[0];
    const dbName = decodeURIComponent(first);
    return dbName || null;
  } catch {
    return null;
  }
}

async function importDialectPackage(dialect, importer = (name) => import(name), displayDialect = dialect) {
  const pkg = DIALECT_PACKAGE[dialect];
  if (!pkg) throw new Error(`Unsupported dialect: ${dialect}`);
  try {
    return await importer(pkg);
  } catch (err) {
    if (isMissingModuleError(err, pkg)) {
      throw new Error(`Missing driver package "${pkg}" for dialect "${displayDialect}". Install it with: npm install ${pkg}`);
    }
    throw err;
  }
}

function resolveModuleExport(mod, name) {
  if (mod && typeof mod === 'object' && name in mod) return mod[name];
  if (mod?.default && typeof mod.default === 'object' && name in mod.default) return mod.default[name];
  return undefined;
}

function normalizePgConfig(config) {
  const raw = (config && typeof config === 'object') ? { ...config } : {};
  const nestedOptions = (raw.options && typeof raw.options === 'object' && !Array.isArray(raw.options))
    ? raw.options
    : null;

  // Allow config.options as a convenience bag for pg Pool options.
  const c = nestedOptions ? { ...nestedOptions, ...raw } : raw;

  if (c.host == null && c.server != null) c.host = c.server;
  if (c.database == null && c.dbName != null) c.database = c.dbName;

  // If options was an object we already flattened it; remove the wrapper key.
  if (nestedOptions) delete c.options;
  return c;
}

function normalizeMySqlConfig(config) {
  const raw = (config && typeof config === 'object') ? { ...config } : {};
  const nestedOptions = (raw.options && typeof raw.options === 'object' && !Array.isArray(raw.options))
    ? raw.options
    : null;

  // Allow config.options as a convenience bag for mysql pool options.
  const c = nestedOptions ? { ...nestedOptions, ...raw } : raw;

  if (c.host == null && c.server != null) c.host = c.server;
  if (c.database == null && c.dbName != null) c.database = c.dbName;

  if (nestedOptions) delete c.options;
  return c;
}

function normalizeMongoConfig(config) {
  const raw = (config && typeof config === 'object') ? { ...config } : {};
  const optionsBag = (raw.options && typeof raw.options === 'object' && !Array.isArray(raw.options))
    ? raw.options
    : null;
  const clientOptionsBag = (raw.clientOptions && typeof raw.clientOptions === 'object' && !Array.isArray(raw.clientOptions))
    ? raw.clientOptions
    : null;

  const c = { ...raw };
  if (c.host == null && c.server != null) c.host = c.server;
  if (c.dbName == null && c.database != null) c.dbName = c.database;

  if (optionsBag || clientOptionsBag) {
    c.clientOptions = { ...(optionsBag ?? {}), ...(clientOptionsBag ?? {}) };
  }

  if (optionsBag) delete c.options;
  return c;
}

function buildMongoUriFromConfig(config) {
  const host = String(config?.host ?? '').trim();
  if (!host) return null;

  const hasPort = config?.port != null && String(config.port).trim() !== '';
  const port = hasPort ? `:${String(config.port).trim()}` : '';

  const userRaw = config?.user ?? config?.username;
  const passRaw = config?.password;
  let auth = '';
  if (userRaw != null && String(userRaw).trim() !== '') {
    const user = encodeURIComponent(String(userRaw));
    const pass = passRaw == null ? '' : `:${encodeURIComponent(String(passRaw))}`;
    auth = `${user}${pass}@`;
  }

  const dbNameRaw = config?.dbName ?? config?.database;
  const dbPath = (dbNameRaw != null && String(dbNameRaw).trim() !== '')
    ? `/${encodeURIComponent(String(dbNameRaw).trim())}`
    : '';

  return `mongodb://${auth}${host}${port}${dbPath}`;
}

function resolveMongoDbHandle(config) {
  const c = (config && typeof config === 'object') ? config : {};
  const directDb = c.db;
  if (directDb && typeof directDb.collection === 'function') {
    if (!directDb.client && c.client) directDb.client = c.client;
    return directDb;
  }

  const candidates = [
    c.connection,
    c.mongooseConnection,
    c.mongoose?.connection,
    c.mongoose,
  ];

  for (const conn of candidates) {
    if (!conn || typeof conn !== 'object') continue;
    const db = conn.db;
    if (!db || typeof db.collection !== 'function') continue;

    let client = db.client ?? null;
    if (!client && typeof conn.getClient === 'function') {
      try { client = conn.getClient(); } catch {}
    }
    if (!client && conn.client) client = conn.client;
    if (!client && c.client) client = c.client;
    if (client && !db.client) db.client = client;
    return db;
  }
  return null;
}

/**
 * Connect to a database and return a DB instance.
 *
 * @param {{dialect: Dialect, config: any, features?: Record<string, any>, importer?: (name:string)=>Promise<any>|any}} options
 * @returns {Promise<DB>}
 */
export async function connect(options) {
  const inputDialect = options?.dialect;
  const dialect = normalizeDialect(inputDialect);
  const config = options?.config;
  const features = options?.features ?? {};
  const importer = options?.importer;

  if (!dialect) throw new Error('connect({dialect, config}) is required');

  if (dialect === 'pg') {
    const pg = await importDialectPackage('pg', importer, inputDialect);
    const Pool = resolveModuleExport(pg, 'Pool');
    if (typeof Pool !== 'function') {
      throw new Error('Invalid driver package "pg": missing Pool constructor export');
    }
    const cfg = normalizePgConfig(config);
    const pool = new Pool(cfg);
    const adapter = new PgAdapter(pool, { features, config: cfg });
    return new DB(adapter, { features });
  }

  if (dialect === 'mysql') {
    const mysql = await importDialectPackage('mysql', importer, inputDialect);
    const createPool = resolveModuleExport(mysql, 'createPool');
    if (typeof createPool !== 'function') {
      throw new Error('Invalid driver package "mysql": missing createPool export');
    }
    const cfg = normalizeMySqlConfig(config);
    const pool = createPool(cfg);
    const adapter = new MySqlAdapter(pool, { features, config: cfg });
    return new DB(adapter, { features });
  }

  if (dialect === 'mssql') {
    const mssql = await importDialectPackage('mssql', importer, inputDialect);
    const connectFn = resolveModuleExport(mssql, 'connect');
    if (typeof connectFn !== 'function') {
      throw new Error('Invalid driver package "mssql": missing connect export');
    }
    const pool = await connectFn(config);
    const adapter = new MsSqlAdapter(pool, { features, config });
    return new DB(adapter, { features });
  }

  if (dialect === 'oracle') {
    const oracledb = await importDialectPackage('oracle', importer, inputDialect);
    const createPool = resolveModuleExport(oracledb, 'createPool');
    if (typeof createPool !== 'function') {
      throw new Error('Invalid driver package "oracledb": missing createPool export');
    }
    const pool = await createPool(config);
    const adapter = new OracleAdapter(pool, { features, config });
    return new DB(adapter, { features });
  }

  if (dialect === 'mongo') {
    const cfg = normalizeMongoConfig(config);
    const providedDb = resolveMongoDbHandle(cfg);
    if (providedDb) {
      const adapter = new MongoAdapter(providedDb, { features, config: cfg });
      return new DB(adapter, { features });
    }

    const mongo = await importDialectPackage('mongo', importer, inputDialect);
    const MongoClient = resolveModuleExport(mongo, 'MongoClient');
    if (typeof MongoClient !== 'function') {
      throw new Error('Invalid driver package "mongodb": missing MongoClient constructor export');
    }
    const uri = cfg.uri ?? cfg.connectionString ?? buildMongoUriFromConfig(cfg);
    const dbName = cfg.dbName ?? cfg.database ?? inferMongoDbName(uri);
    if (!uri || !dbName) {
      throw new Error('Mongo config requires { uri, dbName }, { connectionString } with database name, or { server/host, port?, database/dbName }');
    }
    const client = new MongoClient(uri, cfg.clientOptions ?? {});
    await client.connect();
    const db = client.db(dbName);
    // attach client so adapter can start sessions
    db.client = client;
    const adapter = new MongoAdapter(db, { features, config: cfg });
    return new DB(adapter, { features });
  }

  throw new Error(`Unsupported dialect: ${dialect}`);
}

export { DB };
export { BaseModel };
export { raw, id };
export { SQueryError };
export { ToolsManager };

export default {
  connect,
  DB,
  BaseModel,
  raw,
  id,
  SQueryError,
  ToolsManager,
};
