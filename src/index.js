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
 * @typedef {'pg'|'mysql'|'mssql'|'oracle'|'mongo'|'mongodb'} Dialect
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
    const { Pool } = await importDialectPackage('pg', importer, inputDialect);
    const pool = new Pool(normalizePgConfig(config));
    const adapter = new PgAdapter(pool, { features, config });
    return new DB(adapter, { features });
  }

  if (dialect === 'mysql') {
    const mysql = await importDialectPackage('mysql', importer, inputDialect);
    const pool = mysql.createPool(config);
    const adapter = new MySqlAdapter(pool, { features, config });
    return new DB(adapter, { features });
  }

  if (dialect === 'mssql') {
    const mssql = await importDialectPackage('mssql', importer, inputDialect);
    const pool = await mssql.connect(config);
    const adapter = new MsSqlAdapter(pool, { features, config });
    return new DB(adapter, { features });
  }

  if (dialect === 'oracle') {
    const oracledb = await importDialectPackage('oracle', importer, inputDialect);
    const pool = await oracledb.createPool(config);
    const adapter = new OracleAdapter(pool, { features, config });
    return new DB(adapter, { features });
  }

  if (dialect === 'mongo') {
    const { MongoClient } = await importDialectPackage('mongo', importer, inputDialect);
    const uri = config?.uri ?? config?.connectionString;
    const dbName = config?.dbName ?? config?.database ?? inferMongoDbName(uri);
    if (!uri || !dbName) {
      throw new Error('Mongo config requires { uri, dbName } or { connectionString } with a database name');
    }
    const client = new MongoClient(uri, config?.clientOptions ?? {});
    await client.connect();
    const db = client.db(dbName);
    // attach client so adapter can start sessions
    db.client = client;
    const adapter = new MongoAdapter(db, { features, config });
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
