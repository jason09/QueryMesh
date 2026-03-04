/// <reference types="node" />

import { EventEmitter } from "node:events";

export type Dialect = "pg" | "mysql" | "mssql" | "oracle" | "mongo" | "mongodb" | "mongoose";

export interface ConnectOptions {
  dialect: Dialect;
  config: any;
  features?: Record<string, any>;
  importer?: (name: string) => Promise<any> | any;
}

export interface SwitchDatabaseOptions {
  closeCurrent?: boolean;
  [k: string]: any;
}

export interface SwitchDialectOptions {
  closeCurrent?: boolean;
  features?: Record<string, any>;
  importer?: (name: string) => Promise<any> | any;
  [k: string]: any;
}

export interface ProgressEvent {
  bytes: number;
  percent?: number;
}

export interface BackupStartEvent {
  cmd: string;
  args: string[];
}

export interface BackupLogEvent {
  line: string;
}

export interface BackupDoneEvent {
  code: number | null;
}

export type BackupFormat =
  | "plain"
  | "custom"
  | "directory"
  | "tar"
  | "binary"
  | "archive"
  | "csv"
  | "bacpac";

export interface BackupExportOptions {
  file?: string;
  tables?: string[];
  table?: string | string[];
  schemas?: string[];
  schema?: string | string[];
  excludeTables?: string[];
  excludeTable?: string | string[];
  excludeSchemas?: string[];
  excludeSchema?: string | string[];
  format?: BackupFormat;
  create?: boolean;
  clean?: boolean;
  ifExists?: boolean;
  schemaOnly?: boolean;
  dataOnly?: boolean;
  columnInserts?: boolean;
  inserts?: boolean;
  rowsPerInsert?: number;
  blobs?: boolean;
  largeObjects?: boolean;
  gzip?: boolean;
  useStdout?: boolean;
  progressIntervalMs?: number;
  extraArgs?: string[];
  pgArgs?: string[];
  pgDumpArgs?: string[];
  restoreArgs?: string[];
  psqlArgs?: string[];
  mysqlArgs?: string[];
  mysqlDumpArgs?: string[];
  mysqlImportArgs?: string[];
  mongoArgs?: string[];
  mongoDumpArgs?: string[];
  mongoImportArgs?: string[];
  mssqlArgs?: string[];
  bacpacArgs?: string[];
  sqlcmdArgs?: string[];
  oracleArgs?: string[];
  oracleDumpArgs?: string[];
  oracleImportArgs?: string[];
  [k: string]: any;
}

export interface BackupImportOptions extends BackupExportOptions {
  table?: string;
}

export interface BackupJob extends EventEmitter {
  done: Promise<BackupDoneEvent>;
  abort: () => void;

  on(event: "start", listener: (e: BackupStartEvent) => void): this;
  on(event: "progress", listener: (e: ProgressEvent) => void): this;
  on(event: "log", listener: (e: BackupLogEvent) => void): this;
  on(event: "error", listener: (err: Error) => void): this;
  on(event: "done", listener: (e: BackupDoneEvent) => void): this;
}

export class SQueryError extends Error {
  details?: Record<string, any>;
  constructor(message: string, details?: Record<string, any>);
}

export class Raw {
  sql: string;
  params: any[];
  constructor(sql: string, params?: any[]);
}

export class Identifier {
  name: string;
  constructor(name: string);
}

export class QueryBuilder {
  where(fn: (q: QueryBuilder) => any): this;
  select(cols?: string[] | string): this;
  distinct(): this;
  union(source: QueryBuilder | Raw | string): this;
  unionAll(source: QueryBuilder | Raw | string): this;
  clearUnions(): this;
  aggregate(fn: "count" | "sum" | "avg" | "min" | "max", column?: string, as?: string): this;
  count(column?: string, as?: string): this;
  sum(column: string, as?: string): this;
  avg(column: string, as?: string): this;
  min(column: string, as?: string): this;
  max(column: string, as?: string): this;
  clearAggregates(): this;
  join(type: "inner" | "left" | "right" | "full", table: string, left: string, opOrRight: string, right?: string): this;
  joinAs(type: "inner" | "left" | "right" | "full", table: string, alias: string, left: string, opOrRight: string, right?: string): this;
  joinOn(type: "inner" | "left" | "right" | "full", table: string, left: string, right: string): this;
  joinOnAs(type: "inner" | "left" | "right" | "full", table: string, alias: string, left: string, right: string): this;
  innerJoin(table: string, left: string, opOrRight: string, right?: string): this;
  leftJoin(table: string, left: string, opOrRight: string, right?: string): this;
  rightJoin(table: string, left: string, opOrRight: string, right?: string): this;
  innerJoinAs(table: string, alias: string, left: string, opOrRight: string, right?: string): this;
  leftJoinAs(table: string, alias: string, left: string, opOrRight: string, right?: string): this;
  rightJoinAs(table: string, alias: string, left: string, opOrRight: string, right?: string): this;
  innerJoinOn(table: string, left: string, right: string): this;
  leftJoinOn(table: string, left: string, right: string): this;
  rightJoinOn(table: string, left: string, right: string): this;
  innerJoinOnAs(table: string, alias: string, left: string, right: string): this;
  leftJoinOnAs(table: string, alias: string, left: string, right: string): this;
  rightJoinOnAs(table: string, alias: string, left: string, right: string): this;
  where(column: string | Raw, value: any): this;
  whereGroup(fn: (q: QueryBuilder) => any, bool?: "AND" | "OR"): this;
  whereNot(fn: (q: QueryBuilder) => any, bool?: "AND" | "OR"): this;
  whereOp(column: string | Raw, op: string, value: any, bool?: "AND" | "OR"): this;
  whereAny(column: string | Raw, op: string, value: QueryBuilder | Raw | string | any[], bool?: "AND" | "OR"): this;
  whereAll(column: string | Raw, op: string, value: QueryBuilder | Raw | string | any[], bool?: "AND" | "OR"): this;
  whereIs(column: string | Raw, value: any, bool?: "AND" | "OR"): this;
  whereIsNot(column: string | Raw, value: any, bool?: "AND" | "OR"): this;
  whereIsNull(column: string): this;
  whereIsNotNull(column: string): this;
  orWhere(fn: (q: QueryBuilder) => any): this;
  orWhere(column: string | Raw, value: any): this;
  orWhereGroup(fn: (q: QueryBuilder) => any): this;
  orWhereNot(fn: (q: QueryBuilder) => any): this;
  orWhereAny(column: string | Raw, op: string, value: QueryBuilder | Raw | string | any[]): this;
  orWhereAll(column: string | Raw, op: string, value: QueryBuilder | Raw | string | any[]): this;
  orWhereIs(column: string | Raw, value: any): this;
  orWhereIsNot(column: string | Raw, value: any): this;
  whereIn(column: string, values: any[]): this;
  whereNotIn(column: string, values: any[]): this;
  whereBetween(column: string, a: any, b: any): this;
  whereNotBetween(column: string, a: any, b: any): this;
  whereNull(column: string): this;
  whereNotNull(column: string): this;
  groupBy(cols: string[] | string): this;
  having(column: string | Raw, op: string, value: any): this;
  orderBy(column: string, direction?: "asc" | "desc"): this;
  limit(n: number): this;
  offset(n: number): this;
  returning(cols: string[] | string): this;
  insert(data: Record<string, any> | Array<Record<string, any>>): this;
  insertSelect(columns: string[] | string, source: QueryBuilder | Raw | string): this;
  update(data: Record<string, any>): this;
  delete(): this;
  onConflictDoUpdate(target: string | string[], update: Record<string, any>): this;
  onDuplicateKeyUpdate(update: Record<string, any>): this;
  compile(): { sql?: string; params?: any[]; mongo?: any };
  run(): Promise<any>;
  get(): Promise<any[]>;
  first(): Promise<any | null>;
}

export class BackupManager {
  export(options?: BackupExportOptions): BackupJob;
  import(options?: BackupImportOptions): BackupJob;
}

export interface ToolStatusItem {
  command: string;
  installed: boolean;
  version: string | null;
}

export interface ToolingStatus {
  dialect: Dialect;
  tools: ToolStatusItem[];
  allInstalled: boolean;
}

export class ToolsManager {
  isCommandAvailable(command: string): boolean;
  getCliVersion(command: string, argCandidates?: string[][]): string | null;
  isPostgresInstalled(): boolean;
  isPostgreInstalled(): boolean;
  isMySqlInstalled(): boolean;
  isMsSqlInstalled(): boolean;
  isOracleInstalled(): boolean;
  isMongoInstalled(): boolean;
  getToolingStatus(dialect?: Dialect): ToolingStatus;
  getAllToolingStatus(): Record<Dialect, ToolingStatus>;
  getCurrentDialectCliVersion(): string | null;
  ping(): Promise<boolean>;
  getVersion(): Promise<string | null>;
  getDiagnostics(): Promise<{
    dialect: Dialect;
    ping: boolean;
    serverVersion: string | null;
    cli: ToolingStatus;
  }>;
}

export interface DescColumn {
  name: string | null;
  type: string | null;
  types?: string[];
  nullable: boolean | null;
  default: any;
  maxLength: number | null;
  precision: number | null;
  scale: number | null;
  ordinal: number | null;
  extra: string | null;
}

export interface TableDesc {
  kind: "table";
  dialect: Dialect | null;
  name: string | null;
  schema: string | null;
  columns: DescColumn[];
  sampleSize?: number | null;
  sampledDocuments?: number | null;
  createSql?: string | null;
}

export interface DatabaseDesc {
  kind: "database";
  dialect: Dialect | null;
  name: string | null;
  schema: string | null;
  tables?: string[];
  views?: string[];
  collections?: string[];
  databases?: string[] | null;
  tableDescriptions?: Record<string, TableDesc> | null;
  collectionDescriptions?: Record<string, TableDesc> | null;
  createSql?: string | null;
}

export interface GetDescOptions {
  schema?: string;
  deep?: boolean;
  strict?: boolean;
  includeViews?: boolean;
  includeDatabases?: boolean;
  includeCreateSql?: boolean;
  sampleSize?: number;
  name?: string;
  table?: string;
}

export class SchemaBuilder {
  showTables(opts?: { schema?: string }): Promise<string[]>;
  showDatabases(): Promise<string[]>;
  getDesc(target?: any, opts?: GetDescOptions | any): Promise<TableDesc | DatabaseDesc | any>;
  exec(): Promise<any>;
}

export class DB {
  adapter: any;
  table(name: string): QueryBuilder;
  raw(sql: string, params?: any[]): Raw;
  id(name: string): Identifier;
  quote(name: string): string;
  schema(): any;
  backup(): BackupManager;
  tools(): ToolsManager;
  switchDatabase(name: string, opts?: SwitchDatabaseOptions): Promise<DB>;
  useDatabase(name: string, opts?: SwitchDatabaseOptions): Promise<DB>;
  switchDialect(dialect: Dialect, config: any, opts?: SwitchDialectOptions): Promise<DB>;
  useDialect(dialect: Dialect, config: any, opts?: SwitchDialectOptions): Promise<DB>;
  transaction<T>(fn: (trx: DB) => Promise<T>): Promise<T>;
  close(): Promise<void>;
  model<T>(ModelClass: T): T;
}

export class BaseModel {
  static db: DB;
  static table: string;
  static primaryKey: string;
  static timestamps: boolean;

  constructor(attrs?: Record<string, any>);

  static bind<T extends typeof BaseModel>(this: T, db: DB): T;
  static query(this: typeof BaseModel): QueryBuilder;
  static find(this: typeof BaseModel, id: any): Promise<BaseModel | null>;
  static all(this: typeof BaseModel): Promise<BaseModel[]>;
  static create(this: typeof BaseModel, data: Record<string, any>): Promise<any>;
  static where(this: typeof BaseModel, col: string, val: any): Promise<BaseModel[]>;
  static hydrate(this: typeof BaseModel, row: Record<string, any>): BaseModel;

  save(): Promise<this>;
  delete(): Promise<boolean>;
}

export function connect(options: ConnectOptions): Promise<DB>;
export function raw(sql: string, params?: any[]): Raw;
export function id(name: string): Identifier;

declare const SQuery: {
  connect: typeof connect;
  DB: typeof DB;
  BaseModel: typeof BaseModel;
  raw: typeof raw;
  id: typeof id;
  SQueryError: typeof SQueryError;
  ToolsManager: typeof ToolsManager;
};

export default SQuery;
