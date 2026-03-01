import { spawnSync } from "child_process";

const DIALECT_TO_COMMANDS = {
  pg: ["pg_dump", "pg_restore", "psql"],
  mysql: ["mysqldump", "mysql"],
  mssql: ["sqlpackage", "sqlcmd"],
  oracle: ["expdp", "impdp"],
  mongo: ["mongodump", "mongorestore"],
};

const DIALECT_VERSION_QUERY = {
  pg: "SELECT version() AS version",
  mysql: "SELECT VERSION() AS version",
  mssql: "SELECT @@VERSION AS version",
  oracle: "SELECT BANNER AS version FROM V$VERSION WHERE BANNER LIKE 'Oracle%'",
};

/**
 * Runtime tooling and diagnostics helpers.
 */
export class ToolsManager {
  /**
   * @param {import('../core/DB.js').DB} db
   * @param {{ runner?: (command:string, args?:string[], opts?:Record<string, any>) => any, platform?: string }} [options]
   */
  constructor(db, options = {}) {
    this.db = db;
    this.adapter = db.adapter;
    this._runner = options.runner || defaultRunner;
    this._platform = options.platform || process.platform;
  }

  /**
   * Check if a command exists in PATH.
   * @param {string} command
   */
  isCommandAvailable(command) {
    if (!command) return false;
    if (this._platform === "win32") {
      const r = this._run("where", [command], { timeout: 2500 });
      return !r.error && r.status === 0;
    }
    let r = this._run("which", [command], { timeout: 2500 });
    if (!r.error && r.status === 0) return true;
    r = this._run(command, ["--version"], { timeout: 2500 });
    if (!r.error && r.status === 0) return true;
    const msg = `${r.stderr ?? ""}\n${r.stdout ?? ""}\n${r.error?.message ?? ""}`;
    return !/not found|enoent|is not recognized/i.test(msg);
  }

  /**
   * Read CLI version line.
   * @param {string} command
   * @param {string[][]} [argCandidates]
   * @returns {string|null}
   */
  getCliVersion(command, argCandidates = [["--version"], ["-version"], ["version"]]) {
    if (!this.isCommandAvailable(command)) return null;
    for (const args of argCandidates) {
      const r = this._run(command, args, { timeout: 4000 });
      const line = firstNonEmptyLine((r.stdout || "").trim()) || firstNonEmptyLine((r.stderr || "").trim());
      if (line) return line;
      if (!r.error && r.status === 0) return "installed";
    }
    return "installed";
  }

  isPostgresInstalled() { return this.isCommandAvailable("psql"); }
  // alias for typo-prone usage
  isPostgreInstalled() { return this.isPostgresInstalled(); }
  isMySqlInstalled() { return this.isCommandAvailable("mysql"); }
  isMsSqlInstalled() { return this.isCommandAvailable("sqlcmd") || this.isCommandAvailable("sqlpackage"); }
  isOracleInstalled() { return this.isCommandAvailable("expdp") || this.isCommandAvailable("impdp"); }
  isMongoInstalled() { return this.isCommandAvailable("mongorestore") || this.isCommandAvailable("mongodump"); }

  /**
   * Get required CLI tool status for a given dialect.
   * @param {'pg'|'mysql'|'mssql'|'oracle'|'mongo'} [dialect]
   */
  getToolingStatus(dialect = this.adapter.dialect) {
    const commands = DIALECT_TO_COMMANDS[dialect] ?? [];
    const tools = commands.map((command) => ({
      command,
      installed: this.isCommandAvailable(command),
      version: this.getCliVersion(command),
    }));
    return {
      dialect,
      tools,
      allInstalled: tools.every((t) => t.installed),
    };
  }

  /**
   * Get tooling status for all supported dialects.
   */
  getAllToolingStatus() {
    return {
      pg: this.getToolingStatus("pg"),
      mysql: this.getToolingStatus("mysql"),
      mssql: this.getToolingStatus("mssql"),
      oracle: this.getToolingStatus("oracle"),
      mongo: this.getToolingStatus("mongo"),
    };
  }

  /**
   * Get CLI version for current dialect primary command.
   */
  getCurrentDialectCliVersion() {
    const primary = {
      pg: "psql",
      mysql: "mysql",
      mssql: "sqlcmd",
      oracle: "expdp",
      mongo: "mongorestore",
    }[this.adapter.dialect];
    return primary ? this.getCliVersion(primary) : null;
  }

  /**
   * Ping current DB connection.
   */
  async ping() {
    const d = this.adapter.dialect;
    if (d === "mongo") {
      const out = await this.adapter.db.admin().command({ ping: 1 });
      return out?.ok === 1;
    }
    const sql = d === "oracle" ? "SELECT 1 FROM DUAL" : "SELECT 1 AS ok";
    const rows = await this._executeSql(sql, []);
    return !!this._extractScalar(rows);
  }

  /**
   * Get server version for current dialect.
   * @returns {Promise<string|null>}
   */
  async getVersion() {
    const d = this.adapter.dialect;
    if (d === "mongo") {
      const out = await this.adapter.db.admin().command({ buildInfo: 1 });
      return out?.version ? String(out.version) : null;
    }

    const sql = DIALECT_VERSION_QUERY[d];
    if (!sql) return null;
    const rows = await this._executeSql(sql, []);
    const v = this._extractScalar(rows);
    return v == null ? null : String(v);
  }

  /**
   * Useful summary bundle for health checks.
   */
  async getDiagnostics() {
    const [pingOk, version] = await Promise.all([
      this.ping().catch(() => false),
      this.getVersion().catch(() => null),
    ]);
    return {
      dialect: this.adapter.dialect,
      ping: pingOk,
      serverVersion: version,
      cli: this.getToolingStatus(),
    };
  }

  _run(command, args = [], opts = {}) {
    const res = this._runner(command, args, opts) || {};
    return {
      status: res.status ?? 1,
      stdout: res.stdout ?? "",
      stderr: res.stderr ?? "",
      error: res.error ?? null,
    };
  }

  async _executeSql(sql, params = []) {
    const qb = {
      compile: () => ({ sql, params }),
      _type: "select",
      _returning: null,
      dialect: this.adapter.dialect,
    };
    return this.adapter.execute(qb);
  }

  _extractScalar(rows) {
    if (!Array.isArray(rows) || rows.length === 0) return null;
    const row = rows[0];
    if (row == null) return null;
    if (Array.isArray(row)) return row[0] ?? null;
    if (typeof row === "object") {
      if (Object.prototype.hasOwnProperty.call(row, "version")) return row.version;
      if (Object.prototype.hasOwnProperty.call(row, "VERSION()")) return row["VERSION()"];
      const keys = Object.keys(row);
      return keys.length ? row[keys[0]] : null;
    }
    return row;
  }
}

function defaultRunner(command, args = [], opts = {}) {
  return spawnSync(command, args, {
    encoding: "utf8",
    timeout: opts.timeout ?? 4000,
    env: opts.env ?? process.env,
  });
}

function firstNonEmptyLine(text) {
  const lines = String(text ?? "").split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  return lines[0] || null;
}

