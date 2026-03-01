
import { EventEmitter } from "events";
import { spawn } from "child_process";
import fs from "fs";
import path from "path";
import { pipeline, Transform } from "stream";
import { createGzip, createGunzip } from "zlib";
import { SQueryError } from "../utils/errors.js";

function safeStat(p) { try { return fs.statSync(p); } catch { return null; } }
function dirSizeSync(dir) {
  let total = 0;
  const stack = [dir];
  while (stack.length) {
    const d = stack.pop();
    const entries = (() => { try { return fs.readdirSync(d, { withFileTypes: true }); } catch { return []; } })();
    for (const e of entries) {
      const fp = path.join(d, e.name);
      if (e.isDirectory()) stack.push(fp);
      else {
        const st = safeStat(fp);
        if (st) total += st.size;
      }
    }
  }
  return total;
}

/**
 * Backup/import manager. Uses native database CLIs (pg_dump, psql, pg_restore, mysqldump, mysql, sqlcmd/sqlpackage, expdp/impdp, mongodump/mongorestore).
 * Requirements: corresponding CLI tools must be installed on the machine.
 */
export class BackupManager {
  constructor(db) {
    this.db = db;
  }

  /**
   * Export database or selected tables.
   *
   * Emits: 'start'({cmd,args}), 'progress'({bytes,percent?}), 'log'({line}), 'error'(err), 'done'({code})
   *
   * Huge DB support:
   * - Uses streaming where possible (stdin/stdout piping, gzip streams) to avoid buffering.
   * - Directory/custom formats can use parallel jobs (pg: jobs, mysql: singleTransaction, etc.).
   *
   * @param {object} options
   * @param {string} [options.file] Output file path (or directory for pg directory format).
   * @param {string[]} [options.tables] Optional list of tables/collections to export.
   * @param {"plain"|"custom"|"directory"|"tar"|"binary"|"archive"|"csv"} [options.format] Dialect-dependent. For pg, "csv" uses psql \copy.
   * @param {boolean} [options.schemaOnly]
   * @param {boolean} [options.dataOnly]
   * @param {boolean} [options.gzip] If true, gzip stream where supported (or via Node streams).
   * @param {number} [options.progressIntervalMs] Default 500ms.
   * @param {string[]} [options.extraArgs] Raw pass-through CLI args (dialect-dependent). Aliases: options.args, options.dumpArgs
   * @param {boolean} [options.useStdout] Prefer stdout streaming (pg_dump) when supported.
   * @returns {EventEmitter} emitter (also a promise-like via emitter.done)
   */
  export(options = {}) {
    const emitter = new EventEmitter();
    const adapter = this.db.adapter;
    const interval = options.progressIntervalMs ?? 500;

    emitter.done = new Promise((resolve, reject) => {
      emitter.once("error", reject);
      emitter.once("done", resolve);
    });

    // Special: PG CSV export via psql \copy
    if (adapter.dialect === "pg" && (options.format === "csv" || options.format === "copy")) {
      this._exportPgCsv(emitter, options);
      return emitter;
    }

    const op = adapter.getExportCommand?.(options);
    if (!op) {
      queueMicrotask(() => emitter.emit("error", new SQueryError(`Export not supported for dialect "${adapter.dialect}" (format=${options.format ?? "plain"})`)));
      return emitter;
    }

    const { cmd, args, env, outputFile, outputDir, useStdout, outputFilePath } = op;
    emitter.emit("start", { cmd, args });

    const child = spawn(cmd, args, { env: { ...process.env, ...(env || {}) } });
    emitter.abort = () => { try { child.kill("SIGTERM"); } catch {} };

    // Always forward stderr as logs (useful for progress messages too)
    child.stderr?.on("data", (b) => emitter.emit("log", { line: b.toString() }));

    // Progress
    let timer = null;
    let lastBytes = 0;

    const emitBytes = (bytes, total) => {
      const percent = total ? Math.round((bytes / total) * 1000) / 10 : undefined;
      emitter.emit("progress", { bytes, percent });
    };

    // If stdout streaming, pipe stdout -> (gzip?) -> file
    if (useStdout) {
      const out = options.file ?? outputFilePath ?? "dump.out";
      const ws = fs.createWriteStream(out);
      let total = null;
      // if exporting to stdout we don't know total; emit bytes written
      const gzip = options.gzip ? createGzip() : null;

      let bytes = 0;
      const tap = new Transform({
        transform(chunk, _enc, cb) {
          bytes += chunk.length;
          emitBytes(bytes, total);
          cb(null, chunk);
        }
      });

      const src = child.stdout;
      if (!src) {
        queueMicrotask(() => emitter.emit("error", new SQueryError("Export command has no stdout stream")));
        return emitter;
      }

      const streams = [src, tap];
      if (gzip) streams.push(gzip);
      streams.push(ws);

      pipeline(...streams, (err) => {
        if (err) emitter.emit("error", err);
      });

      child.on("close", (code) => {
        if (code === 0) emitter.emit("done", { code });
        else emitter.emit("error", new SQueryError(`Export failed with code ${code}`, { code, cmd, args }));
      });

      child.on("error", (err) => emitter.emit("error", err));
      return emitter;
    }

    // If writing to a single output file, monitor file size
    if (outputFile) {
      timer = setInterval(() => {
        try {
          const st = fs.statSync(outputFile);
          const bytes = st.size;
          if (bytes !== lastBytes) {
            lastBytes = bytes;
            emitBytes(bytes);
          }
        } catch {}
      }, interval);
    } else if (outputDir) {
      timer = setInterval(() => {
        try {
          const bytes = dirSizeSync(outputDir);
          if (bytes !== lastBytes) {
            lastBytes = bytes;
            emitBytes(bytes);
          }
        } catch {}
      }, interval);
    }

    child.on("error", (err) => {
      if (timer) clearInterval(timer);
      emitter.emit("error", err);
    });

    // Forward stdout to logs (only when not redirected to file by the CLI)
    child.stdout?.on("data", (b) => emitter.emit("log", { line: b.toString() }));

    child.on("close", (code) => {
      if (timer) clearInterval(timer);
      if (code === 0) emitter.emit("done", { code });
      else emitter.emit("error", new SQueryError(`Export failed with code ${code}`, { code, cmd, args }));
    });

    return emitter;
  }

  /**
   * Import a dump into database.
   *
   * Emits: 'start','progress','log','error','done'
   *
   * Huge DB support:
   * - Streams file into stdin when supported (avoids loading file in memory).
   * - For pg_restore directory/custom formats, relies on native restore tool (can use jobs).
   *
   * @param {object} options
   * @param {string} options.file Input file path (or directory for pg directory format).
   * @param {"plain"|"custom"|"directory"|"tar"|"csv"|"bacpac"|"binary"|"archive"} [options.format]
   * @param {string} [options.table] For CSV import (pg). If omitted for pg csv and tables length=1, it will use that table.
   * @param {string[]} [options.extraArgs] Raw pass-through CLI args. Aliases: options.args, options.restoreArgs, options.psqlArgs
   * @param {number} [options.progressIntervalMs] Default 500ms.
   * @param {boolean} [options.gzip] If true, gunzip stream (when importing a gzip file via stdin streaming).
   * @returns {EventEmitter}
   */
  import(options = {}) {
    const emitter = new EventEmitter();
    const adapter = this.db.adapter;
    const interval = options.progressIntervalMs ?? 500;

    if (!options.file) {
      queueMicrotask(() => emitter.emit("error", new SQueryError("import() requires options.file")));
      return emitter;
    }

    emitter.done = new Promise((resolve, reject) => {
      emitter.once("error", reject);
      emitter.once("done", resolve);
    });

    // Special: PG CSV import via psql \copy
    if (adapter.dialect === "pg" && (options.format === "csv" || options.format === "copy")) {
      this._importPgCsv(emitter, options);
      return emitter;
    }

    const op = adapter.getImportCommand?.(options);
    if (!op) {
      queueMicrotask(() => emitter.emit("error", new SQueryError(`Import not supported for dialect "${adapter.dialect}" (format=${options.format ?? "plain"})`)));
      return emitter;
    }

    const { cmd, args, env, inputFile, pipeStdin } = op;
    emitter.emit("start", { cmd, args });

    const child = spawn(cmd, args, { env: { ...process.env, ...(env || {}) } });
    emitter.abort = () => { try { child.kill("SIGTERM"); } catch {} };

    // progress by bytes read from file when streaming.
    // For non-streaming tools, try to parse percentage logs and emit start/end milestones.
    const st = safeStat(inputFile);
    const total = st?.isFile() ? st.size : (st?.isDirectory() ? dirSizeSync(inputFile) : null);
    let readBytes = 0;
    let lastBytes = -1;
    /** @type {number|null} */
    let lastPercent = null;

    const emitProgress = (bytes, percent) => {
      let b = Number.isFinite(bytes) ? Math.max(0, Number(bytes)) : 0;
      let p = Number.isFinite(percent) ? Math.max(0, Math.min(100, Number(percent))) : undefined;

      if (total != null) {
        if (p == null && total > 0) {
          p = Math.round((b / total) * 1000) / 10;
        }
        if (p != null) {
          b = Math.round((p / 100) * total);
        }
      }

      const pKey = p == null ? null : p;
      if (b === lastBytes && pKey === lastPercent) return;
      lastBytes = b;
      lastPercent = pKey;
      emitter.emit("progress", { bytes: b, percent: p });
    };

    const maybeEmitPercentFromText = (text) => {
      const m = String(text).match(/(\d{1,3}(?:\.\d+)?)\s*%/);
      if (!m) return;
      const p = Number(m[1]);
      if (!Number.isFinite(p)) return;
      if (total != null) emitProgress((p / 100) * total, p);
      else emitProgress(lastBytes < 0 ? 0 : lastBytes, p);
    };

    const wireOutput = (stream) => {
      if (!stream) return;
      stream.on("data", (b) => {
        const line = b.toString();
        emitter.emit("log", { line });
        maybeEmitPercentFromText(line);
      });
    };
    wireOutput(child.stderr);
    wireOutput(child.stdout);

    if (!pipeStdin) {
      emitProgress(0, total != null ? 0 : undefined);
    }

    if (pipeStdin) {
      const rs = fs.createReadStream(inputFile);
      rs.on("data", (buf) => {
        readBytes += buf.length;
        emitProgress(readBytes);
      });
      rs.on("error", (err) => emitter.emit("error", err));

      const gunzip = options.gzip ? createGunzip() : null;

      if (gunzip) {
        pipeline(rs, gunzip, child.stdin, (err) => { if (err) emitter.emit("error", err); });
      } else {
        pipeline(rs, child.stdin, (err) => { if (err) emitter.emit("error", err); });
      }
    }

    child.on("error", (err) => emitter.emit("error", err));
    child.on("close", (code) => {
      if (code === 0) {
        if (total != null) emitProgress(total, 100);
        emitter.emit("done", { code });
      }
      else emitter.emit("error", new SQueryError(`Import failed with code ${code}`, { code, cmd, args }));
    });

    return emitter;
  }

  _exportPgCsv(emitter, options) {
    const adapter = this.db.adapter;
    const tables = options.tables || (options.table ? [options.table] : null);
    if (!tables || !tables.length) {
      queueMicrotask(() => emitter.emit("error", new SQueryError("PG CSV export requires options.tables (one or more)")));
      return;
    }
    const base = options.file ?? (tables.length === 1 ? `${tables[0]}.csv` : "csv_export");
    const outIsDir = tables.length > 1 || base.endsWith(path.sep) || safeStat(base)?.isDirectory();

    const runOne = (table, outFile) => new Promise((resolve, reject) => {
      const env = adapter._pgEnv?.() || {};
      const cmd = "psql";
      const sql = `\\copy (SELECT * FROM ${table}) TO STDOUT WITH CSV HEADER`;
      const args = ["-c", sql];
      const extraArgs = options.extraArgs || options.args || options.psqlArgs;
      if (Array.isArray(extraArgs) && extraArgs.length) args.push(...extraArgs);

      emitter.emit("start", { cmd, args });

      const child = spawn(cmd, args, { env: { ...process.env, ...(env || {}) } });
      emitter.abort = () => { try { child.kill("SIGTERM"); } catch {} };
      child.stderr?.on("data", (b) => emitter.emit("log", { line: b.toString() }));

      const ws = fs.createWriteStream(outFile);
      let bytes = 0;
      child.stdout.on("data", (buf) => {
        bytes += buf.length;
        emitter.emit("progress", { bytes });
      });

      pipeline(child.stdout, ws, (err) => { if (err) emitter.emit("error", err); });

      child.on("close", (code) => {
        if (code === 0) resolve();
        else reject(new SQueryError(`PG CSV export failed for table "${table}" (code ${code})`, { code, cmd, args }));
      });
      child.on("error", reject);
    });

    (async () => {
      try {
        if (outIsDir) {
          fs.mkdirSync(base, { recursive: true });
          for (const t of tables) {
            const outFile = path.join(base, `${t}.csv`);
            await runOne(t, outFile);
          }
        } else {
          await runOne(tables[0], base);
        }
        emitter.emit("done", { code: 0 });
      } catch (e) {
        emitter.emit("error", e);
      }
    })();
  }

  _importPgCsv(emitter, options) {
    const adapter = this.db.adapter;
    const file = options.file;
    const table = options.table || (options.tables?.length === 1 ? options.tables[0] : null);
    if (!table) {
      queueMicrotask(() => emitter.emit("error", new SQueryError("PG CSV import requires options.table (or options.tables with exactly one table)")));
      return;
    }
    const env = adapter._pgEnv?.() || {};
    const cmd = "psql";
    const sql = `\\copy ${table} FROM STDIN WITH CSV HEADER`;
    const args = ["-c", sql];
    const extraArgs = options.extraArgs || options.args || options.psqlArgs;
    if (Array.isArray(extraArgs) && extraArgs.length) args.push(...extraArgs);

    emitter.emit("start", { cmd, args });
    const child = spawn(cmd, args, { env: { ...process.env, ...(env || {}) } });
    emitter.abort = () => { try { child.kill("SIGTERM"); } catch {} };

    child.stderr?.on("data", (b) => emitter.emit("log", { line: b.toString() }));
    child.stdout?.on("data", (b) => emitter.emit("log", { line: b.toString() }));

    const st = safeStat(file);
    const total = st?.isFile() ? st.size : null;
    let readBytes = 0;

    const rs = fs.createReadStream(file);
    rs.on("data", (buf) => {
      readBytes += buf.length;
      const percent = total ? Math.round((readBytes / total) * 1000) / 10 : undefined;
      emitter.emit("progress", { bytes: readBytes, percent });
    });

    pipeline(rs, child.stdin, (err) => { if (err) emitter.emit("error", err); });

    child.on("close", (code) => {
      if (code === 0) emitter.emit("done", { code });
      else emitter.emit("error", new SQueryError(`PG CSV import failed (code ${code})`, { code, cmd, args }));
    });
    child.on("error", (err) => emitter.emit("error", err));
  }
}
