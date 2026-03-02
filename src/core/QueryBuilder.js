import { Raw } from './Raw.js';
import { Identifier } from './Identifier.js';
import { quoteIdent, isPlainObject } from '../utils/identifiers.js';

/**
 * @typedef {'pg'|'mysql'|'mssql'|'oracle'|'mongo'} Dialect
 */

function isRaw(v) { return v instanceof Raw; }
function isId(v) { return v instanceof Identifier; }
const WHERE_OPS = new Set(['=', '!=', '<>', '>', '>=', '<', '<=', 'LIKE', 'NOT LIKE']);
const JOIN_OPS = new Set(['=', '!=', '<>', '>', '>=', '<', '<=']);
const QUANTIFIED_OPS = new Set(['=', '!=', '<>', '>', '>=', '<', '<=']);

function normalizeOp(op) {
  return String(op ?? '').trim().replace(/\s+/g, ' ').toUpperCase();
}

function assertOp(op, allowed, kind) {
  const normalized = normalizeOp(op);
  if (!allowed.has(normalized)) {
    throw new Error(`Unsafe ${kind} operator: ${op}`);
  }
  return normalized;
}

/**
 * QueryBuilder builds SQL (or Mongo ops) in a fluent way.
 * Instances are created via DB.table(name).
 */
export class QueryBuilder {
  /**
   * @param {import('../adapters/BaseAdapter.js').BaseAdapter} adapter
   * @param {string} table
   */
  constructor(adapter, table) {
    this.adapter = adapter;
    this.dialect = adapter.dialect;
    this._table = table;

    this._type = 'select'; // select | insert | update | delete
    this._select = ['*'];
    this._aggregates = []; // [{ fn, column, as }]
    this._distinct = false;
    this._joins = [];
    this._wheres = [];
    this._groupBy = [];
    this._havings = [];
    this._orderBy = [];
    this._limit = null;
    this._offset = null;

    this._insert = null;
    this._insertSelect = null; // { columns: string[], source: QueryBuilder|Raw|string }
    this._update = null;

    this._returning = null;

    // upsert helpers
    this._onConflict = null; // { target, update }
    this._onDuplicate = null; // { update }

    // set operations
    this._unions = []; // [{ all:boolean, source: QueryBuilder|Raw|string }]
  }

  // ---------- Helpers ----------
  /**
   * Quote identifier safely for SQL dialects.
   * @param {string|Identifier} name
   */
  q(name) {
    const n = isId(name) ? name.name : String(name);
    return quoteIdent(/** @type {any} */(this.dialect), n);
  }

  /**
   * @param {any} v
   * @param {any[]} params
   */
  pushValue(v, params) {
    if (isRaw(v)) {
      params.push(...v.params);
      return v.sql;
    }
    params.push(v);
    return this.adapter.placeholder(params.length);
  }

  // ---------- Public fluent API (SELECT) ----------

  /**
   * Set selected columns.
   * @param {string[]|string} cols
   */
  select(cols = ['*']) {
    this._type = 'select';
    this._select = Array.isArray(cols) ? cols : [cols];
    return this;
  }

  /**
   * Select distinct rows.
   */
  distinct() {
    this._distinct = true;
    return this;
  }



/**
 * Add an aggregate selection. Works for SQL and for Mongo aggregate compilation.
 * If you use aggregates, SQuery will automatically switch Mongo to `aggregate()` and SQL to SELECT with functions.
 *
 * @param {'count'|'sum'|'avg'|'min'|'max'} fn
 * @param {string} [column='*'] Column name or '*' for count(*)
 * @param {string} [as] Alias for the aggregate field
 */
aggregate(fn, column = '*', as) {
  this._type = 'select';
  const alias = as ?? (fn === 'count' ? 'count' : `${fn}_${String(column).replace(/\W+/g, '_')}`);
  this._aggregates.push({ fn, column, as: alias });
  return this;
}

/**
 * COUNT aggregate.
 * @param {string} [column='*']
 * @param {string} [as='count']
 */
count(column = '*', as = 'count') { return this.aggregate('count', column, as); }

/**
 * SUM aggregate.
 * @param {string} column
 * @param {string} [as]
 */
sum(column, as) { return this.aggregate('sum', column, as); }

/**
 * AVG aggregate.
 * @param {string} column
 * @param {string} [as]
 */
avg(column, as) { return this.aggregate('avg', column, as); }

/**
 * MIN aggregate.
 * @param {string} column
 * @param {string} [as]
 */
min(column, as) { return this.aggregate('min', column, as); }

/**
 * MAX aggregate.
 * @param {string} column
 * @param {string} [as]
 */
max(column, as) { return this.aggregate('max', column, as); }

/**
 * Clear aggregates (keeps normal selected columns).
 */
clearAggregates() {
  this._aggregates = [];
  return this;
}

  /**
   * UNION with another SELECT source.
   * @param {QueryBuilder|Raw|string} source
   */
  union(source) {
    this._type = 'select';
    this._unions.push({ all: false, source });
    return this;
  }

  /**
   * UNION ALL with another SELECT source.
   * @param {QueryBuilder|Raw|string} source
   */
  unionAll(source) {
    this._type = 'select';
    this._unions.push({ all: true, source });
    return this;
  }

  /**
   * Clear union clauses.
   */
  clearUnions() {
    this._unions = [];
    return this;
  }

  /**
   * Add a join.
   * @param {'inner'|'left'|'right'|'full'} type
   * @param {string} table
   * @param {string} left
   * @param {string} op
   * @param {string} right
   */
  join(type, table, left, opOrRight, maybeRight) {
    // Backward compatible:
    // - join(type, table, left, op, right)
    // - join(type, table, left, right)   // defaults op to "="
    const op = assertOp((maybeRight === undefined) ? '=' : opOrRight, JOIN_OPS, 'join');
    const right = (maybeRight === undefined) ? opOrRight : maybeRight;

    this._joins.push({ type, table, left, op, right });
    return this;
  }

  /**
   * Equality JOIN ergonomic shortcut.
   * Equivalent to join(type, table, left, '=', right).
   */
  joinOn(type, table, left, right) { return this.join(type, table, left, '=', right); }


  /**
   * INNER JOIN helper.
   */
  innerJoin(table, left, opOrRight, maybeRight) {
    return (maybeRight === undefined)
      ? this.join('inner', table, left, opOrRight)
      : this.join('inner', table, left, opOrRight, maybeRight);
  }
  /**
   * LEFT JOIN helper.
   */
  leftJoin(table, left, opOrRight, maybeRight) {
    return (maybeRight === undefined)
      ? this.join('left', table, left, opOrRight)
      : this.join('left', table, left, opOrRight, maybeRight);
  }
  /**
   * RIGHT JOIN helper.
   */
  rightJoin(table, left, opOrRight, maybeRight) {
    return (maybeRight === undefined)
      ? this.join('right', table, left, opOrRight)
      : this.join('right', table, left, opOrRight, maybeRight);
  }

  /**
   * INNER JOIN equality helper.
   */
  innerJoinOn(table, left, right) { return this.joinOn('inner', table, left, right); }

  /**
   * LEFT JOIN equality helper.
   */
  leftJoinOn(table, left, right) { return this.joinOn('left', table, left, right); }

  /**
   * RIGHT JOIN equality helper.
   */
  rightJoinOn(table, left, right) { return this.joinOn('right', table, left, right); }


  /**
   * Add a WHERE condition (defaults to '=').
   * Supports:
   * - where(column, value)
   * - where((q) => q.where(...).orWhere(...))
   * @param {string|Raw|((q: QueryBuilder)=>any)} column
   * @param {any} value
   */
  where(column, value) {
    if (typeof column === 'function') return this.whereGroup(column, 'AND');
    return this.whereOp(column, '=', value, 'AND');
  }

  /**
   * Add a WHERE condition with operator.
   * @param {string|Raw} column
   * @param {string} op
   * @param {any} value
   * @param {'AND'|'OR'} [bool]
   */
  whereOp(column, op, value, bool = 'AND') {
    const joiner = String(bool).toUpperCase() === 'OR' ? 'OR' : 'AND';
    this._wheres.push({ bool: joiner, kind: 'basic', column, op: assertOp(op, WHERE_OPS, 'whereOp'), value });
    return this;
  }

  /**
   * WHERE column OP ANY (...)
   * @param {string|Raw} column
   * @param {string} op
   * @param {QueryBuilder|Raw|string|any[]} value
   * @param {'AND'|'OR'} [bool]
   */
  whereAny(column, op, value, bool = 'AND') {
    const joiner = String(bool).toUpperCase() === 'OR' ? 'OR' : 'AND';
    this._wheres.push({
      bool: joiner,
      kind: 'quantified',
      column,
      op: assertOp(op, QUANTIFIED_OPS, 'whereAny'),
      quantifier: 'ANY',
      value,
    });
    return this;
  }

  /**
   * OR WHERE column OP ANY (...)
   * @param {string|Raw} column
   * @param {string} op
   * @param {QueryBuilder|Raw|string|any[]} value
   */
  orWhereAny(column, op, value) {
    return this.whereAny(column, op, value, 'OR');
  }

  /**
   * WHERE column OP ALL (...)
   * @param {string|Raw} column
   * @param {string} op
   * @param {QueryBuilder|Raw|string|any[]} value
   * @param {'AND'|'OR'} [bool]
   */
  whereAll(column, op, value, bool = 'AND') {
    const joiner = String(bool).toUpperCase() === 'OR' ? 'OR' : 'AND';
    this._wheres.push({
      bool: joiner,
      kind: 'quantified',
      column,
      op: assertOp(op, QUANTIFIED_OPS, 'whereAll'),
      quantifier: 'ALL',
      value,
    });
    return this;
  }

  /**
   * OR WHERE column OP ALL (...)
   * @param {string|Raw} column
   * @param {string} op
   * @param {QueryBuilder|Raw|string|any[]} value
   */
  orWhereAll(column, op, value) {
    return this.whereAll(column, op, value, 'OR');
  }

  /**
   * OR WHERE condition.
   * Supports:
   * - orWhere(column, value)
   * - orWhere((q) => q.where(...).where(...))
   * @param {string|Raw|((q: QueryBuilder)=>any)} column
   * @param {any} value
   */
  orWhere(column, value) {
    if (typeof column === 'function') return this.whereGroup(column, 'OR');
    return this.whereOp(column, '=', value, 'OR');
  }

  /**
   * Group WHERE clauses with explicit parentheses.
   * Example:
   *   .whereGroup(q => q.where('a', 1).orWhere('b', 2))
   *
   * @param {(q: QueryBuilder)=>any} fn
   * @param {'AND'|'OR'} [bool]
   */
  whereGroup(fn, bool = 'AND') {
    if (typeof fn !== 'function') throw new Error('whereGroup(fn) expects a function');
    const joiner = String(bool).toUpperCase() === 'OR' ? 'OR' : 'AND';
    const nested = new QueryBuilder(this.adapter, this._table);
    fn(nested);
    if (!nested._wheres.length) return this;
    this._wheres.push({ bool: joiner, kind: 'group', wheres: nested._wheres });
    return this;
  }

  /**
   * OR-group WHERE clauses with explicit parentheses.
   * @param {(q: QueryBuilder)=>any} fn
   */
  orWhereGroup(fn) {
    return this.whereGroup(fn, 'OR');
  }

  /**
   * Add NOT (...) grouped WHERE clause.
   * @param {(q: QueryBuilder)=>any} fn
   * @param {'AND'|'OR'} [bool]
   */
  whereNot(fn, bool = 'AND') {
    if (typeof fn !== 'function') throw new Error('whereNot(fn) expects a function');
    const joiner = String(bool).toUpperCase() === 'OR' ? 'OR' : 'AND';
    const nested = new QueryBuilder(this.adapter, this._table);
    fn(nested);
    if (!nested._wheres.length) return this;
    this._wheres.push({ bool: joiner, kind: 'notGroup', wheres: nested._wheres });
    return this;
  }

  /**
   * OR NOT (...) grouped WHERE clause.
   * @param {(q: QueryBuilder)=>any} fn
   */
  orWhereNot(fn) {
    return this.whereNot(fn, 'OR');
  }

  /**
   * WHERE IN.
   * @param {string} column
   * @param {any[]} values
   */
  whereIn(column, values) {
    this._wheres.push({ bool: 'AND', kind: 'in', column, values, not: false });
    return this;
  }

  /**
   * WHERE NOT IN.
   * @param {string} column
   * @param {any[]} values
   */
  whereNotIn(column, values) {
    this._wheres.push({ bool: 'AND', kind: 'in', column, values, not: true });
    return this;
  }

  /**
   * WHERE BETWEEN.
   * @param {string} column
   * @param {any} a
   * @param {any} b
   */
  whereBetween(column, a, b) {
    this._wheres.push({ bool: 'AND', kind: 'between', column, a, b, not: false });
    return this;
  }

  /**
   * WHERE NOT BETWEEN.
   */
  whereNotBetween(column, a, b) {
    this._wheres.push({ bool: 'AND', kind: 'between', column, a, b, not: true });
    return this;
  }

  /**
   * WHERE column IS value.
   * Supports NULL/TRUE/FALSE/UNKNOWN and raw(...) expressions.
   * @param {string|Raw} column
   * @param {any} value
   * @param {'AND'|'OR'} [bool]
   */
  whereIs(column, value, bool = 'AND') {
    if (value == null) {
      return (String(bool).toUpperCase() === 'OR')
        ? this._pushWhereNull(column, false, 'OR')
        : this.whereNull(column);
    }
    const joiner = String(bool).toUpperCase() === 'OR' ? 'OR' : 'AND';
    this._wheres.push({ bool: joiner, kind: 'is', column, not: false, value });
    return this;
  }

  /**
   * OR WHERE column IS value.
   * @param {string|Raw} column
   * @param {any} value
   */
  orWhereIs(column, value) {
    return this.whereIs(column, value, 'OR');
  }

  /**
   * WHERE column IS NOT value.
   * @param {string|Raw} column
   * @param {any} value
   * @param {'AND'|'OR'} [bool]
   */
  whereIsNot(column, value, bool = 'AND') {
    if (value == null) {
      return (String(bool).toUpperCase() === 'OR')
        ? this._pushWhereNull(column, true, 'OR')
        : this.whereNotNull(column);
    }
    const joiner = String(bool).toUpperCase() === 'OR' ? 'OR' : 'AND';
    this._wheres.push({ bool: joiner, kind: 'is', column, not: true, value });
    return this;
  }

  /**
   * OR WHERE column IS NOT value.
   * @param {string|Raw} column
   * @param {any} value
   */
  orWhereIsNot(column, value) {
    return this.whereIsNot(column, value, 'OR');
  }

  /**
   * WHERE IS NULL.
   */
  whereNull(column) {
    this._pushWhereNull(column, false, 'AND');
    return this;
  }

  /**
   * WHERE IS NOT NULL.
   */
  whereNotNull(column) {
    this._pushWhereNull(column, true, 'AND');
    return this;
  }

  /**
   * Alias of whereNull.
   */
  whereIsNull(column) { return this.whereNull(column); }

  /**
   * Alias of whereNotNull.
   */
  whereIsNotNull(column) { return this.whereNotNull(column); }

  /**
   * GROUP BY.
   * @param {string[]|string} cols
   */
  groupBy(cols) {
    const arr = Array.isArray(cols) ? cols : [cols];
    this._groupBy.push(...arr);
    return this;
  }

  /**
   * HAVING.
   * @param {string|Raw} column
   * @param {string} op
   * @param {any} value
   */
  having(column, op, value) {
    this._havings.push({ bool: 'AND', column, op: assertOp(op, WHERE_OPS, 'having'), value });
    return this;
  }

  /**
   * ORDER BY.
   * @param {string} column
   * @param {'asc'|'desc'} [direction]
   */
  orderBy(column, direction = 'asc') {
    const dir = String(direction).toLowerCase();
    if (dir !== 'asc' && dir !== 'desc') {
      throw new Error(`Invalid order direction: ${direction}`);
    }
    this._orderBy.push({ column, direction: dir });
    return this;
  }

  /**
   * LIMIT.
   * @param {number} n
   */
  limit(n) {
    const v = Number(n);
    if (!Number.isFinite(v)) throw new Error(`Invalid limit value: ${n}`);
    this._limit = v;
    return this;
  }

  /**
   * OFFSET.
   * @param {number} n
   */
  offset(n) {
    const v = Number(n);
    if (!Number.isFinite(v)) throw new Error(`Invalid offset value: ${n}`);
    this._offset = v;
    return this;
  }

  /**
   * Add RETURNING clause (Postgres only by default).
   * For other SQL dialects, it will be ignored unless adapter supports it.
   *
   * @param {string[]|string} cols
   */
  returning(cols) {
    const arr = Array.isArray(cols) ? cols : [cols];
    this._returning = arr;
    return this;
  }

  _pushWhereNull(column, not, bool) {
    const joiner = String(bool).toUpperCase() === 'OR' ? 'OR' : 'AND';
    this._wheres.push({ bool: joiner, kind: 'null', column, not: !!not });
    return this;
  }

  // ---------- INSERT / UPDATE / DELETE ----------

  /**
   * INSERT.
   * @param {Record<string, any>|Record<string, any>[]} data
   */
  insert(data) {
    this._type = 'insert';
    this._insert = Array.isArray(data) ? data : [data];
    this._insertSelect = null;
    return this;
  }

  /**
   * INSERT INTO ... SELECT ...
   * @param {string[]|string} columns
   * @param {QueryBuilder|Raw|string} source
   */
  insertSelect(columns, source) {
    const arr = Array.isArray(columns) ? columns : [columns];
    const cols = arr.map(c => String(c)).filter(Boolean);
    if (!cols.length) throw new Error('insertSelect() requires at least one column');
    this._type = 'insert';
    this._insert = null;
    this._insertSelect = { columns: cols, source };
    return this;
  }

  /**
   * UPDATE.
   * @param {Record<string, any>} data
   */
  update(data) {
    this._type = 'update';
    this._update = data;
    return this;
  }

  /**
   * DELETE.
   */
  delete() {
    this._type = 'delete';
    return this;
  }

  /**
   * Postgres upsert: ON CONFLICT (target) DO UPDATE SET ...
   * @param {string|string[]} target
   * @param {Record<string, any>} update
   */
  onConflictDoUpdate(target, update) {
    this._onConflict = { target: Array.isArray(target) ? target : [target], update };
    return this;
  }

  /**
   * MySQL upsert: ON DUPLICATE KEY UPDATE ...
   * @param {Record<string, any>} update
   */
  onDuplicateKeyUpdate(update) {
    this._onDuplicate = { update };
    return this;
  }

  // ---------- Execution ----------

  /**
   * Compile into a SQL statement + params (or a Mongo operation).
   * @returns {{ sql?: string, params?: any[], mongo?: any }}
   */
  compile() {
    if (this.dialect === 'mongo') {
      return { mongo: this._compileMongo() };
    }
    const params = [];
    const sql = this._compileSql(params);
    return { sql, params };
  }

  /**
   * Execute and return rows (SELECT) or adapter result (mutations).
   */
  async run() {
    return this.adapter.execute(this);
  }

  /**
   * Convenience for SELECT: returns array of rows.
   */
  async get() {
    this._type = 'select';
    return this.adapter.execute(this);
  }

  /**
   * Convenience for SELECT: return first row or null.
   */
  async first() {
    this._type = 'select';
    if (this._limit == null) this.limit(1);
    const rows = await this.adapter.execute(this);
    return Array.isArray(rows) ? (rows[0] ?? null) : rows;
  }

  // ---------- Internals: SQL ----------
  _compileSql(params) {
    switch (this._type) {
      case 'insert': return this._compileInsert(params);
      case 'update': return this._compileUpdate(params);
      case 'delete': return this._compileDelete(params);
      case 'select':
      default: return this._compileSelect(params);
    }
  }

  _compileSelectBase(params) {
    // SELECT columns + optional aggregates
    const distinct = this._distinct ? 'DISTINCT ' : '';

    let selectParts = [];
    const baseCols = (this._select && this._select[0] !== '*')
      ? this._select.slice()
      : [];

    // If we have GROUP BY, ensure grouped columns are selected (SQL compatibility)
    for (const g of this._groupBy) {
      if (!baseCols.includes(g)) baseCols.push(g);
    }

    // regular columns
    selectParts.push(...baseCols.map(c => this.q(c)));

    // aggregates (COUNT/SUM/AVG/MIN/MAX)
    if (this._aggregates.length) {
      for (const a of this._aggregates) {
        const fn = String(a.fn).toUpperCase();
        const colExpr = (a.fn === 'count' && String(a.column) === '*')
          ? '*'
          : this.q(a.column);
        const alias = quoteIdent(this.dialect, a.as);
        selectParts.push(`${fn}(${colExpr}) AS ${alias}`);
      }
    }

    if (!selectParts.length) selectParts = ['*'];

    const cols = selectParts.join(', ');
    let sql = `SELECT ${distinct}${cols} FROM ${this.q(this._table)}`;

    for (const j of this._joins) {
      sql += ` ${j.type.toUpperCase()} JOIN ${this.q(j.table)} ON ${this.q(j.left)} ${j.op} ${this.q(j.right)}`;
    }

    sql += this._compileWhere(params);

    if (this._groupBy.length) {
      sql += ` GROUP BY ${this._groupBy.map(c => this.q(c)).join(', ')}`;
    }

    if (this._havings.length) {
      const parts = [];
      for (const h of this._havings) {
        const left = isRaw(h.column) ? h.column.sql : this.q(h.column);
        const right = this.pushValue(h.value, params);
        parts.push(`${parts.length ? h.bool + ' ' : ''}${left} ${h.op} ${right}`);
      }
      sql += ` HAVING ${parts.join(' ')}`;
    }

    if (this._orderBy.length) {
      sql += ` ORDER BY ${this._orderBy.map(o => `${this.q(o.column)} ${String(o.direction).toUpperCase()}`).join(', ')}`;
    }

    const limit = this._limit != null ? Math.max(0, this._limit) : null;
    const offset = this._offset != null ? Math.max(0, this._offset) : null;

    if (this.dialect === 'mssql') {
      if (limit != null || offset != null) {
        if (!this._orderBy.length) sql += ' ORDER BY (SELECT 0)';
        sql += ` OFFSET ${offset ?? 0} ROWS`;
        if (limit != null) sql += ` FETCH NEXT ${limit} ROWS ONLY`;
      }
      return sql;
    }

    if (this.dialect === 'oracle') {
      if (offset != null) sql += ` OFFSET ${offset} ROWS`;
      if (limit != null) sql += ` FETCH NEXT ${limit} ROWS ONLY`;
      return sql;
    }

    if (limit != null) sql += ` LIMIT ${limit}`;
    if (offset != null) sql += ` OFFSET ${offset}`;

    return sql;
  }

  _compileSelect(params) {
    let sql = this._compileSelectBase(params);
    if (!this._unions.length) return sql;

    if (this.dialect === 'mongo') {
      throw new Error('UNION is not supported for mongo');
    }

    const parts = [`(${sql})`];
    for (const u of this._unions) {
      const subSql = this._compileSelectSource(u.source, params, 'union');
      parts.push(`${u.all ? 'UNION ALL' : 'UNION'} (${subSql})`);
    }
    return parts.join(' ');
  }

  _compileInsert(params) {
    if (this._onConflict && (this.dialect === 'mssql' || this.dialect === 'oracle')) {
      return this._compileMergeUpsertInsert(params);
    }

    let sql;
    const mssqlOutput = this._compileMssqlOutputClause('insert');
    if (this._insertSelect) {
      const cols = this._insertSelect.columns;
      const colSql = cols.map(c => this.q(c)).join(', ');
      const selectSql = this._compileSelectSource(this._insertSelect.source, params, 'insertSelect');
      sql = `INSERT INTO ${this.q(this._table)} (${colSql})${mssqlOutput} ${selectSql}`;
    } else {
      if (!this._insert || this._insert.length === 0) throw new Error('insert() requires data');
      const rows = this._insert;
      if (!isPlainObject(rows[0])) throw new Error('insert() expects object rows');

      const cols = Object.keys(rows[0]);
      if (cols.length === 0) throw new Error('insert() expects at least one column');

      // ensure consistent columns
      for (const r of rows) {
        const k = Object.keys(r);
        if (k.length !== cols.length || !cols.every(c => Object.prototype.hasOwnProperty.call(r, c))) {
          throw new Error('All insert rows must have same columns');
        }
      }

      const colSql = cols.map(c => this.q(c)).join(', ');
      const valueSql = rows.map(r => {
        const vs = cols.map(c => this.pushValue(r[c], params)).join(', ');
        return `(${vs})`;
      }).join(', ');

      sql = `INSERT INTO ${this.q(this._table)} (${colSql})${mssqlOutput} VALUES ${valueSql}`;
    }

    // upsert
    if (this.dialect === 'pg' && this._onConflict) {
      const target = this._onConflict.target.map(c => this.q(c)).join(', ');
      const set = Object.entries(this._onConflict.update).map(([k, v]) => `${this.q(k)} = ${this.pushValue(v, params)}`).join(', ');
      sql += ` ON CONFLICT (${target}) DO UPDATE SET ${set}`;
    }

    if (this.dialect === 'mysql' && this._onDuplicate) {
      const set = Object.entries(this._onDuplicate.update).map(([k, v]) => `${this.q(k)} = ${this.pushValue(v, params)}`).join(', ');
      sql += ` ON DUPLICATE KEY UPDATE ${set}`;
    }

    if (this._returning && this.adapter.supportsReturning() && this.dialect !== 'mssql') {
      sql += ` RETURNING ${this._returning.map(c => this.q(c)).join(', ')}`;
    }

    return sql;
  }

  _compileMergeUpsertInsert(params) {
    if (this._insertSelect) {
      throw new Error(`${this.dialect}: onConflictDoUpdate() does not support insertSelect()`);
    }
    if (!this._insert || this._insert.length !== 1) {
      throw new Error(`${this.dialect}: onConflictDoUpdate() currently supports a single insert row`);
    }
    if (!isPlainObject(this._insert[0])) {
      throw new Error('insert() expects object rows');
    }

    const row = this._insert[0];
    const cols = Object.keys(row);
    if (!cols.length) throw new Error('insert() expects at least one column');

    const targetCols = (this._onConflict?.target ?? []).map(c => String(c)).filter(Boolean);
    if (!targetCols.length) {
      throw new Error('onConflictDoUpdate() requires at least one target column');
    }

    for (const col of targetCols) {
      if (!Object.prototype.hasOwnProperty.call(row, col)) {
        throw new Error(`onConflictDoUpdate target column "${col}" is missing from insert data`);
      }
    }

    const updateEntries = Object.entries(this._onConflict?.update ?? {});
    if (!updateEntries.length) {
      throw new Error('onConflictDoUpdate() requires at least one update field');
    }

    const sourceSelect = cols.map((c) => {
      const v = this.pushValue(row[c], params);
      return this.dialect === 'oracle'
        ? `${v} AS ${this.q(c)}`
        : `${v} AS ${this.q(c)}`;
    }).join(', ');

    const sourceSql = this.dialect === 'oracle'
      ? `(SELECT ${sourceSelect} FROM dual) source`
      : `(SELECT ${sourceSelect}) AS source`;

    const targetAlias = this.dialect === 'oracle' ? 'target' : 'target';
    const onSql = targetCols
      .map(c => `${targetAlias}.${this.q(c)} = source.${this.q(c)}`)
      .join(' AND ');

    const setSql = updateEntries
      .map(([k, v]) => `${targetAlias}.${this.q(k)} = ${this.pushValue(v, params)}`)
      .join(', ');

    const insertCols = cols.map(c => this.q(c)).join(', ');
    const insertVals = cols.map(c => `source.${this.q(c)}`).join(', ');

    let sql;
    if (this.dialect === 'oracle') {
      sql =
        `MERGE INTO ${this.q(this._table)} target ` +
        `USING ${sourceSql} ` +
        `ON (${onSql}) ` +
        `WHEN MATCHED THEN UPDATE SET ${setSql} ` +
        `WHEN NOT MATCHED THEN INSERT (${insertCols}) VALUES (${insertVals})`;
    } else {
      sql =
        `MERGE INTO ${this.q(this._table)} AS target ` +
        `USING ${sourceSql} ` +
        `ON (${onSql}) ` +
        `WHEN MATCHED THEN UPDATE SET ${setSql} ` +
        `WHEN NOT MATCHED THEN INSERT (${insertCols}) VALUES (${insertVals})`;
    }

    const mssqlOutput = this._compileMssqlOutputClause('insert');
    if (mssqlOutput) sql += mssqlOutput;
    return sql;
  }

  _compileMssqlOutputClause(kind = 'insert') {
    if (this.dialect !== 'mssql' || !this._returning || !this._returning.length) return '';
    const rowAlias = kind === 'delete' ? 'deleted' : 'inserted';
    const cols = this._returning.map((c) => {
      const raw = String(c ?? '').trim();
      if (raw === '*') return `${rowAlias}.*`;
      const normalized = stripPrefix(raw, this._table);
      if (normalized === '*') return `${rowAlias}.*`;
      const leaf = String(normalized).split('.').pop();
      return `${rowAlias}.${this.q(leaf)}`;
    });
    return ` OUTPUT ${cols.join(', ')}`;
  }

  _compileSelectSource(source, params, ctx = 'source') {
    if (isRaw(source)) {
      params.push(...source.params);
      return source.sql;
    }
    if (typeof source === 'string') {
      return source;
    }
    if (source instanceof QueryBuilder) {
      if (source.dialect === 'mongo') throw new Error(`${ctx} does not support mongo query source`);
      if (source._type !== 'select') throw new Error(`${ctx} requires a SELECT query source`);
      return source._compileSelect(params);
    }
    throw new Error(`${ctx} requires QueryBuilder, raw(sql), or SQL string`);
  }

  _compileQuantifiedValue(value, quantifier, params) {
    const q = String(quantifier).toUpperCase() === 'ALL' ? 'ALL' : 'ANY';
    if (Array.isArray(value)) {
      if (this.dialect === 'pg') {
        params.push(value);
        return `${q} (${this.adapter.placeholder(params.length)})`;
      }
      throw new Error(`${q}(array) is compiled inline for ${this.dialect}; expected path from _compileWhereParts`);
    }
    const sourceSql = this._compileSelectSource(value, params, `where${q}`);
    return `${q} (${sourceSql})`;
  }

  _compileQuantifiedArrayExpr(leftExpr, op, quantifier, values, params) {
    const q = String(quantifier).toUpperCase() === 'ALL' ? 'ALL' : 'ANY';
    const list = Array.isArray(values) ? values : [];
    if (!list.length) {
      return q === 'ALL' ? '1=1' : '1=0';
    }

    const joiner = q === 'ALL' ? ' AND ' : ' OR ';
    const parts = list.map((v) => {
      const right = this.pushValue(v, params);
      return `${leftExpr} ${op} ${right}`;
    });
    if (parts.length === 1) return parts[0];
    return `(${parts.join(joiner)})`;
  }

  _compileIsValue(value, params) {
    if (isRaw(value)) {
      params.push(...value.params);
      return value.sql;
    }
    if (value == null) return 'NULL';
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    const keyword = String(value).toUpperCase();
    if (keyword === 'NULL' || keyword === 'TRUE' || keyword === 'FALSE' || keyword === 'UNKNOWN') {
      return keyword;
    }
    throw new Error('whereIs/whereIsNot supports NULL/TRUE/FALSE/UNKNOWN, booleans, or raw(sql)');
  }

  _compileUpdate(params) {
    if (!this._update) throw new Error('update() requires data');
    const set = Object.entries(this._update).map(([k, v]) => `${this.q(k)} = ${this.pushValue(v, params)}`).join(', ');
    let sql = `UPDATE ${this.q(this._table)} SET ${set}`;
    sql += this._compileMssqlOutputClause('update');
    sql += this._compileWhere(params);

    if (this._returning && this.adapter.supportsReturning() && this.dialect !== 'mssql') {
      sql += ` RETURNING ${this._returning.map(c => this.q(c)).join(', ')}`;
    }

    return sql;
  }

  _compileDelete(params) {
    let sql = `DELETE FROM ${this.q(this._table)}`;
    sql += this._compileMssqlOutputClause('delete');
    sql += this._compileWhere(params);

    if (this._returning && this.adapter.supportsReturning() && this.dialect !== 'mssql') {
      sql += ` RETURNING ${this._returning.map(c => this.q(c)).join(', ')}`;
    }

    return sql;
  }

  _compileWhere(params) {
    const compiled = this._compileWhereParts(this._wheres, params);
    return compiled ? ` WHERE ${compiled}` : '';
  }

  _compileWhereParts(wheres, params) {
    if (!wheres?.length) return '';
    const parts = [];

    for (const w of wheres) {
      const prefix = parts.length ? `${w.bool} ` : '';

      if (w.kind === 'group') {
        const inner = this._compileWhereParts(w.wheres, params);
        if (inner) parts.push(`${prefix}(${inner})`);
        continue;
      }

      if (w.kind === 'notGroup') {
        const inner = this._compileWhereParts(w.wheres, params);
        if (inner) parts.push(`${prefix}NOT (${inner})`);
        continue;
      }

      if (w.kind === 'basic') {
        const left = isRaw(w.column) ? w.column.sql : this.q(w.column);
        const right = this.pushValue(w.value, params);
        parts.push(`${prefix}${left} ${w.op} ${right}`);
      }

      if (w.kind === 'quantified') {
        const left = isRaw(w.column) ? w.column.sql : this.q(w.column);
        if (Array.isArray(w.value) && this.dialect !== 'pg') {
          const expanded = this._compileQuantifiedArrayExpr(left, w.op, w.quantifier, w.value, params);
          parts.push(`${prefix}${expanded}`);
        } else {
          const right = this._compileQuantifiedValue(w.value, w.quantifier, params);
          parts.push(`${prefix}${left} ${w.op} ${right}`);
        }
      }

      if (w.kind === 'is') {
        const left = isRaw(w.column) ? w.column.sql : this.q(w.column);
        const right = this._compileIsValue(w.value, params);
        parts.push(`${prefix}${left} IS ${w.not ? 'NOT ' : ''}${right}`);
      }

      if (w.kind === 'in') {
        const left = this.q(w.column);
        const vals = Array.isArray(w.values) ? w.values : [];
        if (vals.length === 0) {
          // WHERE col IN () is invalid; choose false/true shortcut.
          parts.push(`${prefix}${w.not ? '1=1' : '1=0'}`);
        } else {
          const placeholders = vals.map(v => this.pushValue(v, params)).join(', ');
          parts.push(`${prefix}${left} ${w.not ? 'NOT ' : ''}IN (${placeholders})`);
        }
      }

      if (w.kind === 'between') {
        const left = this.q(w.column);
        const a = this.pushValue(w.a, params);
        const b = this.pushValue(w.b, params);
        parts.push(`${prefix}${left} ${w.not ? 'NOT ' : ''}BETWEEN ${a} AND ${b}`);
      }

      if (w.kind === 'null') {
        const left = this.q(w.column);
        parts.push(`${prefix}${left} IS ${w.not ? 'NOT ' : ''}NULL`);
      }
    }

    return parts.join(' ');
  }

  // ---------- Internals: Mongo compilation ----------

  _compileMongo() {
    // Mongo "SQL builder" mapping:
    // - Simple select (no joins, no group/aggregates) -> find()
    // - Joins ($lookup/$unwind), OR filters, groupBy/having/aggregates -> aggregate(pipeline)

    const collection = this._table;

    if (this._unions.length) {
      if (this._type !== 'select') {
        throw new Error('Mongo UNION/UNION ALL is only supported for SELECT queries');
      }
      const unions = this._unions;
      const saved = this._unions;
      this._unions = [];
      try {
        return {
          op: 'union',
          collection,
          base: this._compileMongo(),
          unions: unions.map(u => ({ all: !!u.all, source: compileMongoSource(u.source, 'union') })),
        };
      } finally {
        this._unions = saved;
      }
    }

    const hasJoins = this._joins.length > 0;
    const hasGrouping = this._groupBy.length > 0 || this._aggregates.length > 0 || this._havings.length > 0;

    const filter = buildMongoWhere(this._wheres, collection);

    const projection = this._select && this._select[0] !== '*'
      ? Object.fromEntries(this._select.map(c => [stripPrefix(String(c), collection), 1]))
      : null;

    const sort = this._orderBy.length
      ? Object.fromEntries(this._orderBy.map(o => [stripPrefix(String(o.column), collection), String(o.direction).toLowerCase() === 'desc' ? -1 : 1]))
      : null;

    const limit = this._limit != null ? Math.max(0, this._limit) : null;
    const skip = this._offset != null ? Math.max(0, this._offset) : null;

    // Mutations remain direct operations (no joins/grouping).
    if (this._type === 'insert') {
      if (this._insertSelect) {
        const source = this._insertSelect.source;
        if (!(source instanceof QueryBuilder)) {
          throw new Error('Mongo insertSelect() supports only QueryBuilder source');
        }
        if (source.dialect !== 'mongo') {
          throw new Error('Mongo insertSelect() requires a mongo source query');
        }
        if (source._type !== 'select') {
          throw new Error('Mongo insertSelect() source must be a SELECT query');
        }

        const cols = this._insertSelect.columns.map(c => String(c));
        const sourceCols = source._select.map(c => String(c));
        if (!sourceCols.length || (sourceCols.length === 1 && sourceCols[0] === '*')) {
          throw new Error('Mongo insertSelect() requires explicit source select columns');
        }
        if (sourceCols.length !== cols.length) {
          throw new Error('Mongo insertSelect() requires source select columns count to match target columns');
        }

        return {
          op: 'insertSelect',
          collection,
          source: source._compileMongo(),
          mappings: cols.map((target, i) => ({
            target,
            source: stripPrefix(sourceCols[i], source._table),
          })),
        };
      }

      const docs = this._insert ?? [];
      if (!docs.length) throw new Error('insert() requires data');
      if (this._onDuplicate) {
        throw new Error('Mongo adapter does not support onDuplicateKeyUpdate(); use onConflictDoUpdate(...)');
      }
      if (this._onConflict) {
        if (docs.length !== 1) {
          throw new Error('Mongo onConflictDoUpdate() currently supports a single insert row');
        }
        const target = (this._onConflict.target ?? []).map(c => String(c)).filter(Boolean);
        if (!target.length) throw new Error('onConflictDoUpdate() requires at least one target column');

        const doc = docs[0];
        const filter = {};
        for (const col of target) {
          if (!Object.prototype.hasOwnProperty.call(doc, col)) {
            throw new Error(`onConflictDoUpdate target column "${col}" is missing from insert data`);
          }
          filter[col] = doc[col];
        }

        const setUpdate = isPlainObject(this._onConflict.update) ? this._onConflict.update : {};
        const update = { $setOnInsert: doc };
        if (Object.keys(setUpdate).length) update.$set = setUpdate;

        return {
          op: 'upsertOne',
          collection,
          filter,
          update,
        };
      }
      return { op: docs.length <= 1 ? 'insertOne' : 'insertMany', collection, docs };
    }
    if (this._type === 'update') {
      return { op: 'updateMany', collection, filter, update: { $set: this._update ?? {} } };
    }
    if (this._type === 'delete') {
      return { op: 'deleteMany', collection, filter };
    }

    // SELECT:
    if (!hasJoins && !hasGrouping && !containsOr(this._wheres)) {
      return { op: 'find', collection, filter, projection, sort, limit, skip };
    }

    // Otherwise use aggregate pipeline.
    const pipeline = [];

    if (filter && Object.keys(filter).length) pipeline.push({ $match: filter });

    // Joins via $lookup
    for (const [i, j] of this._joins.entries()) {
      const joinTable = String(j.table);
      const left = stripPrefix(String(j.left), collection);
      const right = stripPrefix(String(j.right), joinTable);
      const op = normalizeOp(j.op || '=');

      if (op === '=') {
        pipeline.push({
          $lookup: {
            from: joinTable,
            localField: left,
            foreignField: right,
            as: joinTable,
          },
        });
      } else {
        const leftVar = `__qm_join_left_${i + 1}`;
        pipeline.push({
          $lookup: {
            from: joinTable,
            let: { [leftVar]: `$${left}` },
            pipeline: [
              {
                $match: {
                  $expr: {
                    [mongoJoinExprOperator(op)]: [`$$${leftVar}`, `$${right}`],
                  },
                },
              },
            ],
            as: joinTable,
          },
        });
      }

      // INNER vs LEFT
      const preserve = String(j.type).toLowerCase() !== 'inner';
      pipeline.push({ $unwind: { path: `$${joinTable}`, preserveNullAndEmptyArrays: preserve } });
    }

    // Grouping / aggregates
    if (hasGrouping) {
      const idSpec = buildMongoGroupId(this._groupBy, collection);
      const groupStage = { _id: idSpec };

      // If user didn't request any aggregates explicitly, provide count(*) as "count"
      const aggs = this._aggregates.length ? this._aggregates : [{ fn: 'count', column: '*', as: 'count' }];

      for (const a of aggs) {
        groupStage[a.as] = mongoAccumulator(a, collection);
      }

      pipeline.push({ $group: groupStage });

      // HAVING becomes $match after $group
      const havingFilter = buildMongoHaving(this._havings, this._groupBy);
      if (havingFilter && Object.keys(havingFilter).length) pipeline.push({ $match: havingFilter });

      // Project grouped fields out of _id
      const project = {};
      for (const g of this._groupBy) {
        const gf = stripPrefix(String(g), collection);
        project[gf] = `$_id.${gf}`;
      }
      for (const a of aggs) project[a.as] = 1;
      pipeline.push({ $project: project });
    } else if (projection) {
      pipeline.push({ $project: projection });
    }

    if (sort) pipeline.push({ $sort: sort });
    if (skip != null) pipeline.push({ $skip: skip });
    if (limit != null) pipeline.push({ $limit: limit });

    return { op: 'aggregate', collection, pipeline };
  }

}


function containsOr(wheres) {
  return (wheres ?? []).some(w => w.bool === 'OR');
}

function stripPrefix(path, table) {
  const s = String(path);
  const prefix = String(table) + '.';
  return s.startsWith(prefix) ? s.slice(prefix.length) : s;
}

function condToFilter(w, baseTable) {
  if (w.kind === 'group') {
    return buildMongoWhere(w.wheres ?? [], baseTable);
  }
  if (w.kind === 'notGroup') {
    const inner = buildMongoWhere(w.wheres ?? [], baseTable);
    if (isEmptyFilter(inner)) return {};
    return { $nor: [inner] };
  }
  if (w.kind === 'basic') {
    const col = stripPrefix(String(w.column), baseTable);
    return { [col]: mongoOp(w.op, w.value) };
  }
  if (w.kind === 'quantified') {
    const col = stripPrefix(String(w.column), baseTable);
    return quantifiedMongoFilter(col, w.op, w.quantifier, w.value);
  }
  if (w.kind === 'is') {
    const col = stripPrefix(String(w.column), baseTable);
    const parsed = parseIsKeyword(w.value);
    if (parsed === '__unknown__') throw new Error('Mongo adapter does not support IS UNKNOWN');
    if (w.not) return { [col]: { $ne: parsed } };
    return { [col]: parsed };
  }
  if (w.kind === 'in') {
    const col = stripPrefix(String(w.column), baseTable);
    return { [col]: w.not ? { $nin: w.values } : { $in: w.values } };
  }
  if (w.kind === 'between') {
    const col = stripPrefix(String(w.column), baseTable);
    return {
      [col]: w.not
        ? { $not: { $gte: w.a, $lte: w.b } }
        : { $gte: w.a, $lte: w.b },
    };
  }
  if (w.kind === 'null') {
    const col = stripPrefix(String(w.column), baseTable);
    return { [col]: w.not ? { $ne: null } : null };
  }
  throw new Error(`Unsupported where kind for mongo: ${w.kind}`);
}

function andFilter(conds) {
  const arr = (conds ?? []).filter(Boolean);
  if (arr.length === 0) return {};
  if (arr.length === 1) return arr[0];
  return { $and: arr };
}

function isEmptyFilter(v) {
  return !v || (typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 0);
}

/**
 * Build Mongo filter with support for OR.
 * We model the builder's sequential AND/OR as:
 *   (A AND B) OR (C AND D) OR ...
 */
function buildMongoWhere(wheres, baseTable) {
  const ws = wheres ?? [];
  if (!ws.length) return {};

  const groups = [];
  let current = [];

  for (const w of ws) {
    const cond = condToFilter(w, baseTable);
    if (isEmptyFilter(cond)) continue;
    if (w.bool === 'OR') {
      if (current.length) groups.push(current);
      current = [cond];
    } else {
      current.push(cond);
    }
  }
  if (current.length) groups.push(current);

  const disj = groups.map(g => andFilter(g)).filter(g => Object.keys(g).length);

  if (disj.length === 0) return {};
  if (disj.length === 1) return disj[0];
  return { $or: disj };
}

function buildMongoGroupId(groupBy, baseTable) {
  const gs = groupBy ?? [];
  if (!gs.length) return null;
  const id = {};
  for (const g of gs) {
    const gf = stripPrefix(String(g), baseTable);
    id[gf] = `$${gf}`;
  }
  return id;
}

function mongoAccumulator(a, baseTable) {
  const fn = String(a.fn).toLowerCase();
  const col = String(a.column);

  if (fn === 'count') {
    if (col === '*') return { $sum: 1 };
    const field = `$${stripPrefix(col, baseTable)}`;
    return { $sum: { $cond: [{ $ne: [field, null] }, 1, 0] } };
  }

  const field = `$${stripPrefix(col, baseTable)}`;
  switch (fn) {
    case 'sum': return { $sum: field };
    case 'avg': return { $avg: field };
    case 'min': return { $min: field };
    case 'max': return { $max: field };
    default: throw new Error(`Unsupported mongo aggregate: ${a.fn}`);
  }
}

function buildMongoHaving(havings, groupBy) {
  const hs = havings ?? [];
  if (!hs.length) return {};

  // Having runs after $group; group fields live in _id.<field>.
  // Aggregate fields live at root (alias).
  const mapped = hs.map(h => {
    const col = String(h.column);
    const isGroup = (groupBy ?? []).some(g => stripPrefix(String(g), '') === stripPrefix(col, ''));
    const key = isGroup ? `_id.${stripPrefix(col, '')}` : col;
    return { bool: h.bool, cond: { [key]: mongoOp(h.op, h.value) } };
  });

  // reuse same AND/OR grouping logic
  const groups = [];
  let current = [];
  for (const m of mapped) {
    if (m.bool === 'OR') {
      groups.push(current);
      current = [m.cond];
    } else {
      current.push(m.cond);
    }
  }
  groups.push(current);

  const disj = groups.map(g => andFilter(g)).filter(g => Object.keys(g).length);
  if (disj.length === 0) return {};
  if (disj.length === 1) return disj[0];
  return { $or: disj };
}

function mongoOp(op, value) {
  const normalized = normalizeOp(op);
  switch (normalized) {
    case '=': return value;
    case '!=':
    case '<>': return { $ne: value };
    case '>': return { $gt: value };
    case '>=': return { $gte: value };
    case '<': return { $lt: value };
    case '<=': return { $lte: value };
    case 'LIKE': {
      // naive LIKE -> regex
      return { $regex: buildLikeRegex(value) };
    }
    case 'NOT LIKE': {
      return { $not: buildLikeRegex(value) };
    }
    default:
      throw new Error(`Mongo adapter unsupported operator: ${op}`);
  }
}

function compileMongoSource(source, ctx = 'source') {
  if (source instanceof QueryBuilder) {
    if (source.dialect !== 'mongo') throw new Error(`${ctx} requires a mongo query source`);
    if (source._type !== 'select') throw new Error(`${ctx} requires a SELECT query source`);
    return source._compileMongo();
  }
  if (isRaw(source) || typeof source === 'string') {
    throw new Error(`${ctx} supports only QueryBuilder sources for mongo`);
  }
  throw new Error(`${ctx} requires QueryBuilder source for mongo`);
}

function mongoJoinExprOperator(op) {
  const normalized = normalizeOp(op);
  switch (normalized) {
    case '=': return '$eq';
    case '!=':
    case '<>': return '$ne';
    case '>': return '$gt';
    case '>=': return '$gte';
    case '<': return '$lt';
    case '<=': return '$lte';
    default:
      throw new Error(`Mongo join does not support operator: ${op}`);
  }
}

function quantifiedMongoFilter(column, op, quantifier, value) {
  if (!Array.isArray(value)) {
    throw new Error('Mongo ANY/ALL currently supports only literal arrays');
  }
  const values = value;
  const q = String(quantifier).toUpperCase();
  const normalizedOp = normalizeOp(op);

  if (q !== 'ANY' && q !== 'ALL') {
    throw new Error(`Unsupported quantifier for mongo: ${quantifier}`);
  }

  if (values.length === 0) {
    return q === 'ANY' ? alwaysFalseFilter() : alwaysTrueFilter();
  }

  if (q === 'ANY') {
    switch (normalizedOp) {
      case '=':
        return { [column]: { $in: values } };
      case '!=':
      case '<>':
        return allSame(values)
          ? { [column]: { $ne: values[0] } }
          : alwaysTrueFilter();
      case '>':
        return { [column]: { $gt: minValue(values) } };
      case '>=':
        return { [column]: { $gte: minValue(values) } };
      case '<':
        return { [column]: { $lt: maxValue(values) } };
      case '<=':
        return { [column]: { $lte: maxValue(values) } };
      default:
        throw new Error(`Mongo ANY does not support operator: ${op}`);
    }
  }

  // ALL
  switch (normalizedOp) {
    case '=':
      return allSame(values)
        ? { [column]: values[0] }
        : alwaysFalseFilter();
    case '!=':
    case '<>':
      return { [column]: { $nin: values } };
    case '>':
      return { [column]: { $gt: maxValue(values) } };
    case '>=':
      return { [column]: { $gte: maxValue(values) } };
    case '<':
      return { [column]: { $lt: minValue(values) } };
    case '<=':
      return { [column]: { $lte: minValue(values) } };
    default:
      throw new Error(`Mongo ALL does not support operator: ${op}`);
  }
}

function alwaysTrueFilter() {
  return { $expr: { $eq: [1, 1] } };
}

function alwaysFalseFilter() {
  return { $expr: { $eq: [1, 0] } };
}

function allSame(values) {
  if (!values.length) return true;
  const first = values[0];
  for (let i = 1; i < values.length; i += 1) {
    if (!Object.is(values[i], first)) return false;
  }
  return true;
}

function minValue(values) {
  let min = values[0];
  for (let i = 1; i < values.length; i += 1) {
    if (values[i] < min) min = values[i];
  }
  return min;
}

function maxValue(values) {
  let max = values[0];
  for (let i = 1; i < values.length; i += 1) {
    if (values[i] > max) max = values[i];
  }
  return max;
}

function parseIsKeyword(value) {
  if (value == null) return null;
  if (typeof value === 'boolean') return value;
  const up = String(value).toUpperCase();
  if (up === 'NULL') return null;
  if (up === 'TRUE') return true;
  if (up === 'FALSE') return false;
  if (up === 'UNKNOWN') return '__unknown__';
  return value;
}

function buildLikeRegex(value) {
  const s = String(value);
  const pattern = '^' + s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/%/g, '.*').replace(/_/g, '.') + '$';
  return new RegExp(pattern, 'i');
}
