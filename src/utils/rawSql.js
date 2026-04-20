/**
 * Normalize raw SQL params to an array.
 * @param {any[]|any} params
 */
export function normalizeRawParams(params) {
  if (params == null) return [];
  return Array.isArray(params) ? params : [params];
}

/**
 * Prepare raw SQL for the current dialect.
 *
 * QueryMesh accepts portable `?` placeholders and common native placeholders
 * (`$1`, `@p1`, `:p1`) then rewrites them to the connected dialect.
 *
 * @param {'pg'|'mysql'|'mssql'|'oracle'|'mongo'|string} dialect
 * @param {string} sql
 * @param {any[]|any} params
 */
export function prepareRawSql(dialect, sql, params = []) {
  const values = normalizeRawParams(params);
  const input = String(sql);
  if (!values.length || !input) return { sql: input, params: values };

  let out = '';
  let i = 0;
  let anonymousIndex = 0;
  let found = false;
  const usedSourceIndexes = new Set();

  const appendPlaceholder = (sourceIndex, token) => {
    if (!Number.isInteger(sourceIndex) || sourceIndex < 0 || sourceIndex >= values.length) {
      throw new Error(`Raw SQL placeholder ${token} has no matching parameter`);
    }
    found = true;
    usedSourceIndexes.add(sourceIndex);
    const paramIndex = preparedParams.length + 1;
    preparedParams.push(values[sourceIndex]);
    out += placeholderForDialect(dialect, paramIndex);
  };

  const nextAnonymousIndex = () => {
    while (usedSourceIndexes.has(anonymousIndex)) anonymousIndex += 1;
    return anonymousIndex++;
  };

  const preparedParams = [];

  while (i < input.length) {
    const ch = input[i];
    const next = input[i + 1];

    if (ch === '-' && next === '-') {
      const end = input.indexOf('\n', i + 2);
      const chunk = end === -1 ? input.slice(i) : input.slice(i, end + 1);
      out += chunk;
      i += chunk.length;
      continue;
    }

    if (ch === '/' && next === '*') {
      const end = input.indexOf('*/', i + 2);
      const chunk = end === -1 ? input.slice(i) : input.slice(i, end + 2);
      out += chunk;
      i += chunk.length;
      continue;
    }

    if (ch === "'") {
      const chunk = readQuoted(input, i, "'", true);
      out += chunk;
      i += chunk.length;
      continue;
    }

    if (ch === '"') {
      const chunk = readQuoted(input, i, '"', true);
      out += chunk;
      i += chunk.length;
      continue;
    }

    if (ch === '`') {
      const chunk = readQuoted(input, i, '`', true);
      out += chunk;
      i += chunk.length;
      continue;
    }

    if (ch === '[') {
      const end = input.indexOf(']', i + 1);
      const chunk = end === -1 ? input.slice(i) : input.slice(i, end + 1);
      out += chunk;
      i += chunk.length;
      continue;
    }

    if (ch === '$') {
      const quoteTag = readDollarQuoteTag(input, i);
      if (quoteTag) {
        const end = input.indexOf(quoteTag, i + quoteTag.length);
        const chunk = end === -1 ? input.slice(i) : input.slice(i, end + quoteTag.length);
        out += chunk;
        i += chunk.length;
        continue;
      }

      const token = readNumberedToken(input, i + 1);
      if (token) {
        appendPlaceholder(Number(token.value) - 1, `$${token.value}`);
        i = token.end;
        continue;
      }
    }

    if (ch === '?' && next !== '?' && next !== '|' && next !== '&') {
      appendPlaceholder(nextAnonymousIndex(), '?');
      i += 1;
      continue;
    }

    if (ch === '@' && (next === 'p' || next === 'P')) {
      const token = readNumberedToken(input, i + 2);
      if (token) {
        appendPlaceholder(Number(token.value) - 1, `@p${token.value}`);
        i = token.end;
        continue;
      }
    }

    if (ch === ':' && input[i - 1] !== ':' && (next === 'p' || next === 'P')) {
      const token = readNumberedToken(input, i + 2);
      if (token) {
        appendPlaceholder(Number(token.value) - 1, `:p${token.value}`);
        i = token.end;
        continue;
      }
    }

    out += ch;
    i += 1;
  }

  return found
    ? { sql: out, params: preparedParams }
    : { sql: input, params: values };
}

/**
 * Return a rows array that also supports pg-style `.rows` and `.rowCount`.
 * Extra metadata is non-enumerable so existing array-based callers still work.
 * @param {any[]} rows
 * @param {Record<string, any>} [meta]
 */
export function makeRawQueryResult(rows = [], meta = {}) {
  const result = Array.isArray(rows) ? rows : [];
  const rowCount = normalizeRowCount(meta.rowCount, result);

  defineHidden(result, 'rows', result);
  defineHidden(result, 'rowCount', rowCount);

  for (const [key, value] of Object.entries(meta)) {
    if (key === 'rows' || key === 'rowCount') continue;
    if (value !== undefined) defineHidden(result, key, value);
  }

  return result;
}

function placeholderForDialect(dialect, index) {
  if (dialect === 'pg') return `$${index}`;
  if (dialect === 'mssql') return `@p${index}`;
  if (dialect === 'oracle') return `:p${index}`;
  return '?';
}

function readQuoted(input, start, quote, doubledQuoteEscapes = true) {
  let i = start + 1;
  while (i < input.length) {
    if (input[i] === '\\' && i + 1 < input.length) {
      i += 2;
      continue;
    }
    if (input[i] === quote) {
      if (doubledQuoteEscapes && input[i + 1] === quote) {
        i += 2;
        continue;
      }
      return input.slice(start, i + 1);
    }
    i += 1;
  }
  return input.slice(start);
}

function readDollarQuoteTag(input, start) {
  const rest = input.slice(start);
  const match = rest.match(/^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/);
  return match ? match[0] : null;
}

function readNumberedToken(input, start) {
  let end = start;
  while (end < input.length && input[end] >= '0' && input[end] <= '9') end += 1;
  if (end === start) return null;
  return { value: input.slice(start, end), end };
}

function normalizeRowCount(value, rows) {
  if (Array.isArray(value)) {
    return value.reduce((sum, n) => sum + (Number(n) || 0), 0);
  }
  if (Number.isFinite(value)) return Number(value);
  return rows.length;
}

function defineHidden(target, key, value) {
  Object.defineProperty(target, key, {
    value,
    enumerable: false,
    configurable: true,
    writable: false,
  });
}
