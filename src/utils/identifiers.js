/**
 * Identifier utilities: validate + quote identifiers safely.
 *
 * We take an "eager-safe" approach:
 * - validate identifiers (no spaces, no quotes, no SQL operators)
 * - quote identifiers based on dialect
 * - allow dotted paths like schema.table or table.column
 */

const IDENT_PART_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * @param {string} name
 * @returns {string[]}
 */
export function splitIdent(name) {
  return String(name).split('.').filter(Boolean);
}

/**
 * @param {string} name
 * @returns {void}
 */
export function assertIdent(name) {
  const parts = splitIdent(name);
  if (parts.length === 0) throw new Error(`Invalid identifier: "${name}"`);
  for (const p of parts) {
    if (!IDENT_PART_RE.test(p)) {
      throw new Error(`Unsafe identifier part: "${p}" (from "${name}")`);
    }
  }
}

/**
 * @param {'pg'|'mysql'|'mssql'|'oracle'} dialect
 * @param {string} name
 * @returns {string}
 */
export function quoteIdent(dialect, name) {
  assertIdent(name);
  const parts = splitIdent(name);
  switch (dialect) {
    case 'mysql':
      return parts.map(p => `\`${p}\``).join('.');
    case 'mssql':
      return parts.map(p => `[${p}]`).join('.');
    case 'oracle':
    case 'pg':
    default:
      return parts.map(p => `"${p}"`).join('.');
  }
}

/**
 * Quote a qualified wildcard like "users.*" or "public.users.*".
 * Returns null when the input is not a qualified wildcard.
 *
 * @param {'pg'|'mysql'|'mssql'|'oracle'} dialect
 * @param {string} name
 * @returns {string|null}
 */
export function quoteQualifiedWildcard(dialect, name) {
  const raw = String(name ?? '').trim();
  if (!raw.endsWith('.*')) return null;
  const base = raw.slice(0, -2);
  if (!base) return null;
  return `${quoteIdent(dialect, base)}.*`;
}

/**
 * @param {any} v
 * @returns {boolean}
 */
export function isPlainObject(v) {
  return v != null && typeof v === 'object' && (v.constructor === Object || Object.getPrototypeOf(v) === Object.prototype);
}
