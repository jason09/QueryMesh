/**
 * Represents a raw SQL fragment.
 * Use sparingly and only with trusted input.
 */
export class Raw {
  /**
   * @param {string} sql
   * @param {any[]} [params]
   */
  constructor(sql, params = []) {
    this.sql = String(sql);
    this.params = Array.isArray(params) ? params : [params];
  }
}

/**
 * @param {string} sql
 * @param {any[]} [params]
 */
export function raw(sql, params = []) {
  return new Raw(sql, params);
}
