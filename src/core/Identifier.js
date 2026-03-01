/**
 * Represents a trusted identifier (table/column name) that will be validated and quoted.
 */
export class Identifier {
  /**
   * @param {string} name
   */
  constructor(name) {
    this.name = String(name);
  }
}

/**
 * Create an identifier wrapper.
 *
 * @param {string} name
 */
export function id(name) {
  return new Identifier(name);
}
