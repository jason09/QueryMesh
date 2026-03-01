/**
 * Library error with optional structured details payload.
 */
export class SQueryError extends Error {
  /**
   * @param {string} message
   * @param {Record<string, any>} [details]
   */
  constructor(message, details) {
    super(String(message));
    this.name = 'SQueryError';
    if (details && typeof details === 'object') {
      this.details = details;
    }
  }
}

export default SQueryError;
