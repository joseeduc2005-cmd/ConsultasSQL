import { UniversalDbError } from '../core/errors.js';

export default class BaseAdapter {
  constructor(config) {
    this.config = config;
    this.pool = null;
    this.engine = 'base';
  }

  async createPool() {
    throw new UniversalDbError('ADAPTER_NOT_IMPLEMENTED', `${this.engine}: createPool not implemented`);
  }

  async ping() {
    throw new UniversalDbError('ADAPTER_NOT_IMPLEMENTED', `${this.engine}: ping not implemented`);
  }

  async introspectSchema() {
    throw new UniversalDbError('ADAPTER_NOT_IMPLEMENTED', `${this.engine}: introspectSchema not implemented`);
  }

  getPool() {
    return this.pool;
  }

  async close() {
    if (!this.pool) return;

    const closers = ['end', 'close'];
    for (const method of closers) {
      if (typeof this.pool[method] === 'function') {
        await this.pool[method]();
        this.pool = null;
        return;
      }
    }

    this.pool = null;
  }

  normalizeSchema(raw = {}) {
    return raw;
  }
}
