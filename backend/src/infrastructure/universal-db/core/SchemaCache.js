export default class SchemaCache {
  constructor(ttlMs = 60000) {
    this.ttlMs = Math.max(1000, Number(ttlMs) || 60000);
    this._entry = null;
  }

  get() {
    if (!this._entry) return null;

    const now = Date.now();
    if (this._entry.expiresAt <= now) {
      this._entry = null;
      return null;
    }

    return this._entry.value;
  }

  set(value) {
    this._entry = {
      value,
      createdAt: Date.now(),
      expiresAt: Date.now() + this.ttlMs,
    };
  }

  clear() {
    this._entry = null;
  }

  getMeta() {
    if (!this._entry) {
      return {
        hasValue: false,
        createdAt: null,
        expiresAt: null,
        ttlMs: this.ttlMs,
      };
    }

    return {
      hasValue: true,
      createdAt: this._entry.createdAt,
      expiresAt: this._entry.expiresAt,
      ttlMs: this.ttlMs,
    };
  }
}
