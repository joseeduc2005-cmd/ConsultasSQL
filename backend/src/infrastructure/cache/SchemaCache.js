/**
 * SchemaCache - Cache inteligente para schema de BD
 * 
 * Características:
 * - TTL configurable (default: 60 segundos)
 * - Invalidación manual
 * - Prefetch en background
 */

export class SchemaCache {
  constructor(ttlMs = 60000) {
    this.ttlMs = ttlMs;
    this.cache = new Map();
    this.lastFetch = new Map();
    this.isFetching = false;
  }

  /**
   * Guarda datos en cache
   */
  set(key, value) {
    this.cache.set(key, {
      value,
      expiresAt: Date.now() + this.ttlMs,
      createdAt: Date.now()
    });
    this.lastFetch.set(key, Date.now());
  }

  /**
   * Obtiene datos del cache si no han expirado
   */
  get(key) {
    const entry = this.cache.get(key);
    
    if (!entry) return null;
    
    if (entry.expiresAt < Date.now()) {
      this.cache.delete(key);
      return null;
    }
    
    return entry.value;
  }

  /**
   * Verifica si existe y es válido
   */
  has(key) {
    return this.get(key) !== null;
  }

  /**
   * Invalida todo el cache
   */
  invalidate() {
    console.log('🔄 Invalidando cache de schema...');
    this.cache.clear();
    this.lastFetch.clear();
  }

  /**
   * Invalida una clave específica
   */
  invalidateKey(key) {
    this.cache.delete(key);
    this.lastFetch.delete(key);
  }

  /**
   * Obtiene estado del cache
   */
  getStats() {
    let totalSize = 0;
    let validEntries = 0;

    for (const [key, entry] of this.cache.entries()) {
      if (entry.expiresAt > Date.now()) {
        validEntries++;
        totalSize += JSON.stringify(entry.value).length;
      }
    }

    return {
      totalEntries: this.cache.size,
      validEntries,
      estimatedSizeKB: (totalSize / 1024).toFixed(2),
      ttlMs: this.ttlMs,
      cacheName: 'SchemaCache'
    };
  }

  /**
   * Limpiar entradas expiradas
   */
  cleanup() {
    let cleaned = 0;
    for (const [key, entry] of this.cache.entries()) {
      if (entry.expiresAt < Date.now()) {
        this.cache.delete(key);
        cleaned++;
      }
    }
    if (cleaned > 0) {
      console.log(`🧹 Limpiado: ${cleaned} entradas expiradas de cache`);
    }
    return cleaned;
  }

  /**
   * Obtiene o ejecuta con pattern GetOrFetch
   */
  async getOrFetch(key, fetchFn) {
    const cached = this.get(key);
    if (cached) {
      console.log(`📦 Cache hit para: ${key}`);
      return cached;
    }

    console.log(`🔄 Cache miss para: ${key}, obteniendo datos...`);
    const value = await fetchFn();
    this.set(key, value);
    return value;
  }
}

export default SchemaCache;
