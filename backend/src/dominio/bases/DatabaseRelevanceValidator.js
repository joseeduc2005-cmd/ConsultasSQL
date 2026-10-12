/**
 * DatabaseRelevanceValidator.js
 * 
 * Validates whether a user query is actually about the database and should be sent to the backend.
 * Prevents sending non-DB queries to the backend, improving Ollama's intent detection.
 */

export class DatabaseRelevanceValidator {
  constructor() {
    // Keywords that indicate a query is likely about the database
    this.dbKeywords = new Set([
      'usuario', 'usuarios',
      'tabla', 'tablas',
      'registro', 'registros',
      'datos', 'dato',
      'base de datos', 'base datos', 'db', 'database',
      'consulta', 'consultas', 'query', 'queries',
      'rol', 'roles',
      'permiso', 'permisos',
      'log', 'logs', 'bitacora', 'bitácora',
      'sesion', 'sesiones', 'session',
      'auditoria', 'auditoría',
      'columna', 'columnas',
      'campo', 'campos',
      'filtrar', 'filtro', 'filter',
      'buscar', 'search',
      'listar', 'list',
      'contar', 'count',
      'agrupar', 'group',
      'activo', 'inactivo',
      'estado', 'status',
      'id', 'identificador',
      'nombre', 'email', 'cuenta',
      'sql', 'query',
      'error', 'errores',
      'historial', 'history',
      'rastrear', 'tracking',
      'análisis', 'analysis',
      'reporte', 'report',
      'sistema', 'system',
      'aplicación', 'application',
    ]);

    // Keywords that indicate a query is NOT about the database (conversation)
    this.nonDbKeywords = new Set([
      'hola', 'hello',
      'como estás', 'cómo estás', 'how are you',
      'que tal', 'qué tal',
      'ayuda', 'help',
      'gracias', 'thanks', 'thank you',
      'por favor', 'please',
      'chiste', 'broma', 'joke',
      'música', 'music',
      'película', 'movie', 'film', 'películas',
      'receta', 'recipe',
      'clima', 'weather',
      'noticias', 'news',
      'deportes', 'sports',
      'política', 'politics',
      'juego', 'game',
      'viaje', 'travel',
      'comida', 'food',
      'restaurante', 'restaurant',
      'hotel', 'accommodation',
      'precio', 'price',
      'compra', 'shopping',
      'dinero', 'money',
      'consejo', 'advice',
      'cuento', 'story',
      'libro', 'book',
      'escuela', 'school',
      'historia', 'history',
    ]);

    // Ambiguous keywords that need context to determine if they're DB-related
    this.ambiguousKeywords = new Set([
      'información', 'informacion', 'info',
      'detalles', 'details',
      'búsqueda', 'busqueda', 'search',
      'resultado', 'results',
      'mostrar', 'show',
      'obtener', 'get',
      'trae', 'bring',
      'dame', 'give me',
      'dime', 'tell me',
      'cuantos', 'cuántos', 'how many',
      'cuál', 'cual', 'which',
      'quién', 'quien', 'who',
    ]);

    this.minRelevanceScoreForDbQuery = 0.5;
  }

  /**
   * Normalize text for matching
   */
  normalizeText(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9_\s-]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  /**
   * Extract tokens from input
   */
  tokenize(value) {
    return this.normalizeText(value)
      .split(/[\s,.:;!?]+/)
      .filter(Boolean);
  }

  containsIdentifierPattern(value) {
    const source = String(value || '').trim();
    if (!source) return false;

    const hasUuid = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i.test(source);
    const hasNumericId = /\b(?:id|uuid|user_id|usuario_id)\b\s*[:=]?\s*\d{1,20}\b/i.test(source);
    const idWithEntity = /\b(?:usuario|usuarios|user|users|log|logs|sesion|sesiones|session|sessions)\b[\s\S]*?\b(?:id|uuid|user_id|usuario_id)\b/i.test(source);

    return hasUuid || hasNumericId || idWithEntity;
  }

  /**
   * Calculate how database-relevant the query is
   * Returns: { isDbRelated: boolean, relevanceScore: number, reason: string }
   */
  validateRelevance(userQuery) {
    if (!userQuery || typeof userQuery !== 'string' || userQuery.trim().length === 0) {
      return {
        isDbRelated: false,
        relevanceScore: 0,
        reason: 'Consulta vacía o inválida',
      };
    }

    const normalized = this.normalizeText(userQuery);
    const tokens = this.tokenize(userQuery);

    if (this.containsIdentifierPattern(userQuery)) {
      return {
        isDbRelated: true,
        relevanceScore: 0.95,
        reason: 'Patron de identificador (id/uuid) detectado',
        matchedDbKeywords: ['id'],
        matchedNonDbKeywords: [],
        ambiguousKeywordCount: 0,
      };
    }

    if (tokens.length === 0) {
      return {
        isDbRelated: false,
        relevanceScore: 0,
        reason: 'No se pudo extraer palabras clave de la consulta',
      };
    }

    // Count keyword matches
    let dbKeywordCount = 0;
    let nonDbKeywordCount = 0;
    let ambiguousKeywordCount = 0;
    let matchedDbKeywords = [];
    let matchedNonDbKeywords = [];

    for (const token of tokens) {
      if (this.nonDbKeywords.has(token)) {
        nonDbKeywordCount++;
        matchedNonDbKeywords.push(token);
      } else if (this.dbKeywords.has(token)) {
        dbKeywordCount++;
        matchedDbKeywords.push(token);
      } else if (this.ambiguousKeywords.has(token)) {
        ambiguousKeywordCount++;
      }
    }

    // Calculate relevance score
    let relevanceScore = 0;
    let reason = '';

    // Check for negative context FIRST (strong indicator of non-DB query)
    if (nonDbKeywordCount > 0) {
      relevanceScore = 0;
      reason = `Palabras clave detectadas como NO relacionadas con DB: ${matchedNonDbKeywords.join(', ')}`;
      return {
        isDbRelated: false,
        relevanceScore,
        reason,
        matchedDbKeywords,
        matchedNonDbKeywords,
        ambiguousKeywordCount,
      };
    }

    // Strong DB indicators
    if (dbKeywordCount >= 2) {
      relevanceScore = Math.min(1, 0.85 + (dbKeywordCount * 0.05));
      reason = `Detectados ${dbKeywordCount} palabras clave de base de datos: ${matchedDbKeywords.join(', ')}`;
    } else if (dbKeywordCount === 1) {
      relevanceScore = 0.75 + (ambiguousKeywordCount * 0.05);
      reason = `Detectada 1 palabra clave de base de datos: ${matchedDbKeywords[0]}`;
    } else if (ambiguousKeywordCount > 0) {
      // Ambiguous keywords might be DB-related (need entity resolution)
      relevanceScore = 0.4;
      reason = 'Palabras ambiguas detectadas; podría ser relacionada a DB con más contexto';
    } else {
      // No keywords matched - likely not about DB
      relevanceScore = 0.2;
      reason = 'No se detectaron palabras clave claras de base de datos';
    }

    // Boost score if query contains database-specific patterns
    if (/(\bcon\s+\d+\s+|\bmas de\s+\d+\s+|\bmenos de\s+\d+\s+|\nexactamente\s+\d+)/i.test(userQuery)) {
      relevanceScore = Math.min(1, relevanceScore + 0.15);
      reason += '; Patrón de agregación detectado';
    }

    // Penalize if query seems conversational (e.g., starts with common conversation starters)
    if (/^(hola|hi|hey|buenos|buenas|qué tal|que tal|como estás|cómo estás)\b/i.test(userQuery)) {
      relevanceScore = Math.max(0, relevanceScore - 0.3);
      reason = 'Consulta comienza con saludo conversacional';
    }

    const isDbRelated = relevanceScore >= this.minRelevanceScoreForDbQuery;

    return {
      isDbRelated,
      relevanceScore: Number(relevanceScore.toFixed(2)),
      reason,
      matchedDbKeywords,
      matchedNonDbKeywords,
      ambiguousKeywordCount,
    };
  }

  /**
   * Create a rejection response for non-DB queries
   * Uses the same format as the rest of the system (resumenHumano + resultado)
   */
  createNonDbQueryResponse(userQuery, validation) {
    return {
      resumenHumano: 'Esta consulta no parece estar relacionada con la base de datos. Por favor, usa el campo para consultas sobre datos del sistema (usuarios, registros, logs, roles, etc.)',
      resultado: [],
      metadata: {
        entidad: 'no-db-related',
        total: 0,
        executionType: 'rejected',
        sources: [],
        confidence: 0,
        notDbRelated: true,
        relevanceScore: validation.relevanceScore,
        reason: validation.reason,
        suggestions: [
          'Mostrar usuarios activos',
          'Usuarios con más de 5 logs',
          'Contar registros de la tabla usuarios',
          'Logs del sistema por fecha',
        ],
        trazabilidad: {
          interpretadoPor: 'relevance-validator',
          intencion: 'not-db-related',
          confianza: 0,
        },
      },
    };
  }
}

export default DatabaseRelevanceValidator;
