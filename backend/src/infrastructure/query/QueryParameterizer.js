/**
 * QueryParameterizer - Inyecta parámetros en consultas incompletas
 *
 * RESPONSABILIDADES:
 * - Detectar consultas que requieren parámetros
 * - Generar filtros WHERE basados en valores
 * - Clasificar tipo de parámetro (UUID, exacto, parcial)
 * - Inyectar en el input original para QueryBuilder
 * - NO modificar SQL generado (solo pre-procesar input)
 *
 * FLUJO:
 * 1. Usuario: "usuario"
 * 2. Detectar: requiere parámetro
 * 3. Retornar: prompt pidiendo input
 * 4. Usuario: { value: "admin" }
 * 5. Inyectar: "usuario admin" (completo)
 * 6. Ejecutar: flujo normal QueryBuilder
 */

export class QueryParameterizer {
  constructor() {
    this.learnedSemanticDictionary = {
      tableAliases: {},
    };
    this.schemaContext = {
      tables: [],
    };

    // Patrones que indican entidad sola (sin especificar)
    this.incompletePatterns = {
      usuario: /^usuario$/i,
      user: /^user$/i,
      cliente: /^cliente|customer$/i,
      employee: /^employee|empleado$/i,
      sesion: /^sesion|session$/i,
      log: /^log$/i,
    };

    // Patrones que indican ya está completo (no pedir input)
    this.completePatterns = [
      /usuarios?\s+con\s+m[aá]s/i,        // usuarios con más logs
      /usuarios?\s+activos/i,             // usuarios activos
      /todos?\s+los?\s+usuarios?/i,       // todos los usuarios
      /usuario\s+[a-zA-Z0-9_\-\.]/i,      // usuario [valor]
      /user\s+[a-zA-Z0-9_\-\.]/i,         // user [valor]
      /\b[0-9a-f]{8}-[0-9a-f]{4}/i,       // UUID
      /"[^"]*"|'[^']*'/,                  // Texto entre comillas
      /\d+/,                              // Números
    ];

    // Palabras clave que indican tipo de entidad
    this.entityTypeMap = {
      usuario: { field: 'usuario', table: 'users', prompt: 'Ingrese el nombre o ID del usuario' },
      user: { field: 'usuario', table: 'users', prompt: 'Ingrese el nombre o ID del usuario' },
      cliente: { field: 'cliente', table: 'customers', prompt: 'Ingrese el nombre o ID del cliente' },
      customer: { field: 'cliente', table: 'customers', prompt: 'Ingrese el nombre o ID del cliente' },
      empleado: { field: 'empleado', table: 'employees', prompt: 'Ingrese el nombre o ID del empleado' },
      employee: { field: 'empleado', table: 'employees', prompt: 'Ingrese el nombre o ID del empleado' },
      sesion: { field: 'sesion', table: 'sessions', prompt: 'Ingrese el ID de la sesión' },
      session: { field: 'sesion', table: 'sessions', prompt: 'Ingrese el ID de la sesión' },
      log: { field: 'log', table: 'logs', prompt: 'Ingrese el ID del log' },
    };
  }

  setLearnedSemanticDictionary(dictionary = {}) {
    this.learnedSemanticDictionary = {
      tableAliases: { ...(dictionary?.tableAliases || {}) },
    };
  }

  setSchemaContext(schemaContext = {}) {
    const normalizedTables = [...new Set(
      (schemaContext?.tables || [])
        .map((tableName) => this.normalizeTerm(tableName))
        .filter(Boolean)
    )];

    this.schemaContext = {
      tables: normalizedTables,
    };
  }

  normalizeTerm(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '');
  }

  normalizeEntityToken(value) {
    const normalized = this.normalizeTerm(value)
      .replace(/[^a-z0-9_\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    if (!normalized) return '';
    if (normalized.length > 4 && normalized.endsWith('es')) return normalized.slice(0, -2);
    if (normalized.length > 3 && normalized.endsWith('s')) return normalized.slice(0, -1);
    return normalized;
  }

  levenshteinDistance(a, b) {
    const s = String(a || '');
    const t = String(b || '');
    if (s === t) return 0;
    if (!s.length) return t.length;
    if (!t.length) return s.length;

    const dp = Array.from({ length: s.length + 1 }, () => new Array(t.length + 1).fill(0));
    for (let i = 0; i <= s.length; i += 1) dp[i][0] = i;
    for (let j = 0; j <= t.length; j += 1) dp[0][j] = j;

    for (let i = 1; i <= s.length; i += 1) {
      for (let j = 1; j <= t.length; j += 1) {
        const cost = s[i - 1] === t[j - 1] ? 0 : 1;
        dp[i][j] = Math.min(
          dp[i - 1][j] + 1,
          dp[i][j - 1] + 1,
          dp[i - 1][j - 1] + cost
        );
      }
    }

    return dp[s.length][t.length];
  }

  similarityScore(a, b) {
    const left = this.normalizeEntityToken(a);
    const right = this.normalizeEntityToken(b);
    if (!left || !right) return 0;

    if (left === right) return 1;
    if (left.includes(right) || right.includes(left)) return 0.9;

    const distance = this.levenshteinDistance(left, right);
    const maxLen = Math.max(left.length, right.length, 1);
    return 1 - (distance / maxLen);
  }

  resolveSchemaEntityInfo(entityTerm) {
    const normalized = this.normalizeEntityToken(entityTerm);
    if (!normalized) return null;

    const availableTables = this.schemaContext?.tables || [];
    if (!availableTables.length) return null;

    let bestTable = '';
    let bestScore = 0;

    for (const tableName of availableTables) {
      const score = this.similarityScore(normalized, tableName);
      if (score > bestScore) {
        bestScore = score;
        bestTable = tableName;
      }
    }

    if (!bestTable || bestScore < 0.56) return null;

    return {
      field: normalized,
      table: bestTable,
      prompt: `Ingrese el criterio para ${normalized}`,
      inferredFromSchema: true,
      similarity: bestScore,
    };
  }

  escapeRegexLiteral(value) {
    return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  resolveLearnedEntityInfo(entityTerm) {
    const normalized = this.normalizeEntityToken(entityTerm);
    if (!normalized) return null;

    const mappedTables = this.learnedSemanticDictionary?.tableAliases?.[normalized] || [];
    const targetTable = String(mappedTables[0] || '').trim().toLowerCase();
    if (!targetTable) return null;

    return {
      field: normalized,
      table: targetTable,
      prompt: `Ingrese el criterio para ${normalized}`,
    };
  }

  /**
   * FASE 1: Detectar si la consulta es incompleta
   */
  isIncompleteQuery(text) {
    const normalized = this.normalizeTerm(text);

    // Si coincide con un patrón incompleto
    for (const [entity, pattern] of Object.entries(this.incompletePatterns)) {
      if (pattern.test(normalized)) {
        // Verificar que no sea "usuario" + algo completo
        const hasCompletePattern = this.completePatterns.some((p) => p.test(normalized));
        if (!hasCompletePattern) {
          return {
            incompleta: true,
            entity,
            entityInfo: this.entityTypeMap[entity] || { field: entity, prompt: `Ingrese el ${entity}` },
          };
        }
      }
    }

    const learnedEntityInfo = this.resolveLearnedEntityInfo(normalized);
    if (learnedEntityInfo) {
      return {
        incompleta: true,
        entity: normalized,
        entityInfo: learnedEntityInfo,
      };
    }

    const schemaEntityInfo = this.resolveSchemaEntityInfo(normalized);
    if (schemaEntityInfo) {
      return {
        incompleta: true,
        entity: normalized,
        entityInfo: schemaEntityInfo,
      };
    }

    return { incompleta: false };
  }

  /**
   * FASE 2: Generar respuesta pidiendo parámetro
   */
  generateParameterPrompt(incompletQuery) {
    if (!incompletQuery.incompleta) {
      return null;
    }

    const { entity, entityInfo } = incompletQuery;

    return {
      requiresInput: true,
      field: entity,
      fieldLabel: entity.charAt(0).toUpperCase() + entity.slice(1),
      message: entityInfo.prompt || `Ingrese el ${entity}`,
      table: entityInfo.table,
      examples: this.getExamplesForEntity(entity),
    };
  }

  /**
   * Obtener ejemplos para una entidad
   */
  getExamplesForEntity(entity) {
    const examples = {
      usuario: ['admin', 'test_user', '747bd085-f2ea-4e8a-ab88-e2c68c1f5e3e'],
      user: ['admin', 'test_user', '747bd085-f2ea-4e8a-ab88-e2c68c1f5e3e'],
      cliente: ['Acme Corp', 'test_client', '5b3c9e1a-...'],
      customer: ['Acme Corp', 'test_customer', '5b3c9e1a-...'],
      empleado: ['John Doe', 'john_doe', '8f2d4c5e-...'],
      employee: ['John Doe', 'john_doe', '8f2d4c5e-...'],
      sesion: ['a1b2c3d4-...'],
      session: ['a1b2c3d4-...'],
      log: ['log_001', '123456'],
    };

    return examples[entity] || [entity];
  }

  /**
   * FASE 3-4: Clasificar tipo de parámetro
   */
  classifyParameterType(value) {
    if (!value) return null;

    const normalized = String(value).trim();

    // UUID (formato completo)
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(normalized)) {
      return {
        tipo: 'uuid',
        operador: '=',
        descripcion: 'Búsqueda exacta por UUID',
      };
    }

    // UUID corto
    if (/^[0-9a-f]{6,}$/.test(normalized)) {
      return {
        tipo: 'uuid_corto',
        operador: 'ILIKE',
        descripcion: 'Búsqueda parcial por UUID corto',
      };
    }

    // Número
    if (/^\d+$/.test(normalized)) {
      return {
        tipo: 'numero',
        operador: '=',
        descripcion: 'Búsqueda exacta por número',
      };
    }

    // Texto exacto (sin espacios)
    if (!/\s/.test(normalized)) {
      return {
        tipo: 'exacto',
        operador: '=',
        descripcion: 'Búsqueda exacta de texto',
      };
    }

    // Texto parcial (contiene espacios)
    return {
      tipo: 'parcial',
      operador: 'ILIKE',
      descripcion: 'Búsqueda parcial (contiene)',
    };
  }

  /**
   * FASE 5: Validar parámetro
   */
  validateParameter(value) {
    if (!value) {
      return { valido: false, razon: 'Parámetro vacío' };
    }

    const normalized = String(value).trim();

    // Mínimo 1 carácter
    if (normalized.length < 1) {
      return { valido: false, razon: 'Parámetro muy corto' };
    }

    // Máximo 255 caracteres
    if (normalized.length > 255) {
      return { valido: false, razon: 'Parámetro muy largo' };
    }

    // Detectar caracteres peligrosos
    if (/[;'"\\<>]/.test(normalized) && !/^[0-9a-f]{8}-[0-9a-f]{4}/.test(normalized)) {
      return { valido: false, razon: 'Parámetro contiene caracteres no permitidos' };
    }

    return { valido: true };
  }

  /**
   * FASE 6: Inyectar parámetro en el input original
   */
  injectParameter(originalText, parameterValue, entity) {
    if (!originalText || !parameterValue) {
      return originalText;
    }

    const normalized = this.normalizeTerm(originalText);
    const safeEntity = this.escapeRegexLiteral(this.normalizeTerm(entity));

    if (!safeEntity) {
      return `${originalText} "${parameterValue}"`;
    }

    // Si es só lo la entidad, agregar el valor
    if (normalized === entity) {
      return `${originalText} "${parameterValue}"`;
    }

    // Si contiene la entidad, agregar el valor después
    const entityPattern = new RegExp(`(${safeEntity})\\s*$`, 'i');
    if (entityPattern.test(normalized)) {
      return originalText.replace(entityPattern, `$1 "${parameterValue}"`);
    }

    // Si contiene la entidad en medio, insertar después
    const inMiddlePattern = new RegExp(`(${safeEntity})\\s+`, 'i');
    if (inMiddlePattern.test(normalized)) {
      return originalText.replace(inMiddlePattern, `$1 "${parameterValue}" `);
    }

    // Por defecto, agregar al final
    return `${originalText} "${parameterValue}"`;
  }

  /**
   * FASE 7: Método integrador
   */
  processParameterizedQuery(text, parameterValue = null) {
    const normalized = String(text || '').trim();

    // Si no hay valor, detectar si necesita
    if (parameterValue === null) {
      const incomplete = this.isIncompleteQuery(normalized);

      if (incomplete.incompleta) {
        return {
          esParametrizado: true,
          requiereInput: true,
          prompt: this.generateParameterPrompt(incomplete),
          inputOriginal: normalized,
        };
      }

      return {
        esParametrizado: false,
        requiereInput: false,
        inputOriginal: normalized,
      };
    }

    // Si hay valor, validar e inyectar
    const validation = this.validateParameter(parameterValue);
    if (!validation.valido) {
      return {
        esParametrizado: true,
        requiereInput: false,
        error: validation.razon,
        inputOriginal: normalized,
      };
    }

    // Detectar entity del input original
    const incomplete = this.isIncompleteQuery(normalized);
    const entity = incomplete.incompleta ? incomplete.entity : this.detectEntityFromText(normalized);

    // Inyectar parámetro
    const enhancedQuery = this.injectParameter(normalized, parameterValue, entity);

    // Clasificar tipo de parámetro
    const paramType = this.classifyParameterType(parameterValue);

    return {
      esParametrizado: true,
      requiereInput: false,
      parametroInyectado: parameterValue,
      parametroTipo: paramType,
      queryOriginal: normalized,
      queryEnhanced: enhancedQuery,
      entity,
    };
  }

  /**
   * Detectar qué entidad está siendo consultada
   */
  detectEntityFromText(text) {
    const normalized = this.normalizeTerm(text);

    for (const [entity] of Object.entries(this.incompletePatterns)) {
      if (normalized.includes(entity)) {
        return entity;
      }
    }

    for (const learnedEntity of Object.keys(this.learnedSemanticDictionary?.tableAliases || {})) {
      if (normalized.includes(learnedEntity)) {
        return learnedEntity;
      }
    }

    const schemaEntity = this.resolveSchemaEntityInfo(normalized);
    if (schemaEntity?.table) {
      return schemaEntity.table;
    }

    return this.schemaContext?.tables?.[0] || 'usuario';
  }

  /**
   * Compatibilidad: Verificar si la query está completamente especificada
   */
  isFullySpecified(text) {
    const incomplete = this.isIncompleteQuery(text);
    return !incomplete.incompleta;
  }
}

export default QueryParameterizer;
