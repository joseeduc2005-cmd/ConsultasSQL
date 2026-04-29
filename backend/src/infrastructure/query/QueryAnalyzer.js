/**
 * QueryAnalyzer - Análisis de consultas y detección de input requerido
 *
 * FASES:
 * 1. Análisis de consulta (¿general o específica?)
 * 2. Detección de input requerido
 * 3. Validación de input
 * 4. Clasificación de tipo de input (UUID, texto exacto, parcial)
 * 5. Generación de explicación (ANTES del JSON)
 * 6. Respuesta final
 */

export class QueryAnalyzer {
  constructor() {
    // Patrones que indican consultas específicas (requieren input del usuario)
    this.specificQueryPatterns = [
      // Menciona un usuario/entidad sin valor
      /usuario\s+([a-zA-Z0-9_\-\.]+)?(?:\s|$)/i,
      /user\s+([a-zA-Z0-9_\-\.]+)?(?:\s|$)/i,
      /cliente\s+([a-zA-Z0-9_\-\.]+)?(?:\s|$)/i,
      /customer\s+([a-zA-Z0-9_\-\.]+)?(?:\s|$)/i,
      /employee\s+([a-zA-Z0-9_\-\.]+)?(?:\s|$)/i,
      /empleado\s+([a-zA-Z0-9_\-\.]+)?(?:\s|$)/i,
      /admin(?:\s|$)/i,
      /test_user(?:\s|$)/i,
    ];

    // Patrones que indican consultas generales (no requieren input específico)
    this.generalQueryPatterns = [
      /usuarios?\s+con\s+m[aá]s/i,        // usuarios con más logs
      /usuarios?\s+activos/i,               // usuarios activos
      /todos?\s+los?\s+usuarios?/i,         // todos los usuarios
      /listar?\s+usuarios?/i,               // listar usuarios
      /top\s+\d+/i,                         // top 10, top 5, etc
      /(\w+)\s+m[aá]s\s+(\w+)/i,           // elementos más algo
      /promedio/i,                          // promedio
      /total/i,                             // total
      /count/i,                             // contar
      /sum/i,                               // sumar
      /estadísticas/i,                      // estadísticas
      /reporte/i,                           // reporte
      /análisis/i,                          // análisis
    ];

    // Palabras que indican que sigue un valor específico
    this.valueIndicators = [
      'llamado',
      'named',
      'con nombre',
      'con id',
      'con código',
      'called',
      'id',
      'uuid',
    ];

    // Uuids pattern
    this.uuidPattern = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi;
    this.shortUuidPattern = /\b[0-9a-f]{6,}/gi;
  }

  /**
   * FASE 1: Análisis de consulta (¿general o específica?)
   */
  analyzeQueryType(text) {
    const normalizedText = String(text || '').trim().toLowerCase();

    // Buscar patrones específicos
    for (const pattern of this.specificQueryPatterns) {
      if (pattern.test(normalizedText)) {
        return {
          tipo: 'especifica',
          esGeneralizada: false,
          requiereInput: true,
          razon: 'Consulta menciona una entidad específica sin valores',
        };
      }
    }

    // Buscar patrones generales
    for (const pattern of this.generalQueryPatterns) {
      if (pattern.test(normalizedText)) {
        return {
          tipo: 'general',
          esGeneralizada: true,
          requiereInput: false,
          razon: 'Consulta solicita datos generalizados o agregados',
        };
      }
    }

    // Si tiene UUID o string entre comillas, es específica pero tiene valor
    const hasUuid = this.uuidPattern.test(normalizedText);
    const hasQuotedString = /"[^"]*"/.test(normalizedText) || /'[^']*'/.test(normalizedText);

    if (hasUuid || hasQuotedString) {
      return {
        tipo: 'especifica_con_valor',
        esGeneralizada: false,
        requiereInput: false,
        razon: 'Consulta específica pero incluye valor de búsqueda',
      };
    }

    // Por defecto: general
    return {
      tipo: 'general_inferida',
      esGeneralizada: true,
      requiereInput: false,
      razon: 'No se detectaron patrones específicos',
    };
  }

  /**
   * FASE 2: Detección de input requerido
   */
  detectRequiredInput(text) {
    const normalizedText = String(text || '').trim();
    const analysisResult = this.analyzeQueryType(text);

    // Si no requiere input, retornar vacío
    if (!analysisResult.requiereInput) {
      return {
        requiereInput: false,
        inputDetectado: null,
        tipoInput: null,
        sugerencia: null,
      };
    }

    // Buscar valor después de palabras indicadoras
    for (const indicator of this.valueIndicators) {
      const indicatorRegex = new RegExp(`${indicator}\\s+([\\w\\.\\-]+)`, 'i');
      const match = normalizedText.match(indicatorRegex);
      if (match && match[1]) {
        return {
          requiereInput: false,
          inputDetectado: match[1],
          tipoInput: this.classifyInputType(match[1]),
          sugerencia: null,
        };
      }
    }

    // Si hay UUID, es el valor a buscar
    const uuidMatches = [];
    let uuidMatch;
    while ((uuidMatch = this.uuidPattern.exec(normalizedText)) !== null) {
      uuidMatches.push(uuidMatch[0]);
    }

    if (uuidMatches.length > 0) {
      return {
        requiereInput: false,
        inputDetectado: uuidMatches[0],
        tipoInput: 'uuid',
        sugerencia: null,
      };
    }

    // Si hay string entre comillas, es el valor exacto
    const quotedMatch = normalizedText.match(/"([^"]*)"|'([^']*)'/);
    let quotedValue = null;
    if (quotedMatch) {
      quotedValue = quotedMatch[1] || quotedMatch[2];
      return {
        requiereInput: false,
        inputDetectado: quotedValue,
        tipoInput: 'exacto',
        sugerencia: null,
      };
    }

    // Si llegó aquí, sí necesita input del usuario
    return {
      requiereInput: true,
      inputDetectado: null,
      tipoInput: null,
      sugerencia: '⚠️ Esta consulta requiere un usuario específico.\n\nPor favor ingrese el nombre o ID del usuario.',
    };
  }

  /**
   * FASE 3 y 4: Validación y clasificación de tipo de input
   */
  classifyInputType(input) {
    if (!input) return null;

    const normalized = String(input).trim();

    // UUID (formato completo)
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(normalized)) {
      return 'uuid';
    }

    // Short UUID o hash
    if (/^[0-9a-f]{6,}$/.test(normalized) && !isNaN(normalized)) {
      return 'uuid_corto';
    }

    // Texto exacto (sin espacios, seguro de búsqueda)
    if (!/\s/.test(normalized)) {
      return 'exacto';
    }

    // Texto parcial (permite búsqueda ILIKE)
    return 'parcial';
  }

  /**
   * Validar que el input sea razonable
   */
  validateInput(input) {
    if (!input) {
      return {
        valido: false,
        razon: 'Input vacío o nulo',
      };
    }

    const normalized = String(input).trim();

    // Mínimo 2 caracteres
    if (normalized.length < 2) {
      return {
        valido: false,
        razon: 'Input muy corto (mínimo 2 caracteres)',
      };
    }

    // Máximo 255 caracteres
    if (normalized.length > 255) {
      return {
        valido: false,
        razon: 'Input muy largo (máximo 255 caracteres)',
      };
    }

    // No permitir caracteres peligrosos
    if (/[;'"\\<>]/.test(normalized)) {
      return {
        valido: false,
        razon: 'Input contiene caracteres no permitidos',
      };
    }

    return {
      valido: true,
      razon: null,
    };
  }

  /**
   * FASE 5: Generar explicación en lenguaje natural
   */
  generateExplanation(results, inputValue = null) {
    if (!results || !Array.isArray(results.rows)) {
      return 'No se encontraron datos.';
    }

    const count = results.rows.length;
    let explanation = '';

    // Encabezado
    if (count === 0) {
      explanation = 'No se encontraron resultados.';
    } else if (count === 1) {
      explanation = 'Se encontró 1 resultado.';
    } else {
      explanation = `Se encontraron ${count} resultados.`;
    }

    // Detalles por fila (máximo 10 para no saturar)
    const rowsToShow = results.rows.slice(0, 10);
    const details = rowsToShow.map((row) => this.generateRowExplanation(row)).filter(Boolean);

    if (details.length > 0) {
      explanation += '\n\n' + details.join('\n');
    }

    if (count > 10) {
      explanation += `\n\n... y ${count - 10} resultados más.`;
    }

    return explanation;
  }

  /**
   * Generar explicación para una fila individual
   */
  generateRowExplanation(row) {
    if (!row || typeof row !== 'object') {
      return null;
    }

    const entries = Object.entries(row);
    if (entries.length === 0) {
      return null;
    }

    // Columnas principales para explicación
    const keyColumns = [
      'username',
      'nombre',
      'name',
      'email',
      'role',
      'rol',
      'estado',
      'status',
      'id',
      'uuid',
      'created_at',
      'updated_at',
      'total',
      'count',
      'cantidad',
      'descripcion',
      'description',
    ];

    // Construir texto natural
    let text = 'El ';

    // Identificar tipo de entidad
    if (row.username) {
      text += `usuario ${row.username}`;
    } else if (row.nombre || row.name) {
      text += `${row.nombre || row.name}`;
    } else if (row.id || row.uuid) {
      text += `elemento ${row.id || row.uuid}`;
    } else {
      // Usar la primera columna como referencia
      const [firstKey, firstValue] = entries[0];
      text += `${firstKey} ${firstValue}`;
    }

    // Agregar detalles de rol
    if (row.role || row.rol) {
      text += ` tiene el rol ${row.role || row.rol}`;
    }

    // Agregar detalles de estado
    if (row.estado || row.status) {
      text += `, estado: ${row.estado || row.status}`;
    }

    // Agregar números de sesiones/logs
    if (row.cant_sesiones !== undefined || row.session_count !== undefined) {
      const sessions = row.cant_sesiones || row.session_count || 0;
      text += `, con ${sessions} ${sessions === 1 ? 'sesión' : 'sesiones'}`;
    }

    if (row.cant_logs !== undefined || row.log_count !== undefined) {
      const logs = row.cant_logs || row.log_count || 0;
      text += ` y ${logs} ${logs === 1 ? 'log' : 'logs'}`;
    }

    // Agregar otras métricas numéricas
    for (const [key, value] of entries) {
      if (typeof value === 'number' && !['id', 'uuid'].includes(key.toLowerCase())) {
        if (!['cant_sesiones', 'session_count', 'cant_logs', 'log_count'].includes(key)) {
          text += `, ${key}: ${value}`;
        }
      }
    }

    text += '.';

    return text;
  }

  /**
   * Construir sugerencia para input requerido
   */
  buildInputPrompt(queryAnalysis) {
    if (!queryAnalysis.requiereInput) {
      return null;
    }

    return {
      tipo: 'input_requerido',
      mensaje: '⚠️ Esta consulta requiere un usuario específico.',
      prompt: 'Por favor ingrese el nombre o ID del usuario:',
      ejemplos: ['admin', 'test_user', '747bd085-f2ea-4e8a-ab88-e2c68c1f5e3e'],
      esperadoTipo: 'string',
    };
  }

  /**
   * Método integrador: analizar y retornar toda la info
   */
  analyzeQuery(text, inputValue = null) {
    // FASE 1: Análisis de tipo
    const typeAnalysis = this.analyzeQueryType(text);

    // FASE 2: Detección de input
    const inputDetection = this.detectRequiredInput(text);

    // FASE 3: Validación
    let inputValidation = null;
    if (inputValue !== null) {
      inputValidation = this.validateInput(inputValue);
    }

    // FASE 4: Clasificación
    let inputClassification = null;
    if (inputValue !== null && (inputValidation?.valido || inputDetection.inputDetectado)) {
      const valueToClassify = inputValue || inputDetection.inputDetectado;
      inputClassification = this.classifyInputType(valueToClassify);
    }

    return {
      // FASE 1: Análisis de tipo de consulta
      tipoConsulta: typeAnalysis,

      // FASE 2: Detección de input
      inputRequerido: inputDetection,

      // FASE 3: Validación
      inputValidacion: inputValidation,

      // FASE 4: Clasificación
      inputClasificacion: inputClassification,

      // Prompt para usuario si necesita input
      inputPrompt: this.buildInputPrompt(inputDetection),

      // Indicador general
      puedeEjecutarse: !inputDetection.requiereInput && (inputValidation?.valido !== false),
    };
  }
}

export default QueryAnalyzer;
