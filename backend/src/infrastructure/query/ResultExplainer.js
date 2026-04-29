/**
 * ResultExplainer - Genera explicaciones en lenguaje natural de resultados SQL
 *
 * Convierte resultados SQL complejos en texto comprensible para usuarios
 */

export class ResultExplainer {
  constructor() {
    // Mapeo de columnas a nombres amigables
    this.columnAliases = {
      username: 'usuario',
      name: 'nombre',
      email: 'correo',
      role: 'rol',
      rol: 'rol',
      status: 'estado',
      estado: 'estado',
      is_active: 'activo',
      activo: 'activo',
      created_at: 'creado',
      updated_at: 'actualizado',
      last_login: 'último acceso',
      id: 'ID',
      uuid: 'UUID',
      cant_sesiones: 'sesiones',
      session_count: 'sesiones',
      cant_logs: 'logs',
      log_count: 'logs',
      total: 'total',
      count: 'cantidad',
      cantidad: 'cantidad',
      sum: 'suma',
      average: 'promedio',
      min: 'mínimo',
      max: 'máximo',
    };

    // Columnas que se deben ignorar en explicación detallada
    this.columnIgnoreList = [
      'password',
      'passhash',
      'token',
      'secret',
      'api_key',
      'refresh_token',
      'access_token',
    ];

    // Tipos de consultas detectadas por patrones
    this.queryPatterns = {
      usuarios: /usuarios?|users?|empleados?|employees?|clientes?|customers?/i,
      sesiones: /sesiones?|sessions?/i,
      logs: /logs?|bitacora|auditoria/i,
      agregacion: /sum|count|avg|max|min|total|promedio|cantidad/i,
      filtro: /donde|where|filtrados?|filtered?/i,
    };
  }

  /**
   * Generar explicación completa para un conjunto de resultados
   */
  explain(results, sqlQuery = '') {
    if (!results || results.length === 0) {
      return {
        resumen: 'No se encontraron resultados.',
        detalle: [],
        estadisticas: null,
      };
    }

    // Detectar tipo de consulta
    const queryType = this.detectQueryType(results, sqlQuery);

    // Crear resumen general
    const resumen = this.generateSummary(results, queryType);

    // Generar detalles por fila
    const detalle = results.slice(0, 10).map((row) => this.explainRow(row, queryType));

    // Generar estadísticas si aplica
    const estadisticas = this.extractStatistics(results, queryType);

    // Construir respuesta final
    const explanation = `${resumen}${detalle.length > 0 ? '\n\n' + detalle.join('\n') : ''}${estadisticas ? '\n\n' + estadisticas : ''}${results.length > 10 ? `\n\n... y ${results.length - 10} resultados más.` : ''}`;

    return {
      resumen,
      detalle: detalle,
      estadisticas: estadisticas,
      explicacionCompleta: explanation,
      cantidadResultados: results.length,
      mostradosDetalle: Math.min(results.length, 10),
    };
  }

  /**
   * Detectar el tipo de consulta basado en resultados y SQL
   */
  detectQueryType(results, sqlQuery = '') {
    if (!results || results.length === 0) {
      return 'desconocido';
    }

    const firstRow = results[0];
    const hasCountCol = Object.keys(firstRow).some((k) => /count|total|cantidad|suma|sum/i.test(k));
    const hasAggregates = Object.values(firstRow).some((v) => typeof v === 'number' && Math.abs(v) > 100);

    if (hasCountCol || hasAggregates) {
      return 'agregacion';
    }

    if (Object.keys(firstRow).some((k) => /username|email|role/.test(k))) {
      return 'usuarios';
    }

    if (Object.keys(firstRow).some((k) => /session|sesion/i.test(k))) {
      return 'sesiones';
    }

    if (Object.keys(firstRow).some((k) => /log|evento|activity/i.test(k))) {
      return 'logs';
    }

    return 'generico';
  }

  /**
   * Generar resumen (encabezado de la explicación)
   */
  generateSummary(results, queryType) {
    const count = results.length;

    if (count === 0) {
      return 'No se encontraron resultados.';
    }

    if (count === 1) {
      switch (queryType) {
        case 'usuarios':
          return 'Se encontró 1 usuario.';
        case 'sesiones':
          return 'Se encontró 1 sesión.';
        case 'logs':
          return 'Se encontró 1 registro de actividad.';
        case 'agregacion':
          return 'Se calculó 1 resultado agregado.';
        default:
          return 'Se encontró 1 resultado.';
      }
    }

    switch (queryType) {
      case 'usuarios':
        return `Se encontraron ${count} usuarios.`;
      case 'sesiones':
        return `Se encontraron ${count} sesiones.`;
      case 'logs':
        return `Se encontraron ${count} registros de actividad.`;
      case 'agregacion':
        return `Se calcularon ${count} resultados agregados.`;
      default:
        return `Se encontraron ${count} resultados.`;
    }
  }

  /**
   * Generar explicación para una fila individual
   */
  explainRow(row, queryType = 'generico') {
    if (!row || typeof row !== 'object') {
      return null;
    }

    const entries = Object.entries(row).filter(([key]) => !this.columnIgnoreList.includes(key.toLowerCase()));

    if (entries.length === 0) {
      return null;
    }

    switch (queryType) {
      case 'usuarios':
        return this.explainUser(row);
      case 'sesiones':
        return this.explainSession(row);
      case 'logs':
        return this.explainLog(row);
      case 'agregacion':
        return this.explainAggregate(row);
      default:
        return this.explainGeneric(row);
    }
  }

  /**
   * Explicación para usuario
   */
  explainUser(row) {
    const username = row.username || row.nombre || row.name || 'usuario';
    const role = row.role || row.rol || 'no especificado';
    const status = row.is_active === false || row.estado === 'inactivo' ? ' (inactivo)' : '';

    let text = `El usuario ${username} tiene el rol ${role}${status}`;

    // Agregar sesiones y logs
    const sessions = row.cant_sesiones ?? row.session_count ?? 0;
    const logs = row.cant_logs ?? row.log_count ?? 0;

    if (sessions > 0 || logs > 0) {
      const sessionText = sessions === 1 ? 'sesión' : 'sesiones';
      const logsText = logs === 1 ? 'log' : 'logs';
      text += `, con ${sessions} ${sessionText} y ${logs} ${logsText}`;
    }

    // Agregar último acceso si existe
    if (row.last_login || row.ultimo_acceso) {
      text += `, último acceso: ${row.last_login || row.ultimo_acceso}`;
    }

    text += '.';
    return text;
  }

  /**
   * Explicación para sesión
   */
  explainSession(row) {
    const username = row.username || 'usuario';
    const status = row.status || row.estado || 'activa';
    const createdAt = row.created_at || row.fecha_inicio || 'fecha desconocida';

    let text = `La sesión de ${username} está ${status}`;

    if (row.ip_address || row.direccion_ip) {
      text += ` desde IP ${row.ip_address || row.direccion_ip}`;
    }

    if (row.device || row.dispositivo) {
      text += ` en ${row.device || row.dispositivo}`;
    }

    text += `, iniciada en ${createdAt}`;

    if (row.last_activity || row.ultima_actividad) {
      text += `, última actividad: ${row.last_activity || row.ultima_actividad}`;
    }

    text += '.';
    return text;
  }

  /**
   * Explicación para log/actividad
   */
  explainLog(row) {
    const username = row.username || 'usuario desconocido';
    const action = row.action || row.accion || 'acción registrada';
    const timestamp = row.created_at || row.timestamp || row.fecha || 'fecha desconocida';

    let text = `${username} realizó: ${action}`;

    if (row.resource || row.recurso) {
      text += ` en ${row.resource || row.recurso}`;
    }

    text += ` el ${timestamp}`;

    if (row.status || row.estado) {
      text += ` (${row.status || row.estado})`;
    }

    if (row.details || row.detalles) {
      text += `. Detalles: ${row.details || row.detalles}`;
    }

    text += '.';
    return text;
  }

  /**
   * Explicación para agregación
   */
  explainAggregate(row) {
    const entries = Object.entries(row).filter(([key]) => !this.columnIgnoreList.includes(key.toLowerCase()));

    let text = '';
    for (const [key, value] of entries) {
      const friendlyName = this.columnAliases[key] || key;

      if (key.toLowerCase().includes('count') || key.toLowerCase().includes('cantidad')) {
        text += `${friendlyName}: ${value} elementos. `;
      } else if (typeof value === 'number') {
        text += `${friendlyName}: ${value}. `;
      } else {
        text += `${friendlyName}: ${value}. `;
      }
    }

    return text.trim();
  }

  /**
   * Explicación genérica para resultados desconocidos
   */
  explainGeneric(row) {
    const entries = Object.entries(row).filter(([key]) => !this.columnIgnoreList.includes(key.toLowerCase()));

    // Priorizar columnas que parecen ser identificadores
    const identifierCols = entries.filter(([k]) => /id|uuid|nombre|name|username/i.test(k));
    const [idKey, idValue] = identifierCols.length > 0 ? identifierCols[0] : entries[0];

    let text = `${this.columnAliases[idKey] || idKey}: ${idValue}`;

    // Agregar otros datos relevantes
    const otherEntries = entries.filter(([k]) => k !== idKey).slice(0, 3);
    if (otherEntries.length > 0) {
      text += ' (';
      text += otherEntries.map(([k, v]) => `${this.columnAliases[k] || k}: ${v}`).join(', ');
      text += ')';
    }

    text += '.';
    return text;
  }

  /**
   * Extraer estadísticas útiles de resultados
   */
  extractStatistics(results, queryType) {
    if (queryType !== 'agregacion') {
      return null;
    }

    if (!results || results.length === 0) {
      return null;
    }

    const firstRow = results[0];
    const numericCols = Object.entries(firstRow).filter(([, v]) => typeof v === 'number');

    if (numericCols.length === 0) {
      return null;
    }

    let stats = '';
    for (const [key, value] of numericCols) {
      const friendlyName = this.columnAliases[key] || key;
      stats += `• ${friendlyName}: ${value}\n`;
    }

    return stats.trim();
  }

  /**
   * Método simple para retornar solo texto explicativo
   */
  getSimpleExplanation(results) {
    if (!results || results.length === 0) {
      return 'No se encontraron resultados.';
    }

    const explanation = this.explain(results);
    return explanation.explicacionCompleta;
  }
}

export default ResultExplainer;
