/**
 * SchemaDetector - Auto-detección de estructura PostgreSQL
 * 
 * Detecta automáticamente:
 * - Tablas
 * - Columnas y tipos de datos
 * - Claves primarias (PK)
 * - Claves foráneas (FK)
 * - Relaciones entre tablas
 * 
 * Usa information_schema de PostgreSQL (motor determinístico)
 */

export class SchemaDetector {
  constructor(pool) {
    this.pool = pool;
    this.connectionFingerprint = '';
    this.learnedSemanticDictionary = {
      tableAliases: {},
      columnKeywords: {},
    };
  }

  setLearnedSemanticDictionary(dictionary = {}) {
    this.learnedSemanticDictionary = {
      tableAliases: { ...(dictionary?.tableAliases || {}) },
      columnKeywords: { ...(dictionary?.columnKeywords || {}) },
    };
  }

  normalizeIdentifierName(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .replace(/^(tbl_|tb_|t_)/, '');
  }

  tokenizeIdentifier(value) {
    const normalized = this.normalizeIdentifierName(value)
      .replace(/_/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    if (!normalized) return [];
    return normalized.split(' ').filter(Boolean);
  }

  expandToken(token) {
    const clean = String(token || '').trim().toLowerCase();
    if (!clean) return [];

    const expanded = new Set([clean]);
    if (clean.length > 3 && clean.endsWith('s')) {
      expanded.add(clean.slice(0, -1));
    }
    return [...expanded];
  }

  /**
   * Diccionario viviente: mapea palabras clave comunes a columnas específicas
   * Esto permite reconocer variaciones de palabras en lenguaje natural
   */
  getLivingKeywordDictionary() {
    const baseDictionary = {
      // ===== IDENTIFICADORES / NOMBRES / USUARIOS =====
      'nombre': ['username', 'user_name', 'nombre', 'name', 'full_name', 'display_name', 'title', 'titulo', 'razon_social'],
      'llamado': ['username', 'user_name', 'nombre', 'name', 'full_name'],
      'user': ['username', 'user_name', 'nombre', 'name', 'email'],
      'usuario': ['username', 'user_name', 'nombre', 'name', 'email'],
      'cliente': ['username', 'client_name', 'nombre', 'name', 'email', 'customer_name'],
      'personal': ['first_name', 'last_name', 'nombre', 'apellido', 'full_name'],
      'apellido': ['last_name', 'apellido', 'surname'],
      'email': ['email', 'correo', 'correo_electronico', 'email_address', 'mail'],
      'correo': ['email', 'correo', 'correo_electronico', 'email_address'],
      'identificador': ['id', 'identificador', 'identifier', 'uuid', 'codigo'],
      'codigo': ['codigo', 'code', 'sku', 'reference', 'referencia'],
      'referencia': ['reference', 'referencia', 'codigo', 'code'],
      'titulo': ['title', 'titulo', 'nombre', 'name'],
      'alias': ['alias', 'nickname', 'apodo', 'username'],
      'apodo': ['alias', 'apodo', 'nickname'],
      
      // ===== ROLES / PERMISOS / AUTORIZACIÓN =====
      'rol': ['role', 'rol', 'tipo', 'tipo_usuario', 'permission', 'permiso', 'group', 'grupo'],
      'role': ['role', 'rol', 'tipo', 'tipo_usuario', 'permission', 'permiso'],
      'función': ['role', 'rol', 'funcion', 'permission', 'permiso'],
      'tipo': ['role', 'rol', 'tipo', 'tipo_usuario', 'type', 'category', 'categoria'],
      'permiso': ['role', 'rol', 'permission', 'permiso', 'grupo', 'permisos'],
      'admin': ['role', 'rol', 'permission', 'permiso', 'tipo', 'is_admin'],
      'administrador': ['role', 'rol', 'is_admin', 'admin_flag'],
      'supervisor': ['role', 'rol', 'tipo', 'supervisor_flag'],
      'gerente': ['role', 'rol', 'manager_flag'],
      'operador': ['role', 'rol', 'operator_flag'],
      'cuenta': ['account', 'cuenta', 'account_type'],
      'nivel': ['level', 'nivel', 'rank', 'rango'],
      'rango': ['level', 'nivel', 'rank', 'rango'],
      'grupo': ['group', 'grupo', 'team', 'equipo', 'departamento'],
      'equipo': ['team', 'equipo', 'grupo', 'department', 'departamento'],
      'departamento': ['department', 'departamento', 'dept', 'division'],
      'sección': ['section', 'seccion', 'area'],
      'área': ['area', 'area', 'section', 'seccion'],
      
      // ===== ESTADOS / CONDICIONES =====
      'estado': ['estado', 'status', 'activo', 'active', 'habilitado', 'enabled', 'condition'],
      'status': ['estado', 'status', 'activo', 'active'],
      'activo': ['activo', 'active', 'estado', 'status', 'enabled', 'habilitado', 'is_active'],
      'inactivo': ['inactivo', 'inactive', 'estado', 'status', 'disabled', 'deshabilitado'],
      'habilitado': ['habilitado', 'enabled', 'activo', 'active', 'is_enabled'],
      'deshabilitado': ['deshabilitado', 'disabled', 'inactivo', 'inactive'],
      'bloqueado': ['bloqueado', 'blocked', 'lock', 'estado', 'status', 'is_blocked'],
      'suspendido': ['suspendido', 'suspended', 'bloqueado', 'blocked', 'is_suspended'],
      'disponible': ['available', 'disponible', 'in_stock', 'stock'],
      'disponibilidad': ['availability', 'disponibilidad', 'available', 'status'],
      'oferta': ['offer', 'oferta', 'promotion', 'promocion'],
      'promoción': ['promotion', 'promocion', 'oferta', 'offer'],
      
      // ===== VALIDACIÓN / VERIFICACIÓN =====
      'verificado': ['verified', 'verificado', 'email_verified', 'confirma', 'is_verified'],
      'confirmado': ['confirmed', 'confirmado', 'verificado', 'verified'],
      'validado': ['validated', 'validado', 'is_valid'],
      'válido': ['valid', 'valido', 'estado', 'status'],
      'inválido': ['invalid', 'invalido', 'estado', 'status'],
      'pendiente': ['pending', 'pendiente', 'estado', 'status', 'is_pending'],
      'aprobado': ['approved', 'aprobado', 'estado', 'status', 'is_approved'],
      'rechazado': ['rejected', 'rechazado', 'estado', 'status', 'is_rejected'],
      
      // ===== DATOS PERSONALES / SEGURIDAD =====
      'password': ['password', 'passhash', 'hash', 'passwd', 'clave', 'contraseña'],
      'contraseña': ['password', 'passwd', 'clave', 'contraseña', 'passhash'],
      'clave': ['password', 'passwd', 'clave', 'contraseña'],
      'token': ['token', 'access_token', 'refresh_token', 'secret', 'api_key', 'auth_token'],
      'secreto': ['secret', 'secreto', 'token', 'api_key'],
      'acceso': ['token', 'access_token', 'password', 'permiso', 'access_level'],
      'sesión': ['session', 'sesion', 'session_id', 'session_token'],
      'session': ['session', 'sesion', 'session_id', 'session_token'],
      'teléfono': ['phone', 'telefono', 'phone_number', 'numero_telefonico', 'telephone'],
      'dirección': ['address', 'direccion', 'ubicacion', 'location', 'street'],
      'calle': ['street', 'calle', 'address', 'direccion'],
      'ciudad': ['city', 'ciudad', 'location', 'ubicacion'],
      'estado_provincia': ['state', 'estado', 'province', 'provincia'],
      'código_postal': ['postal_code', 'codigo_postal', 'zip_code', 'zipcode'],
      'país': ['country', 'pais', 'nation'],
      'documento': ['document', 'documento', 'id_number', 'cedula'],
      'cédula': ['cedula', 'id_document', 'documento'],
      'pasaporte': ['passport', 'pasaporte'],
      
      // ===== ACTIVIDAD / LOGS / AUDITORÍA =====
      'log': ['log', 'logs', 'evento', 'event', 'action', 'accion', 'activity', 'actividad'],
      'evento': ['evento', 'event', 'log', 'logs', 'accion', 'action', 'occurrence'],
      'acción': ['accion', 'action', 'evento', 'event', 'log', 'activity'],
      'actividad': ['actividad', 'activity', 'log', 'logs', 'evento', 'action'],
      'detalles': ['details', 'detalle', 'descripcion', 'description', 'nota', 'notes', 'remarks'],
      'descripción': ['description', 'descripcion', 'detalle', 'details', 'nota', 'notas', 'memo'],
      'nota': ['note', 'nota', 'notas', 'remarks', 'comment'],
      'comentario': ['comment', 'comentario', 'nota', 'remarks'],
      'mensaje': ['message', 'mensaje', 'texto', 'text', 'content'],
      'contenido': ['content', 'contenido', 'data', 'datos', 'description', 'descripcion', 'body'],
      'texto': ['text', 'texto', 'content', 'contenido', 'message', 'mensaje'],
      'información': ['information', 'informacion', 'data', 'datos'],
      'auditoría': ['audit', 'auditoria', 'log', 'activity', 'actividad'],
      'movimiento': ['movement', 'movimiento', 'transaction', 'transaccion'],
      'operación': ['operation', 'operacion', 'action', 'accion'],
      
      // ===== FECHAS / TIEMPOS =====
      'creado': ['created_at', 'fecha_creacion', 'creation_date', 'createdon', 'created_on', 'date_created'],
      'creación': ['created_at', 'fecha_creacion', 'creation_date', 'created_on'],
      'actualizado': ['updated_at', 'fecha_actualizacion', 'modification_date', 'modifiedon', 'last_modified'],
      'modificado': ['modified_at', 'modificado', 'last_modified', 'updated_at'],
      'fecha': ['created_at', 'updated_at', 'fecha_creacion', 'fecha_actualizacion', 'date', 'fecha_inicio', 'fecha_fin'],
      'hora': ['created_at', 'updated_at', 'time', 'timestamp', 'hora', 'hora_inicio'],
      'tiempo': ['time', 'tiempo', 'timestamp', 'duration', 'duracion'],
      'duración': ['duration', 'duracion', 'tiempo', 'time'],
      'comienzo': ['start_date', 'start_time', 'inicio', 'comienzo'],
      'inicio': ['start', 'inicio', 'start_date', 'comienzo'],
      'fin': ['end', 'fin', 'end_date', 'fecha_fin'],
      'vencimiento': ['expiry', 'vencimiento', 'end_date', 'expires_at'],
      'expiración': ['expiration', 'expiracion', 'expires_at', 'expiry'],
      'día': ['day', 'dia', 'date', 'fecha'],
      'mes': ['month', 'mes', 'date', 'fecha'],
      'año': ['year', 'ano', 'anio', 'date', 'fecha'],
      'semana': ['week', 'semana', 'date'],
      'trimestre': ['quarter', 'trimestre', 'date'],
      
      // ===== NÚMEROS / CANTIDADES / VALORES =====
      'cantidad': ['quantity', 'cantidad', 'count', 'numero', 'amount'],
      'número': ['number', 'numero', 'cantidad', 'quantity', 'count'],
      'total': ['total', 'sum', 'suma', 'total_amount'],
      'suma': ['sum', 'suma', 'total', 'amount'],
      'precio': ['price', 'precio', 'cost', 'costo', 'amount'],
      'costo': ['cost', 'costo', 'precio', 'price'],
      'valor': ['value', 'valor', 'amount', 'data', 'datos'],
      'monto': ['amount', 'monto', 'total', 'valor'],
      'saldo': ['balance', 'saldo', 'remaining', 'disponible'],
      'crédito': ['credit', 'credito', 'credit_limit', 'balance'],
      'débito': ['debit', 'debito', 'charge', 'cobro'],
      'pago': ['payment', 'pago', 'paid', 'pagado'],
      'pagado': ['paid', 'pagado', 'payment', 'pago', 'status'],
      'impago': ['unpaid', 'impago', 'pending', 'pendiente'],
      'descuento': ['discount', 'descuento', 'reduction', 'rebaja'],
      'impuesto': ['tax', 'impuesto', 'iva', 'tasa'],
      'tarifa': ['rate', 'tarifa', 'fee', 'precio'],
      'porcentaje': ['percentage', 'porcentaje', 'percent', 'rate'],
      'comisión': ['commission', 'comision', 'fee', 'tarifa'],
      
      // ===== ÓRDENES / PEDIDOS / COMPRAS =====
      'orden': ['order', 'orden', 'pedido', 'purchase_order'],
      'pedido': ['order', 'pedido', 'request', 'solicitud'],
      'compra': ['purchase', 'compra', 'order', 'orden'],
      'venta': ['sale', 'venta', 'selling', 'vendido'],
      'factura': ['invoice', 'factura', 'bill', 'documento'],
      'recibo': ['receipt', 'recibo', 'invoice', 'comprobante'],
      'remisión': ['shipment', 'remision', 'delivery', 'envio'],
      'envío': ['shipment', 'envio', 'delivery', 'sent'],
      'entrega': ['delivery', 'entrega', 'shipped', 'enviado'],
      'devolución': ['return', 'devolucion', 'refund', 'reembolso'],
      'reembolso': ['refund', 'reembolso', 'return', 'devolucion'],
      'garantía': ['warranty', 'garantia', 'guarantee'],
      'servicio': ['service', 'servicio', 'support', 'asistencia'],
      'producto': ['product', 'producto', 'item', 'articulo'],
      'artículo': ['article', 'articulo', 'item', 'product', 'producto'],
      'categoría': ['category', 'categoria', 'type', 'tipo'],
      'marca': ['brand', 'marca', 'manufacturer'],
      'modelo': ['model', 'modelo', 'version'],
      
      // ===== INVENTARIO / STOCK / ALMACÉN =====
      'stock': ['stock', 'inventory', 'inventario', 'quantity'],
      'inventario': ['inventory', 'inventario', 'stock', 'available'],
      'almacén': ['warehouse', 'almacen', 'storage', 'depot'],
      'existencia': ['stock', 'existencia', 'available', 'quantity'],
      'disponible': ['available', 'disponible', 'in_stock', 'stock'],
      'agotado': ['out_of_stock', 'agotado', 'unavailable'],
      'reorden': ['reorder', 'reorden', 'request'],
      'lote': ['lot', 'lote', 'batch', 'group'],
      'serie': ['serial', 'serie', 'number', 'numero'],
      'código_barras': ['barcode', 'codigo_barras', 'sku'],
      
      // ===== CLIENTES / PROVEEDORES / CONTACTOS =====
      'cliente': ['customer', 'cliente', 'client', 'account', 'customer_name'],
      'proveedor': ['supplier', 'proveedor', 'vendor', 'provider'],
      'vendedor': ['seller', 'vendedor', 'salesman', 'sales_rep'],
      'comprador': ['buyer', 'comprador', 'purchaser'],
      'contacto': ['contact', 'contacto', 'communication', 'data'],
      'empresa': ['company', 'empresa', 'organization', 'organizacion'],
      'organización': ['organization', 'organizacion', 'company', 'empresa'],
      'socio': ['partner', 'socio', 'colleague'],
      'empleado': ['employee', 'empleado', 'staff', 'personal'],
      'personal': ['personnel', 'personal', 'staff', 'employees'],
      'agente': ['agent', 'agente', 'representative', 'rep'],
      'representante': ['representative', 'representante', 'agent', 'rep'],
      
      // ===== UBICACIÓN / GEOGRAFÍA =====
      'ubicación': ['location', 'ubicacion', 'address', 'direccion'],
      'localidad': ['locality', 'localidad', 'place', 'lugar'],
      'sitio': ['site', 'sitio', 'location', 'lugar'],
      'lugar': ['place', 'lugar', 'location', 'ubicacion'],
      'región': ['region', 'region', 'area', 'zone'],
      'zona': ['zone', 'zona', 'region', 'area'],
      'territorio': ['territory', 'territorio', 'region', 'zone'],
      'latitud': ['latitude', 'latitud'],
      'longitud': ['longitude', 'longitud'],
      'coordenadas': ['coordinates', 'coordenadas', 'location'],
      
      // ===== SISTEMAS / DATOS TÉCNICOS =====
      'versión': ['version', 'version', 'release'],
      'actualización': ['update', 'actualizacion', 'upgrade'],
      'instalación': ['installation', 'instalacion', 'setup'],
      'configuración': ['configuration', 'configuracion', 'settings', 'config'],
      'parámetro': ['parameter', 'parametro', 'setting', 'option'],
      'opción': ['option', 'opcion', 'setting', 'choice'],
      'preferencia': ['preference', 'preferencia', 'setting', 'option'],
      'formato': ['format', 'formato', 'type', 'tipo'],
      'tipo de dato': ['data_type', 'tipo_dato', 'format'],
      'valor por defecto': ['default', 'default_value', 'predeterminado'],
      'predeterminado': ['default', 'predeterminado', 'standard'],
      
      // ===== REPORTES / ANÁLISIS =====
      'reporte': ['report', 'reporte', 'informe', 'analytics'],
      'informe': ['report', 'informe', 'reporte', 'document'],
      'análisis': ['analysis', 'analisis', 'report', 'analytics'],
      'estadística': ['statistic', 'estadistica', 'analytics', 'data'],
      'métrica': ['metric', 'metrica', 'measurement', 'data'],
      'indicador': ['indicator', 'indicador', 'metric', 'measurement'],
      'promedio': ['average', 'promedio', 'mean', 'medio'],
      'máximo': ['maximum', 'maximo', 'max', 'highest'],
      'mínimo': ['minimum', 'minimo', 'min', 'lowest'],
      'mediana': ['median', 'mediana', 'middle', 'average'],
      'desviación': ['deviation', 'desviacion', 'variation'],
      'correlación': ['correlation', 'correlacion', 'relationship'],
      
      // ===== CALIDAD / EVALUACIÓN =====
      'calidad': ['quality', 'calidad', 'rating', 'assessment'],
      'puntuación': ['score', 'puntuacion', 'rating', 'points'],
      'calificación': ['rating', 'calificacion', 'grade', 'assessment'],
      'nota': ['grade', 'nota', 'rating', 'score'],
      'aprobado': ['passed', 'aprobado', 'approved', 'status'],
      'reprobado': ['failed', 'reprobado', 'rejected'],
      'satisfacción': ['satisfaction', 'satisfaccion', 'rating', 'feedback'],
      'retroalimentación': ['feedback', 'retroalimentacion', 'comment', 'review'],
      'revisión': ['review', 'revision', 'audit', 'check'],
      'inspección': ['inspection', 'inspeccion', 'check', 'review'],
      
      // ===== COMUNICACIÓN / NOTIFICACIÓN =====
      'notificación': ['notification', 'notificacion', 'alert', 'message'],
      'alerta': ['alert', 'alerta', 'warning', 'notification'],
      'aviso': ['notice', 'aviso', 'notification', 'alert'],
      'advertencia': ['warning', 'advertencia', 'alert', 'caution'],
      'error': ['error', 'error_message', 'exception', 'fault'],
      'excepción': ['exception', 'excepcion', 'error', 'issue'],
      'fallo': ['failure', 'fallo', 'error', 'issue'],
      'problema': ['problem', 'problema', 'issue', 'error'],
      'solución': ['solution', 'solucion', 'fix', 'resolution'],
      'respuesta': ['response', 'respuesta', 'reply', 'answer'],
      'solicitud': ['request', 'solicitud', 'order', 'demand'],
      'demanda': ['demand', 'demanda', 'request', 'need'],
      'necesidad': ['need', 'necesidad', 'requirement', 'demand'],
      
      // ===== USUARIOS / AUTENTICACIÓN =====
      'contraseña': ['password', 'passwd', 'clave', 'contraseña'],
      'autenticación': ['authentication', 'autenticacion', 'login', 'auth'],
      'autorización': ['authorization', 'autorizacion', 'permission', 'access'],
      'acceso': ['access', 'acceso', 'login', 'permission'],
      'entrada': ['login', 'entrada', 'access', 'sign_in'],
      'salida': ['logout', 'salida', 'exit', 'sign_out'],
      'registro': ['registration', 'registro', 'sign_up', 'account_creation'],
      'inscripción': ['enrollment', 'inscripcion', 'registration', 'signup'],
      'perfil': ['profile', 'perfil', 'account', 'user_info'],
      'preferencias': ['preferences', 'preferencias', 'settings', 'options'],
    };

    const merged = { ...baseDictionary };
    for (const [term, columns] of Object.entries(this.learnedSemanticDictionary?.columnKeywords || {})) {
      const normalizedTerm = this.normalizeIdentifierName(term);
      if (!normalizedTerm) continue;
      merged[normalizedTerm] = [...new Set([...(merged[normalizedTerm] || []), ...(columns || [])])];
    }

    return merged;
  }

  /**
   * Expande tokens de columnas con palabras clave sinónimas
   */
  expandColumnTokensWithKeywords(columnName, keywords) {
    const columnTokens = [];
    const normalized = this.normalizeIdentifierName(columnName);
    
    // Agregar tokens del nombre de columna
    const baseTokens = this.tokenizeIdentifier(columnName)
      .flatMap((token) => this.expandToken(token));
    columnTokens.push(...baseTokens);
    
    // Buscar palabras clave que mapeen a esta columna
    const livingDict = this.getLivingKeywordDictionary();
    for (const [keyword, columnMappings] of Object.entries(livingDict)) {
      if (columnMappings.some((col) => this.normalizeIdentifierName(col) === normalized)) {
        columnTokens.push(keyword);
        // También agregar variaciones de singulares/plurales del keyword
        columnTokens.push(...this.expandToken(keyword));
      }
    }
    
    return [...new Set(columnTokens)];
  }

  buildSemanticIndex(schemaByTable) {
    const semanticIndex = {};
    const livingDict = this.getLivingKeywordDictionary();
    const learnedTableAliases = this.learnedSemanticDictionary?.tableAliases || {};

    for (const [tableName, tableMeta] of Object.entries(schemaByTable || {})) {
      const tableTokens = this.tokenizeIdentifier(tableName)
        .flatMap((token) => this.expandToken(token));

      for (const [term, mappedTables] of Object.entries(learnedTableAliases)) {
        if (!(mappedTables || []).some((entry) => this.normalizeIdentifierName(entry) === this.normalizeIdentifierName(tableName))) {
          continue;
        }

        tableTokens.push(this.normalizeIdentifierName(term));
        tableTokens.push(...this.expandToken(term));
      }

      const columnTokens = {};
      for (const column of tableMeta.columnas || []) {
        // Expandir tokens de columna con diccionario viviente
        const expandedTokens = this.expandColumnTokensWithKeywords(column.nombre, livingDict);
        columnTokens[column.nombre] = [...new Set(expandedTokens)];
      }

      semanticIndex[tableName] = {
        tokens: [...new Set(tableTokens)],
        columnas: columnTokens,
        // Agregar palabras clave de tabla para fácil acceso
        keywordMappings: livingDict,
      };
    }

    return semanticIndex;
  }

  sanitizeFingerprintPart(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9._:@-]+/g, '_');
  }

  buildEnvConnectionFingerprint() {
    let dbUrl = String(process.env.DATABASE_URL || '').trim();
    if (!dbUrl) {
      try {
        const configFile = String(
          process.env.MULTI_DB_CONFIG_FILE
          || process.env.DATABASES_CONFIG_FILE
          || './config/multidb.databases.json'
          || ''
        ).trim();

        if (configFile) {
          const resolvedPath = path.isAbsolute(configFile)
            ? configFile
            : path.resolve(process.cwd(), configFile);

          if (fs.existsSync(resolvedPath)) {
            const parsed = JSON.parse(fs.readFileSync(resolvedPath, 'utf8') || '{}');
            const databases = Array.isArray(parsed?.databases) ? parsed.databases : [];
            const primary = databases.find((entry) => entry?.enabled !== false && (entry?.primary === true || entry?.isPrimary === true || String(entry?.role || '').toLowerCase() === 'primary'))
              || (databases.length === 1 ? databases[0] : null);

            if (primary) {
              const direct = String(primary.connectionString || primary.url || '').trim();
              if (direct) {
                dbUrl = direct;
              }
            }
          }
        }
      } catch {
        dbUrl = String(process.env.DATABASE_URL || '').trim();
      }
    }

    if (dbUrl) {
      try {
        const parsed = new URL(dbUrl);
        const host = this.sanitizeFingerprintPart(parsed.hostname);
        const port = this.sanitizeFingerprintPart(parsed.port || '5432');
        const database = this.sanitizeFingerprintPart(parsed.pathname.replace(/^\//, ''));
        const user = this.sanitizeFingerprintPart(parsed.username);
        return `env:${host}|${port}|${database}|${user}`;
      } catch {
        return `env-url:${this.sanitizeFingerprintPart(dbUrl)}`;
      }
    }

    const host = this.sanitizeFingerprintPart(process.env.DB_HOST || '');
    const port = this.sanitizeFingerprintPart(process.env.DB_PORT || '');
    const database = this.sanitizeFingerprintPart(process.env.DB_NAME || '');
    const user = this.sanitizeFingerprintPart(process.env.DB_USER || '');
    return `env-parts:${host}|${port}|${database}|${user}`;
  }

  setConnectionFingerprint(value) {
    const normalized = this.sanitizeFingerprintPart(value);
    if (normalized) {
      this.connectionFingerprint = normalized;
      return this.connectionFingerprint;
    }
    return this.connectionFingerprint;
  }

  async refreshConnectionFingerprint() {
    try {
      const result = await this.pool.query(`
        SELECT
          current_database()::text AS db_name,
          current_schema()::text AS schema_name,
          current_user::text AS db_user,
          COALESCE(inet_server_addr()::text, 'localhost') AS db_host,
          COALESCE(inet_server_port()::text, '5432') AS db_port
      `);

      const row = result.rows?.[0] || {};
      const host = this.sanitizeFingerprintPart(row.db_host);
      const port = this.sanitizeFingerprintPart(row.db_port);
      const database = this.sanitizeFingerprintPart(row.db_name);
      const schemaName = this.sanitizeFingerprintPart(row.schema_name || 'public');
      const user = this.sanitizeFingerprintPart(row.db_user);

      this.connectionFingerprint = `runtime:${host}|${port}|${database}|${schemaName}|${user}`;
      return this.connectionFingerprint;
    } catch {
      this.connectionFingerprint = this.buildEnvConnectionFingerprint();
      return this.connectionFingerprint;
    }
  }

  getConnectionFingerprint() {
    return this.connectionFingerprint || this.buildEnvConnectionFingerprint();
  }

  /**
   * Detecta todas las tablas en el esquema public
   */
  async getTables() {
    try {
      const result = await this.pool.query(`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name ASC
      `);
      return result.rows.map(row => row.table_name);
    } catch (error) {
      console.error('❌ Error detectando tablas:', error.message);
      throw error;
    }
  }

  /**
   * Obtiene el esquema COMPLETO de la BD (todas las tablas con toda su metadata)
   */
  async getFullSchema() {
    try {
      console.log('🔍 Iniciando detección automática de schema...');

      const [tablesResult, columnsResult, constraintsResult] = await Promise.all([
        this.pool.query(`
          SELECT table_name
          FROM information_schema.tables
          WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
          ORDER BY table_name ASC
        `),
        this.pool.query(`
          SELECT
            table_name,
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
          FROM information_schema.columns
          WHERE table_schema = 'public'
          ORDER BY table_name ASC, ordinal_position ASC
        `),
        this.pool.query(`
          SELECT
            tc.table_name,
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_name AS referenced_table,
            ccu.column_name AS referenced_column,
            kcu.ordinal_position
          FROM information_schema.table_constraints tc
          LEFT JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            AND tc.table_name = kcu.table_name
          LEFT JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.table_schema
          WHERE tc.table_schema = 'public'
            AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
          ORDER BY tc.table_name ASC, tc.constraint_name ASC, kcu.ordinal_position ASC
        `),
      ]);

      const tables = tablesResult.rows.map((row) => row.table_name);
      const schema = {};
      const columns = {};
      const foreignKeys = [];

      for (const tableName of tables) {
        schema[tableName] = {
          nombre: tableName,
          columnas: [],
          pkPrincipal: null,
          clavesPrimarias: [],
          clavesForaneas: [],
          relacionesInversas: [],
          uniqueConstraints: [],
          totalColumnas: 0,
        };
        columns[tableName] = [];
      }

      for (const row of columnsResult.rows) {
        if (!schema[row.table_name]) continue;
        const columnMeta = {
          nombre: row.column_name,
          tipo: row.data_type,
          nullable: row.is_nullable === 'YES',
          default: row.column_default,
        };

        schema[row.table_name].columnas.push(columnMeta);
        columns[row.table_name].push(columnMeta);
      }

      const pkByTable = new Map();
      const fkRows = [];

      for (const row of constraintsResult.rows) {
        const tableName = row.table_name;
        if (!schema[tableName]) continue;

        if (row.constraint_type === 'PRIMARY KEY') {
          const list = pkByTable.get(tableName) || [];
          if (row.column_name) list.push(row.column_name);
          pkByTable.set(tableName, list);
        }

        if (row.constraint_type === 'FOREIGN KEY' && row.column_name && row.referenced_table && row.referenced_column) {
          fkRows.push(row);
        }
      }

      for (const [tableName, pkColumns] of pkByTable.entries()) {
        const orderedPk = [...new Set(pkColumns)];
        schema[tableName].clavesPrimarias = orderedPk;
        schema[tableName].pkPrincipal = orderedPk.length <= 1 ? (orderedPk[0] || null) : orderedPk;
      }

      for (const row of fkRows) {
        if (!schema[row.table_name]) continue;

        const fk = {
          columna: row.column_name,
          tablaReferenciada: row.referenced_table,
          columnaReferenciada: row.referenced_column,
          nombreConstraint: row.constraint_name,
        };

        schema[row.table_name].clavesForaneas.push(fk);
        foreignKeys.push({
          table: row.table_name,
          column: row.column_name,
          referencedTable: row.referenced_table,
          referencedColumn: row.referenced_column,
          constraintName: row.constraint_name,
        });

        if (schema[row.referenced_table]) {
          schema[row.referenced_table].relacionesInversas.push({
            tabla: row.table_name,
            columna: row.column_name,
            columnaReferenciada: row.referenced_column,
          });
        }
      }

      for (const tableName of tables) {
        schema[tableName].totalColumnas = schema[tableName].columnas.length;
      }

      const semanticIndex = this.buildSemanticIndex(schema);

      console.log(`✅ Schema detectado automáticamente: ${tables.length} tablas`);

      return {
        detectadoEn: new Date().toISOString(),
        connectionFingerprint: this.getConnectionFingerprint(),
        totalTablas: tables.length,
        tables: Object.keys(schema).sort(),
        columns,
        foreignKeys,
        tablas: Object.keys(schema).sort(),
        schema,
        semanticIndex,
      };
    } catch (error) {
      console.error('❌ Error detectando schema completo:', error.message);
      throw error;
    }
  }

  /**
   * Detecta relaciones posibles entre dos tablas
   * Útil para sugerir JOINs automáticos
   */
  async detectRelationPath(tabla1, tabla2) {
    try {
      // Relación directa
      const directFks = await this.pool.query(`
        SELECT kcu.column_name, ccu.table_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = $1
          AND ccu.table_name = $2
      `, [tabla1, tabla2]);

      if (directFks.rows.length > 0) {
        return {
          tipo: 'directo',
          tabla1,
          tabla2,
          relacion: directFks.rows.map(r => ({
            columna: r.column_name,
            referenciaTabla: r.referenced_table || tabla2
          }))
        };
      }

      // Relación inversa
      const inverseFks = await this.pool.query(`
        SELECT kcu.column_name, tc.table_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = $1
          AND ccu.table_name = $2
      `, [tabla2, tabla1]);

      if (inverseFks.rows.length > 0) {
        return {
          tipo: 'inverso',
          tabla1: tabla2,
          tabla2: tabla1,
          relacion: inverseFks.rows.map(r => ({
            columna: r.column_name,
            referenciaTabla: tabla1
          }))
        };
      }

      return null;
    } catch (error) {
      console.error(`❌ Error detectando relación entre ${tabla1} y ${tabla2}:`, error.message);
      return null;
    }
  }
}

export default SchemaDetector;
