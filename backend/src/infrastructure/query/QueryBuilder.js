
import QueryIntelligenceEngine from './QueryIntelligenceEngine.js';
import { askOllama } from '../ai/OllamaClient.js';

/**
 * QueryBuilder - deterministic SQL generator (schema-driven)
 *
 * Rules:
 * - No AI, no external APIs, no manual table dictionaries
 * - Everything is derived from real PostgreSQL schema metadata
 */

export class QueryBuilder {
  constructor(schemaDetector, schemaCache) {
    this.schemaDetector = schemaDetector;
    this.schemaCache = schemaCache;
    this.lastKnownTables = new Set();
    this.queryIntelligenceEngine = new QueryIntelligenceEngine(this);

    this.stopWords = new Set([
      // Preposiciones y artículos ES
      'con', 'de', 'del', 'la', 'el', 'los', 'las', 'y', 'por', 'para', 'un', 'una', 'unos', 'unas', 'en', 'al',
      // Determinantes ES
      'todos', 'todas', 'que', 'es', 'son', 'sus', 'su', 'ese', 'esta', 'este',
      // Verbos de consulta ES
      'ver', 'dame', 'muestra', 'mostrar', 'lista', 'busca', 'buscar', 'trae', 'obtener', 'obtén',
      // Términos de agregación/orden (no deben actuar como valores de filtro literal)
      'mas', 'más', 'mayor', 'mayores', 'menor', 'menores', 'top', 'cuantos', 'cuantas', 'cuanto', 'cantidad', 'total',
      // EN equivalents
      'the', 'all', 'get', 'show', 'list', 'find', 'fetch', 'give', 'me', 'a', 'an', 'count', 'average', 'avg', 'max', 'min',
    ]);
    this.genericPenaltyPatterns = [/\bcomments?\b/i, /\blogs?\b/i, /\baudit\b/i, /\bhistory\b/i, /\btmp\b/i, /\bbackup\b/i];
    this.semanticAliases = {
      usuarios: ['user', 'users'],
      usuario: ['user', 'users'],
      sesiones: ['session', 'sessions'],
      sesion: ['session', 'sessions'],
      logs: ['log', 'logs', 'bitacora', 'auditoria'],
      log: ['log', 'logs', 'bitacora', 'auditoria'],
      pagos: ['payment', 'payments'],
      pago: ['payment', 'payments'],
      pedidos: ['order', 'orders'],
      pedido: ['order', 'orders'],
      clientes: ['customer', 'customers'],
      cliente: ['customer', 'customers'],
      ordenes: ['order', 'orders'],
      orden: ['order', 'orders'],
    };
    this.sensitiveColumnPatterns = [
      /(^|_)(password|passwd|passhash|hash)($|_)/i,
      /(^|_)(token|refresh_token|access_token|secret|api_key)($|_)/i,
    ];
    this.learnedSemanticDictionary = {
      tableAliases: {},
      columnKeywords: {},
    };
    
    // Diccionario viviente: mapea palabras clave comunes a columnas
    this.livingKeywordDictionary = {
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
  }

  setLearnedSemanticDictionary(dictionary = {}) {
    this.learnedSemanticDictionary = {
      tableAliases: { ...(dictionary?.tableAliases || {}) },
      columnKeywords: { ...(dictionary?.columnKeywords || {}) },
    };
  }

  getMergedSemanticAliases() {
    const merged = { ...(this.semanticAliases || {}) };

    for (const [term, tables] of Object.entries(this.learnedSemanticDictionary?.tableAliases || {})) {
      const normalizedTerm = this.normalizeText(term);
      if (!normalizedTerm) continue;
      merged[normalizedTerm] = [...new Set([...(merged[normalizedTerm] || []), ...(tables || [])])];
    }

    return merged;
  }

  getMergedLivingKeywordDictionary() {
    const merged = { ...(this.livingKeywordDictionary || {}) };

    for (const [term, columns] of Object.entries(this.learnedSemanticDictionary?.columnKeywords || {})) {
      const normalizedTerm = this.normalizeText(term);
      if (!normalizedTerm) continue;
      merged[normalizedTerm] = [...new Set([...(merged[normalizedTerm] || []), ...(columns || [])])];
    }

    return merged;
  }

  normalizeText(text) {
    return String(text || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9_\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  normalizeIdentifierName(value) {
    return this.normalizeText(value).replace(/^(tbl_|tb_|t_)/, '');
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
    const clean = this.normalizeText(token);
    if (!clean) return [];

    const expanded = new Set([clean]);
    if (clean.length > 3 && clean.endsWith('s')) {
      expanded.add(clean.slice(0, -1));
    }
    return [...expanded];
  }

  tokenizeInput(text) {
    const semanticAliases = this.getMergedSemanticAliases();
    const rawTokens = this.tokenizeIdentifier(text)
      .filter((token) => token && !this.stopWords.has(token));

    const normalized = rawTokens.flatMap((token) => {
      const corrected = this.getCorrectedToken(token);
      const expanded = this.expandToken(corrected);
      const aliasKey = this.normalizeText(corrected || token);
      const aliases = semanticAliases[aliasKey] || [];
      return [...expanded, ...aliases.flatMap((alias) => this.expandToken(alias))];
    });
    return [...new Set(normalized)].filter((token) => token && !this.stopWords.has(token));
  }

  tokenizeInputRaw(text) {
    return this.tokenizeIdentifier(text)
      .filter((token) => token && !this.stopWords.has(token));
  }

  normalizeTokenBase(token) {
    const t = this.normalizeText(token);
    if (t.length > 4 && t.endsWith('es')) return t.slice(0, -2);
    if (t.length > 3 && t.endsWith('s')) return t.slice(0, -1);
    return t;
  }

  getTokenCorrectionVocabulary() {
    const semanticAliases = this.getMergedSemanticAliases();
    const livingKeywordDictionary = this.getMergedLivingKeywordDictionary();
    const vocabulary = new Set();

    for (const [aliasKey, aliasValues] of Object.entries(semanticAliases || {})) {
      this.expandToken(aliasKey).forEach((tk) => vocabulary.add(this.normalizeTokenBase(tk)));
      for (const v of aliasValues || []) {
        this.expandToken(v).forEach((tk) => vocabulary.add(this.normalizeTokenBase(tk)));
      }
    }

    for (const key of Object.keys(livingKeywordDictionary || {})) {
      this.expandToken(key).forEach((tk) => vocabulary.add(this.normalizeTokenBase(tk)));
    }

    for (const table of this.lastKnownTables || []) {
      this.tokenizeIdentifier(table).forEach((tk) => {
        this.expandToken(tk).forEach((et) => vocabulary.add(this.normalizeTokenBase(et)));
      });
    }

    return vocabulary;
  }

  getEntityMentionVocabulary() {
    const semanticAliases = this.getMergedSemanticAliases();
    const vocabulary = new Set();

    for (const [aliasKey, aliasValues] of Object.entries(semanticAliases || {})) {
      this.expandToken(aliasKey).forEach((tk) => vocabulary.add(this.normalizeTokenBase(tk)));
      for (const v of aliasValues || []) {
        this.expandToken(v).forEach((tk) => vocabulary.add(this.normalizeTokenBase(tk)));
      }
    }

    for (const table of this.lastKnownTables || []) {
      this.tokenizeIdentifier(table).forEach((tk) => {
        this.expandToken(tk).forEach((et) => vocabulary.add(this.normalizeTokenBase(et)));
      });
    }

    return vocabulary;
  }

  getCorrectedToken(token) {
    const normalized = this.normalizeTokenBase(token);
    if (!normalized || normalized.length < 4) return normalized;

    const vocabulary = this.getTokenCorrectionVocabulary();
    if (vocabulary.has(normalized)) return normalized;

    let best = normalized;
    let bestScore = 0;
    for (const candidate of vocabulary) {
      if (!candidate || Math.abs(candidate.length - normalized.length) > 3) continue;
      const match = this.getTokenMatchType(normalized, candidate);
      if (match.type === 'fuzzy' && match.sim >= 0.75 && match.sim > bestScore) {
        best = candidate;
        bestScore = match.sim;
      }
    }

    return best;
  }

  isLikelyEntityMentionToken(token) {
    const normalized = this.normalizeTokenBase(token);
    if (!normalized) return false;

    if (this.lastKnownTables.has(normalized)) return true;
    const vocabulary = this.getEntityMentionVocabulary();
    return vocabulary.has(normalized);
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
    const s = this.normalizeText(a);
    const t = this.normalizeText(b);
    if (!s || !t) return 0;
    const distance = this.levenshteinDistance(s, t);
    const maxLen = Math.max(s.length, t.length, 1);
    return 1 - (distance / maxLen);
  }

  getTokenMatchType(token, candidate) {
    const left = this.normalizeText(token);
    const right = this.normalizeText(candidate);
    if (!left || !right) return { type: 'none', sim: 0 };

    if (left === right || this.normalizeTokenBase(left) === this.normalizeTokenBase(right)) {
      return { type: 'exact', sim: 1 };
    }

    if (left.includes(right) || right.includes(left)) {
      return { type: 'partial', sim: 0.9 };
    }

    const sim = this.similarityScore(left, right);
    if (sim >= 0.5) {
      return { type: 'fuzzy', sim };
    }

    return { type: 'none', sim };
  }

  escapeLiteral(value) {
    return String(value || '').replace(/'/g, "''");
  }

  quoteIdent(identifier) {
    const safe = String(identifier || '').replace(/"/g, '');
    return `"${safe}"`;
  }

  isSensitiveColumn(columnName) {
    const normalized = String(columnName || '').trim().toLowerCase();
    return this.sensitiveColumnPatterns.some((pattern) => pattern.test(normalized));
  }

  getSafeSelectableColumns(tableSchema) {
    const columnas = Array.isArray(tableSchema?.columnas) ? tableSchema.columnas : [];
    const safeColumns = columnas.filter((c) => !this.isSensitiveColumn(c.nombre));
    return safeColumns.length > 0 ? safeColumns : columnas;
  }

  getRelevantSelectableColumns(tableSchema, maxColumns = 5) {
    const safeColumns = this.getSafeSelectableColumns(tableSchema);
    const selected = [];
    const selectedSet = new Set();

    const addByName = (name) => {
      const found = safeColumns.find((c) => this.normalizeText(c.nombre) === this.normalizeText(name));
      if (found && !selectedSet.has(found.nombre)) {
        selected.push(found.nombre);
        selectedSet.add(found.nombre);
      }
    };

    const pks = Array.isArray(tableSchema?.clavesPrimarias) ? tableSchema.clavesPrimarias : [];
    pks.forEach(addByName);

    ['username', 'nombre', 'name', 'email', 'role', 'rol', 'estado', 'activo', 'created_at'].forEach(addByName);

    for (const col of safeColumns) {
      if (selected.length >= maxColumns) break;
      if (!selectedSet.has(col.nombre)) {
        selected.push(col.nombre);
        selectedSet.add(col.nombre);
      }
    }

    return selected;
  }

  detectAggregationIntent(textInput) {
    const text = this.normalizeText(textInput);
    if (/\b(cuantos|cuantas|cuanto|cantidad|total|count|top|mas|mayor|mayores)\b/.test(text)) return 'COUNT';
    if (/\b(promedio|media|average|avg)\b/.test(text)) return 'AVG';
    if (/\b(maximo|maxima|mayor|max)\b/.test(text)) return 'MAX';
    if (/\b(minimo|minima|menor|min)\b/.test(text)) return 'MIN';
    return null;
  }

  detectGroupIntent(textInput) {
    const text = this.normalizeText(textInput);
    return /\b(por|agrupado|segun)\b/.test(text);
  }

  detectOrderIntent(textInput) {
    const text = this.normalizeText(textInput);
    if (/\b(top|mas|mayor|mayores|ordenar)\b/.test(text)) return 'DESC';
    if (/\b(menores)\b/.test(text)) return 'ASC';
    return null;
  }

  detectRelationalIntent(textInput) {
    const text = this.normalizeText(textInput);
    return /\b(con|junto|relacion|unido|union|join|y)\b/.test(text);
  }

  extractQuotedStrings(textInput) {
    if (!textInput) return [];
    const matches = Array.from(String(textInput).matchAll(/"([^"]+)"|'([^']+)'/g));
    return matches.map((m) => String(m[1] || m[2] || '').trim()).filter(Boolean);
  }

  extractUuid(textInput) {
    const match = String(textInput || '').match(/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i);
    return match ? match[0] : null;
  }

  isLikelyUuid(value) {
    return /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i.test(String(value || ''));
  }

  isLikelyNumericId(value) {
    return /^\d{1,18}$/.test(String(value || '').trim());
  }

  isLikelyUuidFragment(value) {
    const token = String(value || '').trim().toLowerCase();
    // Typical fragment after splitting UUID by '-': 8+ hex chars.
    return /^[0-9a-f]{8,}$/.test(token);
  }

  buildIdentifierClause(identifierValue, tableSchema, tableAlias = null) {
    const value = String(identifierValue || '').trim();
    if (!value || !tableSchema) return null;

    const qualify = (column) => {
      if (!tableAlias) return this.quoteIdent(column);
      return `${this.quoteIdent(tableAlias)}.${this.quoteIdent(column)}`;
    };

    const idCols = (tableSchema?.columnas || []).filter((c) => {
      const name = this.normalizeText(c.nombre || '');
      return /(^id$|_id$|id_|uuid)/.test(name) && !this.isSensitiveColumn(c.nombre);
    });

    if (idCols.length === 0) return null;

    const parts = idCols.map((c) => {
      const type = this.normalizeText(c.tipo || '');

      // UUID: use exact comparison on compatible columns
      if (this.isLikelyUuid(value)) {
        if (!/(uuid|char|text|varchar)/.test(type)) return null;
        return `${qualify(c.nombre)} = '${this.escapeLiteral(value)}'`;
      }

      // Numeric ids: prefer numeric equality, fallback to text equality
      if (this.isLikelyNumericId(value)) {
        if (/(smallint|integer|bigint|numeric|decimal|real|double)/.test(type)) {
          return `${qualify(c.nombre)} = ${Number(value)}`;
        }
        if (/(uuid)/.test(type)) return null;
        return `${qualify(c.nombre)}::text = '${this.escapeLiteral(value)}'`;
      }

      // Generic identifier token fallback: text exact
      if (/(char|text|varchar)/.test(type)) {
        return `${qualify(c.nombre)} = '${this.escapeLiteral(value)}'`;
      }

      return `${qualify(c.nombre)}::text = '${this.escapeLiteral(value)}'`;
    }).filter(Boolean);

    if (parts.length === 0) return null;
    return `(${parts.join(' OR ')})`;
  }

  getLikelyIdentifierTokens(textInput) {
    const raw = this.tokenizeInputRaw(textInput);
    const normalized = raw.map((tk) => this.normalizeTokenBase(tk)).filter(Boolean);
    return normalized.filter((tk) => this.isLikelyUuid(tk) || this.isLikelyNumericId(tk));
  }

  getFirstValidInputToken(textInput) {
    const raw = this.tokenizeInputRaw(textInput).map((t) => this.normalizeTokenBase(t));
    return raw.find((t) => t && !this.stopWords.has(t)) || null;
  }

  getEntityTokenCandidates(token) {
    const base = this.normalizeTokenBase(token);
    if (!base) return [];

    const semanticAliases = this.getMergedSemanticAliases();

    const fromAlias = (semanticAliases[this.normalizeText(base)] || [])
      .flatMap((alias) => this.expandToken(alias).map((tk) => this.normalizeTokenBase(tk)));

    return [...new Set([base, ...fromAlias].filter(Boolean))];
  }

  detectExplicitMentionedTables(textInput, semanticIndex = {}) {
    const rawTokens = this.tokenizeInputRaw(textInput).map((t) => this.normalizeTokenBase(t));
    const found = new Set();

    for (const token of rawTokens) {
      const candidates = this.getEntityTokenCandidates(token);
      for (const [tableName, entry] of Object.entries(semanticIndex)) {
        const tableTokens = Array.isArray(entry?.tokens) ? entry.tokens : [];
        // For explicit mentions prefer exact/partial table-token matches to avoid noisy fuzzy routing.
        const match = candidates.some((cand) => tableTokens.some((tk) => {
          const m = this.getTokenMatchType(cand, this.normalizeTokenBase(tk));
          return m.type === 'exact' || m.type === 'partial';
        }));
        if (match) found.add(tableName);
      }
    }

    return [...found];
  }

  // Busca un valor en columnas relevantes de una tabla, con prioridad de exactitud
  buildSmartValueWhere(value, tableSchema, tableAlias = null) {
    const valueText = String(value || '').trim();
    if (!valueText || !tableSchema) return null;

    const columns = Array.isArray(tableSchema?.columnas) ? tableSchema.columnas : [];
    if (columns.length === 0) return null;

    const qualify = (column) => {
      if (!tableAlias) return this.quoteIdent(column);
      return `${this.quoteIdent(tableAlias)}.${this.quoteIdent(column)}`;
    };

    const idLikeColumns = columns.filter((c) => /(^id$|_id$|uuid)/i.test(String(c.nombre || '')));
    const identityColumns = columns.filter((c) => /(username|user_name|name|nombre|email|correo)/i.test(String(c.nombre || '')));
    const textColumns = columns.filter((c) => {
      const tipo = this.normalizeText(c.tipo || '');
      return tipo.includes('char') || tipo.includes('text') || tipo.includes('varchar');
    });

    // Regla critica: UUID nunca usa LIKE
    if (this.isLikelyUuid(valueText)) {
      const targetColumns = idLikeColumns.length > 0 ? idLikeColumns : columns.filter((c) => /(user|uuid|identifier|token)/i.test(String(c.nombre || '')));
      if (targetColumns.length === 0) return null;
      return `(${targetColumns.map((c) => `${qualify(c.nombre)} = '${this.escapeLiteral(valueText)}'`).join(' OR ')})`;
    }

    // String entre comillas => exact match
    const exactCandidates = identityColumns.length > 0 ? identityColumns : textColumns;
    if (exactCandidates.length > 0) {
      return `(${exactCandidates.map((c) => `${qualify(c.nombre)} = '${this.escapeLiteral(valueText)}'`).join(' OR ')})`;
    }

    // Texto suelto => ILIKE parcial
    if (textColumns.length > 0) {
      return `(${textColumns.map((c) => `${qualify(c.nombre)}::text ILIKE '%${this.escapeLiteral(valueText)}%'`).join(' OR ')})`;
    }

    return null;
  }

  async getAvailableTables() {
    const fingerprint = this.schemaDetector.getConnectionFingerprint();
    return this.schemaCache.getOrFetch(`available-tables:${fingerprint}`, () => this.schemaDetector.getTables());
  }

  async getFullSchema() {
    const fingerprint = this.schemaDetector.getConnectionFingerprint();
    const schema = await this.schemaCache.getOrFetch(`full-schema:${fingerprint}`, () => this.schemaDetector.getFullSchema());
    this.lastKnownTables = new Set(Array.isArray(schema?.tables) ? schema.tables : []);
    return schema;
  }

  scoreInputAgainstTable(inputTokens, tableName, tableEntry) {
    const tableTokens = new Set(Array.isArray(tableEntry?.tokens) ? tableEntry.tokens : []);
    const columnTokensMap = tableEntry?.columnas || {};

    let score = 0;
    let totalMatches = 0;
    const matches = [];

    for (const token of inputTokens) {
      let tokenMatched = false;

      for (const tableToken of tableTokens) {
        const match = this.getTokenMatchType(token, tableToken);
        if (match.type === 'exact') {
          score += 3;
          totalMatches += 1;
          tokenMatched = true;
          matches.push({ token, scope: 'table', kind: 'exact', points: 3 });
          break;
        }
        if (match.type === 'partial' || match.type === 'fuzzy') {
          score += 2;
          totalMatches += 1;
          tokenMatched = true;
          matches.push({ token, scope: 'table', kind: match.type, points: 2, sim: Number(match.sim.toFixed(3)) });
          break;
        }
      }

      for (const [columnName, columnTokens] of Object.entries(columnTokensMap)) {
        let matchedColumn = false;
        for (const columnToken of columnTokens || []) {
          const match = this.getTokenMatchType(token, columnToken);
          if (match.type === 'exact') {
            score += 4;
            totalMatches += 1;
            matchedColumn = true;
            tokenMatched = true;
            matches.push({ token, scope: 'column', column: columnName, kind: 'exact', points: 4 });
            break;
          }
          if (match.type === 'partial' || match.type === 'fuzzy') {
            score += 2;
            totalMatches += 1;
            matchedColumn = true;
            tokenMatched = true;
            matches.push({ token, scope: 'column', column: columnName, kind: match.type, points: 2, sim: Number(match.sim.toFixed(3)) });
            break;
          }
        }
        if (matchedColumn) break;
      }

      if (!tokenMatched) {
        matches.push({ token, scope: 'none', kind: 'none', points: 0 });
      }
    }

    if (totalMatches > 1) {
      score += 1;
      matches.push({ token: '__bonus__', scope: 'system', kind: 'multi-match-bonus', points: 1 });
    }

    if (this.genericPenaltyPatterns.some((pattern) => pattern.test(tableName))) {
      score -= 2;
      matches.push({ token: '__penalty__', scope: 'system', kind: 'generic-table-penalty', points: -2 });
    }

    const maxPerToken = 4;
    const maxBase = Math.max(inputTokens.length * maxPerToken, 1);
    const confidence = Math.max(0, Math.min(1, score / maxBase));

    return {
      score,
      confidence,
      matches,
      totalMatches,
    };
  }

  findPrimaryEntityTable(firstToken, semanticIndex = {}) {
    if (!firstToken) return { table: null, points: 0, matchType: 'none' };

    const candidates = this.getEntityTokenCandidates(firstToken);
    let best = { table: null, points: 0, matchType: 'none' };

    for (const [tableName, entry] of Object.entries(semanticIndex)) {
      const tableTokens = Array.isArray(entry?.tokens) ? entry.tokens : [];
      for (const candidate of candidates) {
        for (const tk of tableTokens) {
          const match = this.getTokenMatchType(candidate, tk);
          let points = 0;
          if (match.type === 'exact') points = 12;
          else if (match.type === 'partial') points = 8;
          else if (match.type === 'fuzzy' && match.sim >= 0.72) points = 4;

          if (points > best.points) {
            best = { table: tableName, points, matchType: match.type };
          }
        }
      }
    }

    return best;
  }

  async analyzeKeywords(textInput) {
    const schema = await this.getFullSchema();
    const normalizedText = this.normalizeText(textInput);

    // Quitar UUIDs y valores entre comillas antes de detectar entidad/tabla
    // para que fragmentos hex del UUID no puntuen contra tablas incorrectas
    const _uuid = this.extractUuid(textInput);
    const _quoted = this.extractQuotedStrings(textInput);
    let cleanText = String(textInput || '');
    if (_uuid) cleanText = cleanText.replace(_uuid, ' ');
    for (const qv of _quoted) {
      cleanText = cleanText.replace(`"${qv}"`, ' ').replace(`'${qv}'`, ' ');
    }
    cleanText = cleanText.replace(/\s+/g, ' ').trim();
    const effectiveText = cleanText || textInput;

    const tokens = this.tokenizeInput(effectiveText);
    const semanticIndex = schema?.semanticIndex || {};
    const firstValidToken = this.getFirstValidInputToken(effectiveText) || tokens[0] || null;
    const primaryEntity = this.findPrimaryEntityTable(firstValidToken, semanticIndex);
    const primaryEntityTable = primaryEntity.table;
    const explicitTables = this.detectExplicitMentionedTables(effectiveText, semanticIndex);

    const scoredTables = Object.entries(semanticIndex)
      .map(([table, entry]) => {
        const scored = this.scoreInputAgainstTable(tokens, table, entry);
        let boostedScore = scored.score;
        if (table === primaryEntityTable) {
          boostedScore += primaryEntity.points;
        } else if (primaryEntityTable && this.genericPenaltyPatterns.some((pattern) => pattern.test(table))) {
          boostedScore -= 5;
        }

        const boostedMatches = table === primaryEntityTable
          ? [...scored.matches, { token: firstValidToken, scope: 'table', kind: `first-token-${primaryEntity.matchType || 'priority'}`, points: primaryEntity.points }]
          : scored.matches;

        return {
          table,
          score: boostedScore,
          confidence: scored.confidence,
          breakdown: boostedMatches,
          totalMatches: scored.totalMatches,
        };
      })
      .sort((a, b) => b.score - a.score);

    const rankedDetectedTables = scoredTables.filter((t) => t.score > 0).map((t) => t.table);
    const tablaBase = primaryEntityTable || explicitTables[0] || rankedDetectedTables[0] || null;

    // Regla critica: la entidad principal detectada desde la primera palabra valida fija la base.
    const detectedTables = [
      ...(tablaBase ? [tablaBase] : []),
      ...explicitTables.filter((t) => t !== tablaBase),
    ];

    const keywords = scoredTables
      .filter((entry) => entry.score > 0)
      .slice(0, 8)
      .map((entry) => ({
        tabla: entry.table,
        score: entry.score,
        confianza: Number(entry.confidence.toFixed(4)),
        coincidencias: entry.breakdown,
      }));

    const aggregate = this.detectAggregationIntent(textInput);
    const groupByDetected = this.detectGroupIntent(textInput);
    const orderDirection = this.detectOrderIntent(textInput);

    const tipoConsulta = aggregate
      ? (groupByDetected ? 'analytic-grouped' : 'analytic')
      : (detectedTables.length > 1 ? 'relational' : 'simple');

    const confidence = scoredTables[0]?.confidence || 0;

    return {
      textOriginal: textInput,
      textoNormalizado: normalizedText,
      tokens,
      tokensCanonicos: tokens,
      tablasDetectadas: detectedTables,
      tablaBase,
      keywords,
      ranking: scoredTables,
      topScore: scoredTables[0]?.score || 0,
      firstValidToken,
      primaryEntityTable,
      entidadDetectada: primaryEntityTable,
      tieneMultiplesTables: detectedTables.length > 1,
      intencionRelacional: this.detectRelationalIntent(textInput),
      agregacionDetectada: aggregate,
      groupByDetected,
      orderDirection,
      tipoConsulta,
      confianza: confidence,
      tablasMencionadasExplicitas: explicitTables,
    };
  }

  buildWhereFromKeywords(textInput, tableSchema, tableAlias = null) {
    const text = this.normalizeText(textInput);
    const hasFullUuidInInput = Boolean(this.extractUuid(textInput));
    const columns = Array.isArray(tableSchema?.columnas) ? tableSchema.columnas : [];
    if (columns.length === 0) return [];

    const boolCols = columns.filter((c) => {
      const tipo = this.normalizeText(c.tipo || '');
      return tipo.includes('bool') || tipo.includes('boolean');
    });

    const textCols = columns.filter((c) => {
      const tipo = this.normalizeText(c.tipo || '');
      return tipo.includes('char') || tipo.includes('text') || tipo.includes('varchar');
    });

    const findByNames = (names, pool) => {
      const candidates = names.map((n) => this.normalizeText(n));
      return pool.find((col) => candidates.some((cand) => this.normalizeText(col.nombre).includes(cand)));
    };

    const qualify = (column) => {
      if (!tableAlias) return this.quoteIdent(column);
      return `${this.quoteIdent(tableAlias)}.${this.quoteIdent(column)}`;
    };

    const clauses = [];

    const blockedBool = findByNames(['bloqueado', 'blocked', 'lock', 'suspend'], boolCols);
    const blockedText = findByNames(['bloqueado', 'blocked', 'estado', 'status'], textCols);
    const activeBool = findByNames(['activo', 'active', 'enabled', 'habilitado'], boolCols);
    const activeText = findByNames(['activo', 'active', 'estado', 'status'], textCols);

    if (/\bno\s+bloquead[oa]s?\b/.test(text)) {
      if (blockedBool) {
        clauses.push(`${qualify(blockedBool.nombre)} = FALSE`);
      } else if (blockedText) {
        clauses.push(`${qualify(blockedText.nombre)} NOT IN ('bloqueado', 'blocked', 'suspendido')`);
      }
    } else if (/\bbloquead[oa]s?\b/.test(text)) {
      if (blockedBool) {
        clauses.push(`${qualify(blockedBool.nombre)} = TRUE`);
      } else if (blockedText) {
        clauses.push(`${qualify(blockedText.nombre)} IN ('bloqueado', 'blocked', 'suspendido')`);
      }
    }

    if (/\bactiv[oa]s?\b/.test(text)) {
      if (activeBool) {
        clauses.push(`${qualify(activeBool.nombre)} = TRUE`);
      } else if (activeText) {
        clauses.push(`${qualify(activeText.nombre)} IN ('activo', 'active', 'enabled')`);
      }
    }

    if (/\binactiv[oa]s?\b/.test(text)) {
      if (activeBool) {
        clauses.push(`${qualify(activeBool.nombre)} = FALSE`);
      } else if (activeText) {
        clauses.push(`${qualify(activeText.nombre)} IN ('inactivo', 'inactive', 'disabled', 'deshabilitado')`);
      }
    }

    if (/\bsuspendid[oa]s?\b/.test(text)) {
      const suspendBool = findByNames(['suspendido', 'suspended', 'locked', 'bloqueado'], boolCols);
      const suspendText = findByNames(['estado', 'status', 'suspendido', 'blocked'], textCols);
      if (suspendBool) {
        clauses.push(`${qualify(suspendBool.nombre)} = TRUE`);
      } else if (suspendText) {
        clauses.push(`${qualify(suspendText.nombre)} IN ('suspendido', 'suspended')`);
      }
    }

    if (/\bpendiente[s]?\b/.test(text) || /\bpending\b/.test(text)) {
      const statusText = findByNames(['estado', 'status', 'estado_pago', 'payment_status'], textCols);
      if (statusText) {
        clauses.push(`${qualify(statusText.nombre)} IN ('pendiente', 'pending')`);
      }
    }

    if (/\bcompletad[oa]s?\b/.test(text) || /\bcompleted\b/.test(text) || /\bpagad[oa]s?\b/.test(text)) {
      const statusText = findByNames(['estado', 'status', 'estado_pago', 'payment_status'], textCols);
      if (statusText) {
        clauses.push(`${qualify(statusText.nombre)} IN ('completado', 'completed', 'pagado', 'paid')`);
      }
    }

    if (/\bcancelad[oa]s?\b/.test(text) || /\bcancelled?\b/.test(text)) {
      const statusText = findByNames(['estado', 'status', 'estado_pago', 'payment_status'], textCols);
      if (statusText) {
        clauses.push(`${qualify(statusText.nombre)} IN ('cancelado', 'cancelled', 'canceled')`);
      }
    }

    if (/\bno\s+verific[ao]d[oa]s?\b/.test(text)) {
      const verifyBool = findByNames(['verificado', 'verified', 'email_verified', 'confirma'], boolCols);
      if (verifyBool) clauses.push(`${qualify(verifyBool.nombre)} = FALSE`);
    } else if (/\bverific[ao]d[oa]s?\b/.test(text)) {
      const verifyBool = findByNames(['verificado', 'verified', 'email_verified', 'confirma'], boolCols);
      if (verifyBool) clauses.push(`${qualify(verifyBool.nombre)} = TRUE`);
    }

    // Natural-language recency filter (e.g., "ultimamente", "recientes", "ultimos 30 dias").
    if (/\b(ultim|ultimamente|ultimamente|reciente|recientes|hoy|ultima\s+semana|ultimos?\s+30\s+dias?)\b/.test(text)) {
      const dateCols = columns.filter((c) => {
        const name = this.normalizeText(c.nombre || '');
        const type = this.normalizeText(c.tipo || '');
        const looksTemporalType = /(date|time|timestamp)/.test(type);
        const looksTemporalName = /(created|updated|fecha|time|timestamp|last_login|ultimo|logged_at|occurred_at)/.test(name);
        return looksTemporalType || looksTemporalName;
      });

      const preferredRecentCol = findByNames(
        ['created_at', 'updated_at', 'fecha', 'timestamp', 'logged_at', 'occurred_at', 'last_login'],
        dateCols,
      ) || dateCols[0];

      if (preferredRecentCol) {
        clauses.push(`${qualify(preferredRecentCol.nombre)} >= NOW() - INTERVAL '30 days'`);
      }
    }

    // Generic value filter: if none of the above patterns matched and there are remaining
    // non-entity tokens, try them as values in role/type/status columns
    if (clauses.length === 0) {
      const roleTypeCols = columns.filter((c) => {
        const n = this.normalizeText(c.nombre || '');
        // Exclude id/uuid columns (e.g. group_id) to avoid invalid UUID casts with free-text values.
        if (/(^id$|_id$|uuid)/.test(n)) return false;
        return /^(role|rol|tipo|type|status|estado|perfil|nivel|grupo|group|categoria|category)/.test(n);
      });
      const rawTks = this.tokenizeInputRaw(textInput).map((tk) => this.normalizeTokenBase(tk));
      const valueTks = rawTks.slice(1).filter((tk) => {
        if (!tk || this.stopWords.has(tk)) return false;
        if (this.isStateIntentToken(tk)) return false;
        if (this.isLikelyUuid(tk) || this.isLikelyNumericId(tk)) return false;
        if (this.isLikelyUuidFragment(tk)) return false;
        if (hasFullUuidInInput && /^[0-9a-f]{4,}$/i.test(tk)) return false;
        // If token is another detected entity/table mention (e.g. "usuarios con logs"),
        // do not force it as a role/status value.
        if (this.isLikelyEntityMentionToken(tk)) return false;
        return true;
      });
      for (const vtk of valueTks) {
        for (const rc of roleTypeCols) {
          // Always compare as text to keep deterministic behavior across enum/non-text types.
          const clause = `${qualify(rc.nombre)}::text ILIKE '%${this.escapeLiteral(vtk)}%'`;
          clauses.push(clause);
        }
        if (roleTypeCols.length > 0) break; // only first unmatched token
      }
    }

    return [...new Set(clauses)];
  }

  findBestAttributeColumn(attributeToken, tableSchema) {
    const columns = Array.isArray(tableSchema?.columnas) ? tableSchema.columnas : [];
    if (!attributeToken || columns.length === 0) return null;

    const normalizedAttr = this.normalizeText(attributeToken);
    const livingKeywordDictionary = this.getMergedLivingKeywordDictionary();
    let best = null;

    // Primero, intentar encontrar usando el diccionario viviente
    const candidatesFromDictionary = livingKeywordDictionary[normalizedAttr] || [];
    if (candidatesFromDictionary.length > 0) {
      // Buscar una columna que coincida exactamente con uno de los candidatos del diccionario
      for (const candidate of candidatesFromDictionary) {
        const found = columns.find((col) => this.normalizeText(col.nombre) === this.normalizeText(candidate));
        if (found) {
          return found.nombre;
        }
      }
    }

    // Fallback: buscar por similitud y coincidencia parcial
    for (const col of columns) {
      const colName = this.normalizeText(col.nombre);
      let points = 0;
      if (colName === normalizedAttr) points += 10;
      if (colName.includes(normalizedAttr) || normalizedAttr.includes(colName)) points += 4;
      const sim = this.similarityScore(normalizedAttr, colName);
      if (sim >= 0.5) points += 3;

      if (['name', 'nombre', 'username', 'email'].some((cand) => colName.includes(cand))) {
        if (['nombre', 'name', 'usuario', 'username', 'correo', 'email'].includes(normalizedAttr)) {
          points += 8;
        }
      }

      const type = this.normalizeText(col.tipo || '');
      const isText = type.includes('char') || type.includes('text') || type.includes('varchar');
      if (!isText) points -= 2;

      if (!best || points > best.points) {
        best = { column: col.nombre, points, isText };
      }
    }

    return best && best.points > 0 && best.isText ? best.column : null;
  }

  pickBestIdentityColumn(tableSchema) {
    const columns = Array.isArray(tableSchema?.columnas) ? tableSchema.columnas : [];
    if (columns.length === 0) return null;

    const preferred = ['username', 'user_name', 'name', 'nombre', 'email', 'correo'];
    let best = null;

    for (const col of columns) {
      const colName = this.normalizeText(col.nombre);
      const colType = this.normalizeText(col.tipo || '');
      const isText = colType.includes('char') || colType.includes('text') || colType.includes('varchar');
      if (!isText) continue;

      let points = 0;
      for (const pref of preferred) {
        if (colName === pref) points += 10;
        else if (colName.includes(pref) || pref.includes(colName)) points += 6;
      }

      if (colName.endsWith('_name') || colName.endsWith('_username')) points += 4;

      if (!best || points > best.points) {
        best = { column: col.nombre, points };
      }
    }

    if (best && best.points > 0) return best.column;

    const firstText = columns.find((c) => {
      const t = this.normalizeText(c.tipo || '');
      return t.includes('char') || t.includes('text') || t.includes('varchar');
    });

    return firstText ? firstText.nombre : null;
  }

  isLikelyAttributeToken(token) {
    const normalized = this.normalizeText(token);
    if (!normalized) return false;
    const livingKeywordDictionary = this.getMergedLivingKeywordDictionary();
    
    // Palabras clave comunes que siempre son atributos
    const commonAttributes = /^(id|codigo|code|name|nombre|username|usuario|email|correo|estado|status|role|rol|tipo|type)$/;
    if (commonAttributes.test(normalized)) return true;
    
    // Revisar si está en el diccionario viviente como palabra clave conocida
    if (livingKeywordDictionary[normalized]) return true;
    
    return false;
  }

  isStateIntentToken(token) {
    const normalized = this.normalizeTokenBase(token);
    if (!normalized) return false;
    return /^(activo|inactivo|bloqueado|suspendido|habilitado|deshabilitado|enabled|disabled|blocked|locked|lock)$/.test(normalized);
  }

  shouldPreferOrWhereMerge(textInput) {
    const rawTokens = this.tokenizeInputRaw(textInput).map((tk) => this.normalizeTokenBase(tk)).filter(Boolean);
    if (rawTokens.length < 2 || rawTokens.length > 4) return false;
    if (this.detectAggregationIntent(textInput)) return false;
    if (this.detectRelationalIntent(textInput)) return false;
    if (rawTokens.some((tk) => this.isStateIntentToken(tk))) return false;
    if (rawTokens.some((tk) => this.isLikelyUuid(tk) || this.isLikelyNumericId(tk))) return false;
    return true;
  }

  buildLiteralWhereFromInput(textInput, tableSchema, tableAlias = null, options = {}) {
    const rawTokens = this.tokenizeInputRaw(textInput);
    const tokens = rawTokens.map((tk) => this.normalizeTokenBase(tk));
    if (tokens.length < 2) return [];

    const reservedEntityTokens = new Set(
      Array.isArray(options?.reservedEntityTokens)
        ? options.reservedEntityTokens.map((tk) => this.normalizeTokenBase(tk)).filter(Boolean)
        : []
    );

    const schemaTokens = new Set();
    const tableTokens = this.tokenizeIdentifier(tableSchema?.nombre || '').map((tk) => this.normalizeTokenBase(tk));
    tableTokens.forEach((tk) => schemaTokens.add(tk));
    for (const col of tableSchema?.columnas || []) {
      this.tokenizeIdentifier(col.nombre).forEach((tk) => schemaTokens.add(this.normalizeTokenBase(tk)));
    }

    const semanticAliases = this.getMergedSemanticAliases();
    const aliasTokens = (semanticAliases[tokens[0]] || []).map((tk) => this.normalizeTokenBase(tk));
    const firstTokenLooksLikeEntity = tableTokens.some((tk) => this.getTokenMatchType(tokens[0], tk).type !== 'none')
      || aliasTokens.some((tk) => tableTokens.some((tableTk) => this.getTokenMatchType(tk, tableTk).type !== 'none'));

    const qualify = (column) => {
      if (!tableAlias) return this.quoteIdent(column);
      return `${this.quoteIdent(tableAlias)}.${this.quoteIdent(column)}`;
    };

    const usableTokens = tokens.filter((tk) => !this.stopWords.has(tk));
    const clauses = [];

    const quotedValues = this.extractQuotedStrings(textInput);
    const detectedUuid = this.extractUuid(textInput);
    const detectedIdentifierTokens = this.getLikelyIdentifierTokens(textInput);

    // Prioridad 1: UUID => exact match, nunca LIKE
    if (detectedUuid) {
      const uuidClause = this.buildIdentifierClause(detectedUuid, tableSchema, tableAlias);
      if (uuidClause) {
        clauses.push(uuidClause);
        return [...new Set(clauses)];
      }
    }

    // Prioridad 1.5: IDs numéricos detectados en texto libre
    const numericId = detectedIdentifierTokens.find((tk) => this.isLikelyNumericId(tk));
    if (numericId) {
      const idClause = this.buildIdentifierClause(numericId, tableSchema, tableAlias);
      if (idClause) {
        clauses.push(idClause);
        return [...new Set(clauses)];
      }
    }

    // Prioridad 1.6: cualquier identificador literal encontrado (uuid o número)
    const anyIdentifier = detectedIdentifierTokens[0] || null;
    if (anyIdentifier) {
      const idClause = this.buildIdentifierClause(anyIdentifier, tableSchema, tableAlias);
      if (idClause) {
        clauses.push(idClause);
        return [...new Set(clauses)];
      }
    }

    // Prioridad 2: string entre comillas => exact match
    if (quotedValues.length > 0) {
      const quotedValue = quotedValues[0];
      const exactClause = this.buildSmartValueWhere(quotedValue, tableSchema, tableAlias);
      if (exactClause) {
        clauses.push(exactClause);
        return [...new Set(clauses)];
      }
    }

    // Pattern: <entidad> <id> <valor> (e.g. usuarios id 3)
    if (
      usableTokens.length === 3 &&
      firstTokenLooksLikeEntity &&
      this.normalizeTokenBase(usableTokens[1]) === 'id'
    ) {
      // Buscar columna id
      const idCol = (tableSchema?.columnas || []).find(c => this.normalizeTokenBase(c.nombre) === 'id');
      if (idCol) {
        clauses.push(`${qualify(idCol.nombre)} = '${this.escapeLiteral(usableTokens[2])}'`);
        return [...new Set(clauses)];
      }
    }

    // Pattern: <entidad> <valor> (e.g. "usuarios admin", "pagos pendientes")
    // NOTE: reservedEntityTokens check intentionally omitted here — in a 2-token query the
    // second token is always a value filter, even if it shares a name with another table.
    if (
      usableTokens.length === 2
      && firstTokenLooksLikeEntity
      && !this.isLikelyAttributeToken(usableTokens[1])
      && !this.isStateIntentToken(usableTokens[1])
      && !this.isLikelyEntityMentionToken(usableTokens[1])
    ) {
      const directValue = usableTokens[1];

      // Si el valor parece identificador (uuid o numérico), priorizar columnas id/_id
      if (this.isLikelyUuid(directValue) || this.isLikelyNumericId(directValue)) {
        const idClause = this.buildIdentifierClause(directValue, tableSchema, tableAlias);
        if (idClause) {
          clauses.push(idClause);
          return [...new Set(clauses)];
        }
      }

      // Text columns (varchar/char/text): ILIKE partial match
      const textCols = (tableSchema?.columnas || []).filter((c) => {
        const t = this.normalizeText(c.tipo || '');
        if (this.isSensitiveColumn(c.nombre)) return false;
        return t.includes('char') || t.includes('text') || t.includes('varchar');
      });
      // Role/type/status columns that may be ENUM or non-text: use exact match
      const roleTypeCols = (tableSchema?.columnas || []).filter((c) => {
        const n = this.normalizeText(c.nombre || '');
        // Exclude id/uuid columns (e.g. group_id) to avoid invalid UUID comparisons.
        if (/(^id$|_id$|uuid)/.test(n)) return false;
        const isRoleStatus = /^(role|rol|tipo|type|status|estado|perfil|nivel|grupo|group|categoria|category)/.test(n);
        const alreadyInText = textCols.find((tc) => tc.nombre === c.nombre);
        return isRoleStatus && !alreadyInText;
      });
      const ilikeParts = textCols.map((c) => `${qualify(c.nombre)}::text ILIKE '%${this.escapeLiteral(directValue)}%'`);
      const castedParts = roleTypeCols.map((c) => `${qualify(c.nombre)}::text ILIKE '%${this.escapeLiteral(directValue)}%'`);
      const allParts = [...ilikeParts, ...castedParts];
      if (allParts.length > 0) {
        clauses.push(`(${allParts.join(' OR ')})`);
        return [...new Set(clauses)];
      }
      // Fallback: identity column
      const identityColumn = this.pickBestIdentityColumn(tableSchema);
      if (identityColumn) {
        clauses.push(`${qualify(identityColumn)}::text ILIKE '%${this.escapeLiteral(directValue)}%'`);
        return [...new Set(clauses)];
      }
    }

    // Pattern: <entidad> <atributo> <valor> (e.g. usuarios nombre admin)
    if (usableTokens.length >= 3 && firstTokenLooksLikeEntity) {
      const attributeToken = usableTokens[1];
      const valueToken = usableTokens.slice(2).join(' ');
      const column = this.isStateIntentToken(attributeToken)
        ? null
        : this.findBestAttributeColumn(attributeToken, tableSchema);
      if (column && valueToken) {
        // Si el atributo es id, usar igualdad exacta
        if (this.normalizeTokenBase(attributeToken) === 'id') {
          clauses.push(`${qualify(column)} = '${this.escapeLiteral(valueToken)}'`);
        } else {
          clauses.push(`${qualify(column)}::text ILIKE '%${this.escapeLiteral(valueToken)}%'`);
        }
      }
    }

    for (let i = 0; i < usableTokens.length - 1; i += 1) {
      const attributeToken = usableTokens[i];
      const valueToken = usableTokens[i + 1];

      if (i === 0 && firstTokenLooksLikeEntity) continue;
      if (this.isStateIntentToken(attributeToken)) continue;
      if (/^(nombre|name|usuario|username|correo|email)$/.test(valueToken)) continue;

      if (schemaTokens.has(valueToken)) continue;
      if (reservedEntityTokens.has(this.normalizeTokenBase(valueToken))) continue;
      if (this.isLikelyEntityMentionToken(valueToken)) continue;
      if (/^(activo|inactivo|bloqueado|no)$/.test(valueToken) || this.isStateIntentToken(valueToken)) continue;

      const column = this.findBestAttributeColumn(attributeToken, tableSchema);
      if (!column) continue;

      // Si el atributo es id, usar igualdad exacta
      if (this.normalizeTokenBase(attributeToken) === 'id') {
        clauses.push(`${qualify(column)} = '${this.escapeLiteral(valueToken)}'`);
      } else {
        clauses.push(`${qualify(column)}::text ILIKE '%${this.escapeLiteral(valueToken)}%'`);
      }
    }

    if (clauses.length === 0 && usableTokens.length >= 2) {
      const fallbackValue = usableTokens[usableTokens.length - 1];
      if (
        !schemaTokens.has(fallbackValue)
        && !this.isStateIntentToken(fallbackValue)
        && !reservedEntityTokens.has(this.normalizeTokenBase(fallbackValue))
        && !this.isLikelyEntityMentionToken(fallbackValue)
      ) {
        const fallbackColumn = this.findBestAttributeColumn('name', tableSchema)
          || this.findBestAttributeColumn('username', tableSchema)
          || this.findBestAttributeColumn('email', tableSchema);

        if (fallbackColumn) {
          clauses.push(`${qualify(fallbackColumn)}::text ILIKE '%${this.escapeLiteral(fallbackValue)}%'`);
        }
      }
    }

    return [...new Set(clauses)];
  }

  inferSemanticWhere(tableSchema, textInput, tableAlias = null) {
    const clauses = this.buildWhereFromKeywords(textInput, tableSchema, tableAlias);
    return clauses.length > 0 ? clauses.join(' AND ') : null;
  }

  buildSemanticWhereForTables(schema, tables, textInput, aliasMap = {}) {
    const requestedTables = [...new Set((tables || []).filter(Boolean))];
    const clauses = [];
    const reservedEntityTokens = requestedTables.flatMap((table) => {
      const semanticTokens = Array.isArray(schema?.semanticIndex?.[table]?.tokens) ? schema.semanticIndex[table].tokens : [];
      return semanticTokens.map((token) => this.normalizeTokenBase(token));
    });

    for (const table of requestedTables) {
      const tableSchema = schema?.schema?.[table];
      if (!tableSchema) continue;

      const tableAlias = aliasMap?.[table] || null;
      const tableClauses = this.buildWhereFromKeywords(textInput, tableSchema, tableAlias);
      const literalClauses = this.buildLiteralWhereFromInput(textInput, tableSchema, tableAlias, { reservedEntityTokens });
      clauses.push(...tableClauses);
      clauses.push(...literalClauses);
    }

    return [...new Set(clauses)].join(' AND ') || null;
  }

  buildRelationshipEdges(schema) {
    const edges = [];
    const dedupe = new Set();

    for (const [tableName, tableSchema] of Object.entries(schema.schema || {})) {
      const fks = Array.isArray(tableSchema?.clavesForaneas) ? tableSchema.clavesForaneas : [];
      for (const fk of fks) {
        const leftTable = tableName;
        const leftColumn = fk.columna;
        const rightTable = fk.tablaReferenciada;
        const rightColumn = fk.columnaReferenciada;

        if (!leftTable || !leftColumn || !rightTable || !rightColumn) continue;

        const key = `${leftTable}.${leftColumn}->${rightTable}.${rightColumn}`;
        if (dedupe.has(key)) continue;

        dedupe.add(key);
        edges.push({ leftTable, leftColumn, rightTable, rightColumn });
      }
    }

    return edges;
  }

  findPathBetweenTables(edges, startTable, targetTable) {
    if (startTable === targetTable) return [];

    const queue = [{ table: startTable, path: [] }];
    const visited = new Set([startTable]);

    while (queue.length > 0) {
      const current = queue.shift();

      for (const edge of edges) {
        let nextTable = null;
        let step = null;

        if (edge.leftTable === current.table) {
          nextTable = edge.rightTable;
          step = { ...edge };
        } else if (edge.rightTable === current.table) {
          nextTable = edge.leftTable;
          step = {
            leftTable: edge.rightTable,
            leftColumn: edge.rightColumn,
            rightTable: edge.leftTable,
            rightColumn: edge.leftColumn,
          };
        }

        if (!nextTable || visited.has(nextTable)) continue;

        const nextPath = [...current.path, step];
        if (nextTable === targetTable) return nextPath;

        visited.add(nextTable);
        queue.push({ table: nextTable, path: nextPath });
      }
    }

    return null;
  }

  buildJoinPlan(schema, requestedTables = []) {
    const tables = [...new Set((requestedTables || []).filter(Boolean))];
    if (tables.length <= 1) {
      return {
        baseTable: tables[0] || null,
        joinEdges: [],
        joinedTables: new Set(tables),
        missingTables: [],
      };
    }

    const edges = this.buildRelationshipEdges(schema);
    const baseTable = tables[0];
    const joinedTables = new Set([baseTable]);
    const joinEdges = [];
    const missingTables = [];

    for (const target of tables.slice(1)) {
      let bestPath = null;
      for (const source of joinedTables) {
        const path = this.findPathBetweenTables(edges, source, target);
        if (!path || path.length === 0) continue;
        if (!bestPath || path.length < bestPath.length) bestPath = path;
      }

      if (!bestPath) {
        missingTables.push(target);
        continue;
      }

      for (const step of bestPath) {
        const edgeKey = `${step.leftTable}.${step.leftColumn}->${step.rightTable}.${step.rightColumn}`;
        const exists = joinEdges.some((e) => `${e.leftTable}.${e.leftColumn}->${e.rightTable}.${e.rightColumn}` === edgeKey);
        if (!exists) joinEdges.push(step);
        joinedTables.add(step.leftTable);
        joinedTables.add(step.rightTable);
      }
    }

    return { baseTable, joinEdges, joinedTables, missingTables };
  }

  hasUsableJoinPlan(joinPlan, requestedTables = []) {
    const requested = [...new Set((requestedTables || []).filter(Boolean))];
    if (requested.length <= 1) return false;
    if (!joinPlan?.baseTable) return false;
    if ((joinPlan?.missingTables || []).length > 0) return false;

    const joinedCount = joinPlan?.joinedTables instanceof Set
      ? joinPlan.joinedTables.size
      : Array.isArray(joinPlan?.joinedTables)
        ? joinPlan.joinedTables.length
        : 0;

    return joinedCount >= requested.length && Array.isArray(joinPlan?.joinEdges) && joinPlan.joinEdges.length > 0;
  }

  buildAliasMap(tables = []) {
    const map = {};
    let index = 1;
    for (const table of tables) {
      if (!map[table]) {
        map[table] = `t${index}`;
        index += 1;
      }
    }
    return map;
  }

  buildFromAndJoins(joinPlan, aliasMap) {
    let sql = `FROM ${this.quoteIdent(joinPlan.baseTable)} ${this.quoteIdent(aliasMap[joinPlan.baseTable])}`;

    const alreadyJoined = new Set([joinPlan.baseTable]);

    for (const edge of joinPlan.joinEdges) {
      const leftAlias = aliasMap[edge.leftTable];
      const rightAlias = aliasMap[edge.rightTable];
      if (!leftAlias || !rightAlias) continue;

      const joinTarget = alreadyJoined.has(edge.leftTable) && !alreadyJoined.has(edge.rightTable)
        ? edge.rightTable
        : alreadyJoined.has(edge.rightTable) && !alreadyJoined.has(edge.leftTable)
          ? edge.leftTable
          : edge.rightTable;

      const joinedSide = joinTarget === edge.rightTable
        ? {
            table: edge.rightTable,
            alias: rightAlias,
            onLeftAlias: leftAlias,
            onLeftColumn: edge.leftColumn,
            onRightAlias: rightAlias,
            onRightColumn: edge.rightColumn,
          }
        : {
            table: edge.leftTable,
            alias: leftAlias,
            onLeftAlias: rightAlias,
            onLeftColumn: edge.rightColumn,
            onRightAlias: leftAlias,
            onRightColumn: edge.leftColumn,
          };

      if (alreadyJoined.has(joinedSide.table)) continue;

      sql += ` LEFT JOIN ${this.quoteIdent(joinedSide.table)} ${this.quoteIdent(joinedSide.alias)}`;
      sql += ` ON ${this.quoteIdent(joinedSide.onLeftAlias)}.${this.quoteIdent(joinedSide.onLeftColumn)} = ${this.quoteIdent(joinedSide.onRightAlias)}.${this.quoteIdent(joinedSide.onRightColumn)}`;
      alreadyJoined.add(joinedSide.table);
    }

    return sql;
  }

  buildStructuredWhereClause(conditions = [], aliasMap = {}) {
    const clauses = [];

    for (const condition of conditions || []) {
      const tableAlias = aliasMap?.[condition.table] || 't1';
      const qualifiedColumn = `${this.quoteIdent(tableAlias)}.${this.quoteIdent(condition.column)}`;

      if (condition.operator === 'IS NULL' || condition.operator === 'IS NOT NULL') {
        clauses.push(`${qualifiedColumn} ${condition.operator}`);
        continue;
      }

      if (condition.operator === 'ILIKE') {
        const expr = condition.castText ? `${qualifiedColumn}::text` : qualifiedColumn;
        clauses.push(`${expr} ILIKE '${this.escapeLiteral(condition.value)}'`);
        continue;
      }

      if (condition.value !== undefined) {
        clauses.push(`${qualifiedColumn} ${condition.operator} ${Number.isFinite(condition.value) ? condition.value : `'${this.escapeLiteral(condition.value)}'`}`);
      }
    }

    return [...new Set(clauses)].join(' AND ') || null;
  }

  buildStructuredHavingClause(havingCondition, aliasMap = {}) {
    if (!havingCondition?.table || !havingCondition?.column || !havingCondition?.operator) return null;
    const tableAlias = aliasMap?.[havingCondition.table];
    if (!tableAlias) return null;

    const aggregate = havingCondition.aggregation || 'COUNT';
    return `${aggregate}(${this.quoteIdent(tableAlias)}.${this.quoteIdent(havingCondition.column)}) ${havingCondition.operator} ${Number(havingCondition.value)}`;
  }

  mergeWhereClauses(...clauses) {
    const filtered = clauses.map((clause) => String(clause || '').trim()).filter(Boolean);
    return [...new Set(filtered)].join(' AND ') || null;
  }

  async buildSimpleSelect(tableName, options = {}) {
    const schema = await this.getFullSchema();
    if (!schema.schema[tableName]) {
      throw new Error(`Tabla no encontrada: ${tableName}`);
    }

    const { limit = 50, offset = 0 } = options;
    const tableSchema = schema.schema[tableName];
    const alias = 't1';

    const selectedColumns = this.getRelevantSelectableColumns(tableSchema);
    const selectSql = selectedColumns
      .map((column) => `${this.quoteIdent(alias)}.${this.quoteIdent(column)} AS ${this.quoteIdent(`${tableName}_${column}`)}`)
      .join(', ');

    let sql = `SELECT ${selectSql} FROM ${this.quoteIdent(tableName)} ${this.quoteIdent(alias)}`;

    const literalWhere = this.buildLiteralWhereFromInput(options.textInput, tableSchema, alias, {
      reservedEntityTokens: options.reservedEntityTokens || [],
    });
    const semanticWhere = String(options.semanticWhere || '').trim();
    const literalExpr = [...new Set(literalWhere.filter(Boolean))].join(' AND ');

    let mergedWhere = '';
    if (semanticWhere && literalExpr) {
      mergedWhere = this.shouldPreferOrWhereMerge(options.textInput)
        ? `(${semanticWhere}) OR (${literalExpr})`
        : `${semanticWhere} AND ${literalExpr}`;
    } else {
      mergedWhere = semanticWhere || literalExpr || '';
    }

    if (options.where) {
      sql += ` WHERE ${options.where}`;
    } else if (mergedWhere) {
      sql += ` WHERE ${mergedWhere}`;
    }

    const pk = Array.isArray(tableSchema.pkPrincipal) ? tableSchema.pkPrincipal[0] : tableSchema.pkPrincipal;
    if (pk) {
      sql += ` ORDER BY ${this.quoteIdent(alias)}.${this.quoteIdent(pk)} DESC`;
    }

    sql += ` LIMIT ${Math.min(parseInt(limit, 10) || 50, 1000)} OFFSET ${Math.max(parseInt(offset, 10) || 0, 0)}`;

    return {
      sql,
      tipo: 'simple',
      tabla: tableName,
      columnas: selectedColumns,
      columnasUsadas: selectedColumns,
      filtrosAplicados: options.where || mergedWhere || null,
      pk: tableSchema.pkPrincipal,
    };
  }

  async buildRelationalSelect(tablas, options = {}) {
    const schema = await this.getFullSchema();
    const { limit = 10, offset = 0 } = options;
    const joinTables = [...new Set((options.joinTables || tablas || []).filter(Boolean))];
    const selectTables = [...new Set((options.selectTables || tablas || []).filter(Boolean))];

    if (!Array.isArray(joinTables) || joinTables.length === 0) {
      throw new Error('Se requiere al menos una tabla');
    }

    if (joinTables.length === 1) {
      return this.buildSimpleSelect(joinTables[0], options);
    }

    for (const tabla of joinTables) {
      if (!schema.schema[tabla]) {
        throw new Error(`Tabla no encontrada: ${tabla}`);
      }
    }

    const joinPlan = this.buildJoinPlan(schema, joinTables, options.textInput);
    if (!this.hasUsableJoinPlan(joinPlan, joinTables)) {
      const fallbackTable = joinPlan.baseTable || tablas[0];
      return this.buildSimpleSelect(fallbackTable, { ...options, textInput: options.textInput || '' });
    }

    const allTables = [...joinPlan.joinedTables];
    const aliasMap = this.buildAliasMap(allTables);

    const selectParts = [];
    for (const table of selectTables) {
      const tableAlias = aliasMap[table];
      if (!tableAlias) continue;
      const columns = this.getRelevantSelectableColumns(schema.schema[table]);
      for (const column of columns) {
        selectParts.push(`${this.quoteIdent(tableAlias)}.${this.quoteIdent(column)} AS ${this.quoteIdent(`${table}_${column}`)}`);
      }
    }

    let sql = `SELECT ${selectParts.join(', ')} `;
    sql += this.buildFromAndJoins(joinPlan, aliasMap);

    const semanticWhere = options.where || options.semanticWhere || this.buildSemanticWhereForTables(schema, joinTables, options.textInput, aliasMap);
    const structuredWhere = this.buildStructuredWhereClause(options?.smartPlan?.whereConditions || [], aliasMap);
    const mergedWhere = this.mergeWhereClauses(semanticWhere, structuredWhere);

    if (options.where) {
      sql += ` WHERE ${options.where}`;
    } else if (mergedWhere) {
      sql += ` WHERE ${mergedWhere}`;
    }

    sql += ` LIMIT ${Math.min(parseInt(limit, 10) || 50, 1000)} OFFSET ${Math.max(parseInt(offset, 10) || 0, 0)}`;

    return {
      sql,
      tipo: 'relacional',
      tablas: joinTables,
      alias: aliasMap,
      columnasUsadas: selectParts,
      filtrosAplicados: mergedWhere || null,
      tablasNoRelacionadas: joinPlan.missingTables,
    };
  }

  pickGroupByColumn(tableSchema) {
    const candidates = this.getRelevantSelectableColumns(tableSchema, 4);
    const preferred = ['username', 'nombre', 'name', 'email', 'role', 'rol'];

    for (const pref of preferred) {
      const found = candidates.find((c) => this.normalizeText(c) === pref);
      if (found) return found;
    }

    const pks = Array.isArray(tableSchema?.clavesPrimarias) ? tableSchema.clavesPrimarias : [];
    if (pks.length > 0) return pks[0];

    return candidates[0] || null;
  }

  pickPrimaryKey(tableSchema) {
    const pks = Array.isArray(tableSchema?.clavesPrimarias) ? tableSchema.clavesPrimarias : [];
    if (pks.length > 0) return pks[0];

    const mainPk = Array.isArray(tableSchema?.pkPrincipal) ? tableSchema.pkPrincipal[0] : tableSchema?.pkPrincipal;
    return mainPk || null;
  }

  pickMetricColumn(tableSchema) {
    const safe = this.getSafeSelectableColumns(tableSchema);
    const numeric = safe.find((c) => {
      const t = this.normalizeText(c.tipo || '');
      return t.includes('int') || t.includes('numeric') || t.includes('decimal') || t.includes('real') || t.includes('double');
    });

    if (numeric) return numeric.nombre;

    const pks = Array.isArray(tableSchema?.clavesPrimarias) ? tableSchema.clavesPrimarias : [];
    if (pks.length > 0) return pks[0];

    return safe[0]?.nombre || 'id';
  }

  async buildAnalyticQuery(tablas, aggregationFn, textInput, options = {}) {
    const schema = await this.getFullSchema();
    const requested = [...new Set((options?.smartPlan?.joinTables || tablas || []))].filter(Boolean);
    if (requested.length === 0) throw new Error('No hay tablas para consulta analitica');

    const joinPlan = this.buildJoinPlan(schema, requested, textInput);
    if (requested.length > 1 && !this.hasUsableJoinPlan(joinPlan, requested)) {
      return this.buildSimpleSelect(requested[0], options);
    }

    const allTables = [...joinPlan.joinedTables];
    const aliasMap = this.buildAliasMap(allTables);

    const smartPlan = options.smartPlan || null;
    const groupTable = smartPlan?.baseTable || requested[0];
    const metricTable = smartPlan?.metricTable || requested.find((t) => t !== groupTable) || groupTable;

    const groupSchema = schema.schema[groupTable];
    const metricSchema = schema.schema[metricTable];

    const groupPk = this.pickPrimaryKey(groupSchema);
    const groupLabelCol = this.pickBestIdentityColumn(groupSchema) || this.pickGroupByColumn(groupSchema);
    const metricCol = smartPlan?.metricColumn || this.pickMetricColumn(metricSchema);

    const groupAlias = aliasMap[groupTable];
    const metricAlias = aliasMap[metricTable] || groupAlias;

    if (!(groupPk || groupLabelCol) || !groupAlias) {
      throw new Error('No se pudo construir GROUP BY para consulta analitica');
    }

    const effectiveAggregationFn = smartPlan?.aggregationFn || aggregationFn;
    let aggregateExpr = `${effectiveAggregationFn}(${this.quoteIdent(metricAlias)}.${this.quoteIdent(metricCol)})`;
    if (effectiveAggregationFn === 'COUNT') {
      aggregateExpr = `COUNT(${this.quoteIdent(metricAlias)}.${this.quoteIdent(metricCol)})`;
    }

    const metricAliasName = effectiveAggregationFn === 'COUNT'
      ? `total_${metricTable}`
      : `metric_${this.normalizeText(metricCol || 'value') || 'value'}`;

    const selectParts = [];
    const groupByParts = [];

    if (groupPk) {
      selectParts.push(`${this.quoteIdent(groupAlias)}.${this.quoteIdent(groupPk)} AS ${this.quoteIdent(`${groupTable}_${groupPk}`)}`);
      groupByParts.push(`${this.quoteIdent(groupAlias)}.${this.quoteIdent(groupPk)}`);
    }

    if (groupLabelCol && groupLabelCol !== groupPk) {
      selectParts.push(`${this.quoteIdent(groupAlias)}.${this.quoteIdent(groupLabelCol)} AS ${this.quoteIdent(`${groupTable}_${groupLabelCol}`)}`);
      groupByParts.push(`${this.quoteIdent(groupAlias)}.${this.quoteIdent(groupLabelCol)}`);
    }

    if (groupLabelCol === groupPk && groupLabelCol) {
      selectParts.push(`${this.quoteIdent(groupAlias)}.${this.quoteIdent(groupLabelCol)} AS ${this.quoteIdent(`${groupTable}_${groupLabelCol}`)}`);
    }

    let sql = `SELECT ${selectParts.join(', ')}, ${aggregateExpr} AS ${this.quoteIdent(metricAliasName)} `;
    sql += this.buildFromAndJoins(joinPlan, aliasMap);

    const semanticWhere = options.semanticWhere || this.buildSemanticWhereForTables(schema, requested, textInput, aliasMap);
    const structuredWhere = this.buildStructuredWhereClause(smartPlan?.whereConditions || [], aliasMap);
    const mergedWhere = this.mergeWhereClauses(semanticWhere, structuredWhere);
    if (mergedWhere) {
      sql += ` WHERE ${mergedWhere}`;
    }

    if (groupByParts.length > 0) {
      sql += ` GROUP BY ${groupByParts.join(', ')}`;
    }
    const havingClause = this.buildStructuredHavingClause(smartPlan?.havingCondition, aliasMap);
    if (havingClause) {
      sql += ` HAVING ${havingClause}`;
    }
    const orderDirection = options.orderDirection === 'ASC' ? 'ASC' : 'DESC';
    sql += ` ORDER BY ${this.quoteIdent(metricAliasName)} ${orderDirection}`;
    sql += ` LIMIT ${Math.min(parseInt(options.limit, 10) || 50, 1000)} OFFSET ${Math.max(parseInt(options.offset, 10) || 0, 0)}`;

    return {
      sql,
      tipo: 'analitico',
      tablas: requested,
      agregacion: effectiveAggregationFn,
      groupBy: groupByParts.join(', '),
      metric: `${metricTable}.${metricCol}`,
      columnasUsadas: [...groupByParts, `${effectiveAggregationFn}(${metricTable}.${metricCol})`],
      filtrosAplicados: this.mergeWhereClauses(mergedWhere, havingClause) || null,
      tablasNoRelacionadas: joinPlan.missingTables,
    };
  }

  async buildFallbackQuery(options = {}, reason = 'fallback') {
    const schema = await this.getFullSchema();
    const defaultTable = Array.isArray(schema.tablas) ? schema.tablas[0] : null;

    if (!defaultTable) {
      return {
        exito: false,
        error: 'No hay tablas disponibles para construir una consulta de fallback',
        razon: reason,
      };
    }

    const simple = await this.buildSimpleSelect(defaultTable, options);

    return {
      exito: true,
      query: {
        ...simple,
        tipo: 'fallback',
      },
      mensaje: `Consulta generada en modo seguro (${reason})`,
      sugerencias: (schema.tablas || []).slice(0, 3),
    };
  }

  findTableByNaturalToken(textInput, schema = {}) {
    const normalizedText = this.normalizeText(textInput);
    const semanticIndex = schema?.semanticIndex || {};
    const candidates = Object.keys(semanticIndex || {});
    const direct = candidates.find((tableName) => {
      const token = this.normalizeIdentifierName(tableName);
      return new RegExp(`\\b${token}\\b`, 'i').test(normalizedText);
    });

    if (direct) return direct;

    const aliases = this.getMergedSemanticAliases();
    for (const [alias, tables] of Object.entries(aliases || {})) {
      if (!new RegExp(`\\b${this.normalizeText(alias)}\\b`, 'i').test(normalizedText)) continue;
      const matched = (tables || []).find((tableName) => Boolean(schema?.schema?.[tableName]));
      if (matched) return matched;
    }

    return null;
  }

  parseManualIntent(textInput, schema = {}) {
    const originalText = String(textInput || '').trim();
    const normalizedText = this.normalizeText(originalText);
    if (!normalizedText) return { matched: false };

    const idSearchMatch = originalText.match(/(?:busca(?:r)?\s+(?:el|la)?\s*)?([a-zA-Z_][a-zA-Z0-9_]*)\s+([0-9a-fA-F-]{8,}|\d{1,20})/i);
    const requestedColumnFromSearch = idSearchMatch ? String(idSearchMatch[1] || '').trim() : '';
    const derivedTableFromColumn = requestedColumnFromSearch.includes('_')
      ? this.normalizeIdentifierName(requestedColumnFromSearch.split('_')[0])
      : null;

    const table = this.findTableByNaturalToken(normalizedText, schema)
      || (derivedTableFromColumn && schema?.schema?.[derivedTableFromColumn] ? derivedTableFromColumn : null);
    if (!table) return { matched: false };

    const limitMatch = normalizedText.match(/\blimit\s+(\d{1,4})\b/i);
    const parsedLimit = limitMatch ? Math.min(Math.max(Number(limitMatch[1]), 1), 1000) : null;

    if (idSearchMatch) {
      const requestedColumn = String(idSearchMatch[1] || '').trim();
      const requestedValue = String(idSearchMatch[2] || '').trim();
      const tableSchema = schema?.schema?.[table];
      const columns = Array.isArray(tableSchema?.columnas) ? tableSchema.columnas : [];
      const normalizedRequested = this.normalizeText(requestedColumn);
      const matchedColumn = columns.find((column) => this.normalizeText(column?.nombre) === normalizedRequested);
      const idColumn = matchedColumn?.nombre || this.pickPrimaryKey(tableSchema) || 'id';

      const quoteValue = this.escapeLiteral(requestedValue);
      const where = `${this.quoteIdent('t1')}.${this.quoteIdent(idColumn)}::text = '${quoteValue}'`;

      return {
        matched: true,
        type: 'id-search',
        table,
        limit: 1,
        where,
        idColumn,
      };
    }

    if (new RegExp(`^${this.normalizeIdentifierName(table)}$`, 'i').test(normalizedText) || /\blimit\s+\d+\b/i.test(normalizedText)) {
      return {
        matched: true,
        type: parsedLimit ? 'simple-limit' : 'simple-table',
        table,
        limit: parsedLimit,
      };
    }

    return { matched: false };
  }

  shouldUseAiCopilot(textInput, analisis = {}) {
    const confidence = Number(analisis?.confianza || 0);
    const hasUuid = Boolean(this.extractUuid(textInput));
    if (hasUuid) return false;

    const hasNoTable = !analisis?.tablaBase && (!Array.isArray(analisis?.tablasDetectadas) || analisis.tablasDetectadas.length === 0);
    const isUnstructuredInput = String(textInput || '').trim().split(/\s+/).length >= 4;
    const hasWeakScore = Number(analisis?.topScore || 0) < 2;
    const isLowConfidence = confidence < 0.6;

    return isLowConfidence || (hasNoTable && isUnstructuredInput) || hasWeakScore;
  }

  async requestLowConfidenceSqlRewrite(textInput, analisis = {}, schema = {}) {
    const baseConfidence = Number(analisis?.confianza || 0);
    if (!this.shouldUseAiCopilot(textInput, analisis)) return null;

    const schemaSummary = Object.entries(schema?.schema || {})
      .slice(0, 12)
      .map(([tableName, tableSchema]) => {
        const columns = Array.isArray(tableSchema?.columnas)
          ? tableSchema.columnas.slice(0, 12).map((column) => String(column?.nombre || '').trim()).filter(Boolean)
          : [];
        return `${tableName}: ${columns.join(', ')}`;
      })
      .filter(Boolean)
      .join('\n');

    const prompt = [
      'Eres un asistente experto en SQL semántico para backend Node.js.',
      'Solo debes apoyar al parser. No inventes datos.',
      'Responde SOLO JSON válido con este formato:',
      '{"consulta_reescrita":"...","sql":"SELECT ...","confianza":0.0}',
      'Si no estás seguro, deja sql vacío y usa consulta_reescrita.',
      '',
      `Consulta original: ${String(textInput || '').trim()}`,
      `Confianza actual: ${baseConfidence}`,
      'Esquema disponible (tablas y columnas):',
      schemaSummary || 'Sin esquema disponible',
    ].join('\n');

    try {
      const aiResponse = await askOllama(prompt, { timeoutMs: 3000 });
      const aiJson = aiResponse?.json && typeof aiResponse.json === 'object' ? aiResponse.json : null;
      if (!aiResponse?.ok || !aiJson) return null;

      const rewritten = String(aiJson?.consulta_reescrita || '').trim();
      const sql = String(aiJson?.sql || '').trim();
      const confidence = Number(aiJson?.confianza || 0);

      if (!rewritten && !sql) return null;

      return {
        rewritten,
        sql,
        confidence: Number.isFinite(confidence) ? confidence : 0,
      };
    } catch {
      return null;
    }
  }

  async generateQuery(textInput, options = {}) {
    let effectiveTextInput = textInput;
    let analisis = await this.analyzeKeywords(effectiveTextInput);
    const schema = await this.getFullSchema();
    const lowConfidenceThreshold = 0.5;
    const _inputUuid = this.extractUuid(textInput);
    const _inputQuoted = this.extractQuotedStrings(textInput);

    const manualIntent = this.parseManualIntent(effectiveTextInput, schema);
    if (manualIntent?.matched && manualIntent?.table) {
      const manualQuery = await this.buildSimpleSelect(manualIntent.table, {
        ...options,
        textInput: effectiveTextInput,
        where: manualIntent.where || undefined,
        limit: manualIntent.limit || options.limit || 50,
      });

      return {
        exito: true,
        analisis,
        query: {
          ...manualQuery,
          tipo: manualIntent.type || 'manual-parser',
        },
        warning: null,
        warnings: ['Consulta interpretada por parser determinístico.'],
        mensaje: `Consulta interpretada automáticamente (${manualIntent.type || 'manual-parser'}).`,
        sugerencias: analisis.ranking.slice(0, 3).map((entry) => entry.table),
        debug: {
          entidadPrincipal: analisis.entidadDetectada || manualIntent.table,
          entidadDetectada: analisis.entidadDetectada || manualIntent.table,
          tokensInput: analisis.tokens,
          tablasEvaluadas: analisis.ranking,
          tablaSeleccionada: manualIntent.table,
          score: analisis.topScore || 0,
          joins: [],
          columnas: manualQuery?.columnasUsadas || manualQuery?.columnas || [],
          columnasUsadas: manualQuery?.columnasUsadas || manualQuery?.columnas || [],
          filtrosAplicados: manualQuery?.filtrosAplicados || null,
          tipoConsulta: manualIntent.type || analisis.tipoConsulta,
          confianza: analisis.confianza || 0,
          parser: 'manual',
        },
      };
    }

    const aiRewrite = await this.requestLowConfidenceSqlRewrite(effectiveTextInput, analisis, schema);

    if (aiRewrite) {
      try {
        const aiSql = String(aiRewrite?.sql || '').trim();
        const aiConfidence = Number(aiRewrite?.confidence || 0);
        if (aiSql && aiConfidence >= 0.6) {
          const validation = this.validateSQL(aiSql, 'user');
          if (validation?.valido) {
            return {
              exito: true,
              analisis,
              query: {
                sql: aiSql,
                tipo: 'ai-assisted-sql',
                tabla: analisis.tablaBase || analisis.tablasDetectadas?.[0] || null,
                columnas: [],
                columnasUsadas: [],
                filtrosAplicados: null,
              },
              warning: 'SQL sugerido por IA local bajo baja confianza.',
              warnings: ['SQL sugerido por IA local bajo baja confianza.'],
              mensaje: 'Consulta generada con apoyo de IA local (copiloto).',
              sugerencias: analisis.ranking.slice(0, 3).map((entry) => entry.table),
              debug: {
                entidadPrincipal: analisis.entidadDetectada || null,
                entidadDetectada: analisis.entidadDetectada || null,
                tokensInput: analisis.tokens,
                tablasEvaluadas: analisis.ranking,
                tablaSeleccionada: analisis.tablaBase || null,
                score: analisis.topScore || 0,
                joins: [],
                columnas: [],
                columnasUsadas: [],
                filtrosAplicados: null,
                tipoConsulta: analisis.tipoConsulta,
                confianza: analisis.confianza || 0,
                parser: 'ai-copilot-sql',
              },
            };
          }
        }

        const rewrittenText = String(aiRewrite?.rewritten || '').trim();
        if (!rewrittenText) {
          throw new Error('No rewrite available');
        }

        const aiAnalysis = await this.analyzeKeywords(rewrittenText);
        const improvedConfidence = Number(aiAnalysis?.confianza || 0) > Number(analisis?.confianza || 0);
        const hasDetectedTable = Boolean(aiAnalysis?.tablaBase) || (Array.isArray(aiAnalysis?.tablasDetectadas) && aiAnalysis.tablasDetectadas.length > 0);
        if (improvedConfidence && hasDetectedTable) {
          effectiveTextInput = rewrittenText;
          analisis = aiAnalysis;
        }
      } catch {
        // Keep deterministic analysis when AI rewrite cannot be evaluated.
      }
    }

    const smartIntelligence = this.queryIntelligenceEngine.buildSmartQuery(effectiveTextInput, schema, analisis);
    const intelligenceWarnings = [
      ...new Set([
        ...(smartIntelligence?.warnings || []).filter(Boolean),
        ...(aiRewrite && effectiveTextInput === String(aiRewrite?.rewritten || '').trim() ? ['Reinterpretación IA local aplicada por baja confianza.'] : []),
      ]),
    ];

    if (smartIntelligence?.error) {
      return {
        exito: false,
        error: smartIntelligence.error,
        warning: smartIntelligence.warning || null,
        warnings: intelligenceWarnings,
        sugerencias: smartIntelligence.suggestions || analisis.ranking.slice(0, 3).map((entry) => entry.table),
        analisis,
        debug: {
          entidadPrincipal: analisis.entidadDetectada || null,
          entidadDetectada: analisis.entidadDetectada || null,
          tokensInput: analisis.tokens,
          tablasEvaluadas: analisis.ranking,
          tablaSeleccionada: analisis.tablaBase || null,
          score: analisis.topScore || 0,
          joins: [],
          columnas: [],
          columnasUsadas: [],
          filtrosAplicados: null,
          tipoConsulta: analisis.tipoConsulta,
          confianza: analisis.confianza || 0,
        },
      };
    }

    if ((!analisis.tablasDetectadas || analisis.tablasDetectadas.length === 0) && !smartIntelligence?.plan?.baseTable) {
      // UUID sin entidad -> guiar al usuario en lugar de devolver tabla incorrecta
      if (_inputUuid) {
        const tableHints = (schema?.tables || []).slice(0, 5).map((t) => `${t} ${_inputUuid}`);
        return {
          exito: false,
          error: `Se detectó un UUID pero no se indicó la entidad. Prueba: "usuario ${_inputUuid}", "log ${_inputUuid}", etc.`,
          sugerencias: tableHints,
          analisis,
          debug: {
            entidadPrincipal: null,
            entidadDetectada: null,
            tokensInput: analisis.tokens,
            tablasEvaluadas: analisis.ranking,
            tablaSeleccionada: null,
            score: 0, joins: [], columnas: [], columnasUsadas: [],
            filtrosAplicados: null,
            tipoConsulta: 'uuid-sin-entidad',
            confianza: 0,
          },
        };
      }
      const fallback = await this.buildFallbackQuery({ ...options, textInput }, 'sin-tablas-detectadas');
      return {
        ...fallback,
        analisis,
        debug: {
          entidadPrincipal: analisis.entidadDetectada || null,
          entidadDetectada: analisis.entidadDetectada || null,
          tokensInput: analisis.tokens,
          tablasEvaluadas: analisis.ranking,
          tablaSeleccionada: null,
          score: 0,
          joins: [],
          columnas: [],
          columnasUsadas: [],
          filtrosAplicados: null,
          tipoConsulta: analisis.tipoConsulta,
          confianza: 0,
        },
      };
    }

    if ((analisis.confianza || 0) < lowConfidenceThreshold && !analisis.tablaBase && !smartIntelligence?.plan?.baseTable) {
      const fallback = await this.buildFallbackQuery({ ...options, textInput }, 'score-bajo');
      return {
        ...fallback,
        analisis,
        sugerencias: analisis.ranking.slice(0, 3).map((entry) => entry.table),
        debug: {
          entidadPrincipal: analisis.entidadDetectada || analisis.tablaBase || null,
          entidadDetectada: analisis.entidadDetectada || analisis.tablaBase || null,
          tokensInput: analisis.tokens,
          tablasEvaluadas: analisis.ranking,
          tablaSeleccionada: analisis.tablaBase,
          score: analisis.topScore || 0,
          joins: [],
          columnas: [],
          columnasUsadas: [],
          filtrosAplicados: null,
          tipoConsulta: analisis.tipoConsulta,
          confianza: analisis.confianza || 0,
        },
      };
    }

    try {
      const baseTable = smartIntelligence?.plan?.baseTable || analisis.tablaBase || analisis.tablasDetectadas[0];
      const baseSchema = schema.schema?.[baseTable];
      const semanticWhere = this.inferSemanticWhere(baseSchema, effectiveTextInput, 't1');
      const shouldJoin = Boolean(smartIntelligence?.plan?.requiresJoin) || (analisis.tieneMultiplesTables && analisis.intencionRelacional);
      const orderedTables = smartIntelligence?.plan?.joinTables?.length
        ? smartIntelligence.plan.joinTables
        : [
            baseTable,
            ...((analisis.tablasMencionadasExplicitas || []).filter((table) => table !== baseTable)),
          ];

      const detectedUuid = _inputUuid;
      const detectedQuoted = _inputQuoted;
      const forceLimit = detectedUuid ? 1 : (options.limit || 50);

      let query;
      if (smartIntelligence?.plan?.requiresAggregation || analisis.agregacionDetectada) {
        query = await this.buildAnalyticQuery(
          orderedTables,
          smartIntelligence?.plan?.aggregationFn || analisis.agregacionDetectada || 'COUNT',
          effectiveTextInput,
          { ...options, limit: forceLimit, orderDirection: analisis.orderDirection, smartPlan: smartIntelligence?.plan, semanticWhere }
        );
      } else if (shouldJoin) {
        query = await this.buildRelationalSelect(orderedTables, {
          ...options,
          limit: forceLimit,
          semanticWhere,
          textInput: effectiveTextInput,
          reservedEntityTokens: orderedTables,
          smartPlan: smartIntelligence?.plan,
          joinTables: orderedTables,
          selectTables: smartIntelligence?.plan?.selectTables || undefined,
        });
      } else {
        // Simple query: extra tokens are VALUE filters, not entity references.
        // Passing them as reservedEntityTokens would block valid WHERE clauses (e.g. role='admin').
        query = await this.buildSimpleSelect(baseTable, { ...options, limit: forceLimit, semanticWhere, textInput: effectiveTextInput });
      }

      // Regla critica: no permitir tablas no mencionadas explicitamente en consultas no-join
      if (!shouldJoin && analisis.tablasMencionadasExplicitas && analisis.tablasMencionadasExplicitas.length > 0) {
        const onlyBaseAllowed = new Set([baseTable]);
        const referencedTables = Array.from(String(query?.sql || '').matchAll(/\b(?:from|join)\s+"?([a-zA-Z0-9_]+)"?/gi)).map((m) => this.normalizeIdentifierName(m[1]));
        const hasUnexpected = referencedTables.some((t) => !onlyBaseAllowed.has(t));
        if (hasUnexpected) {
          query = await this.buildSimpleSelect(baseTable, {
            ...options,
            limit: forceLimit,
            semanticWhere,
            textInput: effectiveTextInput,
            reservedEntityTokens: orderedTables.slice(1),
          });
        }
      }

      const joinDebug = Array.isArray(query?.tablasNoRelacionadas)
        ? (query.tablasNoRelacionadas.length > 0 ? [] : orderedTables.slice(1))
        : [];

      return {
        exito: true,
        analisis,
        query,
        warning: smartIntelligence?.warning || null,
        warnings: intelligenceWarnings,
        mensaje: `Consulta generada automaticamente para: ${analisis.tablasDetectadas.join(', ')}`,
        sugerencias: analisis.ranking.slice(0, 3).map((entry) => entry.table),
        debug: {
          entidadPrincipal: analisis.entidadDetectada || baseTable,
          entidadDetectada: analisis.entidadDetectada || baseTable,
          tokensInput: analisis.tokens,
          tablasEvaluadas: analisis.ranking,
          tablaSeleccionada: baseTable,
          score: analisis.topScore || 0,
          joins: joinDebug,
          columnas: query?.columnasUsadas || query?.columnas || [],
          columnasUsadas: query?.columnasUsadas || query?.columnas || [],
          filtrosAplicados: query?.filtrosAplicados || null,
          valorUUID: detectedUuid || null,
          valorExacto: detectedQuoted[0] || null,
          tipoConsulta: analisis.tipoConsulta,
          confianza: analisis.confianza || 0,
        },
      };
    } catch (error) {
      const fallback = await this.buildFallbackQuery(options, `error-generacion:${error.message}`);
      return {
        ...fallback,
        analisis,
        debug: {
          entidadPrincipal: analisis.entidadDetectada || analisis.tablaBase || null,
          entidadDetectada: analisis.entidadDetectada || analisis.tablaBase || null,
          tokensInput: analisis.tokens,
          tablasEvaluadas: analisis.ranking,
          tablaSeleccionada: analisis.tablaBase,
          score: analisis.topScore || 0,
          joins: [],
          columnas: [],
          columnasUsadas: [],
          filtrosAplicados: null,
          tipoConsulta: analisis.tipoConsulta,
          confianza: analisis.confianza || 0,
        },
      };
    }
  }

  handleEmptyResults(result, context = {}) {
    return this.queryIntelligenceEngine.handleEmptyResults(result, context);
  }

  validateSQL(sql, userRole = 'user') {
    void userRole;

    const rawSql = String(sql || '').trim();
    const normalized = this.normalizeText(rawSql).toUpperCase();

    if (!normalized.startsWith('SELECT')) {
      return {
        valido: false,
        razon: 'Solo se permiten consultas SELECT en modo automatico',
      };
    }

    if (rawSql.includes(';')) {
      return {
        valido: false,
        razon: 'No se permiten multiples sentencias SQL',
      };
    }

    const referencedTables = Array.from(rawSql.matchAll(
      /\b(?:from|join)\s+((?:"[^"]+"|[a-zA-Z0-9_]+)(?:\s*\.\s*(?:"[^"]+"|[a-zA-Z0-9_]+))?)/gi,
    ))
      .flatMap((match) => {
        const qualified = String(match?.[1] || '').trim();
        if (!qualified) return [];

        const parts = qualified
          .split('.')
          .map((part) => String(part || '').trim().replace(/^"|"$/g, ''))
          .filter(Boolean);

        if (parts.length === 0) return [];

        const normalizedQualified = this.normalizeIdentifierName(parts.join('.'));
        const normalizedPlain = this.normalizeIdentifierName(parts[parts.length - 1]);
        return [normalizedQualified, normalizedPlain].filter(Boolean);
      });
    if (this.lastKnownTables.size > 0) {
      const knownTables = new Set(this.lastKnownTables);
      for (const known of [...knownTables]) {
        const plainKnown = String(known || '').split('.').pop() || '';
        if (plainKnown) {
          knownTables.add(plainKnown);
        }
      }

      for (const tableName of referencedTables) {
        if (!knownTables.has(tableName)) {
          return {
            valido: false,
            razon: `Tabla no reconocida en schema actual: ${tableName}`,
          };
        }
      }
    }

    const forbidden = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE', 'GRANT', 'REVOKE'];
    for (const keyword of forbidden) {
      const keywordPattern = new RegExp(`\\b${keyword}\\b`, 'i');
      if (keywordPattern.test(normalized)) {
        return {
          valido: false,
          razon: `Operacion '${keyword}' no permitida en modo automatico`,
        };
      }
    }

    return { valido: true };
  }
}

export default QueryBuilder;
