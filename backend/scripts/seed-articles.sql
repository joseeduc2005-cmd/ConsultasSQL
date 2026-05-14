-- ═══════════════════════════════════════════════════════════════════════════
-- ARTÍCULOS DE PRUEBA — SISTEMA MULTI-BASE
-- MySQL, Oracle, PostgreSQL, MongoDB — estático y dinámico
-- ═══════════════════════════════════════════════════════════════════════════

-- ─── MYSQL: Estático complejo (tu ejemplo + más) ────────────────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, script_json, visible_para, creado_por_username)
VALUES (
  'MySQL — Análisis completo de transacciones por institución',
  'Resumen financiero agrupado por institución y servicio con métricas de comisión, estado y tiempo',
  ARRAY['mysql','finanzas','transacciones','group-by','having'],
  'Finanzas', 'Análisis',
  '## Análisis de transacciones por institución y servicio',
  'script', 'manual-sql',
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'mysql_test',
    'sql', 'SELECT
    i.nombre                                    AS institucion,
    i.tipo                                      AS tipo_institucion,
    sps.nombre                                  AS servicio,
    COUNT(op.id)                                AS total_transacciones,
    SUM(op.monto)                               AS monto_total,
    AVG(op.monto)                               AS monto_promedio,
    MAX(op.monto)                               AS transaccion_mayor,
    MIN(op.monto)                               AS transaccion_menor,
    SUM(op.monto * sps.comision)                AS comision_generada,
    COUNT(CASE WHEN op.estado = 1 THEN 1 END)   AS procesadas,
    COUNT(CASE WHEN op.estado = 0 THEN 1 END)   AS pendientes,
    MAX(op.fecha_pago)                          AS ultima_transaccion,
    DATEDIFF(NOW(), MAX(op.fecha_pago))         AS dias_desde_ultima
FROM ordenes_pago op
JOIN servicios_punto_venta sps ON op.sale_point_service_id = sps.id
JOIN instituciones i           ON sps.institucion_id = i.id
WHERE op.fecha_pago >= DATE_SUB(NOW(), INTERVAL 100 DAY)
  AND i.activo = 1
  AND sps.activo = 1
GROUP BY i.id, i.nombre, i.tipo, sps.id, sps.nombre, sps.comision
HAVING total_transacciones > 0
   AND monto_total > 100
ORDER BY monto_total DESC, total_transacciones DESC'
  ),
  'admin', 'admin'
);

-- ─── MYSQL: Dinámico — filtro por institución y rango de fechas ─────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, campos_formulario, script_json, visible_para, creado_por_username)
VALUES (
  'MySQL — Transacciones por institución (dinámico)',
  'Filtra por nombre de institución y rango de fechas. Variables: institucion, fecha_desde, fecha_hasta, monto_min',
  ARRAY['mysql','dinamico','filtros','transacciones'],
  'Finanzas', 'Consultas Dinámicas',
  '## Transacciones filtradas por institución y período',
  'script', 'manual-sql',
  '[
    {"nombre":"institucion","etiqueta":"Institución","tipo":"text","requerido":false,"placeholder":"Ej: Banco del Pichincha"},
    {"nombre":"fecha_desde","etiqueta":"Fecha desde","tipo":"date","requerido":true},
    {"nombre":"fecha_hasta","etiqueta":"Fecha hasta","tipo":"date","requerido":true},
    {"nombre":"monto_min","etiqueta":"Monto mínimo","tipo":"number","requerido":false,"placeholder":"0"}
  ]'::jsonb,
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'mysql_test',
    'sql', 'SELECT
    op.id,
    i.nombre                            AS institucion,
    sps.nombre                          AS servicio,
    op.monto,
    op.estado,
    op.fecha_pago,
    DATEDIFF(NOW(), op.fecha_pago)      AS dias_atras,
    CASE op.estado WHEN 1 THEN ''Procesada'' ELSE ''Pendiente'' END AS estado_texto
FROM ordenes_pago op
JOIN servicios_punto_venta sps ON op.sale_point_service_id = sps.id
JOIN instituciones i           ON sps.institucion_id = i.id
WHERE op.fecha_pago BETWEEN {{fecha_desde}} AND {{fecha_hasta}}
  AND op.monto >= COALESCE({{monto_min}}, 0)
  AND ({{institucion}} IS NULL OR i.nombre LIKE CONCAT(''%'', {{institucion}}, ''%''))
ORDER BY op.monto DESC'
  ),
  'todos', 'admin'
);

-- ─── MYSQL: CTE + Ranking de empleados por salario ─────────────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, script_json, visible_para, creado_por_username)
VALUES (
  'MySQL — Ranking salarial por departamento (CTE)',
  'Usa WITH para calcular promedio departamental y comparar cada empleado contra su departamento',
  ARRAY['mysql','cte','ranking','window-functions','empleados'],
  'RRHH', 'Análisis Salarial',
  '## Ranking salarial con CTE y métricas por departamento',
  'script', 'manual-sql',
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'mysql_test',
    'sql', 'WITH stats_depto AS (
    SELECT
        d.id                                        AS depto_id,
        d.nombre                                    AS departamento,
        COUNT(e.id)                                 AS total_empleados,
        AVG(e.salario)                              AS salario_promedio,
        MAX(e.salario)                              AS salario_max,
        MIN(e.salario)                              AS salario_min,
        SUM(e.salario)                              AS masa_salarial,
        d.presupuesto,
        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct
    FROM empleados e
    JOIN departamentos d ON e.departamento_id = d.id
    WHERE e.activo = 1
    GROUP BY d.id, d.nombre, d.presupuesto
),
ranking_empleados AS (
    SELECT
        e.id,
        CONCAT(e.nombre, '' '', e.apellido)         AS nombre_completo,
        e.cargo,
        e.salario,
        e.fecha_ingreso,
        d.nombre                                    AS departamento,
        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,
        RANK() OVER (
            PARTITION BY e.departamento_id
            ORDER BY e.salario DESC
        )                                           AS ranking_depto
    FROM empleados e
    JOIN departamentos d ON e.departamento_id = d.id
    WHERE e.activo = 1
)
SELECT
    r.nombre_completo,
    r.cargo,
    r.departamento,
    r.salario,
    ROUND(r.anos_empresa, 1)                        AS anos_empresa,
    r.ranking_depto,
    s.salario_promedio,
    ROUND((r.salario - s.salario_promedio), 2)      AS diferencia_vs_promedio,
    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,
    s.uso_presupuesto_pct
FROM ranking_empleados r
JOIN stats_depto s ON r.departamento = s.departamento
ORDER BY r.departamento, r.ranking_depto'
  ),
  'admin', 'admin'
);

-- ─── ORACLE: Estático — analíticas con PARTITION BY y ROLLUP ────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, script_json, visible_para, creado_por_username)
VALUES (
  'Oracle — Ventas con ROLLUP y funciones analíticas',
  'ROLLUP para subtotales, LAG para comparativa mes anterior, RATIO_TO_REPORT para participación',
  ARRAY['oracle','rollup','analiticas','lag','window'],
  'Ventas', 'Reportes Oracle',
  '## Análisis de ventas con funciones analíticas Oracle',
  'script', 'manual-sql',
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'oracle_local',
    'sql', 'SELECT
    NVL(p.categoria, ''TOTAL GENERAL'')         AS categoria,
    NVL(p.nombre,    ''-- Subtotal --'')        AS producto,
    COUNT(v.id)                                 AS num_ventas,
    SUM(v.total)                                AS total_ventas,
    AVG(v.total)                                AS ticket_promedio,
    MAX(v.total)                                AS venta_max,
    ROUND(
        RATIO_TO_REPORT(SUM(v.total))
        OVER (PARTITION BY p.categoria) * 100, 2
    )                                           AS participacion_categoria_pct,
    ROUND(
        RATIO_TO_REPORT(SUM(v.total))
        OVER () * 100, 2
    )                                           AS participacion_total_pct,
    COUNT(CASE WHEN v.pagado = 1 THEN 1 END)    AS pagadas,
    COUNT(CASE WHEN v.pagado = 0 THEN 1 END)    AS pendientes,
    SUM(CASE WHEN v.pagado = 0 THEN v.total ELSE 0 END) AS monto_pendiente
FROM ventas v
JOIN productos p ON v.producto_id = p.id
GROUP BY ROLLUP(p.categoria, p.nombre)
ORDER BY
    p.categoria NULLS LAST,
    total_ventas DESC NULLS LAST'
  ),
  'admin', 'admin'
);

-- ─── ORACLE: Dinámico — empleados filtrados con ROWNUM ──────────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, campos_formulario, script_json, visible_para, creado_por_username)
VALUES (
  'Oracle — Empleados por departamento y rango salarial (dinámico)',
  'Busca empleados filtrando por departamento, salario mínimo/máximo y estado activo. Paginación con ROWNUM',
  ARRAY['oracle','dinamico','empleados','paginacion'],
  'RRHH', 'Consultas Dinámicas',
  '## Búsqueda de empleados con filtros dinámicos',
  'script', 'manual-sql',
  '[
    {"nombre":"departamento","etiqueta":"Departamento","tipo":"text","requerido":false,"placeholder":"Ej: TI"},
    {"nombre":"salario_min","etiqueta":"Salario mínimo","tipo":"number","requerido":false,"placeholder":"0"},
    {"nombre":"salario_max","etiqueta":"Salario máximo","tipo":"number","requerido":false,"placeholder":"999999"},
    {"nombre":"solo_activos","etiqueta":"Solo activos (1=Sí, 0=Todos)","tipo":"number","requerido":false,"placeholder":"1"}
  ]'::jsonb,
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'oracle_local',
    'sql', 'SELECT *
FROM (
    SELECT
        e.id,
        e.nombre || '' '' || e.apellido        AS nombre_completo,
        e.cargo,
        e.departamento,
        e.salario,
        CASE e.activo WHEN 1 THEN ''Activo'' ELSE ''Inactivo'' END AS estado,
        ROUND(MONTHS_BETWEEN(SYSDATE, e.fecha_ingreso) / 12, 1)   AS anos_servicio,
        RANK() OVER (
            PARTITION BY e.departamento
            ORDER BY e.salario DESC
        )                                       AS ranking_depto,
        ROWNUM AS rn
    FROM empleados e
    WHERE ({{departamento}} IS NULL OR e.departamento = {{departamento}})
      AND e.salario >= NVL({{salario_min}}, 0)
      AND e.salario <= NVL({{salario_max}}, 999999)
      AND ({{solo_activos}} IS NULL OR e.activo = {{solo_activos}})
    ORDER BY e.salario DESC
)
WHERE rn <= 50'
  ),
  'todos', 'admin'
);

-- ─── ORACLE: Multi-paso — resumen ejecutivo completo ─────────────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, script_json, visible_para, creado_por_username)
VALUES (
  'Oracle — Reporte ejecutivo multi-paso',
  'Paso 1: KPIs globales. Paso 2: Top productos. Paso 3: Empleados sobre promedio salarial',
  ARRAY['oracle','multi-paso','ejecutivo','kpi'],
  'Gerencia', 'Reportes Ejecutivos',
  '## Reporte ejecutivo completo en 3 pasos',
  'script', 'manual-sql',
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'oracle_local',
    'pasos', jsonb_build_array(
      jsonb_build_object(
        'titulo', 'KPIs Globales',
        'sql', 'SELECT
    COUNT(DISTINCT e.id)                                AS total_empleados,
    COUNT(DISTINCT d.id)                                AS total_departamentos,
    ROUND(AVG(e.salario), 2)                            AS salario_promedio_global,
    SUM(e.salario)                                      AS masa_salarial_total,
    (SELECT COUNT(*) FROM ventas WHERE pagado = 0)      AS ventas_pendientes_cobro,
    (SELECT SUM(total) FROM ventas WHERE pagado = 0)    AS monto_pendiente_cobro,
    (SELECT COUNT(*) FROM productos WHERE stock < 10)   AS productos_stock_bajo
FROM empleados e
JOIN departamentos d ON e.departamento = d.nombre
WHERE e.activo = 1'
      ),
      jsonb_build_object(
        'titulo', 'Top 5 productos más vendidos',
        'sql', 'SELECT
    p.nombre                            AS producto,
    p.categoria,
    p.precio                            AS precio_unitario,
    SUM(v.cantidad)                     AS unidades_vendidas,
    SUM(v.total)                        AS ingresos_totales,
    COUNT(v.id)                         AS num_transacciones,
    ROUND(AVG(v.total), 2)              AS ticket_promedio,
    p.stock                             AS stock_actual,
    CASE
        WHEN p.stock < 5  THEN ''CRITICO''
        WHEN p.stock < 15 THEN ''BAJO''
        ELSE ''OK''
    END                                 AS nivel_stock
FROM ventas v
JOIN productos p ON v.producto_id = p.id
GROUP BY p.id, p.nombre, p.categoria, p.precio, p.stock
ORDER BY ingresos_totales DESC
FETCH FIRST 5 ROWS ONLY'
      ),
      jsonb_build_object(
        'titulo', 'Empleados sobre promedio salarial',
        'sql', 'SELECT
    e.nombre || '' '' || e.apellido     AS empleado,
    e.cargo,
    e.departamento,
    e.salario,
    ROUND(e.salario - global_avg.promedio, 2)   AS sobre_promedio,
    ROUND((e.salario / global_avg.promedio - 1) * 100, 1) AS pct_sobre_promedio
FROM empleados e
CROSS JOIN (
    SELECT ROUND(AVG(salario), 2) AS promedio FROM empleados WHERE activo = 1
) global_avg
WHERE e.activo = 1
  AND e.salario > global_avg.promedio
ORDER BY e.salario DESC'
      )
    )
  ),
  'admin', 'admin'
);

-- ─── POSTGRESQL: CTE recursiva + Window Functions avanzadas ─────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, script_json, visible_para, creado_por_username)
VALUES (
  'PostgreSQL — Auditoría: actividad de usuarios con window functions',
  'Análisis de login_audit con LAG, LEAD, particiones por usuario, detección de sesiones largas',
  ARRAY['postgres','auditoria','window-functions','lag','lead','sesiones'],
  'Seguridad', 'Auditoría',
  '## Análisis de actividad de usuarios — auditoría avanzada',
  'script', 'manual-sql',
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'pg_main',
    'sql', 'WITH sesiones AS (
    SELECT
        username,
        rol,
        ip,
        estado,
        fecha,
        LAG(fecha)  OVER (PARTITION BY username ORDER BY fecha) AS login_anterior,
        LEAD(fecha) OVER (PARTITION BY username ORDER BY fecha) AS siguiente_evento,
        ROW_NUMBER() OVER (PARTITION BY username ORDER BY fecha DESC) AS nro_evento
    FROM login_audit
    WHERE fecha >= NOW() - INTERVAL ''30 days''
),
metricas AS (
    SELECT
        username,
        COUNT(*)                                            AS total_eventos,
        COUNT(CASE WHEN estado = ''ok''     THEN 1 END)   AS logins_ok,
        COUNT(CASE WHEN estado = ''fail''   THEN 1 END)   AS logins_fail,
        COUNT(CASE WHEN estado = ''logout'' THEN 1 END)   AS logouts,
        MIN(fecha)                                          AS primer_evento,
        MAX(fecha)                                          AS ultimo_evento,
        COUNT(DISTINCT ip)                                  AS ips_distintas,
        ROUND(
            COUNT(CASE WHEN estado = ''fail'' THEN 1 END)::numeric
            / NULLIF(COUNT(*), 0) * 100, 1
        )                                                   AS pct_fallos
    FROM login_audit
    WHERE fecha >= NOW() - INTERVAL ''30 days''
    GROUP BY username
)
SELECT
    m.username,
    m.total_eventos,
    m.logins_ok,
    m.logins_fail,
    m.logouts,
    m.ips_distintas,
    m.pct_fallos,
    EXTRACT(EPOCH FROM (m.ultimo_evento - m.primer_evento)) / 3600
                                                            AS horas_activo_periodo,
    m.primer_evento,
    m.ultimo_evento,
    CASE
        WHEN m.pct_fallos > 50  THEN ''ALERTA: muchos fallos''
        WHEN m.ips_distintas > 3 THEN ''AVISO: múltiples IPs''
        ELSE ''Normal''
    END                                                     AS alerta
FROM metricas m
ORDER BY m.pct_fallos DESC, m.total_eventos DESC'
  ),
  'admin', 'admin'
);

-- ─── POSTGRESQL: Dinámico — historial de ejecuciones filtrado ───────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, campos_formulario, script_json, visible_para, creado_por_username)
VALUES (
  'PostgreSQL — Historial de ejecuciones del sistema (dinámico)',
  'Consulta query_history con filtros por usuario, base de datos, estado y período',
  ARRAY['postgres','dinamico','historial','query-history'],
  'Administración', 'Monitoreo',
  '## Historial de ejecuciones con filtros',
  'script', 'manual-sql',
  '[
    {"nombre":"usuario","etiqueta":"Usuario","tipo":"text","requerido":false,"placeholder":"Todos los usuarios"},
    {"nombre":"estado","etiqueta":"Estado (ok/error)","tipo":"text","requerido":false,"placeholder":"ok o error"},
    {"nombre":"dias","etiqueta":"Últimos N días","tipo":"number","requerido":false,"placeholder":"7"},
    {"nombre":"min_ms","etiqueta":"Duración mínima (ms)","tipo":"number","requerido":false,"placeholder":"0"}
  ]'::jsonb,
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'pg_main',
    'sql', 'SELECT
    qh.id,
    qh.username,
    qh.role,
    LEFT(qh.query_text, 120)            AS consulta,
    qh.execution_ms,
    qh.row_count,
    qh.status,
    qh.was_cached,
    qh.created_at,
    CASE
        WHEN qh.execution_ms > 5000  THEN ''LENTA''
        WHEN qh.execution_ms > 1000  THEN ''MEDIA''
        ELSE ''RAPIDA''
    END                                 AS velocidad,
    PERCENT_RANK() OVER (
        ORDER BY qh.execution_ms
    ) * 100                             AS percentil_velocidad
FROM query_history qh
WHERE ({{usuario}} IS NULL OR qh.username = {{usuario}})
  AND ({{estado}}  IS NULL OR qh.status = {{estado}})
  AND qh.created_at >= NOW() - (COALESCE({{dias}}, 7) || '' days'')::INTERVAL
  AND qh.execution_ms >= COALESCE({{min_ms}}, 0)
ORDER BY qh.created_at DESC
LIMIT 100'
  ),
  'todos', 'admin'
);

-- ─── MONGODB: Estático — agregación pipeline complejo ───────────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, script_json, visible_para, creado_por_username)
VALUES (
  'MongoDB — Pipeline de ventas por vendedor y producto',
  'Agregación con $lookup (join), $group, $sort, $project, $addFields para análisis completo de ventas',
  ARRAY['mongodb','aggregation','pipeline','lookup','group'],
  'Ventas', 'Análisis MongoDB',
  '## Pipeline de agregación: ventas por vendedor',
  'script', 'manual-sql',
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'mongo_local',
    'collection', 'ventas',
    'operation', 'aggregate',
    'pipeline', '[
      { "$addFields": {
          "mes": { "$month": "$fecha" },
          "anio": { "$year": "$fecha" }
      }},
      { "$group": {
          "_id": {
            "vendedor": "$vendedor",
            "producto": "$producto_codigo",
            "pagado": "$pagado"
          },
          "total_ventas": { "$sum": "$total" },
          "total_unidades": { "$sum": "$cantidad" },
          "num_operaciones": { "$sum": 1 },
          "ticket_promedio": { "$avg": "$total" },
          "venta_max": { "$max": "$total" },
          "venta_min": { "$min": "$total" }
      }},
      { "$group": {
          "_id": "$_id.vendedor",
          "total_facturado": { "$sum": "$total_ventas" },
          "total_operaciones": { "$sum": "$num_operaciones" },
          "operaciones_pagadas": {
            "$sum": { "$cond": [{ "$eq": ["$_id.pagado", true] }, "$num_operaciones", 0] }
          },
          "monto_cobrado": {
            "$sum": { "$cond": [{ "$eq": ["$_id.pagado", true] }, "$total_ventas", 0] }
          },
          "monto_pendiente": {
            "$sum": { "$cond": [{ "$eq": ["$_id.pagado", false] }, "$total_ventas", 0] }
          },
          "productos_distintos": { "$addToSet": "$_id.producto" }
      }},
      { "$addFields": {
          "pct_cobrado": {
            "$round": [{ "$multiply": [{ "$divide": ["$monto_cobrado", { "$max": ["$total_facturado", 1] }] }, 100] }, 1]
          },
          "num_productos": { "$size": "$productos_distintos" }
      }},
      { "$sort": { "total_facturado": -1 } },
      { "$project": {
          "_id": 0,
          "vendedor": "$_id",
          "total_facturado": 1,
          "total_operaciones": 1,
          "operaciones_pagadas": 1,
          "monto_cobrado": 1,
          "monto_pendiente": 1,
          "pct_cobrado": 1,
          "num_productos": 1
      }}
    ]'
  ),
  'admin', 'admin'
);

-- ─── MONGODB: Dinámico — búsqueda de empleados con filtro ───────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, campos_formulario, script_json, visible_para, creado_por_username)
VALUES (
  'MongoDB — Empleados por departamento y salario (dinámico)',
  'find() filtrado por departamento y salario mínimo. Parámetros dinámicos.',
  ARRAY['mongodb','dinamico','find','empleados'],
  'RRHH', 'Consultas Dinámicas',
  '## Búsqueda de empleados en MongoDB',
  'script', 'manual-sql',
  '[
    {"nombre":"departamento","etiqueta":"Departamento","tipo":"text","requerido":false,"placeholder":"TI, Finanzas, RRHH..."},
    {"nombre":"salario_min","etiqueta":"Salario mínimo","tipo":"number","requerido":false,"placeholder":"0"},
    {"nombre":"solo_activos","etiqueta":"Solo activos","tipo":"text","requerido":false,"placeholder":"true"}
  ]'::jsonb,
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'mongo_local',
    'collection', 'empleados',
    'operation', 'find',
    'filter', '{ "departamento": {{departamento}}, "salario": { "$gte": {{salario_min}} }, "activo": {{solo_activos}} }',
    'sort', '{ "salario": -1 }',
    'limit', 50
  ),
  'todos', 'admin'
);

-- ─── MYSQL: INSERT dinámico — alta de empleado ──────────────────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, campos_formulario, script_json, visible_para, creado_por_username)
VALUES (
  'MySQL — Alta de nuevo empleado (INSERT dinámico)',
  'Registra un nuevo empleado en MySQL. Todos los campos son variables del formulario.',
  ARRAY['mysql','insert','dinamico','empleados','escritura'],
  'RRHH', 'Gestión',
  '## Registrar nuevo empleado',
  'script', 'manual-sql',
  '[
    {"nombre":"nombre","etiqueta":"Nombre","tipo":"text","requerido":true},
    {"nombre":"apellido","etiqueta":"Apellido","tipo":"text","requerido":true},
    {"nombre":"email","etiqueta":"Email","tipo":"text","requerido":false},
    {"nombre":"departamento_id","etiqueta":"ID Departamento","tipo":"number","requerido":true},
    {"nombre":"cargo","etiqueta":"Cargo","tipo":"text","requerido":true},
    {"nombre":"salario","etiqueta":"Salario","tipo":"number","requerido":true},
    {"nombre":"fecha_ingreso","etiqueta":"Fecha ingreso","tipo":"date","requerido":true}
  ]'::jsonb,
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'mysql_test',
    'sql', 'INSERT INTO empleados (nombre, apellido, email, departamento_id, cargo, salario, fecha_ingreso, activo)
VALUES ({{nombre}}, {{apellido}}, {{email}}, {{departamento_id}}, {{cargo}}, {{salario}}, {{fecha_ingreso}}, 1)'
  ),
  'admin', 'admin'
);

-- ─── ORACLE: UPDATE dinámico — actualizar salario ───────────────────────────
INSERT INTO knowledge_base (titulo, descripcion, tags, categoria, subcategoria, contenido_md, tipo_solucion, accion_script, campos_formulario, script_json, visible_para, creado_por_username)
VALUES (
  'Oracle — Ajuste salarial por departamento (UPDATE dinámico)',
  'Aplica un porcentaje de aumento a todos los empleados activos de un departamento',
  ARRAY['oracle','update','dinamico','salarios','escritura'],
  'RRHH', 'Gestión Salarial',
  '## Ajuste masivo de salarios por departamento',
  'script', 'manual-sql',
  '[
    {"nombre":"departamento","etiqueta":"Departamento","tipo":"text","requerido":true,"placeholder":"TI"},
    {"nombre":"pct_aumento","etiqueta":"% de aumento","tipo":"number","requerido":true,"placeholder":"5"}
  ]'::jsonb,
  jsonb_build_object(
    'modo', 'script',
    'origen', 'manual-sql',
    'database_id', 'oracle_local',
    'sql', 'UPDATE empleados
SET salario = ROUND(salario * (1 + {{pct_aumento}} / 100), 2)
WHERE departamento = {{departamento}}
  AND activo = 1'
  ),
  'admin', 'admin'
);

SELECT COUNT(*) AS articulos_creados FROM knowledge_base WHERE creado_por_username = 'admin' AND fecha >= NOW() - INTERVAL '5 minutes';
