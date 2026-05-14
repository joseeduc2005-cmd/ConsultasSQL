-- ═══════════════════════════════════════════════════════════════════════════
-- ARTÍCULOS DE PRUEBA — MULTI-BASE  (MySQL · Oracle · PostgreSQL · MongoDB)
-- ═══════════════════════════════════════════════════════════════════════════
\set admin_id '747bd085-6444-49a4-9919-64660808316c'

-- ────────────────────────────────────────────────────────────────────────────
-- 1. MySQL — Estático complejo (tu ejemplo original)
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'MySQL — Análisis de transacciones por institución',
  'GROUP BY multitable, HAVING, DATEDIFF, SUM condicional, comisiones. Query de 100+ líneas lógicas.',
  ARRAY['mysql','finanzas','group-by','having','joins','estatico'],
  'Análisis financiero MySQL',
  'Finanzas', 'Transacciones',
  '## Análisis completo de transacciones por institución y servicio',
  'script',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"mysql_test","sql":"SELECT\n    i.nombre                                    AS institucion,\n    i.tipo                                      AS tipo_institucion,\n    sps.nombre                                  AS servicio,\n    COUNT(op.id)                                AS total_transacciones,\n    SUM(op.monto)                               AS monto_total,\n    AVG(op.monto)                               AS monto_promedio,\n    MAX(op.monto)                               AS transaccion_mayor,\n    MIN(op.monto)                               AS transaccion_menor,\n    SUM(op.monto * sps.comision)                AS comision_generada,\n    COUNT(CASE WHEN op.estado = 1 THEN 1 END)   AS procesadas,\n    COUNT(CASE WHEN op.estado = 0 THEN 1 END)   AS pendientes,\n    MAX(op.fecha_pago)                          AS ultima_transaccion,\n    DATEDIFF(NOW(), MAX(op.fecha_pago))         AS dias_desde_ultima\nFROM ordenes_pago op\nJOIN servicios_punto_venta sps ON op.sale_point_service_id = sps.id\nJOIN instituciones i           ON sps.institucion_id = i.id\nWHERE op.fecha_pago >= DATE_SUB(NOW(), INTERVAL 100 DAY)\n  AND i.activo = 1\n  AND sps.activo = 1\nGROUP BY i.id, i.nombre, i.tipo, sps.id, sps.nombre, sps.comision\nHAVING total_transacciones > 0\n   AND monto_total > 100\nORDER BY monto_total DESC, total_transacciones DESC"}$JSON$,
  'admin', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 2. MySQL — CTE + RANK() salarial por departamento
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'MySQL — Ranking salarial con CTE y Window Functions',
  'WITH anidado: stats por depto + RANK() por salario + diferencia vs promedio + % uso presupuesto',
  ARRAY['mysql','cte','window-functions','rank','salarios','estatico'],
  'Ranking salarial MySQL',
  'RRHH', 'Análisis Salarial',
  '## Ranking salarial por departamento con CTE',
  'script',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"mysql_test","sql":"WITH stats_depto AS (\n    SELECT\n        d.id                                        AS depto_id,\n        d.nombre                                    AS departamento,\n        COUNT(e.id)                                 AS total_empleados,\n        AVG(e.salario)                              AS salario_promedio,\n        MAX(e.salario)                              AS salario_max,\n        MIN(e.salario)                              AS salario_min,\n        SUM(e.salario)                              AS masa_salarial,\n        d.presupuesto,\n        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n    GROUP BY d.id, d.nombre, d.presupuesto\n),\nranking_empleados AS (\n    SELECT\n        e.id,\n        CONCAT(e.nombre, ' ', e.apellido)           AS nombre_completo,\n        e.cargo,\n        e.salario,\n        e.fecha_ingreso,\n        d.nombre                                    AS departamento,\n        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,\n        RANK() OVER (\n            PARTITION BY e.departamento_id\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n)\nSELECT\n    r.nombre_completo,\n    r.cargo,\n    r.departamento,\n    r.salario,\n    ROUND(r.anos_empresa, 1)                        AS anos_empresa,\n    r.ranking_depto,\n    ROUND(s.salario_promedio, 2)                    AS promedio_depto,\n    ROUND(r.salario - s.salario_promedio, 2)        AS diferencia_vs_promedio,\n    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,\n    s.uso_presupuesto_pct\nFROM ranking_empleados r\nJOIN stats_depto s ON r.departamento = s.departamento\nORDER BY r.departamento, r.ranking_depto"}$JSON$,
  'todos', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 3. MySQL — Dinámico: transacciones filtradas
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, campos_formulario, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'MySQL — Transacciones por institución (filtros dinámicos)',
  'Parámetros: institución, fecha_desde, fecha_hasta, monto_min. COALESCE para valores opcionales.',
  ARRAY['mysql','dinamico','filtros','transacciones','between'],
  'Transacciones dinámico MySQL',
  'Finanzas', 'Consultas Dinámicas',
  '## Transacciones filtradas — parámetros opcionales',
  'script',
  '[{"nombre":"institucion","etiqueta":"Institución","tipo":"text","requerido":false,"placeholder":"Ej: Banco del Pichincha"},{"nombre":"fecha_desde","etiqueta":"Fecha desde","tipo":"date","requerido":true},{"nombre":"fecha_hasta","etiqueta":"Fecha hasta","tipo":"date","requerido":true},{"nombre":"monto_min","etiqueta":"Monto mínimo","tipo":"number","requerido":false,"placeholder":"0"}]',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"mysql_test","sql":"SELECT\n    op.id,\n    i.nombre                            AS institucion,\n    sps.nombre                          AS servicio,\n    op.monto,\n    op.estado,\n    op.fecha_pago,\n    DATEDIFF(NOW(), op.fecha_pago)      AS dias_atras,\n    CASE op.estado WHEN 1 THEN 'Procesada' ELSE 'Pendiente' END AS estado_texto\nFROM ordenes_pago op\nJOIN servicios_punto_venta sps ON op.sale_point_service_id = sps.id\nJOIN instituciones i           ON sps.institucion_id = i.id\nWHERE op.fecha_pago BETWEEN {{fecha_desde}} AND {{fecha_hasta}}\n  AND op.monto >= COALESCE({{monto_min}}, 0)\n  AND ({{institucion}} IS NULL OR i.nombre LIKE CONCAT('%', {{institucion}}, '%'))\nORDER BY op.monto DESC"}$JSON$,
  'todos', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 4. MySQL — INSERT dinámico: alta de empleado
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, campos_formulario, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'MySQL — Alta de empleado (INSERT dinámico)',
  'Registra nuevo empleado en MySQL. Todos los campos son variables del formulario.',
  ARRAY['mysql','insert','dinamico','empleados','escritura'],
  'INSERT dinamico MySQL',
  'RRHH', 'Gestión',
  '## Registrar nuevo empleado en MySQL',
  'script',
  '[{"nombre":"nombre","etiqueta":"Nombre","tipo":"text","requerido":true},{"nombre":"apellido","etiqueta":"Apellido","tipo":"text","requerido":true},{"nombre":"email","etiqueta":"Email","tipo":"text","requerido":false},{"nombre":"departamento_id","etiqueta":"ID Departamento","tipo":"number","requerido":true},{"nombre":"cargo","etiqueta":"Cargo","tipo":"text","requerido":true},{"nombre":"salario","etiqueta":"Salario","tipo":"number","requerido":true},{"nombre":"fecha_ingreso","etiqueta":"Fecha ingreso","tipo":"date","requerido":true}]',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"mysql_test","sql":"INSERT INTO empleados (nombre, apellido, email, departamento_id, cargo, salario, fecha_ingreso, activo)\nVALUES ({{nombre}}, {{apellido}}, {{email}}, {{departamento_id}}, {{cargo}}, {{salario}}, {{fecha_ingreso}}, 1)"}$JSON$,
  'admin', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 5. Oracle — ROLLUP + RATIO_TO_REPORT + funciones analíticas
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'Oracle — Ventas con ROLLUP y RATIO_TO_REPORT',
  'ROLLUP para subtotales automáticos, RATIO_TO_REPORT para % participación por categoría y global',
  ARRAY['oracle','rollup','ratio-to-report','analiticas','ventas','estatico'],
  'Analíticas Oracle',
  'Ventas', 'Reportes Oracle',
  '## Ventas con ROLLUP y funciones analíticas Oracle',
  'script',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"oracle_local","sql":"SELECT\n    NVL(p.categoria, 'TOTAL GENERAL')           AS categoria,\n    NVL(p.nombre,    '-- Subtotal --')          AS producto,\n    COUNT(v.id)                                 AS num_ventas,\n    SUM(v.total)                                AS total_ventas,\n    AVG(v.total)                                AS ticket_promedio,\n    MAX(v.total)                                AS venta_max,\n    ROUND(\n        RATIO_TO_REPORT(SUM(v.total))\n        OVER (PARTITION BY p.categoria) * 100, 2\n    )                                           AS participacion_categoria_pct,\n    ROUND(\n        RATIO_TO_REPORT(SUM(v.total))\n        OVER () * 100, 2\n    )                                           AS participacion_total_pct,\n    COUNT(CASE WHEN v.pagado = 1 THEN 1 END)    AS pagadas,\n    COUNT(CASE WHEN v.pagado = 0 THEN 1 END)    AS pendientes,\n    SUM(CASE WHEN v.pagado = 0 THEN v.total ELSE 0 END) AS monto_pendiente\nFROM ventas v\nJOIN productos p ON v.producto_id = p.id\nGROUP BY ROLLUP(p.categoria, p.nombre)\nORDER BY p.categoria NULLS LAST, total_ventas DESC NULLS LAST"}$JSON$,
  'admin', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 6. Oracle — Dinámico: empleados con ROWNUM y RANK
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, campos_formulario, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'Oracle — Empleados por rango salarial con RANK (dinámico)',
  'Subquery con ROWNUM para paginación Oracle. RANK() por departamento. Filtros NVL para valores opcionales.',
  ARRAY['oracle','dinamico','rownum','rank','empleados','paginacion'],
  'Dinamico Oracle empleados',
  'RRHH', 'Consultas Dinámicas',
  '## Empleados filtrados con paginación Oracle',
  'script',
  '[{"nombre":"departamento","etiqueta":"Departamento","tipo":"text","requerido":false,"placeholder":"TI, Finanzas..."},{"nombre":"salario_min","etiqueta":"Salario mínimo","tipo":"number","requerido":false,"placeholder":"0"},{"nombre":"salario_max","etiqueta":"Salario máximo","tipo":"number","requerido":false,"placeholder":"999999"},{"nombre":"solo_activos","etiqueta":"Solo activos (1=Sí)","tipo":"number","requerido":false,"placeholder":"1"}]',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"oracle_local","sql":"SELECT *\nFROM (\n    SELECT\n        e.id,\n        e.nombre || ' ' || e.apellido              AS nombre_completo,\n        e.cargo,\n        e.departamento,\n        e.salario,\n        CASE e.activo WHEN 1 THEN 'Activo' ELSE 'Inactivo' END  AS estado,\n        ROUND(MONTHS_BETWEEN(SYSDATE, e.fecha_ingreso) / 12, 1) AS anos_servicio,\n        RANK() OVER (\n            PARTITION BY e.departamento\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    WHERE ({{departamento}} IS NULL OR e.departamento = {{departamento}})\n      AND e.salario >= NVL({{salario_min}}, 0)\n      AND e.salario <= NVL({{salario_max}}, 999999)\n      AND ({{solo_activos}} IS NULL OR e.activo = {{solo_activos}})\n    ORDER BY e.salario DESC\n)\nWHERE ROWNUM <= 50"}$JSON$,
  'todos', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 7. Oracle — UPDATE dinámico: ajuste salarial masivo
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, campos_formulario, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'Oracle — Ajuste salarial masivo por departamento (UPDATE)',
  'Aplica % de aumento a todos los empleados activos de un departamento. Escribe en Oracle.',
  ARRAY['oracle','update','dinamico','salarios','escritura'],
  'UPDATE Oracle salarios',
  'RRHH', 'Gestión Salarial',
  '## Ajuste masivo de salarios en Oracle',
  'script',
  '[{"nombre":"departamento","etiqueta":"Departamento","tipo":"text","requerido":true,"placeholder":"TI"},{"nombre":"pct_aumento","etiqueta":"% de aumento","tipo":"number","requerido":true,"placeholder":"5"}]',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"oracle_local","sql":"UPDATE empleados\nSET salario = ROUND(salario * (1 + {{pct_aumento}} / 100), 2)\nWHERE departamento = {{departamento}}\n  AND activo = 1"}$JSON$,
  'admin', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 8. PostgreSQL — CTE + Window: auditoría de sesiones
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'PostgreSQL — Auditoría de sesiones con LAG y LEAD',
  'CTE doble: sesiones individuales con LAG/LEAD + métricas por usuario. Detección de patrones sospechosos.',
  ARRAY['postgres','auditoria','cte','lag','lead','window-functions','seguridad','estatico'],
  'Auditoría PostgreSQL',
  'Seguridad', 'Auditoría',
  '## Análisis de actividad de usuarios — window functions avanzadas',
  'script',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"pg_main","sql":"WITH sesiones AS (\n    SELECT\n        username,\n        rol,\n        ip,\n        estado,\n        fecha,\n        LAG(fecha)  OVER (PARTITION BY username ORDER BY fecha) AS login_anterior,\n        LEAD(fecha) OVER (PARTITION BY username ORDER BY fecha) AS siguiente_evento,\n        ROW_NUMBER() OVER (PARTITION BY username ORDER BY fecha DESC) AS nro_evento\n    FROM login_audit\n    WHERE fecha >= NOW() - INTERVAL '30 days'\n),\nmetricas AS (\n    SELECT\n        username,\n        COUNT(*)                                            AS total_eventos,\n        COUNT(CASE WHEN estado = 'ok'     THEN 1 END)      AS logins_ok,\n        COUNT(CASE WHEN estado = 'fail'   THEN 1 END)      AS logins_fail,\n        COUNT(CASE WHEN estado = 'logout' THEN 1 END)      AS logouts,\n        MIN(fecha)                                          AS primer_evento,\n        MAX(fecha)                                          AS ultimo_evento,\n        COUNT(DISTINCT ip)                                  AS ips_distintas,\n        ROUND(\n            COUNT(CASE WHEN estado = 'fail' THEN 1 END)::numeric\n            / NULLIF(COUNT(*), 0) * 100, 1\n        )                                                   AS pct_fallos\n    FROM login_audit\n    WHERE fecha >= NOW() - INTERVAL '30 days'\n    GROUP BY username\n)\nSELECT\n    m.username,\n    m.total_eventos,\n    m.logins_ok,\n    m.logins_fail,\n    m.logouts,\n    m.ips_distintas,\n    m.pct_fallos,\n    ROUND(EXTRACT(EPOCH FROM (m.ultimo_evento - m.primer_evento)) / 3600, 1) AS horas_activo,\n    m.primer_evento,\n    m.ultimo_evento,\n    CASE\n        WHEN m.pct_fallos > 50   THEN 'ALERTA: muchos fallos'\n        WHEN m.ips_distintas > 3 THEN 'AVISO: multiples IPs'\n        ELSE 'Normal'\n    END                                                     AS alerta\nFROM metricas m\nORDER BY m.pct_fallos DESC, m.total_eventos DESC"}$JSON$,
  'admin', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 9. PostgreSQL — Dinámico: historial de ejecuciones con PERCENT_RANK
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, campos_formulario, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'PostgreSQL — Historial de ejecuciones del sistema (dinámico)',
  'query_history filtrado por usuario, estado, días y duración. PERCENT_RANK para clasificar velocidad.',
  ARRAY['postgres','dinamico','query-history','percent-rank','monitoreo'],
  'Historial dinámico PostgreSQL',
  'Administración', 'Monitoreo',
  '## Historial de ejecuciones con filtros y percentil de velocidad',
  'script',
  '[{"nombre":"usuario","etiqueta":"Usuario","tipo":"text","requerido":false,"placeholder":"Todos"},{"nombre":"estado","etiqueta":"Estado","tipo":"text","requerido":false,"placeholder":"ok o error"},{"nombre":"dias","etiqueta":"Últimos N días","tipo":"number","requerido":false,"placeholder":"7"},{"nombre":"min_ms","etiqueta":"Duración mínima (ms)","tipo":"number","requerido":false,"placeholder":"0"}]',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"pg_main","sql":"SELECT\n    qh.id,\n    qh.username,\n    qh.role,\n    LEFT(qh.query_text, 100)            AS consulta,\n    qh.execution_ms,\n    qh.row_count,\n    qh.status,\n    qh.was_cached,\n    qh.created_at,\n    CASE\n        WHEN qh.execution_ms > 5000 THEN 'LENTA'\n        WHEN qh.execution_ms > 1000 THEN 'MEDIA'\n        ELSE 'RAPIDA'\n    END                                 AS velocidad,\n    ROUND(PERCENT_RANK() OVER (ORDER BY qh.execution_ms) * 100, 1) AS percentil\nFROM query_history qh\nWHERE ({{usuario}} IS NULL OR qh.username = {{usuario}})\n  AND ({{estado}}  IS NULL OR qh.status   = {{estado}})\n  AND qh.created_at >= NOW() - (COALESCE({{dias}}, 7) || ' days')::INTERVAL\n  AND qh.execution_ms >= COALESCE({{min_ms}}, 0)\nORDER BY qh.created_at DESC\nLIMIT 100"}$JSON$,
  'todos', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 10. MongoDB — Pipeline de agregación complejo: ventas por vendedor
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'MongoDB — Pipeline ventas: doble $group + $addFields',
  'Aggregation pipeline: $addFields fecha, doble $group (por vendedor+producto → por vendedor), % cobrado, sort',
  ARRAY['mongodb','aggregate','pipeline','group','addfields','estatico'],
  'Pipeline MongoDB ventas',
  'Ventas', 'Análisis MongoDB',
  '## Pipeline de agregación: rendimiento por vendedor',
  'script',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"mongo_local","collection":"ventas","operation":"aggregate","pipeline":"[{\"$addFields\":{\"mes\":{\"$month\":\"$fecha\"},\"anio\":{\"$year\":\"$fecha\"}}},{\"$group\":{\"_id\":{\"vendedor\":\"$vendedor\",\"producto\":\"$producto_codigo\",\"pagado\":\"$pagado\"},\"total_ventas\":{\"$sum\":\"$total\"},\"total_unidades\":{\"$sum\":\"$cantidad\"},\"num_operaciones\":{\"$sum\":1},\"ticket_promedio\":{\"$avg\":\"$total\"},\"venta_max\":{\"$max\":\"$total\"}}},{\"$group\":{\"_id\":\"$_id.vendedor\",\"total_facturado\":{\"$sum\":\"$total_ventas\"},\"total_operaciones\":{\"$sum\":\"$num_operaciones\"},\"monto_cobrado\":{\"$sum\":{\"$cond\":[{\"$eq\":[\"$_id.pagado\",true]},\"$total_ventas\",0]}},\"monto_pendiente\":{\"$sum\":{\"$cond\":[{\"$eq\":[\"$_id.pagado\",false]},\"$total_ventas\",0]}},\"productos\":{\"$addToSet\":\"$_id.producto\"}}},{\"$addFields\":{\"pct_cobrado\":{\"$round\":[{\"$multiply\":[{\"$divide\":[\"$monto_cobrado\",{\"$max\":[\"$total_facturado\",1]}]},100]},1]},\"num_productos\":{\"$size\":\"$productos\"}}},{\"$sort\":{\"total_facturado\":-1}},{\"$project\":{\"_id\":0,\"vendedor\":\"$_id\",\"total_facturado\":1,\"total_operaciones\":1,\"monto_cobrado\":1,\"monto_pendiente\":1,\"pct_cobrado\":1,\"num_productos\":1}}]"}$JSON$,
  'admin', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- 11. MongoDB — Dinámico: find empleados con filtros
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO knowledge_base
  (titulo, descripcion, tags, contenido, categoria, subcategoria,
   contenido_md, tipo_solucion, campos_formulario, script_json, visible_para, creado_por, creado_por_username)
VALUES (
  'MongoDB — Empleados por departamento y salario (dinámico)',
  'find() con $gte en salario y filtro por departamento. Parámetros opcionales dinámicos.',
  ARRAY['mongodb','dinamico','find','empleados','gte'],
  'find dinamico MongoDB',
  'RRHH', 'Consultas Dinámicas',
  '## Búsqueda de empleados en MongoDB con filtros',
  'script',
  '[{"nombre":"departamento","etiqueta":"Departamento","tipo":"text","requerido":false,"placeholder":"TI, Finanzas, RRHH"},{"nombre":"salario_min","etiqueta":"Salario mínimo","tipo":"number","requerido":false,"placeholder":"0"}]',
  $JSON${"modo":"script","origen":"manual-sql","database_id":"mongo_local","collection":"empleados","operation":"find","filter":"{\"departamento\":{{departamento}},\"salario\":{\"$gte\":{{salario_min}}}}","sort":"{\"salario\":-1}","limit":50}$JSON$,
  'todos', :'admin_id', 'admin'
);

-- ────────────────────────────────────────────────────────────────────────────
-- Verificar
-- ────────────────────────────────────────────────────────────────────────────
SELECT
    categoria,
    COUNT(*) AS total,
    STRING_AGG(titulo, ' | ') AS articulos
FROM knowledge_base
WHERE creado_por = '747bd085-6444-49a4-9919-64660808316c'
  AND fecha >= NOW() - INTERVAL '10 minutes'
GROUP BY categoria
ORDER BY categoria;
