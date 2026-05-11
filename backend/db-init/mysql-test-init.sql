-- ═══════════════════════════════════════════════════════════════════
--  MySQL Test Database — motor SQL multi-base
--  Base de datos de pruebas para validar:
--    ✅ SELECT con y sin parámetros
--    ✅ INSERT / UPDATE / DELETE con parámetros ($1→?)
--    ✅ CALL stored procedures
--    ✅ SET de variables MySQL
--    ✅ Detección automática de tablas y routing
-- ═══════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS testdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE testdb;

-- Permisos completos para testuser (incluyendo SHOW en information_schema)
GRANT ALL PRIVILEGES ON testdb.* TO 'testuser'@'%';
FLUSH PRIVILEGES;

-- ── Tabla: departamentos ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS departamentos (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  nombre     VARCHAR(100) NOT NULL,
  ubicacion  VARCHAR(100),
  presupuesto DECIMAL(12,2) DEFAULT 0,
  activo     TINYINT(1) DEFAULT 1,
  creado_en  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Tabla: empleados ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS empleados (
  id              INT AUTO_INCREMENT PRIMARY KEY,
  cedula          VARCHAR(20) UNIQUE NOT NULL,
  nombre          VARCHAR(100) NOT NULL,
  apellido        VARCHAR(100) NOT NULL,
  email           VARCHAR(150),
  departamento_id INT,
  cargo           VARCHAR(100),
  salario         DECIMAL(10,2) DEFAULT 0,
  fecha_ingreso   DATE,
  activo          TINYINT(1) DEFAULT 1,
  creado_en       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (departamento_id) REFERENCES departamentos(id)
);

-- ── Tabla: productos ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS productos (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  codigo      VARCHAR(50) UNIQUE NOT NULL,
  nombre      VARCHAR(200) NOT NULL,
  categoria   VARCHAR(100),
  precio      DECIMAL(10,2) NOT NULL,
  stock       INT DEFAULT 0,
  activo      TINYINT(1) DEFAULT 1,
  creado_en   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Tabla: pedidos ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pedidos (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  empleado_id   INT,
  producto_id   INT,
  cantidad      INT NOT NULL DEFAULT 1,
  precio_total  DECIMAL(10,2),
  estado        ENUM('pendiente','procesado','cancelado') DEFAULT 'pendiente',
  creado_en     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (empleado_id) REFERENCES empleados(id),
  FOREIGN KEY (producto_id) REFERENCES productos(id)
);

-- ── Datos: departamentos ─────────────────────────────────────────
INSERT INTO departamentos (nombre, ubicacion, presupuesto) VALUES
('Tecnología',      'Piso 3 - Ala Norte',  150000.00),
('Ventas',          'Piso 1 - Atención',    80000.00),
('Recursos Humanos','Piso 2 - Central',     50000.00),
('Finanzas',        'Piso 2 - Sur',        120000.00),
('Operaciones',     'Planta Baja',          95000.00);

-- ── Datos: empleados ─────────────────────────────────────────────
INSERT INTO empleados (cedula, nombre, apellido, email, departamento_id, cargo, salario, fecha_ingreso) VALUES
('0101234567', 'Juan',     'Pérez',     'juan.perez@empresa.com',     1, 'Desarrollador Senior',  3500.00, '2021-03-15'),
('0209876543', 'María',    'García',    'maria.garcia@empresa.com',   2, 'Ejecutiva de Ventas',   2800.00, '2022-06-01'),
('0312345678', 'Carlos',   'López',     'carlos.lopez@empresa.com',   1, 'Arquitecto de Software',4200.00, '2020-01-20'),
('0487654321', 'Ana',      'Martínez',  'ana.martinez@empresa.com',   3, 'Coordinadora RRHH',     2500.00, '2023-07-10'),
('0556789012', 'Luis',     'Rodríguez', 'luis.rodriguez@empresa.com', 4, 'Analista Financiero',   3100.00, '2021-11-05'),
('0623456789', 'Carmen',   'Flores',    'carmen.flores@empresa.com',  1, 'Desarrolladora Junior', 2200.00, '2024-02-01'),
('0745678901', 'Roberto',  'Torres',    'roberto.torres@empresa.com', 2, 'Gerente de Ventas',     4800.00, '2019-08-12'),
('0834567890', 'Patricia', 'Vega',      'patricia.vega@empresa.com',  5, 'Supervisora Operativa', 3300.00, '2022-03-22');

-- ── Datos: productos ─────────────────────────────────────────────
INSERT INTO productos (codigo, nombre, categoria, precio, stock) VALUES
('TECH-001', 'Laptop Dell XPS 15',       'Computadoras', 1299.99,  15),
('TECH-002', 'Monitor LG 27" 4K',        'Periféricos',   499.99,  30),
('TECH-003', 'Teclado Mecánico Logitech','Periféricos',    89.99,  50),
('OFF-001',  'Escritorio Ejecutivo',      'Mobiliario',   350.00,   8),
('OFF-002',  'Silla Ergonómica',          'Mobiliario',   280.00,  12),
('SOFT-001', 'Licencia Office 365',       'Software',      99.00, 999),
('SOFT-002', 'Antivirus Kaspersky',       'Software',      45.00, 999);

-- ── Datos: pedidos ───────────────────────────────────────────────
INSERT INTO pedidos (empleado_id, producto_id, cantidad, precio_total, estado) VALUES
(1, 1, 1, 1299.99, 'procesado'),
(1, 3, 2,  179.98, 'procesado'),
(2, 6, 1,   99.00, 'pendiente'),
(3, 2, 2,  999.98, 'procesado'),
(5, 6, 1,   99.00, 'pendiente'),
(7, 1, 3, 3899.97, 'procesado'),
(4, 5, 2,  560.00, 'pendiente');

-- ══════════════════════════════════════════════════════════════════
--  TABLAS DE PAGOS (replica estructura tipo bpc_ordenes / bpc_comercios)
-- ══════════════════════════════════════════════════════════════════

-- ── Tabla: instituciones ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS instituciones (
  id        INT AUTO_INCREMENT PRIMARY KEY,
  codigo    VARCHAR(20) UNIQUE NOT NULL,
  nombre    VARCHAR(200) NOT NULL,
  tipo      VARCHAR(50) DEFAULT 'banco',
  activo    TINYINT(1) DEFAULT 1
);

-- ── Tabla: servicios_punto_venta (≈ sale_point_service) ───────────
CREATE TABLE IF NOT EXISTS servicios_punto_venta (
  id              INT AUTO_INCREMENT PRIMARY KEY,
  codigo          VARCHAR(50) UNIQUE NOT NULL,
  nombre          VARCHAR(200) NOT NULL,
  institucion_id  INT,
  comision        DECIMAL(5,4) DEFAULT 0.0150,
  activo          TINYINT(1) DEFAULT 1,
  FOREIGN KEY (institucion_id) REFERENCES instituciones(id)
);

-- ── Tabla: ordenes_pago (≈ bpc_ordenes.ordenes_pago) ─────────────
CREATE TABLE IF NOT EXISTS ordenes_pago (
  id                      INT AUTO_INCREMENT PRIMARY KEY,
  transaccion_interna_id  VARCHAR(50) UNIQUE NOT NULL,
  orden_uuid              VARCHAR(36) NOT NULL,
  monto                   DECIMAL(12,2) NOT NULL,
  fecha_pago              DATETIME NOT NULL,
  estado                  TINYINT(1) DEFAULT 0,
  movimiento_debito       VARCHAR(50),
  movimiento_credito      VARCHAR(50),
  sale_point_service_id   INT,
  descripcion             VARCHAR(500),
  creado_en               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (sale_point_service_id) REFERENCES servicios_punto_venta(id)
);

-- ── Datos: instituciones ──────────────────────────────────────────
INSERT INTO instituciones (codigo, nombre, tipo) VALUES
('BCE',    'Banco Central del Ecuador',     'banco_central'),
('BP',     'Banco del Pacífico',            'banco'),
('BG',     'Banco Guayaquil',               'banco'),
('COOP_JA','Cooperativa Jardín Azuayo',     'cooperativa'),
('COOP_JEP','Cooperativa JEP',             'cooperativa');

-- ── Datos: servicios_punto_venta ──────────────────────────────────
INSERT INTO servicios_punto_venta (codigo, nombre, institucion_id, comision) VALUES
('SPS-001', 'POS Central Quito',       2, 0.0150),
('SPS-002', 'POS Sucursal Guayaquil',  3, 0.0150),
('SPS-003', 'Terminal Virtual Web',    2, 0.0100),
('SPS-004', 'Cobro Cooperativa JA',    4, 0.0080),
('SPS-005', 'Cobro Cooperativa JEP',   5, 0.0080),
('SPS-006', 'POS Aeropuerto',          3, 0.0200),
('SPS-007', 'Terminal ATM Banco P',    2, 0.0120),
('SPS-008', 'Cobro Móvil',             2, 0.0090),
('SPS-009', 'E-commerce Gateway',      2, 0.0110),
('SPS-010', 'Ventanilla Presencial',   4, 0.0070),
('SPS-011', 'Recaudación Empresas',    2, 0.0060);

-- ── Datos: ordenes_pago ───────────────────────────────────────────
INSERT INTO ordenes_pago (transaccion_interna_id, orden_uuid, monto, fecha_pago, estado, movimiento_debito, movimiento_credito, sale_point_service_id, descripcion) VALUES
('TXN-2026-000001','uuid-a1b2c3d4-0001', 125.50, DATE_SUB(NOW(), INTERVAL 2  DAY), 1,'DEB-001','CRE-001', 11,'Pago servicio eléctrico'),
('TXN-2026-000002','uuid-a1b2c3d4-0002', 320.00, DATE_SUB(NOW(), INTERVAL 5  DAY), 1,'DEB-002','CRE-002', 11,'Transferencia nómina'),
('TXN-2026-000003','uuid-a1b2c3d4-0003',  89.99, DATE_SUB(NOW(), INTERVAL 10 DAY), 1,'DEB-003','CRE-003', 11,'Pago tarjeta crédito'),
('TXN-2026-000004','uuid-a1b2c3d4-0004',1500.00, DATE_SUB(NOW(), INTERVAL 15 DAY), 1,'DEB-004','CRE-004', 11,'Pago proveedor ABC'),
('TXN-2026-000005','uuid-a1b2c3d4-0005', 450.75, DATE_SUB(NOW(), INTERVAL 20 DAY), 1,'DEB-005','CRE-005', 11,'Cobro cuota préstamo'),
('TXN-2026-000006','uuid-a1b2c3d4-0006', 200.00, DATE_SUB(NOW(), INTERVAL 25 DAY), 1,'DEB-006','CRE-006', 11,'Recarga saldo'),
('TXN-2026-000007','uuid-a1b2c3d4-0007',  75.00, DATE_SUB(NOW(), INTERVAL 30 DAY), 1,'DEB-007','CRE-007', 11,'Pago agua potable'),
('TXN-2026-000008','uuid-a1b2c3d4-0008',3200.00, DATE_SUB(NOW(), INTERVAL 40 DAY), 1,'DEB-008','CRE-008', 11,'Transferencia empresa'),
('TXN-2026-000009','uuid-a1b2c3d4-0009', 560.20, DATE_SUB(NOW(), INTERVAL 50 DAY), 0,'DEB-009',NULL,        11,'Pago pendiente validación'),
('TXN-2026-000010','uuid-a1b2c3d4-0010', 980.00, DATE_SUB(NOW(), INTERVAL 60 DAY), 1,'DEB-010','CRE-010', 11,'Pago seguro médico'),
-- Órdenes de otros servicios para probar filtros
('TXN-2026-000011','uuid-a1b2c3d4-0011', 100.00, DATE_SUB(NOW(), INTERVAL 3  DAY), 1,'DEB-011','CRE-011',  1,'POS Quito - venta retail'),
('TXN-2026-000012','uuid-a1b2c3d4-0012', 250.00, DATE_SUB(NOW(), INTERVAL 7  DAY), 1,'DEB-012','CRE-012',  2,'Venta sucursal GYE'),
('TXN-2026-000013','uuid-a1b2c3d4-0013', 180.00, DATE_SUB(NOW(), INTERVAL 90 DAY), 1,'DEB-013','CRE-013', 11,'Fuera de rango 100 días'),
('TXN-2026-000014','uuid-a1b2c3d4-0014',  45.00, DATE_SUB(NOW(), INTERVAL 8  DAY), 0,'DEB-014',NULL,        4,'Coop JA - pendiente'),
('TXN-2026-000015','uuid-a1b2c3d4-0015', 720.00, DATE_SUB(NOW(), INTERVAL 12 DAY), 1,'DEB-015','CRE-015',  4,'Coop JA - cobro cuota');

-- ══════════════════════════════════════════════════════════════════
--  STORED PROCEDURES (para probar CALL)
-- ══════════════════════════════════════════════════════════════════

-- Procedimiento 1: buscar empleados por departamento
DROP PROCEDURE IF EXISTS sp_empleados_por_depto;
CREATE PROCEDURE sp_empleados_por_depto(IN p_depto_id INT)
BEGIN
  SELECT
    e.id,
    CONCAT(e.nombre, ' ', e.apellido) AS nombre_completo,
    e.cargo,
    e.salario,
    d.nombre AS departamento
  FROM empleados e
  JOIN departamentos d ON d.id = e.departamento_id
  WHERE e.departamento_id = p_depto_id
    AND e.activo = 1
  ORDER BY e.salario DESC;
END;

-- Procedimiento 2: resumen de pedidos por empleado
DROP PROCEDURE IF EXISTS sp_resumen_pedidos;
CREATE PROCEDURE sp_resumen_pedidos(IN p_empleado_id INT)
BEGIN
  SELECT
    e.cedula,
    CONCAT(e.nombre, ' ', e.apellido) AS empleado,
    COUNT(p.id) AS total_pedidos,
    SUM(p.precio_total) AS monto_total,
    MAX(p.creado_en) AS ultimo_pedido
  FROM empleados e
  LEFT JOIN pedidos p ON p.empleado_id = e.id
  WHERE e.id = p_empleado_id
  GROUP BY e.id, e.cedula, e.nombre, e.apellido;
END;

-- Procedimiento 3: actualizar salario con auditoría
DROP PROCEDURE IF EXISTS sp_actualizar_salario;
CREATE PROCEDURE sp_actualizar_salario(
  IN p_empleado_id INT,
  IN p_nuevo_salario DECIMAL(10,2)
)
BEGIN
  DECLARE v_salario_anterior DECIMAL(10,2);
  SELECT salario INTO v_salario_anterior FROM empleados WHERE id = p_empleado_id;
  UPDATE empleados SET salario = p_nuevo_salario WHERE id = p_empleado_id;
  SELECT
    p_empleado_id AS id,
    v_salario_anterior AS salario_anterior,
    p_nuevo_salario AS salario_nuevo,
    NOW() AS actualizado_en;
END;
