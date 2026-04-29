-- ============================================================================
-- BASE DE DATOS EMPRESARIAL PARA SISTEMA DE WORKFLOWS
-- ============================================================================

-- Crear extensión UUID si no existe
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- 1. TABLA: roles
-- ============================================================================
CREATE TABLE IF NOT EXISTS roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre VARCHAR(50) NOT NULL UNIQUE,
    descripcion TEXT
);

-- ============================================================================
-- 2. TABLA: permissions
-- ============================================================================
CREATE TABLE IF NOT EXISTS permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre VARCHAR(100) NOT NULL UNIQUE,
    descripcion TEXT
);

-- ============================================================================
-- 3. TABLA: role_permissions
-- ============================================================================
CREATE TABLE IF NOT EXISTS role_permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    role_id UUID NOT NULL,
    permission_id UUID NOT NULL,
    CONSTRAINT fk_role_permissions_role_id FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE,
    CONSTRAINT fk_role_permissions_permission_id FOREIGN KEY (permission_id) REFERENCES permissions(id) ON DELETE CASCADE,
    CONSTRAINT unique_role_permission UNIQUE (role_id, permission_id)
);

-- ============================================================================
-- 4. TABLA: departments
-- ============================================================================
CREATE TABLE IF NOT EXISTS departments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 5. TABLA: users
-- ============================================================================
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(100) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    role_id UUID NOT NULL,
    activo BOOLEAN DEFAULT TRUE,
    bloqueado BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_users_role_id FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE RESTRICT
);

-- ============================================================================
-- 6. TABLA: employees
-- ============================================================================
CREATE TABLE IF NOT EXISTS employees (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL UNIQUE,
    department_id UUID NOT NULL,
    cargo VARCHAR(100) NOT NULL,
    salario DECIMAL(10, 2),
    fecha_contratacion DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_employees_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_employees_department_id FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE RESTRICT
);

-- ============================================================================
-- 7. TABLA: sessions
-- ============================================================================
CREATE TABLE IF NOT EXISTS sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    token VARCHAR(500) NOT NULL UNIQUE,
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    CONSTRAINT fk_sessions_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- ============================================================================
-- 8. TABLA: logs
-- ============================================================================
CREATE TABLE IF NOT EXISTS logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID,
    accion VARCHAR(100) NOT NULL,
    detalle TEXT,
    ip_address VARCHAR(45),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_logs_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
);

-- ============================================================================
-- ÍNDICES PARA OPTIMIZACIÓN
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_users_role_id ON users(role_id);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_employees_user_id ON employees(user_id);
CREATE INDEX IF NOT EXISTS idx_employees_department_id ON employees(department_id);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
CREATE INDEX IF NOT EXISTS idx_logs_user_id ON logs(user_id);
CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at);
CREATE INDEX IF NOT EXISTS idx_role_permissions_role_id ON role_permissions(role_id);
CREATE INDEX IF NOT EXISTS idx_role_permissions_permission_id ON role_permissions(permission_id);

-- ============================================================================
-- INSERCIÓN DE DATOS DE PRUEBA
-- ============================================================================

-- ============================================================================
-- 1. INSERTAR ROLES
-- ============================================================================
INSERT INTO roles (nombre, descripcion) VALUES
('admin', 'Administrador del sistema con acceso total'),
('user', 'Usuario estándar con permisos limitados'),
('manager', 'Gerente con permisos de supervisión'),
('analyst', 'Analista de datos con acceso a reportes');

-- ============================================================================
-- 2. INSERTAR PERMISOS
-- ============================================================================
INSERT INTO permissions (nombre, descripcion) VALUES
('users.create', 'Crear nuevos usuarios'),
('users.read', 'Ver información de usuarios'),
('users.update', 'Actualizar usuarios'),
('users.delete', 'Eliminar usuarios'),
('roles.manage', 'Gestionar roles y permisos'),
('reports.view', 'Ver reportes'),
('reports.export', 'Exportar reportes'),
('logs.view', 'Ver registros de auditoría'),
('workflows.execute', 'Ejecutar workflows'),
('workflows.create', 'Crear nuevos workflows'),
('employees.manage', 'Gestionar empleados'),
('departments.manage', 'Gestionar departamentos');

-- ============================================================================
-- 3. ASIGNAR PERMISOS A ROLES
-- ============================================================================
-- Admin tiene todos los permisos
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r, permissions p WHERE r.nombre = 'admin';

-- User tiene permisos limitados
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r, permissions p 
WHERE r.nombre = 'user' AND p.nombre IN ('users.read', 'reports.view', 'workflows.execute');

-- Manager tiene permisos de supervisión
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r, permissions p 
WHERE r.nombre = 'manager' AND p.nombre IN ('users.read', 'employees.manage', 'reports.view', 'reports.export', 'workflows.execute', 'logs.view');

-- Analyst tiene permisos de lectura
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r, permissions p 
WHERE r.nombre = 'analyst' AND p.nombre IN ('users.read', 'reports.view', 'reports.export', 'logs.view');

-- ============================================================================
-- 4. INSERTAR DEPARTAMENTOS
-- ============================================================================
INSERT INTO departments (nombre) VALUES
('Tecnología e Innovación'),
('Finanzas y Contabilidad'),
('Recursos Humanos'),
('Ventas y Marketing'),
('Operaciones'),
('Legal y Cumplimiento');

-- ============================================================================
-- 5. INSERTAR USUARIOS
-- ============================================================================
INSERT INTO users (username, password, role_id, activo, bloqueado) VALUES
('admin_user', 'hashed_password_admin_123', (SELECT id FROM roles WHERE nombre = 'admin'), TRUE, FALSE),
('juan_martinez', 'hashed_password_user_456', (SELECT id FROM roles WHERE nombre = 'manager'), TRUE, FALSE),
('maria_garcia', 'hashed_password_user_789', (SELECT id FROM roles WHERE nombre = 'user'), TRUE, FALSE),
('carlos_lopez', 'hashed_password_user_012', (SELECT id FROM roles WHERE nombre = 'analyst'), TRUE, FALSE),
('sofia_rodriguez', 'hashed_password_user_345', (SELECT id FROM roles WHERE nombre = 'user'), TRUE, FALSE),
('template_bloqueado', 'hashed_password_blocked_678', (SELECT id FROM roles WHERE nombre = 'user'), FALSE, TRUE);

-- ============================================================================
-- 6. INSERTAR EMPLEADOS
-- ============================================================================
INSERT INTO employees (user_id, department_id, cargo, salario, fecha_contratacion) VALUES
((SELECT id FROM users WHERE username = 'admin_user'), (SELECT id FROM departments WHERE nombre = 'Tecnología e Innovación'), 'Director de TI', 85000.00, '2020-01-15'),
((SELECT id FROM users WHERE username = 'juan_martinez'), (SELECT id FROM departments WHERE nombre = 'Finanzas y Contabilidad'), 'Gerente de Finanzas', 72000.00, '2021-03-20'),
((SELECT id FROM users WHERE username = 'maria_garcia'), (SELECT id FROM departments WHERE nombre = 'Recursos Humanos'), 'Especialista en RRHH', 55000.00, '2022-06-10'),
((SELECT id FROM users WHERE username = 'carlos_lopez'), (SELECT id FROM departments WHERE nombre = 'Tecnología e Innovación'), 'Analista de Datos', 62000.00, '2021-11-05'),
((SELECT id FROM users WHERE username = 'sofia_rodriguez'), (SELECT id FROM departments WHERE nombre = 'Ventas y Marketing'), 'Ejecutiva de Ventas', 58000.00, '2023-02-14');

-- ============================================================================
-- 7. INSERTAR SESIONES ACTIVAS
-- ============================================================================
INSERT INTO sessions (user_id, token, activo, created_at, expires_at) VALUES
((SELECT id FROM users WHERE username = 'admin_user'), 'token_admin_abc123def456ghi789jkl', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '24 hours'),
((SELECT id FROM users WHERE username = 'juan_martinez'), 'token_juan_xyz789uvw456rst123opq', TRUE, CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_TIMESTAMP + INTERVAL '22 hours'),
((SELECT id FROM users WHERE username = 'maria_garcia'), 'token_maria_456mno123pqr789stu', FALSE, CURRENT_TIMESTAMP - INTERVAL '1 day', CURRENT_TIMESTAMP - INTERVAL '1 second'),
((SELECT id FROM users WHERE username = 'carlos_lopez'), 'token_carlos_opq987nml654kji321hgf', TRUE, CURRENT_TIMESTAMP - INTERVAL '30 minutes', CURRENT_TIMESTAMP + INTERVAL '23.5 hours');

-- ============================================================================
-- 8. INSERTAR LOGS DE AUDITORÍA
-- ============================================================================
INSERT INTO logs (user_id, accion, detalle, ip_address, created_at) VALUES
((SELECT id FROM users WHERE username = 'admin_user'), 'LOGIN', 'Usuario iniciación de sesión exitosa', '192.168.1.100', CURRENT_TIMESTAMP - INTERVAL '1 hour'),
((SELECT id FROM users WHERE username = 'admin_user'), 'USER_CREATED', 'Nuevo usuario creado: maria_garcia', '192.168.1.100', CURRENT_TIMESTAMP - INTERVAL '45 minutes'),
((SELECT id FROM users WHERE username = 'juan_martinez'), 'WORKFLOW_EXECUTED', 'Workflow de aprobación de presupuesto iniciado', '192.168.1.101', CURRENT_TIMESTAMP - INTERVAL '30 minutes'),
((SELECT id FROM users WHERE username = 'maria_garcia'), 'EMPLOYEE_UPDATED', 'Información de empleado actualizada', '192.168.1.102', CURRENT_TIMESTAMP - INTERVAL '20 minutes'),
((SELECT id FROM users WHERE username = 'carlos_lopez'), 'REPORT_GENERATED', 'Reporte mensual de análisis de datos generado', '192.168.1.103', CURRENT_TIMESTAMP - INTERVAL '15 minutes'),
((SELECT id FROM users WHERE username = 'admin_user'), 'PERMISSION_ASSIGNED', 'Permiso asignado a rol manager', '192.168.1.100', CURRENT_TIMESTAMP - INTERVAL '10 minutes'),
((SELECT id FROM users WHERE username = 'sofia_rodriguez'), 'WORKFLOW_EXECUTED', 'Workflow de registro de cliente iniciado', '192.168.1.104', CURRENT_TIMESTAMP - INTERVAL '5 minutes'),
(NULL, 'SYSTEM_STARTUP', 'Sistema iniciado correctamente', NULL, CURRENT_TIMESTAMP);

-- ============================================================================
-- VERIFICACIÓN: Mostrar resumen de datos insertados
-- ============================================================================
SELECT 'Roles' as tabla, COUNT(*) as cantidad FROM roles
UNION ALL
SELECT 'Permisos', COUNT(*) FROM permissions
UNION ALL
SELECT 'Permisos por Rol', COUNT(*) FROM role_permissions
UNION ALL
SELECT 'Usuarios', COUNT(*) FROM users
UNION ALL
SELECT 'Empleados', COUNT(*) FROM employees
UNION ALL
SELECT 'Departamentos', COUNT(*) FROM departments
UNION ALL
SELECT 'Sesiones', COUNT(*) FROM sessions
UNION ALL
SELECT 'Logs', COUNT(*) FROM logs;
