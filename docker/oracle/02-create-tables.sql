-- ============================================================================
-- ORACLE XE 21c — Tablas del sistema (equivalente al schema PostgreSQL)
-- Conecta como APP en XEPDB1
-- ============================================================================

CONNECT APP/1234@//localhost:1521/XEPDB1

-- ============================================================================
-- 1. ROLES
-- ============================================================================
CREATE TABLE ROLES (
    ID         VARCHAR2(36) DEFAULT SYS_GUID() PRIMARY KEY,
    NOMBRE     VARCHAR2(50)  NOT NULL UNIQUE,
    DESCRIPCION VARCHAR2(500)
);

-- ============================================================================
-- 2. PERMISSIONS
-- ============================================================================
CREATE TABLE PERMISSIONS (
    ID          VARCHAR2(36) DEFAULT SYS_GUID() PRIMARY KEY,
    NOMBRE      VARCHAR2(100) NOT NULL UNIQUE,
    DESCRIPCION VARCHAR2(500)
);

-- ============================================================================
-- 3. ROLE_PERMISSIONS
-- ============================================================================
CREATE TABLE ROLE_PERMISSIONS (
    ID            VARCHAR2(36) DEFAULT SYS_GUID() PRIMARY KEY,
    ROLE_ID       VARCHAR2(36) NOT NULL,
    PERMISSION_ID VARCHAR2(36) NOT NULL,
    CONSTRAINT FK_RP_ROLE       FOREIGN KEY (ROLE_ID)       REFERENCES ROLES(ID)       ON DELETE CASCADE,
    CONSTRAINT FK_RP_PERMISSION FOREIGN KEY (PERMISSION_ID) REFERENCES PERMISSIONS(ID) ON DELETE CASCADE,
    CONSTRAINT UQ_ROLE_PERMISSION UNIQUE (ROLE_ID, PERMISSION_ID)
);

-- ============================================================================
-- 4. DEPARTMENTS
-- ============================================================================
CREATE TABLE DEPARTMENTS (
    ID         VARCHAR2(36)  DEFAULT SYS_GUID() PRIMARY KEY,
    NOMBRE     VARCHAR2(100) NOT NULL UNIQUE,
    CREATED_AT TIMESTAMP     DEFAULT SYSDATE
);

-- ============================================================================
-- 5. USERS
-- ============================================================================
CREATE TABLE USERS (
    ID         VARCHAR2(36)  DEFAULT SYS_GUID() PRIMARY KEY,
    USERNAME   VARCHAR2(100) NOT NULL UNIQUE,
    PASSWORD   VARCHAR2(255) NOT NULL,
    ROLE_ID    VARCHAR2(36)  NOT NULL,
    ACTIVO     NUMBER(1)     DEFAULT 1 CHECK (ACTIVO IN (0,1)),
    BLOQUEADO  NUMBER(1)     DEFAULT 0 CHECK (BLOQUEADO IN (0,1)),
    CREATED_AT TIMESTAMP     DEFAULT SYSDATE,
    UPDATED_AT TIMESTAMP     DEFAULT SYSDATE,
    CONSTRAINT FK_USERS_ROLE FOREIGN KEY (ROLE_ID) REFERENCES ROLES(ID)
);

-- ============================================================================
-- 6. EMPLOYEES
-- ============================================================================
CREATE TABLE EMPLOYEES (
    ID                VARCHAR2(36)   DEFAULT SYS_GUID() PRIMARY KEY,
    USER_ID           VARCHAR2(36)   NOT NULL UNIQUE,
    DEPARTMENT_ID     VARCHAR2(36)   NOT NULL,
    CARGO             VARCHAR2(100)  NOT NULL,
    SALARIO           NUMBER(10,2),
    FECHA_CONTRATACION DATE          DEFAULT SYSDATE,
    CREATED_AT        TIMESTAMP      DEFAULT SYSDATE,
    CONSTRAINT FK_EMP_USER   FOREIGN KEY (USER_ID)       REFERENCES USERS(ID)       ON DELETE CASCADE,
    CONSTRAINT FK_EMP_DEPT   FOREIGN KEY (DEPARTMENT_ID) REFERENCES DEPARTMENTS(ID)
);

-- ============================================================================
-- 7. SESSIONS
-- ============================================================================
CREATE TABLE SESSIONS (
    ID         VARCHAR2(36)  DEFAULT SYS_GUID() PRIMARY KEY,
    USER_ID    VARCHAR2(36)  NOT NULL,
    TOKEN      VARCHAR2(500) NOT NULL UNIQUE,
    ACTIVO     NUMBER(1)     DEFAULT 1 CHECK (ACTIVO IN (0,1)),
    CREATED_AT TIMESTAMP     DEFAULT SYSDATE,
    EXPIRES_AT TIMESTAMP,
    CONSTRAINT FK_SESS_USER FOREIGN KEY (USER_ID) REFERENCES USERS(ID) ON DELETE CASCADE
);

-- ============================================================================
-- 8. LOGS
-- ============================================================================
CREATE TABLE LOGS (
    ID         VARCHAR2(36)  DEFAULT SYS_GUID() PRIMARY KEY,
    USER_ID    VARCHAR2(36),
    ACCION     VARCHAR2(100) NOT NULL,
    DETALLE    CLOB,
    IP_ADDRESS VARCHAR2(45),
    CREATED_AT TIMESTAMP     DEFAULT SYSDATE,
    CONSTRAINT FK_LOGS_USER FOREIGN KEY (USER_ID) REFERENCES USERS(ID) ON DELETE SET NULL
);

-- ============================================================================
-- 9. QUERY_HISTORY  (tabla específica de este sistema)
-- ============================================================================
CREATE TABLE QUERY_HISTORY (
    ID           NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    USERNAME     VARCHAR2(100),
    QUERY_TEXT   CLOB,
    DATABASE_ID  VARCHAR2(50),
    SQL_GENERATED CLOB,
    EXECUTED_AT  TIMESTAMP DEFAULT SYSDATE,
    DURATION_MS  NUMBER,
    STATUS       VARCHAR2(20) DEFAULT 'OK' CHECK (STATUS IN ('OK','ERROR','TIMEOUT')),
    ROW_COUNT    NUMBER DEFAULT 0,
    ERROR_MSG    VARCHAR2(1000)
);

-- ============================================================================
-- ÍNDICES
-- ============================================================================
CREATE INDEX IDX_USERS_ROLE      ON USERS(ROLE_ID);
CREATE INDEX IDX_USERS_USERNAME  ON USERS(USERNAME);
CREATE INDEX IDX_EMP_USER        ON EMPLOYEES(USER_ID);
CREATE INDEX IDX_EMP_DEPT        ON EMPLOYEES(DEPARTMENT_ID);
CREATE INDEX IDX_SESS_USER       ON SESSIONS(USER_ID);
CREATE INDEX IDX_LOGS_USER       ON LOGS(USER_ID);
CREATE INDEX IDX_LOGS_CREATED    ON LOGS(CREATED_AT);
CREATE INDEX IDX_QH_DB           ON QUERY_HISTORY(DATABASE_ID);
CREATE INDEX IDX_QH_USER         ON QUERY_HISTORY(USERNAME);

-- ============================================================================
-- DATOS DE PRUEBA
-- ============================================================================

-- Roles
INSERT INTO ROLES (NOMBRE, DESCRIPCION) VALUES ('admin',   'Administrador del sistema con acceso total');
INSERT INTO ROLES (NOMBRE, DESCRIPCION) VALUES ('user',    'Usuario estándar con permisos limitados');
INSERT INTO ROLES (NOMBRE, DESCRIPCION) VALUES ('manager', 'Gerente con permisos de supervisión');
INSERT INTO ROLES (NOMBRE, DESCRIPCION) VALUES ('analyst', 'Analista de datos con acceso a reportes');

-- Departments
INSERT INTO DEPARTMENTS (NOMBRE) VALUES ('Tecnología e Innovación');
INSERT INTO DEPARTMENTS (NOMBRE) VALUES ('Finanzas y Contabilidad');
INSERT INTO DEPARTMENTS (NOMBRE) VALUES ('Recursos Humanos');
INSERT INTO DEPARTMENTS (NOMBRE) VALUES ('Ventas y Marketing');
INSERT INTO DEPARTMENTS (NOMBRE) VALUES ('Operaciones');

-- Users
INSERT INTO USERS (USERNAME, PASSWORD, ROLE_ID, ACTIVO, BLOQUEADO)
  VALUES ('admin_user',        'hashed_admin_123',   (SELECT ID FROM ROLES WHERE NOMBRE='admin'),   1, 0);
INSERT INTO USERS (USERNAME, PASSWORD, ROLE_ID, ACTIVO, BLOQUEADO)
  VALUES ('juan_martinez',     'hashed_user_456',    (SELECT ID FROM ROLES WHERE NOMBRE='manager'), 1, 0);
INSERT INTO USERS (USERNAME, PASSWORD, ROLE_ID, ACTIVO, BLOQUEADO)
  VALUES ('maria_garcia',      'hashed_user_789',    (SELECT ID FROM ROLES WHERE NOMBRE='user'),    1, 0);
INSERT INTO USERS (USERNAME, PASSWORD, ROLE_ID, ACTIVO, BLOQUEADO)
  VALUES ('carlos_lopez',      'hashed_user_012',    (SELECT ID FROM ROLES WHERE NOMBRE='analyst'), 1, 0);
INSERT INTO USERS (USERNAME, PASSWORD, ROLE_ID, ACTIVO, BLOQUEADO)
  VALUES ('sofia_rodriguez',   'hashed_user_345',    (SELECT ID FROM ROLES WHERE NOMBRE='user'),    1, 0);

-- Employees
INSERT INTO EMPLOYEES (USER_ID, DEPARTMENT_ID, CARGO, SALARIO, FECHA_CONTRATACION) VALUES
  ((SELECT ID FROM USERS WHERE USERNAME='admin_user'),
   (SELECT ID FROM DEPARTMENTS WHERE NOMBRE='Tecnología e Innovación'),
   'Director de TI', 85000, DATE '2020-01-15');
INSERT INTO EMPLOYEES (USER_ID, DEPARTMENT_ID, CARGO, SALARIO, FECHA_CONTRATACION) VALUES
  ((SELECT ID FROM USERS WHERE USERNAME='juan_martinez'),
   (SELECT ID FROM DEPARTMENTS WHERE NOMBRE='Finanzas y Contabilidad'),
   'Gerente de Finanzas', 72000, DATE '2021-03-20');
INSERT INTO EMPLOYEES (USER_ID, DEPARTMENT_ID, CARGO, SALARIO, FECHA_CONTRATACION) VALUES
  ((SELECT ID FROM USERS WHERE USERNAME='maria_garcia'),
   (SELECT ID FROM DEPARTMENTS WHERE NOMBRE='Recursos Humanos'),
   'Especialista en RRHH', 55000, DATE '2022-06-10');
INSERT INTO EMPLOYEES (USER_ID, DEPARTMENT_ID, CARGO, SALARIO, FECHA_CONTRATACION) VALUES
  ((SELECT ID FROM USERS WHERE USERNAME='carlos_lopez'),
   (SELECT ID FROM DEPARTMENTS WHERE NOMBRE='Tecnología e Innovación'),
   'Analista de Datos', 62000, DATE '2021-11-05');

-- Sessions
INSERT INTO SESSIONS (USER_ID, TOKEN, ACTIVO, EXPIRES_AT) VALUES
  ((SELECT ID FROM USERS WHERE USERNAME='admin_user'),
   'token_admin_abc123def456ghi789', 1, SYSDATE + 1);
INSERT INTO SESSIONS (USER_ID, TOKEN, ACTIVO, EXPIRES_AT) VALUES
  ((SELECT ID FROM USERS WHERE USERNAME='juan_martinez'),
   'token_juan_xyz789uvw456rst123', 1, SYSDATE + 1);

-- Logs
INSERT INTO LOGS (USER_ID, ACCION, DETALLE, IP_ADDRESS) VALUES
  ((SELECT ID FROM USERS WHERE USERNAME='admin_user'),
   'LOGIN', 'Inicio de sesión exitoso', '192.168.1.100');
INSERT INTO LOGS (USER_ID, ACCION, DETALLE, IP_ADDRESS) VALUES
  ((SELECT ID FROM USERS WHERE USERNAME='juan_martinez'),
   'WORKFLOW_EXECUTED', 'Workflow de aprobación iniciado', '192.168.1.101');
INSERT INTO LOGS (USER_ID, ACCION, DETALLE, IP_ADDRESS) VALUES
  ((SELECT ID FROM USERS WHERE USERNAME='carlos_lopez'),
   'REPORT_GENERATED', 'Reporte mensual generado', '192.168.1.103');

-- Query history de ejemplo
INSERT INTO QUERY_HISTORY (USERNAME, QUERY_TEXT, DATABASE_ID, SQL_GENERATED, STATUS, ROW_COUNT) VALUES
  ('admin_user', 'dame los usuarios', 'oracle_test',
   'SELECT * FROM USERS WHERE ROWNUM <= 50', 'OK', 5);
INSERT INTO QUERY_HISTORY (USERNAME, QUERY_TEXT, DATABASE_ID, SQL_GENERATED, STATUS, ROW_COUNT) VALUES
  ('carlos_lopez', 'dame las tablas de oracle', 'oracle_test',
   'SELECT table_name FROM USER_TABLES ORDER BY table_name', 'OK', 9);

COMMIT;
