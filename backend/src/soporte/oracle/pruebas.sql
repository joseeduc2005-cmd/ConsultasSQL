-- Esquema de pruebas para la Oracle local (contenedor gvenzl/oracle-free).
--
-- Reproduce las tablas que consulta `catalogo.ts` y los escenarios del modo
-- demostración, para poder ejercitar los diagnósticos contra un Oracle de verdad
-- sin tocar producción.
--
-- En Oracle un "esquema" es un usuario, así que se crean tres y se le concede
-- SELECT al usuario con el que se conecta el módulo.
--
-- Uso: sqlplus sys/admin123@localhost/FREEPDB1 as sysdba @pruebas.sql

ALTER SESSION SET CONTAINER = FREEPDB1;

-- Se recrea por si quedó de una ejecución anterior con otro tipo de columna.
BEGIN EXECUTE IMMEDIATE 'DROP TABLE corresponsales.usuario_pos'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- Usuarios que hacen de esquemas ---------------------------------------------
DECLARE
  faltan NUMBER;
BEGIN
  FOR u IN (SELECT 'CORRESPONSALES' n FROM dual UNION ALL
            SELECT 'FISJA' FROM dual UNION ALL
            SELECT 'JST_PERSONAS' FROM dual) LOOP
    SELECT COUNT(*) INTO faltan FROM dba_users WHERE username = u.n;
    IF faltan = 0 THEN
      EXECUTE IMMEDIATE 'CREATE USER ' || u.n || ' IDENTIFIED BY demo123 QUOTA UNLIMITED ON USERS';
      EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO ' || u.n;
    END IF;
  END LOOP;
END;
/

-- Tablas ----------------------------------------------------------------------
DECLARE
  PROCEDURE crear(sentencia VARCHAR2) IS
  BEGIN
    EXECUTE IMMEDIATE sentencia;
  EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;  -- -955: ya existe
  END;
BEGIN
  crear('CREATE TABLE corresponsales.corresponsalrepresentante (
           cr_id NUMBER PRIMARY KEY, cr_cod_socio NUMBER, cr_identificacion VARCHAR2(13),
           codigo_sucursal NUMBER, cr_estado NUMBER)');
  crear('CREATE TABLE corresponsales.corresponsalestablecimiento (
           ce_id NUMBER PRIMARY KEY, ce_representante NUMBER, ce_razon_social VARCHAR2(120),
           ce_estado NUMBER)');
  crear('CREATE TABLE corresponsales.pos (
           pos_id NUMBER PRIMARY KEY, pos_mac VARCHAR2(20), pos_estado_id NUMBER,
           establecimiento_ce_id NUMBER)');
  crear('CREATE TABLE corresponsales.usuario (
           usr_codigo NUMBER PRIMARY KEY, usr_usuario VARCHAR2(60), usr_cedula VARCHAR2(13),
           usr_estado NUMBER, usr_intentos NUMBER, usr_intentos_pin NUMBER,
           usr_is_clave_temporal NUMBER, usr_is_pin_temporal NUMBER, codigo_sucursal NUMBER)');
  crear('CREATE TABLE corresponsales.usuario_pos (
           up_id NUMBER PRIMARY KEY, up_usuario NUMBER, up_pos NUMBER)');
  crear('CREATE TABLE corresponsales.actacomprobantes (
           aco_codigo NUMBER PRIMARY KEY, aco_representante NUMBER, aco_numero VARCHAR2(30))');
  crear('CREATE TABLE corresponsales.pago_corresponsal (
           id NUMBER PRIMARY KEY, numero_acta NUMBER, estado VARCHAR2(2))');
  crear('CREATE TABLE corresponsales.parametro (
           par_id NUMBER PRIMARY KEY, par_descripcion VARCHAR2(120), par_valor VARCHAR2(120))');
  crear('CREATE TABLE corresponsales.transaccion (
           trans_id NUMBER PRIMARY KEY, trans_pos NUMBER, trans_tipo NUMBER,
           trans_monto NUMBER(12,2), trans_fecha DATE, trans_movimiento_tendero NUMBER)');
  crear('CREATE TABLE corresponsales.movimiento_fondeo (
           id NUMBER PRIMARY KEY, id_transaccion_rev NUMBER)');
  crear('CREATE TABLE corresponsales.reverso (
           rev_id NUMBER PRIMARY KEY, rev_transaccion NUMBER)');
  crear('CREATE TABLE fisja.clientes (codigo_socio NUMBER PRIMARY KEY, codigo_sucursal NUMBER)');
  crear('CREATE TABLE fisja.ca_sucursales (codigo_sucursal NUMBER PRIMARY KEY, descripcion VARCHAR2(80))');
  crear('CREATE TABLE jst_personas.vwt_cliente (codigo_socio NUMBER PRIMARY KEY, codigo_sucursal NUMBER)');
  crear('CREATE OR REPLACE VIEW corresponsales.pos_view AS
           SELECT p.pos_mac, p.pos_estado_id, u.usr_usuario
           FROM corresponsales.pos p
           LEFT JOIN corresponsales.usuario_pos up ON up.up_pos = p.pos_id
           LEFT JOIN corresponsales.usuario u ON u.usr_codigo = up.up_usuario');
END;
/

-- Permisos para el usuario del módulo -----------------------------------------
DECLARE
BEGIN
  FOR t IN (SELECT owner, table_name FROM all_tables
            WHERE owner IN ('CORRESPONSALES', 'FISJA', 'JST_PERSONAS')) LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT ON ' || t.owner || '.' || t.table_name || ' TO TESTUSER';
  END LOOP;
  FOR v IN (SELECT owner, view_name FROM all_views WHERE owner = 'CORRESPONSALES') LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT ON ' || v.owner || '.' || v.view_name || ' TO TESTUSER';
  END LOOP;
END;
/

-- Datos: los mismos escenarios del modo demostración --------------------------
DELETE FROM corresponsales.usuario_pos;
DELETE FROM corresponsales.transaccion;
DELETE FROM corresponsales.movimiento_fondeo;
DELETE FROM corresponsales.reverso;
DELETE FROM corresponsales.pos;
DELETE FROM corresponsales.corresponsalestablecimiento;
DELETE FROM corresponsales.corresponsalrepresentante;
DELETE FROM corresponsales.usuario;
DELETE FROM fisja.clientes;
DELETE FROM fisja.ca_sucursales;
DELETE FROM jst_personas.vwt_cliente;

-- Acta con pago cerrado (ticket 2026090874).
DELETE FROM corresponsales.pago_corresponsal;
DELETE FROM corresponsales.actacomprobantes;
INSERT INTO corresponsales.actacomprobantes VALUES (53656, 9002, 'ACTA-53656');
INSERT INTO corresponsales.pago_corresponsal VALUES (1, 53656, 'C');
INSERT INTO corresponsales.actacomprobantes VALUES (53657, 9002, 'ACTA-53657');
INSERT INTO corresponsales.pago_corresponsal VALUES (2, 53657, 'A');

-- Politica de claves (ticket 2026085232).
DELETE FROM corresponsales.parametro;
INSERT INTO corresponsales.parametro VALUES (1, 'LONGITUD MINIMA CLAVE', '8');
INSERT INTO corresponsales.parametro VALUES (2, 'LONGITUD MAXIMA CLAVE', '15');
INSERT INTO corresponsales.parametro VALUES (3, 'CLAVES ANTERIORES NO REUTILIZABLES', '3');
INSERT INTO corresponsales.parametro VALUES (4, 'LONGITUD PIN', '4');

INSERT INTO fisja.ca_sucursales VALUES (1,  'Matriz Cuenca');
INSERT INTO fisja.ca_sucursales VALUES (12, 'Mall del Alto');
INSERT INTO fisja.ca_sucursales VALUES (28, 'Yanuncay');
INSERT INTO fisja.ca_sucursales VALUES (32, 'Gualaceo');

INSERT INTO corresponsales.corresponsalrepresentante VALUES (9001, 700001, '0100000018', 1,  1);
INSERT INTO corresponsales.corresponsalrepresentante VALUES (9002, 700002, '0100000026', 32, 1);
-- 9003 es el caso Mall Alto: corresponsales dice 12, fisja se quedó en 28.
INSERT INTO corresponsales.corresponsalrepresentante VALUES (9003, 700003, '0100000033', 12, 1);
INSERT INTO corresponsales.corresponsalrepresentante VALUES (9004, 700004, '0100000041', 28, 0);

INSERT INTO jst_personas.vwt_cliente VALUES (700001, 1);
INSERT INTO jst_personas.vwt_cliente VALUES (700002, 32);
INSERT INTO jst_personas.vwt_cliente VALUES (700003, 1);
INSERT INTO jst_personas.vwt_cliente VALUES (700004, 28);

INSERT INTO fisja.clientes VALUES (700001, 1);
INSERT INTO fisja.clientes VALUES (700002, 32);
INSERT INTO fisja.clientes VALUES (700003, 28);
INSERT INTO fisja.clientes VALUES (700004, 28);

INSERT INTO corresponsales.corresponsalestablecimiento VALUES (8001, 9001, 'Tienda Demo Norte', 1);
INSERT INTO corresponsales.corresponsalestablecimiento VALUES (8002, 9002, 'Minimarket Demo Centro', 1);
INSERT INTO corresponsales.corresponsalestablecimiento VALUES (8003, 9003, 'Bazar Demo Sur', 1);
INSERT INTO corresponsales.corresponsalestablecimiento VALUES (8004, 9004, 'Farmacia Demo (cerrada)', 0);

INSERT INTO corresponsales.pos VALUES (5001, '860000000000011', 1, 8001);
-- 5002 y 5004 comparten MAC: es el escenario de MAC duplicada.
INSERT INTO corresponsales.pos VALUES (5002, '860000000000022', 1, 8002);
INSERT INTO corresponsales.pos VALUES (5003, '860000000000033', 1, 8003);
INSERT INTO corresponsales.pos VALUES (5004, '860000000000022', 2, 8004);
INSERT INTO corresponsales.pos VALUES (5005, '860000000000044', 1, 8001);

INSERT INTO corresponsales.usuario VALUES (3001, 'demo.activo',     '0100000018', 1, 0, 0, 0, 0, 1);
INSERT INTO corresponsales.usuario VALUES (3002, 'demo.duplicado',  '0100000026', 1, 0, 1, 0, 0, 28);
-- 3003 bloqueado por intentos de PIN, con PIN temporal pendiente.
INSERT INTO corresponsales.usuario VALUES (3003, 'demo.bloqueado',  '0100000058', 1, 0, 3, 0, 1, 1);
INSERT INTO corresponsales.usuario VALUES (3004, 'demo.inactivo',   '0100000066', 0, 2, 0, 0, 0, 28);
INSERT INTO corresponsales.usuario VALUES (3005, 'demo.antiguo',    '0100000041', 0, 0, 0, 0, 0, 28);

INSERT INTO corresponsales.usuario_pos VALUES (1, 3001, 5001);
INSERT INTO corresponsales.usuario_pos VALUES (2, 3002, 5002);
INSERT INTO corresponsales.usuario_pos VALUES (3, 3003, 5005);

-- Transacciones recientes; algunas sin movimiento de tendero (escenario del reporte).
INSERT INTO corresponsales.transaccion
  SELECT 6000 + LEVEL, 5001, CASE WHEN MOD(LEVEL, 7) = 0 THEN 13 ELSE 1 END,
         ROUND(DBMS_RANDOM.VALUE(5, 300), 2), SYSDATE - MOD(LEVEL, 10),
         CASE WHEN MOD(LEVEL, 7) = 0 THEN NULL ELSE 7000 + LEVEL END
  FROM dual CONNECT BY LEVEL <= 40;

INSERT INTO corresponsales.movimiento_fondeo VALUES (1, NULL);
INSERT INTO corresponsales.movimiento_fondeo VALUES (2, 6005);
INSERT INTO corresponsales.reverso VALUES (1, 6010);

COMMIT;

SELECT 'Esquema de pruebas listo' AS estado FROM dual;
EXIT;
