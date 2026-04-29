-- ============================================================================
-- ORACLE XE 21c — Crear usuario APP en el PDB XEPDB1
-- Este script corre como SYSDBA en el CDB, cambia al PDB y crea el usuario.
-- ============================================================================

ALTER SESSION SET CONTAINER = XEPDB1;

-- Eliminar usuario si ya existe (para re-ejecuciones seguras)
DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'APP';
  IF v_count > 0 THEN
    EXECUTE IMMEDIATE 'DROP USER APP CASCADE';
  END IF;
END;
/

CREATE USER APP IDENTIFIED BY 1234
  DEFAULT TABLESPACE USERS
  TEMPORARY TABLESPACE TEMP;

GRANT CONNECT, RESOURCE, DBA TO APP;
GRANT UNLIMITED TABLESPACE TO APP;
GRANT CREATE SESSION TO APP;
GRANT CREATE TABLE TO APP;
GRANT CREATE SEQUENCE TO APP;
GRANT CREATE PROCEDURE TO APP;
GRANT CREATE VIEW TO APP;

COMMIT;
