-- Población de referencia para la Oracle local de pruebas.
--
-- La exploración compara al usuario del caso contra los que SÍ están operando, así
-- que necesita una población con volumen: con cinco usuarios ficticios cualquier
-- valor dominante sería ruido, y el módulo se niega a proponer nada (a propósito).
--
-- Crea 40 corresponsales sanos con su POS y transacciones recientes, más un poco de
-- variación para que el valor dominante no sea un 100 % artificial.
--
-- Uso: sqlplus sys/admin123@localhost/FREEPDB1 as sysdba @poblacion.sql
-- Requiere haber corrido antes pruebas.sql.

ALTER SESSION SET CONTAINER = FREEPDB1;

DELETE FROM corresponsales.transaccion WHERE trans_pos BETWEEN 5100 AND 5199;
DELETE FROM corresponsales.usuario_pos WHERE up_usuario BETWEEN 3100 AND 3199;
DELETE FROM corresponsales.pos WHERE pos_id BETWEEN 5100 AND 5199;
DELETE FROM corresponsales.usuario WHERE usr_codigo BETWEEN 3100 AND 3199;
DELETE FROM corresponsales.corresponsalestablecimiento WHERE ce_id BETWEEN 8100 AND 8199;
DELETE FROM corresponsales.corresponsalrepresentante WHERE cr_id BETWEEN 9100 AND 9199;

BEGIN
  FOR i IN 0 .. 39 LOOP
    INSERT INTO corresponsales.corresponsalrepresentante
      VALUES (9100 + i, 710000 + i, LPAD(TO_CHAR(2000000 + i), 10, '0'), 1, 1);
    INSERT INTO corresponsales.corresponsalestablecimiento
      VALUES (8100 + i, 9100 + i, 'Corresponsal sano ' || TO_CHAR(i), 1);
    INSERT INTO corresponsales.pos
      VALUES (5100 + i, '8601' || LPAD(TO_CHAR(100000000 + i), 11, '0'), 1, 8100 + i);

    -- La mayoría opera sin novedad. Se deja algo de variación real: dos usuarios
    -- con un intento fallido suelto y uno con el PIN temporal todavía pendiente.
    INSERT INTO corresponsales.usuario VALUES (
      3100 + i,
      'sano.' || LPAD(TO_CHAR(i), 2, '0'),
      LPAD(TO_CHAR(2000000 + i), 10, '0'),
      1,                                              -- usr_estado
      CASE WHEN i IN (7, 23) THEN 1 ELSE 0 END,       -- usr_intentos
      CASE WHEN i = 15 THEN 1 ELSE 0 END,             -- usr_intentos_pin
      0,                                              -- usr_is_clave_temporal
      CASE WHEN i = 31 THEN 1 ELSE 0 END,             -- usr_is_pin_temporal
      1);

    INSERT INTO corresponsales.usuario_pos VALUES (100 + i, 3100 + i, 5100 + i);

    -- Transacciones recientes: es lo que los vuelve «usuarios que están operando».
    INSERT INTO corresponsales.transaccion
      SELECT 60000 + (i * 10) + LEVEL, 5100 + i, 1,
             ROUND(DBMS_RANDOM.VALUE(5, 300), 2),
             SYSDATE - MOD(LEVEL * (i + 1), 25),
             80000 + (i * 10) + LEVEL
      FROM dual CONNECT BY LEVEL <= 6;
  END LOOP;
END;
/

COMMIT;

SELECT COUNT(*) AS usuarios_sanos FROM corresponsales.usuario u
WHERE EXISTS (SELECT 1 FROM corresponsales.usuario_pos up
              JOIN corresponsales.transaccion t ON t.trans_pos = up.up_pos
              WHERE up.up_usuario = u.usr_codigo AND t.trans_fecha >= SYSDATE - 30);
EXIT;
