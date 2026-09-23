-- Escenario del ticket EJEMPLO-005, en la Oracle local de pruebas.
--
-- Un caso enredado a propósito, con varias cosas mal a la vez sobre el mismo
-- corresponsal: el usuario quedó inactivo y con los dos contadores de intentos
-- agotados, arrastra una clave temporal sin cambiar, su sucursal no coincide con la
-- del corresponsal, y el POS que usa comparte MAC con otro de un establecimiento
-- cerrado. Es la clase de caso en que el catálogo encuentra varias correcciones y la
-- exploración las junta en una sola sentencia.
--
-- Uso: sqlplus sys/admin123@localhost/FREEPDB1 as sysdba @caso-005.sql

ALTER SESSION SET CONTAINER = FREEPDB1;

-- El corresponsal y su establecimiento.
MERGE INTO corresponsales.corresponsalrepresentante d
USING (SELECT 9005 cr_id FROM dual) s ON (d.cr_id = s.cr_id)
WHEN NOT MATCHED THEN INSERT (cr_id, cr_cod_socio, cr_identificacion, codigo_sucursal, cr_estado)
  VALUES (9005, 700005, '0100000074', 32, 1);

MERGE INTO corresponsales.corresponsalestablecimiento d
USING (SELECT 8005 ce_id FROM dual) s ON (d.ce_id = s.ce_id)
WHEN NOT MATCHED THEN INSERT (ce_id, ce_representante, ce_razon_social, ce_estado)
  VALUES (8005, 9005, 'Despensa Gualaceo', 1);

-- El establecimiento viejo, ya cerrado, cuyo POS quedó con la misma MAC.
MERGE INTO corresponsales.corresponsalestablecimiento d
USING (SELECT 8006 ce_id FROM dual) s ON (d.ce_id = s.ce_id)
WHEN NOT MATCHED THEN INSERT (ce_id, ce_representante, ce_razon_social, ce_estado)
  VALUES (8006, 9005, 'Despensa Gualaceo (local anterior)', 0);

-- Los dos POS comparten MAC: el activo y el del local cerrado.
MERGE INTO corresponsales.pos d
USING (SELECT 5006 pos_id FROM dual) s ON (d.pos_id = s.pos_id)
WHEN NOT MATCHED THEN INSERT (pos_id, pos_mac, pos_estado_id, establecimiento_ce_id)
  VALUES (5006, '860000000000055', 1, 8005);

MERGE INTO corresponsales.pos d
USING (SELECT 5007 pos_id FROM dual) s ON (d.pos_id = s.pos_id)
WHEN NOT MATCHED THEN INSERT (pos_id, pos_mac, pos_estado_id, establecimiento_ce_id)
  VALUES (5007, '860000000000055', 2, 8006);

-- El usuario: inactivo, con los dos contadores agotados, clave temporal pendiente y
-- la sucursal desalineada (tiene 28, el corresponsal está en 32).
MERGE INTO corresponsales.usuario d
USING (SELECT 3006 usr_codigo FROM dual) s ON (d.usr_codigo = s.usr_codigo)
WHEN MATCHED THEN UPDATE SET
  usr_estado = 0, usr_intentos = 3, usr_intentos_pin = 3,
  usr_is_clave_temporal = 1, usr_is_pin_temporal = 0, codigo_sucursal = 28
WHEN NOT MATCHED THEN INSERT
  (usr_codigo, usr_usuario, usr_cedula, usr_estado, usr_intentos, usr_intentos_pin,
   usr_is_clave_temporal, usr_is_pin_temporal, codigo_sucursal)
  VALUES (3006, 'rocio.chimbo', '0100000074', 0, 3, 3, 1, 0, 28);

MERGE INTO corresponsales.usuario_pos d
USING (SELECT 6 up_id FROM dual) s ON (d.up_id = s.up_id)
WHEN NOT MATCHED THEN INSERT (up_id, up_usuario, up_pos) VALUES (6, 3006, 5006);

-- fisja se quedó con la sucursal vieja, como en el caso Mall Alto.
MERGE INTO fisja.clientes d
USING (SELECT 700005 codigo_socio FROM dual) s ON (d.codigo_socio = s.codigo_socio)
WHEN MATCHED THEN UPDATE SET codigo_sucursal = 28
WHEN NOT MATCHED THEN INSERT (codigo_socio, codigo_sucursal) VALUES (700005, 28);

MERGE INTO jst_personas.vwt_cliente d
USING (SELECT 700005 codigo_socio FROM dual) s ON (d.codigo_socio = s.codigo_socio)
WHEN MATCHED THEN UPDATE SET codigo_sucursal = 32
WHEN NOT MATCHED THEN INSERT (codigo_socio, codigo_sucursal) VALUES (700005, 32);

COMMIT;

SELECT usr_usuario, usr_estado, usr_intentos, usr_intentos_pin, usr_is_clave_temporal, codigo_sucursal
FROM corresponsales.usuario WHERE usr_codigo = 3006;
EXIT;
