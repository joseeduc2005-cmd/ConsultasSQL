/**
 * Pruebas de caracterización de la validación de scripts.
 *
 * Estas reglas deciden qué se deja ejecutar desde un artículo. Son la última barrera
 * antes de que algo toque una base de producción, y hasta ahora no había forma de
 * comprobar que siguieran diciendo lo mismo después de un cambio.
 *
 *   node --test src/dominio/scripts/validacion.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  ALLOWED_WHERE_OPERATORS,
  asegurarSoloConsulta,
  assertDynamicScriptAllowed,
  assertManualSqlAllowed,
  detectSqlOperation,
  DYNAMIC_SCRIPT_ALLOWED_TYPES,
  isPlainObject,
  normalizeDynamicScriptType,
  parseQualifiedFieldRef,
  validateBusinessScriptDefinition,
  validateRelationalMutationScriptDefinition,
} from './validacion.js';

const lanza = (fn, porque) => assert.throws(fn, Error, porque);
const pasa = (fn, porque) => assert.doesNotThrow(fn, porque);

describe('qué es un objeto de verdad', () => {
  it('un objeto plano sí', () => {
    assert.equal(isPlainObject({}), true);
    assert.equal(isPlainObject({ a: 1 }), true);
  });

  it('un arreglo, null y lo demás no', () => {
    // Importa porque un arreglo pasa el typeof 'object' y colaría si no se mirara.
    assert.equal(isPlainObject([]), false);
    assert.equal(isPlainObject(null), false);
    assert.equal(isPlainObject('texto'), false);
    assert.equal(isPlainObject(5), false);
  });
});

describe('qué operación hace un SQL', () => {
  it('reconoce las de lectura y las de escritura', () => {
    assert.equal(detectSqlOperation('SELECT 1'), 'select');
    assert.equal(detectSqlOperation('UPDATE t SET a=1'), 'update');
    assert.equal(detectSqlOperation('INSERT INTO t VALUES (1)'), 'insert');
    assert.equal(detectSqlOperation('DELETE FROM t'), 'delete');
  });

  it('no se deja engañar por espacios ni mayúsculas', () => {
    assert.equal(detectSqlOperation('   select 1'), 'select');
  });
});

describe('referencias a campos con tabla', () => {
  it('parte tabla y columna', () => {
    assert.deepEqual(parseQualifiedFieldRef('usuarios.nombre'), { table: 'usuarios', column: 'nombre' });
  });

  it('sin tabla lanza, no devuelve algo a medias', () => {
    // Caracterización: exige tabla explícita y lo dice. Es lo correcto — un campo sin
    // tabla en una consulta con varios JOIN es ambiguo, y resolverlo adivinando sería
    // peor que negarse.
    lanza(() => parseQualifiedFieldRef('nombre'), 'sin tabla');
  });
});

describe('tipos de script permitidos', () => {
  it('es uno, y es select', () => {
    // Esta prueba decía «son cuatro, y son los que tocan datos», y era cierto: la
    // lista era ['update', 'insert', 'delete', 'select'] y el sistema escribía en
    // producción con una confirmación. Cambió por decisión, no por un arreglo: un
    // artículo pide los datos que le falten y consulta, y ahí acaba.
    assert.deepEqual(DYNAMIC_SCRIPT_ALLOWED_TYPES, ['select']);
  });

  it('normalizar solo baja a minúsculas; no valida', () => {
    // Caracterización que conviene tener presente: esta función no rechaza nada. Quien
    // decide si el tipo está permitido es `assertDynamicScriptAllowed`, y confundirlas
    // dejaría pasar un tipo inventado.
    assert.equal(normalizeDynamicScriptType('UPDATE'), 'update');
    assert.equal(normalizeDynamicScriptType('tipo_inventado'), 'tipo_inventado');
  });
});

describe('operadores de condición permitidos', () => {
  it('es un conjunto cerrado de siete', () => {
    // Que sea cerrado es la garantía: un operador nuevo hay que añadirlo a mano, y
    // nada que no esté aquí llega a formar parte de una condición.
    assert.deepEqual([...ALLOWED_WHERE_OPERATORS].sort(),
      ['!=', '<', '<=', '=', '>', '>=', 'LIKE'].sort());
    assert.equal(ALLOWED_WHERE_OPERATORS.has('; drop table'), false);
  });
});

describe('SQL manual dentro de un artículo', () => {
  it('rechaza lo que no es lectura', () => {
    for (const sql of ['DROP TABLE usuarios', 'TRUNCATE usuarios', 'ALTER TABLE usuarios ADD x int']) {
      lanza(() => assertManualSqlAllowed(sql), sql);
    }
  });

  it('acepta un SELECT', () => {
    pasa(() => assertManualSqlAllowed('SELECT 1'));
  });
});

describe('la definición completa de un script de artículo', () => {
  it('rechaza lo que no es un objeto', () => {
    for (const malo of [null, [], 'texto', 5]) {
      lanza(() => validateBusinessScriptDefinition(malo), String(malo));
    }
  });

  it('un SQL manual vacío no pasa', () => {
    lanza(() => validateBusinessScriptDefinition({ modo: 'script', origen: 'manual-sql', sql: '   ' }));
  });

  it('un SQL manual de lectura pasa', () => {
    pasa(() => validateBusinessScriptDefinition({
      modo: 'script', origen: 'manual-sql', sql: 'SELECT 1',
    }));
  });

  it('un artículo de Mongo pasa sin SQL ninguno', () => {
    // Caracterización: con colección y una operación basta, y no se le exige sql.
    pasa(() => validateBusinessScriptDefinition({
      modo: 'script', origen: 'manual-sql',
      collection: 'conciliacion', operation: 'find',
    }));
  });
});

describe('el sistema ya no escribe', () => {
  // El cambio de política: un artículo pide los datos que le falten y consulta. Nada
  // más. Estas pruebas son la frontera — si alguien vuelve a añadir 'update' a la
  // lista de permitidas, aquí se entera.
  const esquema = {
    permiteTabla: () => true,
    columnasDe: () => ['id', 'cedula', 'nombre', 'saldo'],
    hayRelacion: () => true,
  };

  it('la lista de operaciones permitidas es de una, y es select', () => {
    assert.deepEqual(DYNAMIC_SCRIPT_ALLOWED_TYPES, ['select']);
  });

  it('rechaza UPDATE, INSERT y DELETE, y dice por qué', () => {
    for (const tipo of ['update', 'insert', 'delete']) {
      assert.throws(
        () => assertDynamicScriptAllowed({ tipo, tabla: 'socios', set: { a: 1 }, where: { id: 1 }, data: { a: 1 } }, esquema),
        new RegExp('ya no ejecuta ' + tipo.toUpperCase()),
        tipo + ' debería rechazarse con su nombre en el mensaje',
      );
    }
  });

  it('acepta un select que pide una cédula', () => {
    // El caso que sí tiene que funcionar: el artículo pregunta por una cédula.
    pasa(() => assertDynamicScriptAllowed(
      { tipo: 'select', tabla: 'socios', where: { cedula: '{{cedula}}' } }, esquema));
  });

  it('un tipo inventado también se rechaza, y se dice cuál era', () => {
    assert.throws(
      () => assertDynamicScriptAllowed({ tipo: 'truncate', tabla: 'socios' }, esquema),
      /Operación no permitida: truncate/,
    );
  });

  it('las mutaciones relacionales se niegan antes de mirar nada', () => {
    // Ni siquiera llega a comprobar tablas ni columnas: se niega y punto.
    for (const tipo of ['update', 'delete']) {
      assert.throws(
        () => validateRelationalMutationScriptDefinition({ tipo, tabla: 'socios' }, tipo, esquema),
        /ya no ejecuta/,
        tipo + ' relacional debería negarse',
      );
    }
  });

  it('un SQL manual de escritura sigue bloqueado', () => {
    for (const sql of ['UPDATE socios SET a=1 WHERE id=2', 'DELETE FROM socios WHERE id=2', 'INSERT INTO socios VALUES (1)']) {
      lanza(() => assertManualSqlAllowed(sql), sql);
    }
  });

  it('y un SQL manual con parámetros con nombre pasa', () => {
    // Es la forma de un artículo dinámico escrito a mano.
    pasa(() => assertManualSqlAllowed('SELECT nombre, saldo FROM socios WHERE cedula = :cedula'));
  });
});

describe('la puerta: se bloquea lo que cambia datos', () => {
  // Esta puerta se ha movido tres veces, y las tres se descubrio probando a mano. La
  // regla de ahora: se bloquea lo que cambia datos o estructura, y lo que no se puede
  // leer lo bastante como para saberlo. Todo lo demas se ejecuta, por complicado que sea.
  const rechaza = (sql) => {
    assert.throws(() => asegurarSoloConsulta(sql), undefined, `deberia rechazar: ${sql}`);
  };
  const deja = (sql) => {
    assert.doesNotThrow(() => asegurarSoloConsulta(sql), `deberia dejar pasar: ${sql}`);
  };

  it('rechaza lo que cambia los datos', () => {
    rechaza("UPDATE socios SET estado = 'activo'");
    rechaza('DELETE FROM socios');
    rechaza("INSERT INTO socios (cedula) VALUES ('x')");
    rechaza('MERGE INTO socios s USING t ON (s.id=t.id) WHEN MATCHED THEN UPDATE SET s.e = t.e');
    rechaza('TRUNCATE transacciones');
    rechaza("INSERT ALL INTO tareas (id) VALUES (1) SELECT * FROM dual");
  });

  it('rechaza lo que cambia la estructura', () => {
    // Un DROP TABLE se lleva por delante mas que cualquier DELETE.
    rechaza('DROP TABLE socios');
    rechaza('CREATE TABLE copia AS SELECT * FROM socios');
    rechaza('ALTER TABLE socios ADD x NUMBER');
    rechaza('GRANT ALL ON socios TO public');
    rechaza('SELECT * INTO copia FROM socios');
  });

  it('rechaza lo que mete filas aunque no lo diga su primera palabra', () => {
    rechaza("REPLACE INTO socios (id, cedula) VALUES (1,'x')");
    rechaza("LOAD DATA INFILE '/tmp/x.csv' INTO TABLE socios");
    rechaza("COPY socios FROM '/tmp/x.csv' CSV");
  });

  it('rechaza lo que escribe aunque empiece por SELECT', () => {
    rechaza('WITH ido AS (DELETE FROM socios WHERE id = 1 RETURNING *) SELECT * FROM ido');
    rechaza("WITH u AS (UPDATE socios SET estado='x' RETURNING *) SELECT count(*) FROM u");
    rechaza('WITH i AS (INSERT INTO socios VALUES (1) RETURNING *) SELECT * FROM i');
  });

  it('mira todas las sentencias, no solo la primera', () => {
    rechaza('SELECT 1; DROP TABLE socios');
    rechaza("SELECT 1; UPDATE socios SET estado='x'");
    rechaza('(SELECT 1); DELETE FROM socios');
  });

  it('rechaza lo que no se puede leer, y dice por que', () => {
    // No estan prohibidos: es que el UPDATE puede estar dentro del nombre, y la unica
    // forma de saberlo seria ejecutarlos.
    const motivo = /No se puede saber si eso cambia datos/;
    assert.throws(() => asegurarSoloConsulta('BEGIN pkg.recalcular_saldos(); END;'), motivo);
    assert.throws(() => asegurarSoloConsulta('CALL corresponsales.p_cerrar_mes()'), motivo);
    assert.throws(() => asegurarSoloConsulta("BEGIN EXECUTE IMMEDIATE 'DROP TABLE x'; END;"), motivo);
    assert.throws(() => asegurarSoloConsulta("DO $$ BEGIN UPDATE socios SET estado='x'; END $$"), motivo);
  });

  it('deja pasar las consultas complejas, que son la mayoria', () => {
    deja('SELECT * FROM socios');
    deja('WITH t AS (SELECT * FROM socios) SELECT * FROM t');
    deja('SELECT cedula, row_number() OVER (ORDER BY id) FROM socios');
    deja('SELECT cedula FROM socios UNION SELECT identificacion FROM corresponsales');
    deja('SELECT s.cedula, (SELECT count(*) FROM cuentas c WHERE c.socio_id = s.id) FROM socios s');
    deja('SELECT s.cedula, x.* FROM socios s, LATERAL (SELECT max(saldo) FROM cuentas WHERE socio_id = s.id) x');
    deja('SELECT sucursal, estado, count(*) FROM socios GROUP BY GROUPING SETS ((sucursal),(estado))');
    deja('WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 5) SELECT * FROM n');
    deja('(SELECT 1) UNION (SELECT 2)');
    deja('SELECT 1; SELECT 2;');
  });

  it('deja pasar las otras formas de leer', () => {
    // Quedaban como «no se reconoce» y se rechazaban, aunque no tocan un dato. Un
    // EXPLAIN es de las primeras cosas que escribe quien esta investigando algo.
    deja('EXPLAIN SELECT * FROM socios');
    deja('SHOW TABLES');
    deja('DESCRIBE socios');
    deja('TABLE socios');
    deja('VALUES (1),(2)');
    deja('COPY (SELECT * FROM socios) TO STDOUT');
  });

  it('deja pasar lo que no cambia datos aunque toque la sesion', () => {
    // Bloquear de mas obliga a la gente a buscarle la vuelta a la herramienta.
    deja('LOCK TABLE socios IN EXCLUSIVE MODE');
    deja('VACUUM FULL socios');
    deja("SET work_mem = '64MB'");
    deja('ANALYZE socios');
    // Toma candados, pero no cambia un solo dato.
    deja('SELECT * FROM socios WHERE id = 1 FOR UPDATE');
  });

  it('no confunde una palabra escrita con una orden', () => {
    deja("SELECT * FROM logs WHERE accion = 'delete'");
    deja('SELECT updated_at, deleted_at FROM socios');
    deja('WITH delete_candidates AS (SELECT 1 AS x) SELECT * FROM delete_candidates');
    deja('-- ojo: no hacer UPDATE aqui\nSELECT 1');
    deja("SELECT 'a;b' AS x");
  });

  it('lo vacio no es una escritura', () => {
    deja('');
    deja('   ');
    deja('-- nada');
  });
});
