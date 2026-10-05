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
  DYNAMIC_SCRIPT_ALLOWED_TYPES,
  assertManualSqlAllowed,
  detectSqlOperation,
  isPlainObject,
  normalizeDynamicScriptType,
  parseQualifiedFieldRef,
  validateBusinessScriptDefinition,
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
  it('son cuatro, y son los que tocan datos', () => {
    assert.deepEqual(DYNAMIC_SCRIPT_ALLOWED_TYPES, ['update', 'insert', 'delete', 'select']);
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
