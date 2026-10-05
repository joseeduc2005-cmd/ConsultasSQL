/**
 * Pruebas de la detección de escrituras.
 *
 * De esto depende qué se le muestra a un cliente sin permiso, si se pide confirmación
 * antes de lanzar algo y si se guarda una foto de las filas antes de tocarlas. Un
 * falso negativo —decir que algo solo lee cuando escribe— es el fallo grave: deja
 * pasar un cambio en producción sin confirmar y sin copia previa.
 *
 *   node --test src/dominio/sql/mutaciones.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  buildWhereClause,
  detectMongoOperation,
  parseWriteTarget,
  renumberWhereParams,
  scriptContainsMutation,
  sqlTextContainsMutation,
} from './mutaciones.js';

describe('un SQL que escribe', () => {
  it('reconoce las cuatro formas corrientes', () => {
    for (const sql of [
      'UPDATE socios SET nombre = $1',
      'INSERT INTO socios (nombre) VALUES ($1)',
      'DELETE FROM socios WHERE id = $1',
      'TRUNCATE socios',
    ]) {
      assert.equal(sqlTextContainsMutation(sql), true, sql);
    }
  });

  it('no se deja engañar por mayúsculas ni por el sangrado', () => {
    assert.equal(sqlTextContainsMutation('   update socios set a=1'), true);
    assert.equal(sqlTextContainsMutation('\n\n  DeLeTe FROM socios'), true);
  });

  it('un SELECT no escribe', () => {
    assert.equal(sqlTextContainsMutation('SELECT * FROM socios'), false);
    assert.equal(sqlTextContainsMutation('WITH x AS (SELECT 1) SELECT * FROM x'), false);
  });

  it('la palabra dentro de una cadena no cuenta como escritura', () => {
    // El falso positivo clásico: un socio llamado «Delete» o un texto que menciona
    // la palabra no convierten un SELECT en una escritura.
    assert.equal(sqlTextContainsMutation("SELECT * FROM socios WHERE nombre = 'update pendiente'"), false);
  });

  it('la palabra dentro de un comentario tampoco', () => {
    assert.equal(sqlTextContainsMutation('-- update de la semana pasada\nSELECT 1'), false);
  });

  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => sqlTextContainsMutation(''));
    assert.doesNotThrow(() => sqlTextContainsMutation(null));
    assert.doesNotThrow(() => sqlTextContainsMutation(undefined));
  });
});

describe('un script entero', () => {
  it('mira el SQL que lleva dentro', () => {
    assert.equal(scriptContainsMutation({ sql: 'UPDATE t SET a=1' }), true);
    assert.equal(scriptContainsMutation({ sql: 'SELECT 1' }), false);
  });

  it('no revienta con lo que no es un script', () => {
    assert.doesNotThrow(() => scriptContainsMutation(null));
    assert.doesNotThrow(() => scriptContainsMutation({}));
    assert.doesNotThrow(() => scriptContainsMutation('texto'));
  });
});

describe('una operación de Mongo', () => {
  // Recibe el texto JSON de la operación, no su nombre, y responde en el vocabulario
  // del SQL: 'select' si solo lee, 'insert' si escribe.
  it('las de lectura dan select', () => {
    assert.equal(detectMongoOperation('{"find":"socios"}'), 'select');
    assert.equal(detectMongoOperation('{"aggregate":"socios"}'), 'select');
  });

  it('las de escritura dan insert', () => {
    for (const op of ['insertOne', 'insertMany', 'updateOne', 'updateMany', 'deleteOne', 'deleteMany']) {
      assert.equal(detectMongoOperation('{"' + op + '":"socios"}'), 'insert', op);
    }
  });

  it('un JSON roto se trata como lectura', () => {
    // Caracterización de algo que conviene tener a la vista: si el texto no es JSON
    // válido, esto responde 'select'. Para una comprobación de si algo escribe, el
    // lado seguro sería el contrario — y conviene que esté escrito.
    assert.equal(detectMongoOperation('{roto'), 'select');
    assert.equal(detectMongoOperation('updateOne'), 'select', 'el nombre suelto no es JSON');
  });

  it('no revienta con lo ausente', () => {
    assert.doesNotThrow(() => detectMongoOperation(null));
    assert.doesNotThrow(() => detectMongoOperation(''));
  });
});

describe('a qué apunta una escritura', () => {
  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => parseWriteTarget(null));
    assert.doesNotThrow(() => parseWriteTarget(''));
    assert.doesNotThrow(() => parseWriteTarget({}));
  });
});

describe('renumerar los marcadores de una condición', () => {
  // No recibe un desplazamiento: recibe la lista entera de valores y devuelve la
  // condición renumerada desde $1 junto con solo los valores que esa condición usa.
  it('compacta los marcadores y se queda con sus valores', () => {
    const r = renumberWhereParams('a = $3 AND b = $5', ['v1', 'v2', 'v3', 'v4', 'v5']);
    assert.equal(r.clause, 'a = $1 AND b = $2');
    assert.deepEqual(r.values, ['v3', 'v5']);
  });

  it('un marcador repetido se queda en uno', () => {
    // Importa: si $1 sale dos veces, el valor no debe viajar duplicado, porque la
    // numeración y la lista de valores dejarían de cuadrar.
    const r = renumberWhereParams('a = $2 OR b = $2', ['x', 'y']);
    assert.equal(r.clause, 'a = $1 OR b = $1');
    assert.deepEqual(r.values, ['y']);
  });

  it('sin marcadores devuelve la condición igual y sin valores', () => {
    assert.deepEqual(renumberWhereParams('a = b', ['x']), { clause: 'a = b', values: [] });
  });

  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => renumberWhereParams());
  });
});

describe('armar una condición', () => {
  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => buildWhereClause({}, {}, 1));
    assert.doesNotThrow(() => buildWhereClause(null, null, 1));
  });
});
