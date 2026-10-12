/**
 * Pruebas de caracterización de lo que se le muestra a una persona.
 *
 * Estas funciones no deciden qué se ejecuta, deciden qué se lee después — y eso es lo
 * que la gente juzga. Si una cambia de criterio sin querer, el sistema sigue
 * funcionando y simplemente empieza a explicar mal, o a enseñar lo que no debía.
 *
 *   node --test src/dominio/presentacion/explicacion.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  buildHumanSqlExplanation,
  buildProductionSqlResponse,
  sanitizeProductionResultObject,
} from './explicacion.js';

describe('contar en castellano cómo fue', () => {
  // El nombre dice «SqlExplanation» pero no recibe SQL: recibe las filas y describe
  // el resultado. Conviene saberlo antes de buscar aquí una explicación de la consulta.
  it('cuando no hubo filas lo dice', () => {
    assert.match(buildHumanSqlExplanation([]), /no devolvió registros/i);
  });

  it('cuenta los registros cuando los hubo', () => {
    assert.match(buildHumanSqlExplanation([{ id: 1 }]), /1 registro/);
  });

  it('entiende las respuestas de Mongo y las cuenta como lo que son', () => {
    assert.match(buildHumanSqlExplanation([{ insertedId: 'abc123' }]), /insertado/i);
  });

  it('no revienta con lo que no es una lista', () => {
    assert.doesNotThrow(() => buildHumanSqlExplanation(null));
    assert.doesNotThrow(() => buildHumanSqlExplanation('texto'));
  });
});

describe('qué sale y qué no de un resultado', () => {
  it('deja pasar los valores corrientes', () => {
    const salida = sanitizeProductionResultObject({ id: 1, nombre: 'Ana', activo: true });
    assert.equal(salida.id, 1);
    assert.equal(salida.nombre, 'Ana');
    assert.equal(salida.activo, true);
  });

  it('quita las claves sensibles aunque vengan con datos dentro', () => {
    // Esta es la que de verdad importa: si una consulta trae por error una columna de
    // contraseñas o un token, no debe llegar a la pantalla.
    const salida = sanitizeProductionResultObject({
      usuario: 'ana',
      password: 'secreta',
      api_key: 'abc',
      refresh_token: 'xyz',
      hash: 'def',
    });
    assert.equal(salida.usuario, 'ana');
    for (const prohibida of ['password', 'api_key', 'refresh_token', 'hash']) {
      assert.equal(prohibida in salida, false, prohibida + ' no debería salir');
    }
  });

  it('quita lo que es plomería del motor, no datos', () => {
    const salida = sanitizeProductionResultObject({ nombre: 'Ana', sql: 'SELECT 1', debug: 'x', __interno: 1 });
    assert.equal(salida.nombre, 'Ana');
    assert.equal('sql' in salida, false);
    assert.equal('debug' in salida, false);
  });

  it('solo conserva primitivos: una fecha cruda se cae', () => {
    // Caracterización, y conviene tenerla presente: si una consulta devuelve una fecha
    // y no aparece en pantalla, no es un fallo de la consulta, es esta regla.
    const salida = sanitizeProductionResultObject({ cuando: new Date('2026-01-15T10:00:00Z') });
    assert.deepEqual(salida, {});
  });

  it('no revienta con nulos ni con lo vacío', () => {
    assert.doesNotThrow(() => sanitizeProductionResultObject(null));
    assert.doesNotThrow(() => sanitizeProductionResultObject({}));
    assert.doesNotThrow(() => sanitizeProductionResultObject([]));
  });
});

describe('armar la respuesta de una consulta', () => {
  it('trae el resumen en castellano y las filas', () => {
    const r = buildProductionSqlResponse({ rows: [{ id: 1 }], rowCount: 1, command: 'SELECT' });
    assert.match(r.resumenHumano, /1 registro/);
    assert.deepEqual(r.resultado, [{ id: 1 }]);
  });

  it('sin filas, lo dice y no inventa resultado', () => {
    const r = buildProductionSqlResponse({ rows: [], rowCount: 0, command: 'SELECT' });
    assert.match(r.resumenHumano, /no devolvió registros/i);
  });

  it('con un objeto vacío responde, con null revienta', () => {
    // Caracterización de una fragilidad: a diferencia de sus vecinas, esta exige un
    // objeto. Hoy no molesta porque siempre se la llama con el resultado del motor,
    // pero conviene que esté escrito antes de que alguien la llame desde otro sitio.
    assert.doesNotThrow(() => buildProductionSqlResponse({}));
    assert.throws(() => buildProductionSqlResponse(null), TypeError);
  });
});
