/**
 * Pruebas de las decisiones que se toman mirando el texto.
 *
 * La que más importa es la de las escrituras sin WHERE. Un UPDATE al que se le olvidó
 * la condición no da error: hace su trabajo sobre la tabla entera, y para cuando
 * alguien se da cuenta ya está hecho. Esta comprobación es lo único que está entre
 * eso y una tabla de producción, así que conviene probarla con ejemplos raros.
 *
 *   node --test src/dominio/consultas/decidir.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  buildCrossDbSelectOptions,
  collectStructuredRequiredParams,
  detectMainTableFromText,
  enforceStrictProductionSqlPayload,
  hasUnsafeMutationWithoutWhere,
  isStrictSqlResponsePath,
  tableLooksLikeUsers,
} from './decidir.js';

describe('una escritura sin WHERE', () => {
  it('la reconoce en las tres formas que tocan todas las filas', () => {
    assert.equal(hasUnsafeMutationWithoutWhere('UPDATE socios SET activo = false'), true);
    assert.equal(hasUnsafeMutationWithoutWhere('DELETE FROM socios'), true);
  });

  it('con WHERE no la marca', () => {
    assert.equal(hasUnsafeMutationWithoutWhere('UPDATE socios SET activo = false WHERE id = 1'), false);
    assert.equal(hasUnsafeMutationWithoutWhere('DELETE FROM socios WHERE id = 1'), false);
  });

  it('un SELECT sin WHERE no es una escritura', () => {
    assert.equal(hasUnsafeMutationWithoutWhere('SELECT * FROM socios'), false);
  });

  it('no se deja engañar por mayúsculas ni por saltos de línea', () => {
    assert.equal(hasUnsafeMutationWithoutWhere('update socios\n  set activo = false'), true);
    assert.equal(hasUnsafeMutationWithoutWhere('UPDATE socios\nSET a = 1\nWHERE id = 2'), false);
  });

  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => hasUnsafeMutationWithoutWhere(''));
    assert.doesNotThrow(() => hasUnsafeMutationWithoutWhere(null));
  });
});

describe('qué tabla parece la protagonista', () => {
  it('la encuentra por su nombre en el texto', () => {
    assert.equal(detectMainTableFromText('dame los usuarios', ['users', 'roles']), 'users');
    assert.equal(detectMainTableFromText('lista de roles', ['users', 'roles']), 'roles');
  });

  it('cuando no reconoce nada cae en la que parece de usuarios', () => {
    assert.equal(detectMainTableFromText('texto sin pistas', ['roles', 'users']), 'users');
  });

  it('sin lista de tablas devuelve vacío, no revienta', () => {
    // Recibe la lista en vez de ir a buscarla: de dónde sale es cosa de PostgreSQL.
    assert.equal(detectMainTableFromText('usuarios'), '');
    assert.equal(detectMainTableFromText('usuarios', []), '');
  });
});

describe('qué tabla parece ser la de usuarios', () => {
  it('reconoce las formas corrientes', () => {
    assert.equal(tableLooksLikeUsers('users'), true);
    assert.equal(tableLooksLikeUsers('usuarios'), true);
  });

  it('y no confunde otras', () => {
    assert.equal(tableLooksLikeUsers('roles'), false);
    assert.equal(tableLooksLikeUsers(''), false);
  });
});

describe('los parámetros que un script exige', () => {
  it('junta los declarados con los que salen de las variables', () => {
    const r = collectStructuredRequiredParams({ parametros_requeridos: ['cedula'], sql: '{{mes}}' });
    assert.equal(r.includes('cedula'), true);
  });

  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => collectStructuredRequiredParams(null));
    assert.doesNotThrow(() => collectStructuredRequiredParams({}));
  });
});

describe('las bases que se ofrecen para una consulta', () => {
  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => buildCrossDbSelectOptions('', [], []));
    assert.doesNotThrow(() => buildCrossDbSelectOptions(null, null, null));
  });
});

describe('las rutas con respuesta estricta', () => {
  it('distingue las que la llevan de las que no', () => {
    assert.equal(typeof isStrictSqlResponsePath('/api/query/execute-generated'), 'boolean');
    assert.doesNotThrow(() => isStrictSqlResponsePath(null));
  });

  it('exigir el formato estricto no revienta con lo vacío', () => {
    assert.doesNotThrow(() => enforceStrictProductionSqlPayload(null));
    assert.doesNotThrow(() => enforceStrictProductionSqlPayload({}));
  });
});
