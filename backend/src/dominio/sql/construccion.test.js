/**
 * Pruebas de caracterización del armado de SQL.
 *
 * Aquí es donde una definición se convierte en una sentencia. Lo que más importa no es
 * que el texto salga bonito, sino que los valores viajen como parámetros y nunca
 * pegados dentro del SQL — que es la diferencia entre una consulta y una inyección.
 *
 *   node --test src/dominio/sql/construccion.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  countPositionalSqlPlaceholders,
  getValueByPath,
  isMissingParamValue,
  isWriteConfirmed,
  normalizeLimitValue,
  normalizeManualSqlParamsInput,
} from './construccion.js';

describe('contar marcadores de una sentencia', () => {
  it('cuenta los distintos, no las apariciones', () => {
    assert.equal(countPositionalSqlPlaceholders('SELECT $1, $2 FROM t WHERE a = $1'), 2);
  });

  it('una sentencia sin marcadores no tiene ninguno', () => {
    assert.equal(countPositionalSqlPlaceholders('SELECT 1'), 0);
    assert.equal(countPositionalSqlPlaceholders(''), 0);
  });
});

describe('qué cuenta como valor ausente', () => {
  it('null, undefined y el texto vacío faltan', () => {
    assert.equal(isMissingParamValue(null), true);
    assert.equal(isMissingParamValue(undefined), true);
    assert.equal(isMissingParamValue(''), true);
  });

  it('el cero y el false NO faltan', () => {
    // Esta es la trampa clásica: con una comprobación de verdad/falsedad, un saldo de
    // cero o un estado «false» se tratarían como si no los hubieran mandado.
    assert.equal(isMissingParamValue(0), false);
    assert.equal(isMissingParamValue(false), false);
  });
});

describe('leer un valor por su ruta', () => {
  it('baja por los puntos', () => {
    assert.equal(getValueByPath({ a: { b: { c: 7 } } }, 'a.b.c'), 7);
  });

  it('una ruta que no existe da undefined, no revienta', () => {
    assert.equal(getValueByPath({ a: 1 }, 'a.b.c'), undefined);
    assert.equal(getValueByPath(null, 'a'), undefined);
  });
});

describe('confirmación para escribir', () => {
  it('busca una marca concreta dentro de los parámetros', () => {
    // No recibe un booleano: mira `__write_confirmed` dentro de lo que llegó. Que haya
    // que mandar esa marca, y que un valor cualquiera no valga, es lo que evita que una
    // escritura se cuele por un campo mal rellenado.
    assert.equal(isWriteConfirmed({ __write_confirmed: true }), true);
    assert.equal(isWriteConfirmed({ __write_confirmed: 'true' }), true);
    assert.equal(isWriteConfirmed({ __write_confirmed: 'si' }), false);
    assert.equal(isWriteConfirmed({ __write_confirmed: 1 }), false);
    assert.equal(isWriteConfirmed({}), false);
    assert.equal(isWriteConfirmed(undefined), false);
  });

  it('pasarle un booleano suelto no confirma nada', () => {
    // Conviene tenerlo escrito: quien la llame mal se queda sin escribir, que es el
    // lado seguro del error.
    assert.equal(isWriteConfirmed(true), false);
  });
});

describe('normalizar el límite de filas', () => {
  it('acepta un número, venga como número o como texto', () => {
    assert.equal(normalizeLimitValue(50), 50);
    assert.equal(normalizeLimitValue('50'), 50);
  });

  it('lo ausente cae al valor por defecto', () => {
    assert.equal(normalizeLimitValue(null), 0);
    assert.equal(normalizeLimitValue(undefined, 100), 100);
    assert.equal(normalizeLimitValue(''), 0);
  });

  it('lo que no es un número positivo lanza, no se cuela', () => {
    // Importa que lance en vez de caer al valor por defecto: un límite inválido suele
    // ser un error de quien llama, y taparlo devolvería un resultado que no pidió.
    assert.throws(() => normalizeLimitValue('; DROP TABLE t'), /Límite inválido/);
    assert.throws(() => normalizeLimitValue(0), /Límite inválido/);
    assert.throws(() => normalizeLimitValue(-5), /Límite inválido/);
  });
});

describe('normalizar los parámetros que llegan', () => {
  it('un objeto pasa tal cual', () => {
    const r = normalizeManualSqlParamsInput({ cedula: '0102030405' });
    assert.equal(typeof r, 'object');
  });

  it('lo ausente no revienta', () => {
    assert.doesNotThrow(() => normalizeManualSqlParamsInput(null));
    assert.doesNotThrow(() => normalizeManualSqlParamsInput(undefined));
  });
});
