/**
 * Pruebas de caracterización de la normalización de texto.
 *
 * Son funciones chicas, pero de ellas dependen las claves con las que el sistema
 * guarda y compara: un término del aprendizaje semántico, el nombre de un proceso. Si
 * una cambia de criterio, lo guardado antes deja de encontrarse.
 *
 *   node --test src/dominio/texto/normalizar.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  dedupeSemanticValues,
  normalizeGeneratedSelectSql,
  normalizeSemanticLearningTerm,
  slugifyProcessName,
} from './normalizar.js';

describe('término del aprendizaje semántico', () => {
  it('quita tildes y pasa a minúsculas', () => {
    assert.equal(normalizeSemanticLearningTerm('Número'), 'numero');
    assert.equal(normalizeSemanticLearningTerm('  CÓDIGO  '), 'codigo');
  });

  it('conserva el guion bajo, porque los nombres de columna lo llevan', () => {
    assert.equal(normalizeSemanticLearningTerm('codigo_socio'), 'codigo_socio');
  });

  it('convierte lo demás en guion bajo y limpia los extremos', () => {
    assert.equal(normalizeSemanticLearningTerm('saldo pendiente'), 'saldo_pendiente');
    assert.equal(normalizeSemanticLearningTerm('¿cuánto?'), 'cuanto');
    assert.equal(normalizeSemanticLearningTerm('--hola--'), 'hola');
  });

  it('lo vacío queda vacío', () => {
    assert.equal(normalizeSemanticLearningTerm(''), '');
    assert.equal(normalizeSemanticLearningTerm(null), '');
  });
});

describe('nombre de proceso', () => {
  it('hace lo mismo pero sin conservar el guion bajo de entrada', () => {
    // La diferencia con la anterior no es casualidad: aquí el guion bajo es el
    // separador que se produce, no un carácter que se respeta.
    assert.equal(slugifyProcessName('Cierre Diario'), 'cierre_diario');
    assert.equal(slugifyProcessName('Conciliación-2026'), 'conciliacion_2026');
  });

  it('colapsa los separados seguidos en uno', () => {
    assert.equal(slugifyProcessName('a   b'), 'a_b');
  });
});

describe('quitar repetidos de una lista', () => {
  it('recorta, descarta vacíos y no repite', () => {
    assert.deepEqual(dedupeSemanticValues([' a ', 'a', '', null, 'b']), ['a', 'b']);
  });

  it('una lista ausente da una lista vacía', () => {
    assert.deepEqual(dedupeSemanticValues(), []);
    assert.deepEqual(dedupeSemanticValues(null), []);
  });
});

describe('reparar el SQL generado', () => {
  it('pone el cast que falta antes de ILIKE', () => {
    // El generador escribe ILIKE sobre columnas que no son texto y PostgreSQL lo
    // rechaza. Esto lo arregla en vez de devolver un error al usuario.
    const salida = normalizeGeneratedSelectSql('WHERE "t1"."activo" ILIKE \'%x%\'');
    assert.ok(salida.includes('"t1"."activo"::text ILIKE'));
  });

  it('no vuelve a castear lo que ya está casteado', () => {
    const entrada = 'WHERE "t1"."activo"::text ILIKE \'%x%\'';
    assert.equal(normalizeGeneratedSelectSql(entrada), entrada);
  });

  it('deja en paz lo que no tiene ILIKE', () => {
    const entrada = 'SELECT * FROM t WHERE a = 1';
    assert.equal(normalizeGeneratedSelectSql(entrada), entrada);
  });

  it('lo vacío pasa tal cual', () => {
    assert.equal(normalizeGeneratedSelectSql(''), '');
  });
});
