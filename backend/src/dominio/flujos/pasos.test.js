/**
 * Pruebas de las reglas de un flujo.
 *
 * Esto decide si un paso se ejecuta o se salta, cuántas veces corre un bucle y qué
 * valor lleva una variable de un paso al siguiente. Un error aquí no da un mensaje
 * raro: hace que un paso se ejecute cuando no debía, contra una base de producción.
 *
 *   node --test src/dominio/flujos/pasos.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  collectStructuredVariables,
  evaluateSimpleCondition,
  extractVariables,
  formatRaiseNoticeMessage,
  getManualSqlStepTitle,
  isEmptyWorkflowValue,
  normalizeWorkflowStoredValue,
  parseStepOrder,
  replaceVariables,
} from './pasos.js';

describe('evaluar una condición de paso', () => {
  it('compara números', () => {
    assert.equal(evaluateSimpleCondition('v_count > 0', { v_count: 5 }), true);
    assert.equal(evaluateSimpleCondition('v_count > 0', { v_count: 0 }), false);
    assert.equal(evaluateSimpleCondition('v_count = 3', { v_count: 3 }), true);
  });

  it('entiende la variable escrita con llaves', () => {
    assert.equal(evaluateSimpleCondition('{{v_count}} > 0', { v_count: 2 }), true);
    assert.equal(evaluateSimpleCondition('{{ v_count }} > 0', { v_count: 0 }), false);
  });

  it('lee el valor de un paso anterior', () => {
    assert.equal(evaluateSimpleCondition('step1.count > 0', { step1: { count: 4 } }), true);
    assert.equal(evaluateSimpleCondition('step1.count > 0', { step1: { count: 0 } }), false);
  });

  it('compara textos y booleanos', () => {
    assert.equal(evaluateSimpleCondition("estado = 'ok'", { estado: 'ok' }), true);
    assert.equal(evaluateSimpleCondition("estado = 'ok'", { estado: 'mal' }), false);
    assert.equal(evaluateSimpleCondition('activo = true', { activo: true }), true);
  });

  it('lo que no sabe evaluar lo da por cierto, y eso hay que tenerlo escrito', () => {
    // Caracterización de una decisión con consecuencias: una condición que no encaja
    // con el patrón no bloquea el paso, lo deja pasar. Para un paso que escribe en
    // producción, el lado seguro sería el contrario — una condición mal escrita en un
    // artículo ejecuta el paso en vez de detenerlo.
    assert.equal(evaluateSimpleCondition('esto no es una condicion', {}), true);
    assert.equal(evaluateSimpleCondition('', {}), true);
    assert.equal(evaluateSimpleCondition(null, {}), true);
  });
});

describe('el orden de los pasos', () => {
  it('lee el número de step1, step2…', () => {
    assert.equal(parseStepOrder('step1'), 1);
    assert.equal(parseStepOrder('step12'), 12);
    assert.equal(parseStepOrder('STEP3'), 3);
  });

  it('lo que no es un paso numerado va al final', () => {
    // Así un paso con nombre raro no se cuela delante de los numerados.
    assert.equal(parseStepOrder('validacion'), Number.MAX_SAFE_INTEGER);
    assert.equal(parseStepOrder(''), Number.MAX_SAFE_INTEGER);
    assert.equal(parseStepOrder('step'), Number.MAX_SAFE_INTEGER);
  });
});

describe('qué cuenta como valor vacío en un flujo', () => {
  it('el cero y el false NO están vacíos', () => {
    // La trampa clásica: un saldo de cero o un estado «false» son valores, no ausencias.
    assert.equal(isEmptyWorkflowValue(0), false);
    assert.equal(isEmptyWorkflowValue(false), false);
  });

  it('el texto vacío, null y undefined sí', () => {
    assert.equal(isEmptyWorkflowValue(''), true);
    assert.equal(isEmptyWorkflowValue(null), true);
    assert.equal(isEmptyWorkflowValue(undefined), true);
  });
});

describe('las variables que un script necesita', () => {
  it('las encuentra dentro de un objeto anidado', () => {
    assert.deepEqual([...collectStructuredVariables({ a: '{{x}}', b: ['{{y}}'] })].sort(), ['x', 'y']);
  });

  it('también las de un SQL con parámetros con nombre', () => {
    const v = [...collectStructuredVariables({ sql: 'SELECT * FROM t WHERE c = :cedula' })];
    assert.equal(v.includes('cedula'), true);
  });

  it('extractVariables necesita el script, no un texto suelto', () => {
    // Caracterización: recibe el script estructurado (objeto o su JSON), no una
    // plantilla. Pasarle un texto devuelve lista vacía, que es fácil de confundir
    // con «este script no usa variables».
    assert.deepEqual(extractVariables('{{a}} y {{b}}'), []);
    assert.deepEqual(extractVariables({ sql: '{{a}}' }).sort(), ['a']);
  });
});

describe('sustituir las variables de un script', () => {
  it('rellena el hueco con el valor que llegó', () => {
    const r = replaceVariables({ sql: 'SELECT * FROM t WHERE id = {{id}}' }, { id: 7 });
    assert.equal(r.sql.includes('7'), true);
  });

  it('sin script devuelve null, no un objeto a medias', () => {
    assert.equal(replaceVariables(null, { id: 1 }), null);
    assert.equal(replaceVariables('', {}), null);
  });

  it('un texto que no es JSON tampoco pasa por script', () => {
    assert.equal(replaceVariables('hola {{nombre}}', { nombre: 'Ana' }), null);
  });
});

describe('el título de un paso de SQL manual', () => {
  it('lo deduce de lo que hace la sentencia', () => {
    assert.match(getManualSqlStepTitle('SELECT * FROM socios', 0), /Consultando/);
    assert.match(getManualSqlStepTitle('UPDATE socios SET a=1', 1), /Actualizando/);
    assert.match(getManualSqlStepTitle('INSERT INTO socios VALUES (1)', 2), /Insertando/);
    assert.match(getManualSqlStepTitle('DELETE FROM socios', 3), /Eliminando/);
  });

  it('lleva el número del paso', () => {
    assert.match(getManualSqlStepTitle('SELECT 1', 4), /Paso 4/);
  });
});

describe('un aviso de la base', () => {
  it('mete los valores donde van', () => {
    assert.equal(formatRaiseNoticeMessage('saldo: %', [5]), 'saldo: 5');
  });

  it('sin valores no revienta', () => {
    assert.doesNotThrow(() => formatRaiseNoticeMessage('nada que sustituir', []));
    assert.doesNotThrow(() => formatRaiseNoticeMessage(null, null));
  });
});

describe('guardar el resultado de un paso', () => {
  it('conserva las filas tal cual', () => {
    assert.deepEqual(normalizeWorkflowStoredValue([{ a: 1 }]), [{ a: 1 }]);
  });

  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => normalizeWorkflowStoredValue(null));
    assert.doesNotThrow(() => normalizeWorkflowStoredValue([]));
  });
});
