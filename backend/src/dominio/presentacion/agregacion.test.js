/**
 * Pruebas de cómo se junta el resultado de varios pasos.
 *
 * Un flujo de cinco pasos devuelve cinco cosas y a quien lo lanzó hay que darle una.
 * Lo que importa aquí es que al juntarlas no se pierda ni se tape nada: dos pasos que
 * devuelven un campo con el mismo nombre tienen que seguir viéndose los dos, y el
 * recuento de filas tocadas tiene que ser el de verdad.
 *
 *   node --test src/dominio/presentacion/agregacion.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  addWithUniqueKey,
  buildAggregatedResultFromStepPayload,
  buildComplexWorkflowExplanation,
  buildDistributedSyntheticSql,
  buildPerStepWriteSummary,
  extractObjectForAggregation,
} from './agregacion.js';

describe('añadir una clave que ya existe', () => {
  it('la nueva no pisa la vieja: se renombra', () => {
    // Es lo que evita que el resultado de un paso borre el de otro cuando los dos
    // traen un campo con el mismo nombre.
    const o = { a: 1 };
    addWithUniqueKey(o, 'a', 2);
    assert.equal(o.a, 1, 'la primera se conserva');
    assert.deepEqual(Object.values(o).sort(), [1, 2], 'y la segunda sigue estando');
    assert.equal(Object.keys(o).length, 2);
  });

  it('una clave nueva entra con su nombre', () => {
    const o = {};
    addWithUniqueKey(o, 'a', 1);
    assert.deepEqual(o, { a: 1 });
  });
});

describe('sacar el objeto que se va a agregar', () => {
  it('de una lista de filas coge la primera', () => {
    assert.deepEqual(extractObjectForAggregation([{ a: 1 }, { a: 2 }]), { a: 1 });
  });

  it('un objeto suelto pasa tal cual', () => {
    assert.deepEqual(extractObjectForAggregation({ a: 1 }), { a: 1 });
  });

  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => extractObjectForAggregation([]));
    assert.doesNotThrow(() => extractObjectForAggregation(null));
  });
});

describe('juntar lo que devolvieron los pasos', () => {
  it('reúne los campos de todos', () => {
    const r = buildAggregatedResultFromStepPayload({ step1: [{ a: 1 }], step2: [{ b: 2 }] });
    assert.deepEqual(r, { a: 1, b: 2 });
  });

  it('con campos repetidos conserva los dos', () => {
    const r = buildAggregatedResultFromStepPayload({ step1: [{ total: 1 }], step2: [{ total: 9 }] });
    assert.equal(Object.keys(r).length, 2, 'los dos totales tienen que verse');
  });

  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => buildAggregatedResultFromStepPayload({}));
    assert.doesNotThrow(() => buildAggregatedResultFromStepPayload(null));
  });
});

describe('el resumen de lo que escribió cada paso', () => {
  it('sin pasos con detalle de escritura devuelve null', () => {
    // null y no un resumen vacío: así quien lo muestra sabe que no hay nada que contar
    // en vez de pintar una sección en blanco.
    assert.equal(buildPerStepWriteSummary([]), null);
    assert.equal(buildPerStepWriteSummary([{ explicacion: 'nada que ver' }]), null);
  });

  it('cuenta las filas afectadas de los pasos que escribieron', () => {
    const r = buildPerStepWriteSummary([
      { explicacion: '✅ UPDATE socios', affectedRows: 3 },
      { explicacion: '✅ DELETE socios', affectedRows: 2 },
    ]);
    assert.notEqual(r, null);
    assert.match(String(r), /5/, 'el total tiene que ser 3 + 2');
  });

  it('no revienta con lo que no es una lista', () => {
    assert.doesNotThrow(() => buildPerStepWriteSummary(null));
    assert.doesNotThrow(() => buildPerStepWriteSummary('texto'));
  });
});

describe('explicar un flujo completo', () => {
  it('suma las filas afectadas de todas las ejecuciones', () => {
    const r = buildComplexWorkflowExplanation(
      [{ type: 'for_loop' }],
      [{ affectedRows: 2 }, { affectedRows: 3 }],
      { data: [{ a: 1 }] },
    );
    assert.match(String(r), /5/);
  });

  it('con un flujo sin bucles tampoco revienta', () => {
    assert.doesNotThrow(() => buildComplexWorkflowExplanation([], [], { data: [] }));
  });
});

describe('el SQL que se muestra de una consulta repartida', () => {
  it('pone cada sentencia bajo el nombre de su base', () => {
    const r = buildDistributedSyntheticSql([
      { databaseId: 'pg_main', sql: 'SELECT 1' },
      { databaseId: 'oracle', sql: 'SELECT 2' },
    ]);
    assert.match(r, /pg_main/);
    assert.match(r, /oracle/);
    assert.match(r, /SELECT 1/);
    assert.match(r, /SELECT 2/);
  });

  it('una base sin nombre se marca, no se calla', () => {
    assert.match(buildDistributedSyntheticSql([{ sql: 'SELECT 1' }]), /unknown-db/);
  });

  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => buildDistributedSyntheticSql([]));
    assert.doesNotThrow(() => buildDistributedSyntheticSql(null));
  });
});
