/**
 * Pruebas del lector de consultas de Mongo y de su puerta.
 *
 * El documento del que salen la mitad de los casos es uno real de conciliación, con sus
 * `ObjectId`, sus `ISODate`, sus `NumberInt` y un arreglo anidado de detalles. Está aquí
 * porque es exactamente lo que alguien copia de Compass y pega — y lo que antes reventaba
 * con «Unexpected token 'O'».
 *
 *   node --test src/dominio/mongo/consulta.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  asegurarConsultaDeMongo,
  operacionDeMongo,
  parsearConsultaMongo,
} from './consulta.js';

describe('leer una consulta como la escribe la gente', () => {
  it('acepta JSON estricto, que es un subconjunto', () => {
    assert.deepEqual(
      parsearConsultaMongo('{"collection":"c","find":{"a":1}}'),
      { collection: 'c', find: { a: 1 } },
    );
  });

  it('acepta claves sin comillas y comillas simples', () => {
    assert.deepEqual(
      parsearConsultaMongo("{ collection: 'c', find: { estado: 'activo' } }"),
      { collection: 'c', find: { estado: 'activo' } },
    );
  });

  it('entiende los envoltorios de tipo de la consola', () => {
    const texto = "{ _id: ObjectId('6824b0c404c5ed34e4070f02'),"
      + " institutionCode: NumberInt('4'),"
      + " conciliationDate: ISODate('2025-05-14T10:03:32.007Z'),"
      + " debitTotal: Double('234.54'),"
      + " netAmount: Double('-219.54') }";
    assert.deepEqual(parsearConsultaMongo(texto), {
      _id: { $oid: '6824b0c404c5ed34e4070f02' },
      institutionCode: 4,
      conciliationDate: { $date: '2025-05-14T10:03:32.007Z' },
      debitTotal: 234.54,
      netAmount: -219.54,
    });
  });

  it('entra en los arreglos anidados, que es donde vive el detalle', () => {
    const texto = "{ details: [ { id: NumberInt('1'), observation: 'OK',"
      + " transactionDateSwitch: ISODate('2025-05-04T16:50:56.655Z'),"
      + ' transactionDateInstitution: null } ] }';
    const leido = parsearConsultaMongo(texto);
    assert.equal(leido.details.length, 1);
    assert.equal(leido.details[0].id, 1);
    assert.deepEqual(leido.details[0].transactionDateSwitch, { $date: '2025-05-04T16:50:56.655Z' });
    assert.equal(leido.details[0].transactionDateInstitution, null);
  });

  it('acepta new Date, expresiones regulares y comentarios', () => {
    assert.deepEqual(parsearConsultaMongo("{ f: new Date('2025-01-01') }"), { f: { $date: '2025-01-01' } });
    assert.deepEqual(
      parsearConsultaMongo('{ nombre: /juan/i }'),
      { nombre: { $regularExpression: { pattern: 'juan', options: 'i' } } },
    );
    assert.deepEqual(parsearConsultaMongo('{ /* nota */ a: 1 /* otra */ }'), { a: 1 });
  });

  it('tolera la coma de más, que es el error de copiar y pegar', () => {
    assert.deepEqual(parsearConsultaMongo('{ a: 1, b: 2, }'), { a: 1, b: 2 });
    assert.deepEqual(parsearConsultaMongo('[1, 2, ]'), [1, 2]);
  });

  it('un objeto ya construido pasa tal cual', () => {
    const o = { collection: 'c', find: {} };
    assert.equal(parsearConsultaMongo(o), o);
  });

  it('se queja con el sitio, en vez de reventar', () => {
    assert.throws(() => parsearConsultaMongo('{ collection: '), /No se entiende/);
    assert.throws(() => parsearConsultaMongo('{ a: Borrar("todo") }'), /no se reconoce «Borrar/);
    assert.throws(() => parsearConsultaMongo(''), /vacía/);
  });
});

describe('qué operación pide una consulta', () => {
  it('la lee del campo que la declara', () => {
    assert.equal(operacionDeMongo({ collection: 'c', operation: 'findOne' }), 'findone');
  });

  it('o de la clave que la nombra', () => {
    assert.equal(operacionDeMongo({ collection: 'c', aggregate: [] }), 'aggregate');
    assert.equal(operacionDeMongo({ collection: 'c', deleteMany: {} }), 'deletemany');
  });

  it('un filtro suelto es una búsqueda, que es como se guardaron los artículos viejos', () => {
    assert.equal(operacionDeMongo({ collection: 'c', filter: '{}' }), 'find');
    assert.equal(operacionDeMongo({ collection: 'c', pipeline: '[]' }), 'aggregate');
  });

  it('y si no se puede saber, no se inventa', () => {
    assert.equal(operacionDeMongo({ collection: 'c' }), null);
  });
});

describe('la puerta de Mongo: solo lectura, igual que en SQL', () => {
  const rechaza = (c) => assert.throws(() => asegurarConsultaDeMongo(c), undefined, `deberia rechazar: ${JSON.stringify(c)}`);
  const deja = (c) => assert.doesNotThrow(() => asegurarConsultaDeMongo(c), `deberia dejar pasar: ${JSON.stringify(c)}`);

  it('deja consultar', () => {
    deja("{ collection: 'c', find: { a: 1 } }");
    deja("{ collection: 'c', findOne: { a: 1 } }");
    deja("{ collection: 'c', aggregate: [ { $match: { a: 1 } }, { $unwind: '$details' } ] }");
    deja("{ collection: 'c', countDocuments: {} }");
    deja("{ collection: 'c', distinct: 'estado' }");
  });

  it('no deja escribir', () => {
    // Esto no se comprobaba en ningun sitio: el validador de articulos hacia `return`
    // para Mongo con el motivo de que no habia SQL que validar, y con eso un deleteMany
    // se guardaba sin que nadie dijera nada.
    rechaza("{ collection: 'c', deleteMany: {} }");
    rechaza("{ collection: 'c', deleteOne: {} }");
    rechaza("{ collection: 'c', updateMany: { filter: {}, update: { $set: { a: 1 } } } }");
    rechaza("{ collection: 'c', updateOne: { filter: {}, update: {} } }");
    rechaza("{ collection: 'c', insertOne: { a: 1 } }");
    rechaza("{ collection: 'c', insertMany: [] }");
    rechaza("{ collection: 'c', replaceOne: {} }");
    rechaza("{ collection: 'c', drop: {} }");
    rechaza("{ collection: 'c', bulkWrite: [] }");
    rechaza({ collection: 'c', operation: 'deleteMany', filter: '{}' });
  });

  it('un agregado que termina guardando tampoco es una lectura', () => {
    // $out y $merge guardan el resultado en una colección: se llama aggregate y escribe,
    // igual que el CTE que borra se llama SELECT y borra.
    rechaza("{ collection: 'c', aggregate: [ { $match: {} }, { $out: 'copia' } ] }");
    rechaza("{ collection: 'c', aggregate: [ { $merge: { into: 'copia' } } ] }");
  });

  it('lo que no se entiende no entra', () => {
    rechaza({ collection: 'c' });
    rechaza("{ collection: 'c', loQueSea: {} }");
  });
});
