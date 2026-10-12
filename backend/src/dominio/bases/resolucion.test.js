/**
 * Pruebas de caracterización de a qué base va una consulta.
 *
 * Equivocarse aquí es correr una consulta contra la base que no era. Antes no se podía
 * probar sin arrancar el servidor y conectar seis bases; ahora basta con una lista de
 * tres objetos.
 *
 *   node --test src/dominio/bases/resolucion.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { extractDatabaseHintsFromText, resolveRequestedDatabasesForQuery } from './resolucion.js';

/** Las mismas que hay configuradas de verdad, con sus motores. */
const BASES = [
  { id: 'pg_main', type: 'postgres' },
  { id: 'oracle_local', type: 'oracle' },
  { id: 'mysql_test', type: 'mysql' },
  { id: 'mongo_local', type: 'mongodb' },
];

describe('qué base menciona un texto', () => {
  it('reconoce el identificador exacto', () => {
    assert.deepEqual(extractDatabaseHintsFromText('revisa pg_main por favor', BASES), ['pg_main']);
  });

  it('reconoce el motor, y señala todas las de ese motor', () => {
    assert.deepEqual(extractDatabaseHintsFromText('esto es de oracle', BASES), ['oracle_local']);
    assert.deepEqual(extractDatabaseHintsFromText('en mysql', BASES), ['mysql_test']);
  });

  it('entiende las formas de nombrar a PostgreSQL', () => {
    for (const texto of ['en postgres', 'en postgresql', 'en pg']) {
      assert.deepEqual(extractDatabaseHintsFromText(texto, BASES), ['pg_main'], texto);
    }
  });

  it('no confunde «pg» dentro de otra palabra', () => {
    // La frontera de palabra importa: sin ella, cualquier texto con «pgto» o «upgrade»
    // acabaría apuntando a la base principal.
    assert.deepEqual(extractDatabaseHintsFromText('hay que hacer un upgrade', BASES), []);
  });

  it('un texto sin pistas no señala ninguna', () => {
    assert.deepEqual(extractDatabaseHintsFromText('el usuario no puede entrar', BASES), []);
    assert.deepEqual(extractDatabaseHintsFromText('', BASES), []);
  });

  it('sin bases configuradas no hay nada que señalar', () => {
    assert.deepEqual(extractDatabaseHintsFromText('en oracle', []), []);
  });

  it('no repite si el texto la nombra dos veces', () => {
    assert.deepEqual(extractDatabaseHintsFromText('oracle y oracle_local', BASES), ['oracle_local']);
  });

  it('mongo no se reconoce por el nombre del motor', () => {
    // Caracterización: solo oracle, postgres y mysql tienen regla de motor. A Mongo
    // hay que nombrarla por su identificador.
    assert.deepEqual(extractDatabaseHintsFromText('en mongodb', BASES), []);
    assert.deepEqual(extractDatabaseHintsFromText('en mongo_local', BASES), ['mongo_local']);
  });
});

describe('contra qué bases se acaba corriendo', () => {
  it('lo que se pide explícitamente manda', () => {
    // Si alguien eligió una base en la pantalla, no hay nada que deducir.
    assert.deepEqual(
      resolveRequestedDatabasesForQuery('esto es de oracle', ['pg_main'], BASES),
      ['pg_main'],
    );
  });

  it('sin elección explícita, se deduce del texto', () => {
    assert.deepEqual(resolveRequestedDatabasesForQuery('esto es de oracle', [], BASES), ['oracle_local']);
  });

  it('una lista con vacíos no cuenta como elección', () => {
    assert.deepEqual(resolveRequestedDatabasesForQuery('en oracle', ['', '  '], BASES), ['oracle_local']);
  });

  it('quita repetidas de lo pedido', () => {
    assert.deepEqual(
      resolveRequestedDatabasesForQuery('', ['pg_main', 'pg_main'], BASES),
      ['pg_main'],
    );
  });

  it('sin pistas ni elección, ninguna base', () => {
    assert.deepEqual(resolveRequestedDatabasesForQuery('el usuario no entra', [], BASES), []);
  });
});
