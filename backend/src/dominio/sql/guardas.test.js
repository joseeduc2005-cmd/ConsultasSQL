/**
 * Pruebas de caracterización de las guardas de SQL.
 *
 * «De caracterización» quiere decir que describen lo que el sistema hace hoy, no lo que
 * debería hacer. Es la red que faltaba para poder refactorizar `app.js`: si un cambio
 * altera cualquiera de estas respuestas, se entera alguien antes que un usuario.
 *
 * Están escritas contra el código recién extraído, sin tocarlo. Si alguna resulta
 * incómoda de leer, probablemente señale algo que conviene cambiar — pero cambiarlo
 * viene después, y entonces la prueba se actualiza a propósito y no por accidente.
 *
 *   node --test src/dominio/sql/guardas.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  containsBlockedSql,
  extractSqlReferencedTables,
  getDatabaseSchemaTableNames,
  normalizeSchemaIdentifier,
  splitManualSqlSteps,
  stripLeadingSqlComments,
  stripSqlStringsAndComments,
} from './guardas.js';

describe('qué SQL se deja pasar', () => {
  it('deja pasar un SELECT normal', () => {
    assert.equal(containsBlockedSql('SELECT nombre FROM usuarios WHERE id = 1'), false);
  });

  it('deja pasar un WITH, que también es lectura', () => {
    assert.equal(containsBlockedSql('WITH x AS (SELECT 1) SELECT * FROM x'), false);
  });

  it('bloquea lo que no empieza por SELECT ni WITH', () => {
    for (const sql of [
      'UPDATE usuarios SET estado = 0',
      'DELETE FROM usuarios',
      'INSERT INTO usuarios VALUES (1)',
      'DROP TABLE usuarios',
    ]) {
      assert.equal(containsBlockedSql(sql), true, sql);
    }
  });

  it('bloquea sentencias encadenadas, que es el patrón clásico de inyección', () => {
    assert.equal(containsBlockedSql("SELECT 1; DROP TABLE usuarios"), true);
  });

  it('un punto y coma final no cuenta como encadenada', () => {
    assert.equal(containsBlockedSql('SELECT 1;'), false);
  });

  it('un punto y coma dentro de un texto tampoco encadena nada', () => {
    // Si el separador se buscara con un split ingenuo, esto se bloquearía sin motivo.
    assert.equal(containsBlockedSql("SELECT 'hola; adios' AS saludo"), false);
  });

  it('no se deja engañar por palabras peligrosas dentro de un texto', () => {
    // Esta es la razón de ser de stripSqlStringsAndComments: el contenido de una
    // cadena no es código, y bloquearlo convertiría consultas legítimas en errores.
    assert.equal(containsBlockedSql("SELECT 'drop your fears' AS mensaje"), false);
    assert.equal(containsBlockedSql('SELECT 1 -- drop table usuarios'), false);
  });

  it('bloquea la palabra peligrosa cuando sí es código', () => {
    assert.equal(containsBlockedSql('SELECT * FROM t; drop table usuarios'), true);
  });
});

describe('limpiar SQL antes de analizarlo', () => {
  it('quita comentarios de bloque y de línea', () => {
    assert.equal(stripSqlStringsAndComments('SELECT /* nota */ 1').includes('nota'), false);
    assert.equal(stripSqlStringsAndComments('SELECT 1 -- nota').includes('nota'), false);
  });

  it('vacía las cadenas pero deja la estructura', () => {
    const limpio = stripSqlStringsAndComments("SELECT 'hola' FROM t");
    assert.equal(limpio.includes('hola'), false);
    assert.ok(limpio.includes('SELECT') && limpio.includes('FROM'));
  });

  it('entiende la comilla escapada doblándola', () => {
    const limpio = stripSqlStringsAndComments("SELECT 'o''brien' FROM t");
    assert.equal(limpio.includes('brien'), false);
  });

  it('quita los comentarios del principio para ver con qué empieza de verdad', () => {
    assert.equal(stripLeadingSqlComments('-- nota\nSELECT 1'), 'SELECT 1');
    assert.equal(stripLeadingSqlComments('/* nota */ SELECT 1'), 'SELECT 1');
  });
});

describe('partir un script en sentencias', () => {
  it('parte por el punto y coma', () => {
    assert.equal(splitManualSqlSteps('SELECT 1; SELECT 2').length, 2);
  });

  it('no parte por un punto y coma dentro de una cadena', () => {
    assert.equal(splitManualSqlSteps("SELECT 'a;b'").length, 1);
  });

  it('no parte por un punto y coma dentro de un comentario', () => {
    assert.equal(splitManualSqlSteps('SELECT 1 -- a;b').length, 1);
  });

  it('una consulta de Mongo no se parte nunca', () => {
    // Llegan como JSON y un punto y coma dentro no separa nada.
    assert.equal(splitManualSqlSteps('{"collection":"x","filter":{"a":1}}').length, 1);
  });

  it('un texto vacío no da sentencias', () => {
    assert.deepEqual(splitManualSqlSteps('   '), []);
  });
});

describe('qué tablas toca una consulta', () => {
  it('encuentra las de FROM y JOIN', () => {
    const tablas = extractSqlReferencedTables('SELECT * FROM usuarios JOIN roles ON 1=1');
    assert.ok(tablas.includes('usuarios'));
    assert.ok(tablas.includes('roles'));
  });

  it('conserva el esquema y también el nombre suelto', () => {
    // Las listas blancas por base necesitan el nombre cualificado; el suelto sirve de
    // reserva cuando la lista está escrita sin esquema.
    const tablas = extractSqlReferencedTables('SELECT * FROM soporte.tickets');
    assert.ok(tablas.includes('soporte.tickets'));
    assert.ok(tablas.includes('tickets'));
  });

  it('quita las comillas de los identificadores', () => {
    assert.ok(extractSqlReferencedTables('SELECT * FROM "Usuarios"').includes('usuarios'));
  });

  it('una consulta sin FROM no toca ninguna tabla', () => {
    assert.deepEqual(extractSqlReferencedTables('SELECT 1'), []);
  });
});

describe('normalizar nombres', () => {
  it('pasa a minúsculas y recorta', () => {
    assert.equal(normalizeSchemaIdentifier('  Usuarios  '), 'usuarios');
  });

  it('lo vacío y lo ausente dan cadena vacía', () => {
    assert.equal(normalizeSchemaIdentifier(null), '');
    assert.equal(normalizeSchemaIdentifier(undefined), '');
  });

  it('lista las tablas de una base, normalizadas', () => {
    const base = { schema: { tables: [{ name: 'Usuarios' }, { name: ' Roles ' }] } };
    assert.deepEqual(getDatabaseSchemaTableNames(base), ['usuarios', 'roles']);
  });

  it('una base sin esquema no rompe nada', () => {
    assert.deepEqual(getDatabaseSchemaTableNames(null), []);
    assert.deepEqual(getDatabaseSchemaTableNames({}), []);
  });
});
