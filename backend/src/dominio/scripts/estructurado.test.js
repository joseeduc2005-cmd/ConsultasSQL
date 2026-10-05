/**
 * Pruebas del script estructurado.
 *
 *   node --test src/dominio/scripts/estructurado.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  ALIAS_DE_ACCION,
  EXECUTABLE_SOLUTION_TYPES,
  normalizeStructuredParams,
  parseStructuredScript,
} from './estructurado.js';

describe('la lista de acciones', () => {
  it('es cerrada: son cuatro y están escritas a mano', () => {
    // Que sea cerrada es la garantía. Si de aquí saliera cualquier nombre que llegue
    // en una petición, una petición podría elegir qué se ejecuta.
    assert.deepEqual(Object.keys(ALIAS_DE_ACCION).sort(),
      ['desbloquear_cuenta', 'limpiar_sesiones', 'verificar_conexion', 'verificar_estado_cuenta']);
  });

  it('no lleva dentro nada que ejecute', () => {
    // Lo que hace cada acción lo une app.js, que es quien tiene el pool. Si esto
    // trajera un `execute`, el dominio dependería de algo que abre conexiones.
    for (const alias of Object.values(ALIAS_DE_ACCION)) {
      for (const valor of Object.values(alias)) assert.equal(typeof valor, 'string');
    }
  });
});

describe('normalizar los parámetros de una acción', () => {
  it('traduce el nombre en castellano al que usa el código', () => {
    assert.deepEqual(normalizeStructuredParams('verificar_conexion', { usuario: 'ana' }), { username: 'ana' });
  });

  it('lo que no tiene alias pasa con su nombre', () => {
    const r = normalizeStructuredParams('desbloquear_cuenta', { usuario: 'ana', motivo: 'olvido' });
    assert.equal(r.username, 'ana');
    assert.equal(r.motivo, 'olvido');
  });

  it('una acción que no existe da vacío, no la deja pasar a medias', () => {
    assert.deepEqual(normalizeStructuredParams('inventada', { a: 1 }), {});
  });

  it('no revienta sin parámetros', () => {
    assert.doesNotThrow(() => normalizeStructuredParams('verificar_conexion', null));
  });
});

describe('leer el script de un artículo', () => {
  it('un objeto pasa tal cual', () => {
    assert.deepEqual(parseStructuredScript({ accion: 'x' }), { accion: 'x' });
  });

  it('un JSON en texto se convierte', () => {
    assert.deepEqual(parseStructuredScript('{"accion":"x"}'), { accion: 'x' });
  });

  it('lo vacío da null', () => {
    assert.equal(parseStructuredScript(null), null);
    assert.equal(parseStructuredScript(''), null);
  });
});

describe('qué tipos de solución se pueden ejecutar', () => {
  it('son tres; «lectura» no está', () => {
    // Importa: un artículo de tipo lectura no debe poder lanzar nada.
    assert.equal(EXECUTABLE_SOLUTION_TYPES.has('ejecutable'), true);
    assert.equal(EXECUTABLE_SOLUTION_TYPES.has('database'), true);
    assert.equal(EXECUTABLE_SOLUTION_TYPES.has('script'), true);
    assert.equal(EXECUTABLE_SOLUTION_TYPES.has('lectura'), false);
  });
});
