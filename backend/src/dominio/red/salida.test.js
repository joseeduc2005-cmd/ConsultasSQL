/**
 * Pruebas de caracterización de la política de red.
 *
 * El sistema está pensado para no llamar a nada de fuera. Estas pruebas fijan esa
 * promesa: si alguien añade una integración externa por descuido, se entera aquí.
 *
 *   node --test src/dominio/red/salida.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  extractHostFromRequestOptions,
  isOriginAllowed,
  isOutboundBlocked,
  sanitizeOrigin,
} from './salida.js';

describe('a dónde se deja salir', () => {
  it('deja salir a la propia máquina', () => {
    assert.equal(isOutboundBlocked('localhost'), false);
    assert.equal(isOutboundBlocked('127.0.0.1'), false);
    assert.equal(isOutboundBlocked('::1'), false);
  });

  it('bloquea todo lo demás, no solo una lista de prohibidos', () => {
    // Lo importante es que la regla sea «nada salvo local» y no «nada de estos».
    // Una lista de prohibidos se queda corta en cuanto aparece un servicio nuevo.
    for (const host of [
      'api.openai.com',
      'ejemplo.com',
      'cualquier-cosa.net',
      '8.8.8.8',
      'servidor-interno.jardinazuayo.fin.ec',
    ]) {
      assert.equal(isOutboundBlocked(host), true, host);
    }
  });

  it('no se deja engañar por mayúsculas ni espacios', () => {
    assert.equal(isOutboundBlocked('  LOCALHOST  '), false);
  });

  it('sin host no hay nada que bloquear', () => {
    assert.equal(isOutboundBlocked(''), false);
    assert.equal(isOutboundBlocked(null), false);
  });
});

describe('de dónde se aceptan peticiones', () => {
  it('acepta los orígenes del frontend', () => {
    assert.equal(isOriginAllowed('http://localhost:3000'), true);
  });

  it('rechaza un origen desconocido', () => {
    assert.equal(isOriginAllowed('http://malicioso.com'), false);
  });

  it('sin origen pasa: así llegan curl y las peticiones del propio servidor', () => {
    assert.equal(isOriginAllowed(''), true);
    assert.equal(isOriginAllowed(null), true);
  });

  it('limpia espacios alrededor', () => {
    assert.equal(sanitizeOrigin('  http://localhost:3000  '), 'http://localhost:3000');
    assert.equal(sanitizeOrigin(null), '');
  });
});

describe('sacar el host de lo que sea que le pasen', () => {
  it('de una URL en texto', () => {
    assert.equal(extractHostFromRequestOptions('https://ejemplo.com/ruta'), 'ejemplo.com');
  });

  it('de un objeto URL', () => {
    assert.equal(extractHostFromRequestOptions(new URL('https://ejemplo.com')), 'ejemplo.com');
  });

  it('de las opciones de una petición', () => {
    assert.equal(extractHostFromRequestOptions({ hostname: 'Ejemplo.com' }), 'ejemplo.com');
    assert.equal(extractHostFromRequestOptions({ host: 'otro.com' }), 'otro.com');
  });

  it('del segundo argumento cuando el primero no sirve', () => {
    assert.equal(extractHostFromRequestOptions(null, 'https://ejemplo.com'), 'ejemplo.com');
  });

  it('una URL rota no revienta: devuelve vacío', () => {
    // Y vacío no se bloquea, así que conviene saberlo: lo que no se entiende, pasa.
    assert.equal(extractHostFromRequestOptions('no-es-una-url'), '');
    assert.equal(extractHostFromRequestOptions(undefined), '');
  });
});
