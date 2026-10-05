/**
 * Pruebas de la redacción de la auditoría.
 *
 * Lo que se comprueba aquí no es que el formato salga bonito: es que una contraseña
 * no acabe en una tabla que se consulta y se exporta a diario, y que al mismo tiempo
 * quede rastro de que alguien la mandó. Las dos cosas a la vez.
 *
 *   node --test src/dominio/auditoria/redaccion.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  cambiaAlgo,
  enmascararIdentificador,
  redactarCuerpo,
  seAudita,
} from './redaccion.js';

describe('tapar un identificador', () => {
  it('deja los extremos y tapa el medio', () => {
    assert.equal(enmascararIdentificador('0105609176'), '0105****76');
  });

  it('lo corto se tacha entero, no a medias', () => {
    // Tapar el medio de cuatro cifras no tapa nada: quedarían dos de cuatro.
    assert.equal(enmascararIdentificador('1234'), '[omitido]');
    assert.equal(enmascararIdentificador(''), '[omitido]');
    assert.equal(enmascararIdentificador(null), '[omitido]');
  });

  it('no revienta con un número', () => {
    assert.equal(enmascararIdentificador(105609176), '1056***76');
  });
});

describe('redactar el cuerpo de una petición', () => {
  it('la contraseña no se guarda, pero sí que la hubo', () => {
    const r = redactarCuerpo({ username: 'ana', password: 'secreta123' });
    assert.equal(r.username, 'ana');
    assert.equal(r.password, '[omitido]');
    assert.equal('password' in r, true, 'la clave se conserva: saber que la mandó es parte del rastro');
  });

  it('reconoce el secreto aunque la clave venga compuesta', () => {
    const r = redactarCuerpo({
      api_key: 'abc', refreshToken: 'x', user_password: 'y', Authorization: 'Bearer z',
    });
    for (const k of ['api_key', 'refreshToken', 'user_password', 'Authorization']) {
      assert.equal(r[k], '[omitido]', k + ' debería estar tachado');
    }
  });

  it('las cédulas se guardan tapadas, no tachadas', () => {
    // La diferencia importa: de una cédula tapada se reconoce el caso al revisar la
    // auditoría; de una tachada, no.
    const r = redactarCuerpo({ cedula: '0105609176', identificacion: '0912345678' });
    assert.equal(r.cedula, '0105****76');
    assert.equal(r.identificacion, '0912****78');
  });

  it('recorta el texto largo en vez de guardarlo entero', () => {
    const r = redactarCuerpo({ sql: 'x'.repeat(500) });
    assert.equal(r.sql.length, 301);
    assert.match(r.sql, /…$/);
  });

  it('baja por los objetos anidados y acaba tapando el fondo', () => {
    // Con la profundidad por defecto baja tres niveles y poda el cuarto.
    const r = redactarCuerpo({ a: { b: { c: { d: { e: 1 } } } } });
    assert.deepEqual(r.a.b.c, { d: '[…]' });
  });

  it('tacha el secreto aunque esté anidado', () => {
    const r = redactarCuerpo({ conexion: { host: 'localhost', password: 'pg' } });
    assert.equal(r.conexion.host, 'localhost');
    assert.equal(r.conexion.password, '[omitido]');
  });

  it('el cero y el false se guardan, no se pierden', () => {
    const r = redactarCuerpo({ saldo: 0, activo: false });
    assert.equal(r.saldo, 0);
    assert.equal(r.activo, false);
  });

  it('no revienta con lo que no es un objeto', () => {
    assert.deepEqual(redactarCuerpo(null), {});
    assert.deepEqual(redactarCuerpo('texto'), {});
    assert.deepEqual(redactarCuerpo([1, 2]), {});
  });
});

describe('qué se audita', () => {
  it('los cuatro métodos que cambian algo', () => {
    for (const m of ['POST', 'PUT', 'PATCH', 'DELETE']) assert.equal(cambiaAlgo(m), true, m);
    assert.equal(cambiaAlgo('GET'), false);
    assert.equal(cambiaAlgo('post'), true, 'en minúscula también');
  });

  it('solo el sondeo de salud queda fuera', () => {
    assert.equal(seAudita('GET', '/health'), false);
    assert.equal(seAudita('POST', '/api/auth/login'), true);
    assert.equal(seAudita('DELETE', '/api/history'), true);
  });

  it('el sondeo previo de CORS no se audita', () => {
    // Un OPTIONS no lo manda nadie: lo manda el navegador antes de la petición de
    // verdad, y esa sí se audita. Registrarlo duplicaría cada fila sin añadir un dato.
    assert.equal(seAudita('OPTIONS', '/api/comments'), false);
    assert.equal(seAudita('HEAD', '/api/articles'), false);
    assert.equal(seAudita('options', '/api/comments'), false, 'en minúscula también');
  });

  it('un 404 fuera de /api también se audita', () => {
    // Alguien llamando a una puerta que no existe es justo lo que hay que poder ver.
    assert.equal(seAudita('POST', '/ruta-que-no-existe'), true);
    assert.equal(seAudita('GET', '/'), true);
  });

  it('un intento denegado también se audita', () => {
    // Es el caso que más importa poder ver después, así que no hay regla que lo saque.
    assert.equal(seAudita('POST', '/api/databases'), true);
  });
});
