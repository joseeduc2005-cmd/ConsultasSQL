/**
 * Pruebas del middleware de auditoría.
 *
 * Existen porque la parte que más importa no se puede provocar a mano con fiabilidad:
 * que una conexión cortada a medias deje fila depende de ganar una carrera de
 * milisegundos contra el servidor. Con un `res` de mentira se dispara la señal
 * directamente y se comprueba lo que de verdad hay que garantizar: que no se pierde
 * ninguna petición y que no se cuenta ninguna dos veces.
 *
 *   node --test src/adaptadores/http/auditoria.test.js
 */
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { describe, it } from 'node:test';

import { capturarCuerpoParaAuditoria, crearAuditoriaDePeticiones } from './auditoria.js';

const peticion = (extra = {}) => ({
  method: 'POST',
  originalUrl: '/api/comments',
  headers: {},
  ip: '127.0.0.1',
  socket: {},
  ...extra,
});

function respuesta(estado = 200) {
  const res = new EventEmitter();
  res.statusCode = estado;
  return res;
}

function montar(opciones = {}) {
  const filas = [];
  const medio = crearAuditoriaDePeticiones({
    logRequest: async (f) => { filas.push(f); },
    auditarLecturas: true,
    ...opciones,
  });
  return { filas, medio };
}

describe('una petición que termina bien', () => {
  it('deja una fila con lo que hay que poder mirar después', () => {
    const { filas, medio } = montar();
    const req = peticion({ headers: { 'user-agent': 'navegador' }, usuarioSesion: { username: 'ana', role: 'admin' } });
    const res = respuesta(201);

    medio(req, res, () => {});
    res.emit('finish');

    assert.equal(filas.length, 1);
    assert.equal(filas[0].metodo, 'POST');
    assert.equal(filas[0].ruta, '/api/comments');
    assert.equal(filas[0].estado, 201);
    assert.equal(filas[0].usuario, 'ana');
    assert.equal(filas[0].rol, 'admin');
    assert.equal(filas[0].userAgent, 'navegador');
    assert.equal(filas[0].comoAcabo, 'completada');
  });

  it('resuelve la sesión al terminar, no al empezar', () => {
    // Importa porque el middleware va delante de adjuntarSesion: cuando corre todavía
    // no hay usuario. Si lo leyera entonces, toda la auditoría saldría sin nombre.
    const { filas, medio } = montar();
    const req = peticion();
    const res = respuesta();

    medio(req, res, () => {});
    req.usuarioSesion = { username: 'bea', role: 'user' };   // llega después
    res.emit('finish');

    assert.equal(filas[0].usuario, 'bea');
  });
});

describe('una conexión cortada a medias', () => {
  it('deja fila igual, marcada como cortada', () => {
    // El caso del stream: si quien pide la ejecución de un script cierra la pestaña,
    // 'finish' no salta nunca. Antes eso no dejaba una sola línea, y es justo la que
    // más interesa porque nadie vio el final.
    const { filas, medio } = montar();
    medio(peticion({ method: 'GET', originalUrl: '/api/execute/stream/7' }), (() => {
      const r = respuesta(200);
      setImmediate(() => r.emit('close'));
      return r;
    })(), () => {});

    return new Promise((listo) => setImmediate(() => {
      assert.equal(filas.length, 1);
      assert.equal(filas[0].comoAcabo, 'cortada');
      listo();
    }));
  });
});

describe('las dos señales a la vez', () => {
  it('una petición normal deja una fila, no dos', () => {
    // En una petición corriente saltan 'finish' y 'close'. Dos filas de lo mismo
    // convertirían cualquier recuento en algo que no se puede leer.
    const { filas, medio } = montar();
    const res = respuesta();
    medio(peticion(), res, () => {});

    res.emit('finish');
    res.emit('close');

    assert.equal(filas.length, 1);
    assert.equal(filas[0].comoAcabo, 'completada', 'gana la primera, que es la real');
  });
});

describe('qué deja pasar sin registrar', () => {
  it('el sondeo de salud y el OPTIONS previo', () => {
    const { filas, medio } = montar();
    for (const [method, originalUrl] of [['GET', '/health'], ['OPTIONS', '/api/comments']]) {
      const res = respuesta();
      medio(peticion({ method, originalUrl }), res, () => {});
      res.emit('finish');
    }
    assert.equal(filas.length, 0);
  });

  it('con auditarLecturas apagado, un GET no deja fila y un POST sí', () => {
    const { filas, medio } = montar({ auditarLecturas: false });
    for (const method of ['GET', 'POST']) {
      const res = respuesta();
      medio(peticion({ method }), res, () => {});
      res.emit('finish');
    }
    assert.equal(filas.length, 1);
    assert.equal(filas[0].metodo, 'POST');
  });

  it('la ruta se queda sin la cadena de consulta', () => {
    // Para que no acaben en la tabla los datos que alguien mandó por la URL.
    const { filas, medio } = montar();
    const res = respuesta();
    medio(peticion({ method: 'GET', originalUrl: '/api/articles?cedula=0105609176' }), res, () => {});
    res.emit('finish');
    assert.equal(filas[0].ruta, '/api/articles');
  });
});

describe('el cuerpo', () => {
  it('se guarda redactado, y lo que lo cambie después no cuenta', () => {
    const req = peticion({ body: { usuario: 'ana', password: 'secreta' } });
    capturarCuerpoParaAuditoria(req, null, () => {});
    req.body.usuario = 'cambiado por el manejador';

    const { filas, medio } = montar();
    const res = respuesta();
    medio(req, res, () => {});
    res.emit('finish');

    assert.equal(filas[0].cuerpo.usuario, 'ana', 'queda lo que llegó, no lo que quedó');
    assert.equal(filas[0].cuerpo.password, '[omitido]');
  });

  it('sin cuerpo legible la fila existe igual, sin cuerpo', () => {
    // El caso del JSON mal formado: express.json revienta y capturarCuerpo no corre.
    // La fila tiene que estar de todos modos.
    const { filas, medio } = montar();
    const res = respuesta(400);
    medio(peticion(), res, () => {});
    res.emit('finish');

    assert.equal(filas.length, 1);
    assert.equal(filas[0].cuerpo, null);
  });
});
