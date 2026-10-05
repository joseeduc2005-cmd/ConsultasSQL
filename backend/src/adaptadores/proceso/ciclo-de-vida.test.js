/**
 * Pruebas del registro del ciclo de vida.
 *
 * Existen porque la forma obvia de comprobarlo —matar el proceso y mirar la tabla— no
 * funciona en Windows: `process.kill` con SIGINT termina sin ejecutar el manejador.
 * Con un proceso de mentira se comprueba lo que de verdad importa: que antes de salir
 * se espera a que la fila esté escrita.
 *
 *   node --test src/adaptadores/proceso/ciclo-de-vida.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { registrarCicloDeVida } from './ciclo-de-vida.js';

/** Un proceso de mentira: guarda los oyentes y deja disparar la señal a mano. */
function procesoFalso() {
  const oyentes = new Map();
  const salidas = [];
  return {
    on: (evento, fn) => { oyentes.set(evento, fn); },
    exit: (codigo) => { salidas.push(codigo); },
    uptime: () => 123.6,
    emitir: (evento, arg) => oyentes.get(evento)?.(arg),
    oyentes,
    salidas,
  };
}

function registradores() {
  const eventos = [];
  const errores = [];
  return {
    eventos,
    errores,
    logSystemEvent: async (e) => { eventos.push(e); },
    logError: async (e) => { errores.push(e); },
  };
}

describe('qué señales se escuchan', () => {
  it('se registran las cuatro', () => {
    const p = procesoFalso();
    registrarCicloDeVida({ ...registradores(), proceso: p });
    for (const e of ['SIGTERM', 'SIGINT', 'uncaughtException', 'unhandledRejection']) {
      assert.equal(p.oyentes.has(e), true, 'falta ' + e);
    }
  });
});

describe('una parada ordenada', () => {
  it('deja el evento antes de salir, no después', async () => {
    // Es lo que importa de verdad: si se saliera sin esperar, la fila no llegaría a
    // la base y el cierre se perdería igual que cuando no había nada de esto.
    const p = procesoFalso();
    const r = registradores();
    registrarCicloDeVida({ ...r, proceso: p });

    await p.emitir('SIGTERM');

    assert.equal(r.eventos.length, 1);
    assert.equal(r.eventos[0].evento, 'SERVICIO_DETENIDO');
    assert.equal(r.eventos[0].detalle.motivo, 'SIGTERM');
    assert.deepEqual(p.salidas, [0], 'sale con 0 en una parada ordenada');
  });

  it('redondea el tiempo en pie a segundos enteros', () => {
    const p = procesoFalso();
    const r = registradores();
    registrarCicloDeVida({ ...r, proceso: p });
    return p.emitir('SIGINT').then(() => {
      assert.equal(r.eventos[0].detalle.enPieSegundos, 124);
    });
  });
});

describe('una caída', () => {
  it('deja el error y el cierre, y sale con 1', async () => {
    const p = procesoFalso();
    const r = registradores();
    registrarCicloDeVida({ ...r, proceso: p });

    await p.emitir('uncaughtException', new Error('algo revento'));

    assert.equal(r.errores.length, 1);
    assert.equal(r.errores[0].contexto, 'uncaughtException');
    assert.equal(r.errores[0].capa, 'proceso');
    assert.equal(r.eventos[0].detalle.motivo, 'uncaughtException');
    assert.deepEqual(p.salidas, [1], 'una caída sale con 1');
  });

  it('una promesa sin capturar se anota y el servicio sigue', async () => {
    // No cierra nada a propósito: una promesa rechazada no tumba el proceso, y
    // apagar el servicio por eso sería peor que el problema.
    const p = procesoFalso();
    const r = registradores();
    registrarCicloDeVida({ ...r, proceso: p });

    await p.emitir('unhandledRejection', new Error('promesa suelta'));

    assert.equal(r.errores[0].contexto, 'unhandledRejection');
    assert.deepEqual(p.salidas, [], 'no sale: el servicio sigue en pie');
    assert.equal(r.eventos.length, 0, 'no registra una parada que no hubo');
  });
});

describe('dos paradas a la vez', () => {
  it('solo deja una fila', async () => {
    // Un SIGTERM que al cerrar provoca una excepción dejaría dos filas contradictorias
    // sobre la misma parada.
    const p = procesoFalso();
    const r = registradores();
    registrarCicloDeVida({ ...r, proceso: p });

    await p.emitir('SIGTERM');
    await p.emitir('SIGINT');

    assert.equal(r.eventos.length, 1);
    assert.equal(r.eventos[0].detalle.motivo, 'SIGTERM', 'queda el motivo real, el primero');
  });
});

describe('si la auditoría misma falla', () => {
  it('el proceso sale igual, no se queda colgado', async () => {
    // Una auditoría que impide apagar el servicio es peor que no tenerla.
    const p = procesoFalso();
    registrarCicloDeVida({
      logSystemEvent: async () => { throw new Error('base caida'); },
      logError: async () => {},
      proceso: p,
    });

    await p.emitir('SIGTERM');
    assert.deepEqual(p.salidas, [0]);
  });
});
