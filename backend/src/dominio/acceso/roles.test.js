/**
 * Pruebas de caracterización del control de acceso.
 *
 * De estas tres funciones depende quién entra a dónde en todo el panel. La que más
 * importa es la última: el rol sale de la sesión que el backend emitió, y nunca de una
 * cabecera que pone el navegador — si eso se invirtiera por descuido, cualquiera sería
 * administrador mandando una cabecera.
 *
 *   node --test src/dominio/acceso/roles.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { ROLE_LEVELS, getRoleFromRequest, hasRequiredRole, normalizeRole } from './roles.js';

describe('normalizar el rol', () => {
  it('ignora mayúsculas y espacios', () => {
    assert.equal(normalizeRole('  ADMIN  '), 'admin');
    assert.equal(normalizeRole('SuperAdmin'), 'superadmin');
  });

  it('lo que no reconoce cae en «user», no en vacío', () => {
    // Caracterización: hay un valor por defecto. Que sea el rol más bajo es lo
    // correcto — equivocarse hacia abajo deja a alguien sin entrar, equivocarse
    // hacia arriba lo deja entrar donde no debe.
    assert.equal(normalizeRole(null), 'user');
    assert.equal(normalizeRole(undefined), 'user');
    assert.equal(normalizeRole(''), 'user');
    assert.equal(normalizeRole('inventado'), 'user');
  });
});

describe('si un rol alcanza para lo que se pide', () => {
  it('el mismo rol alcanza', () => {
    assert.equal(hasRequiredRole('admin', 'admin'), true);
  });

  it('uno más alto alcanza', () => {
    assert.equal(hasRequiredRole('superadmin', 'admin'), true);
    assert.equal(hasRequiredRole('admin', 'user'), true);
  });

  it('uno más bajo no alcanza', () => {
    assert.equal(hasRequiredRole('user', 'admin'), false);
    assert.equal(hasRequiredRole('admin', 'superadmin'), false);
  });

  it('un rol que no existe cuenta como «user»', () => {
    // Consecuencia de lo anterior: alcanza para lo de usuario y nada más.
    assert.equal(hasRequiredRole('inventado', 'user'), true);
    assert.equal(hasRequiredRole('inventado', 'admin'), false);
    assert.equal(hasRequiredRole(null, 'admin'), false);
  });

  it('los niveles van de menos a más', () => {
    assert.ok(ROLE_LEVELS.user < ROLE_LEVELS.admin);
    assert.ok(ROLE_LEVELS.admin < ROLE_LEVELS.superadmin);
  });
});

describe('de dónde sale el rol de quien pide', () => {
  it('sale del usuario de la sesión', () => {
    const req = { headers: {} };
    assert.equal(getRoleFromRequest(req, { role: 'admin' }), 'admin');
  });

  it('la cabecera del navegador no decide nada', () => {
    // Esto era una escalada de privilegios: se miraba `x-user-role` antes que la
    // sesión, y la cabecera la pone el navegador. Una sesión de rol «user» obtenía
    // 200 en rutas de administrador mandando «x-user-role: superadmin», incluida la
    // que ejecuta SQL manual.
    const req = { headers: { 'x-user-role': 'superadmin' } };
    assert.equal(getRoleFromRequest(req, { role: 'user' }), 'user');
  });

  it('sin cabecera ni sesión, el rol más bajo', () => {
    assert.equal(getRoleFromRequest({ headers: {} }, null), 'user');
  });
});
