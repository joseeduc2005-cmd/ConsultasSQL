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

  it('OJO: hoy la cabecera del navegador gana sobre la sesión', () => {
    // Esto NO es lo que debería pasar, y la prueba está aquí para que conste.
    //
    // `x-user-role` la pone el navegador: cualquiera puede escribir lo que quiera en
    // ella. Hoy se prefiere a lo que dice la sesión, que es lo único que el backend
    // emitió. Verificado contra el backend corriendo: una sesión de rol «user» recibe
    // 403 en /api/query-history, y 200 mandando «x-user-role: superadmin».
    //
    // Afecta a toda ruta que decida con `getRoleFromRequest` + `hasRequiredRole`,
    // incluida POST /api/sql/manual, que ejecuta SQL.
    //
    // El arreglo es quitar la cabecera de esa línea. No se hizo todavía porque el
    // frontend la manda en varias peticiones y hay que comprobar que ninguna pantalla
    // dependa de ella. Cuando se arregle, esta prueba se da la vuelta.
    const req = { headers: { 'x-user-role': 'superadmin' } };
    assert.equal(getRoleFromRequest(req, { role: 'user' }), 'superadmin');
  });

  it('sin cabecera ni sesión, el rol más bajo', () => {
    assert.equal(getRoleFromRequest({ headers: {} }, null), 'user');
  });
});
