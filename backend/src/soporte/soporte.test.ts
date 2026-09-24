/**
 * Pruebas de lo que no puede romperse en silencio.
 *
 * No cubren todo el módulo a propósito: cubren las cuatro piezas que deciden qué SQL
 * se le propone a una persona para que lo ejecute contra Oracle. Si una de ellas
 * cambia de comportamiento sin querer, el sistema sigue arrancando y respondiendo
 * — simplemente empieza a proponer cosas distintas, y nadie se entera.
 *
 *   node --test src/soporte/soporte.test.ts
 *
 * No tocan ninguna base: la fuente de datos se sustituye por uno falso.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { CORRECCIONES, accionDe, resolverPlantilla } from './correcciones.ts';
import { explorarUsuario } from './exploracion.ts';
import { extraerIdentificadores } from './interpreter/extraer.ts';
import { clasificarConReglas } from './interpreter/reglas.ts';
import { asegurarSoloLectura } from './oracle/guardia.ts';
import type { Fila, FuenteDatos, Parametros } from './oracle/fuente.ts';
import type { NombreConsulta } from './oracle/catalogo.ts';

// ─── Guardián de solo lectura ────────────────────────────────────────────────

describe('guardia de solo lectura', () => {
  it('deja pasar un SELECT', () => {
    assert.doesNotThrow(() => asegurarSoloLectura('SELECT 1 FROM dual'));
    assert.doesNotThrow(() => asegurarSoloLectura('WITH a AS (SELECT 1 x FROM dual) SELECT x FROM a'));
  });

  it('rechaza escrituras, aunque vayan escondidas', () => {
    for (const sql of [
      'UPDATE corresponsales.usuario SET usr_estado = 1',
      'DELETE FROM corresponsales.usuario',
      'SELECT 1 FROM dual; DROP TABLE usuario',
      'SELECT 1 FROM dual -- \nUPDATE usuario SET x = 1',
      'BEGIN dbms_output.put_line(1); END;',
    ]) {
      assert.throws(() => asegurarSoloLectura(sql), undefined, `deberia rechazar: ${sql}`);
    }
  });

  it('no se confunde con una palabra prohibida dentro de un literal', () => {
    assert.doesNotThrow(() => asegurarSoloLectura("SELECT 'update' AS x FROM dual"));
  });
});

// ─── Extracción de identificadores ───────────────────────────────────────────

describe('extraer identificadores', () => {
  it('saca el usuario aunque cierre la frase', () => {
    assert.equal(extraerIdentificadores('El usuario es demo.duplicado.').usuarios[0], 'demo.duplicado');
  });

  it('descarta esquema.tabla, que tiene la misma forma que nombre.apellido', () => {
    // Salió del ticket real 2026083744: «fisja.clientes» terminaba como usuario.
    assert.equal(extraerIdentificadores('descuadre en fisja.clientes').usuarios.length, 0);
    assert.equal(extraerIdentificadores('revisar corresponsales.usuario').usuarios.length, 0);
  });

  it('descarta correos y dominios', () => {
    assert.equal(extraerIdentificadores('escribir a juan.perez@banco.fin.ec').usuarios.length, 0);
    assert.equal(extraerIdentificadores('ver sir.jardinazuayo.fin.ec').usuarios.length, 0);
  });

  it('saca cédulas válidas y MAC de 15 dígitos', () => {
    assert.equal(extraerIdentificadores('cédula 1105681702').cedulas[0], '1105681702');
    assert.equal(extraerIdentificadores('mac 860090060131281').macs[0], '860090060131281');
  });

  it('saca el número de acta', () => {
    // Ticket 2026090874: «Se intento eliminar el acta 53656 (julio)».
    assert.equal(extraerIdentificadores('Se intento eliminar el acta 53656 desde la aplicacion').actas[0], '53656');
    assert.equal(extraerIdentificadores('actualizar el codigo 12345').actas.length, 0);
  });

  it('no toma un número cualquiera por cédula', () => {
    assert.equal(extraerIdentificadores('son 1234567890 transacciones').cedulas.length, 0);
  });
});

// ─── Clasificación ───────────────────────────────────────────────────────────

describe('clasificar con reglas', () => {
  it('reconoce los casos de acceso', () => {
    for (const texto of [
      'No permite ingresar al corresponsal',
      'le sale Unexpected Error al poner el PIN',
      'el usuario se inhabilitó por intentos',
    ]) {
      assert.equal(clasificarConReglas(texto).playbook?.id, 'acceso-corresponsal', texto);
    }
  });

  it('reconoce las caidas de un proveedor externo', () => {
    // Antes caia en «sin playbook» y quien atendia se ponia a buscar en Oracle un
    // problema que estaba fuera. Ver ticket 2026091131.
    assert.equal(clasificarConReglas('Error en recargas Claro').playbook?.id, 'servicio-externo');
  });

  it('distingue una queja de formato de clave de un bloqueo de acceso', () => {
    const formato = clasificarConReglas('Formato de contrasena y PIN: el cambio se guarda pero no persiste');
    assert.equal(formato.playbook?.id, 'politica-claves');
    const bloqueo = clasificarConReglas('No permite ingresar, le sale Unexpected Error con el PIN');
    assert.equal(bloqueo.playbook?.id, 'acceso-corresponsal');
  });

  it('no inventa un playbook para un texto ajeno al dominio', () => {
    assert.equal(clasificarConReglas('Solicito vacaciones para el proximo mes').playbook, null);
  });
});

// ─── Catálogo de correcciones ────────────────────────────────────────────────

describe('catalogo de correcciones', () => {
  it('toda plantilla es una escritura de una sola fila, por clave', () => {
    for (const c of Object.values(CORRECCIONES)) {
      assert.match(c.plantilla, /^\s*UPDATE\b/i, `${c.id} deberia ser UPDATE`);
      assert.match(c.plantilla, /\bWHERE\b/i, `${c.id} sin WHERE: tocaria toda la tabla`);
      assert.match(c.plantilla, /\bCOMMIT\b/i, `${c.id} sin COMMIT`);
    }
  });

  it('cada marcador de la plantilla tiene su campo declarado', () => {
    for (const c of Object.values(CORRECCIONES)) {
      const marcadores = [...c.plantilla.matchAll(/:([a-z_][a-z0-9_]*)/gi)].map((m) => m[1].toLowerCase());
      const campos = new Set(c.campos.map((x) => x.nombre.toLowerCase()));
      for (const m of marcadores) {
        assert.ok(campos.has(m), `${c.id}: el marcador :${m} no tiene campo declarado`);
      }
    }
  });

  it('resolver la plantilla escapa las comillas', () => {
    const sql = resolverPlantilla('UPDATE t SET a = :v', { v: "O'Brien" });
    assert.equal(sql, "UPDATE t SET a = 'O''Brien'");
  });

  it('deja el marcador si no le dan valor, en vez de romper el SQL', () => {
    assert.equal(resolverPlantilla('UPDATE t SET a = :v', {}), 'UPDATE t SET a = :v');
  });

  it('la accion conserva la plantilla y los valores del caso', () => {
    const a = accionDe(CORRECCIONES.resetearPin, { usr_codigo: 3003 });
    assert.equal(a.riesgo, 'escritura');
    assert.equal(a.correccion, 'resetear-pin');
    assert.match(a.sql!, /USR_CODIGO = 3003/);
    assert.equal(a.parametros?.usr_codigo, 3003);
  });
});

// ─── Exploración ─────────────────────────────────────────────────────────────

/** Fuente falsa: devuelve el perfil que se le indique, sin tocar ninguna base. */
function fuenteFalsa(perfiles: Record<string, { valor: number; veces: number; total: number }>): FuenteDatos {
  return {
    modo: 'oracle',
    async consultar(nombre: NombreConsulta, _p?: Parametros): Promise<Fila[]> {
      const p = perfiles[nombre as string];
      return p ? [{ valor: p.valor, veces: p.veces, total: p.total }] : [];
    },
    async estado() {
      return { modo: 'oracle' as const, conectado: true, detalle: 'falsa' };
    },
    async cerrar() {},
  };
}

const POBLACION_SANA = {
  perfilUsrEstado: { valor: 1, veces: 98, total: 100 },
  perfilUsrIntentos: { valor: 0, veces: 95, total: 100 },
  perfilUsrIntentosPin: { valor: 0, veces: 97, total: 100 },
  perfilUsrIsClaveTemporal: { valor: 0, veces: 99, total: 100 },
  perfilUsrIsPinTemporal: { valor: 0, veces: 99, total: 100 },
};

const USUARIO_SANO = {
  usr_codigo: 3001, usr_estado: 1, usr_intentos: 0,
  usr_intentos_pin: 0, usr_is_clave_temporal: 0, usr_is_pin_temporal: 0,
};

describe('exploracion', () => {
  it('no propone nada para un usuario igual a los que operan', async () => {
    const r = await explorarUsuario(fuenteFalsa(POBLACION_SANA), USUARIO_SANO);
    assert.equal(r.propuesta, undefined);
    assert.equal(r.explorado, true);
  });

  it('junta varias diferencias en UNA sola sentencia', async () => {
    const r = await explorarUsuario(fuenteFalsa(POBLACION_SANA), {
      ...USUARIO_SANO, usr_estado: 0, usr_intentos: 3, usr_intentos_pin: 3,
    });
    const p = r.propuesta!;
    assert.equal(p.anomalias.length, 3);
    // Una sola sentencia: es lo que evita entregar tres SQL para probar de uno en uno.
    assert.equal(p.sql.match(/UPDATE/gi)?.length, 1);
    assert.match(p.sql, /WHERE USR_CODIGO = 3001/);
    // La confianza es la cuota más floja de las incluidas.
    assert.equal(p.confianza, 0.95);
  });

  it('guarda la plantilla con marcador, no el SQL resuelto', async () => {
    const r = await explorarUsuario(fuenteFalsa(POBLACION_SANA), { ...USUARIO_SANO, usr_estado: 0 });
    assert.match(r.propuesta!.plantilla, /:usr_codigo/);
    assert.match(r.propuesta!.sql, /= 3001/);
  });

  it('se calla si la poblacion de referencia es muy chica', async () => {
    const pocos = Object.fromEntries(
      Object.entries(POBLACION_SANA).map(([k, v]) => [k, { ...v, veces: 4, total: 4 }]),
    );
    const r = await explorarUsuario(fuenteFalsa(pocos), { ...USUARIO_SANO, usr_estado: 0 });
    assert.equal(r.propuesta, undefined);
    assert.match(r.motivo ?? '', /poblacion|población/i);
  });

  it('se calla si la poblacion esta repartida y no dice nada claro', async () => {
    const repartida = { ...POBLACION_SANA, perfilUsrEstado: { valor: 1, veces: 55, total: 100 } };
    const r = await explorarUsuario(fuenteFalsa(repartida), { ...USUARIO_SANO, usr_estado: 0 });
    assert.equal(r.propuesta, undefined);
  });

  it('nunca propone escribir fuera de las columnas permitidas', async () => {
    const r = await explorarUsuario(fuenteFalsa(POBLACION_SANA), {
      ...USUARIO_SANO, usr_estado: 0, trans_monto: 999, codigo_sucursal: 28,
    });
    const p = r.propuesta!;
    assert.doesNotMatch(p.sql, /TRANS_MONTO|CODIGO_SUCURSAL/i);
    assert.equal(p.anomalias.length, 1);
  });

  it('no propone nada si el usuario no trae clave primaria', async () => {
    const r = await explorarUsuario(fuenteFalsa(POBLACION_SANA), { usr_estado: 0 });
    assert.equal(r.propuesta, undefined);
  });

  it('no lanza aunque la fuente falle', async () => {
    const rota: FuenteDatos = {
      modo: 'oracle',
      async consultar() { throw new Error('Oracle caido'); },
      async estado() { return { modo: 'oracle' as const, conectado: false, detalle: '' }; },
      async cerrar() {},
    };
    const r = await explorarUsuario(rota, USUARIO_SANO);
    assert.equal(r.explorado, false);
    assert.match(r.motivo ?? '', /Oracle caido/);
  });
});
