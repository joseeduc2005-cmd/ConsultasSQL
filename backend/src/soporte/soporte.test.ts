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

// ─── Sin Oracle configurada ──────────────────────────────────────────────────

describe('sin base configurada', () => {
  it('no cae en datos de demostración sin que se lo pidan', async () => {
    // Antes, quedarse sin Oracle hacía que el diagnóstico respondiera con hallazgos
    // inventados. En un banco eso es peor que no responder.
    const { config } = await import('./config.ts');
    const { fuentePara } = await import('./oracle/index.ts');
    if (config.oracle.modo === 'oracle' || config.demoPedido) return; // no aplica aquí
    assert.equal(fuentePara(null), null);
    assert.equal(fuentePara('oracle'), null);
  });
});

// ─── Exploración sin catálogo ────────────────────────────────────────────────

describe('exploracion generica', () => {
  const ESQUEMA = [
    { name: 'usuarios', columns: [{ name: 'id' }, { name: 'cedula' }, { name: 'estado' }, { name: 'intentos' }, { name: 'creado_at' }] },
    { name: 'usuario_rol', columns: [{ name: 'usuario_id' }, { name: 'rol' }] },
    { name: 'facturas', columns: [{ name: 'id' }, { name: 'monto' }, { name: 'cliente_documento' }] },
  ];

  it('encuentra la tabla que guarda ese dato sin saber el esquema', async () => {
    const { tablasCandidatas } = await import('./exploracion-generica.ts');
    const c = tablasCandidatas(ESQUEMA, 'cedula');
    assert.equal(c[0].tabla, 'usuarios', 'la coincidencia exacta debe ganar');
    assert.equal(c[0].columna, 'cedula');
    // «cliente_documento» también vale, pero después.
    assert.ok(c.some((x) => x.tabla === 'facturas'));
  });

  it('señala la columna que se sale de lo normal, sin proponer UPDATE', async () => {
    const { explorarTabla } = await import('./exploracion-generica.ts');
    const ejecutar = async (sql: string) => {
      if (sql.includes('WHERE')) return [{ id: 7, cedula: '0102030405', estado: 0, intentos: 3, creado_at: 'x' }];
      if (sql.includes('COUNT(*) AS total')) return [{ total: 100 }];
      if (sql.includes('"estado"')) return [{ valor: 1, veces: 97 }, { valor: 0, veces: 3 }];
      if (sql.includes('"intentos"')) return [{ valor: 0, veces: 95 }, { valor: 3, veces: 5 }];
      return [];
    };
    const r = await explorarTabla(ejecutar, { tabla: 'usuarios', columna: 'cedula', columnas: 5 }, '0102030405', ESQUEMA);
    assert.ok(r, 'deberia encontrar algo');
    assert.deepEqual(r!.diferencias.map((d) => d.columna).sort(), ['estado', 'intentos']);
    assert.ok(!JSON.stringify(r).includes('UPDATE'), 'no debe redactar ninguna escritura');
  });

  it('no dice nada de una fila igual a las demas', async () => {
    const { explorarTabla } = await import('./exploracion-generica.ts');
    const ejecutar = async (sql: string) => {
      if (sql.includes('WHERE')) return [{ id: 1, cedula: '0102030405', estado: 1, intentos: 0 }];
      if (sql.includes('COUNT(*) AS total')) return [{ total: 100 }];
      if (sql.includes('"estado"')) return [{ valor: 1, veces: 97 }];
      if (sql.includes('"intentos"')) return [{ valor: 0, veces: 95 }];
      return [];
    };
    assert.equal(
      await explorarTabla(ejecutar, { tabla: 'usuarios', columna: 'cedula', columnas: 5 }, '0102030405', ESQUEMA),
      null,
    );
  });

  it('se calla con poblacion chica: una rareza entre diez no es una rareza', async () => {
    const { explorarTabla } = await import('./exploracion-generica.ts');
    const ejecutar = async (sql: string) => {
      if (sql.includes('WHERE')) return [{ id: 1, cedula: '0102030405', estado: 0 }];
      if (sql.includes('COUNT(*) AS total')) return [{ total: 8 }];
      return [];
    };
    assert.equal(
      await explorarTabla(ejecutar, { tabla: 'usuarios', columna: 'cedula', columnas: 5 }, '0102030405', ESQUEMA),
      null,
    );
  });

  it('el identificador viaja como parametro, no dentro del SQL', async () => {
    const { explorarTabla } = await import('./exploracion-generica.ts');
    let visto = '';
    const ejecutar = async (sql: string, p: unknown[]) => {
      if (sql.includes('WHERE')) {
        visto = sql;
        assert.deepEqual(p, ["0102030405' OR 1=1--"]);
      }
      return [];
    };
    await explorarTabla(ejecutar, { tabla: 'usuarios', columna: 'cedula', columnas: 5 }, "0102030405' OR 1=1--", ESQUEMA);
    assert.ok(visto.includes('$1') && !visto.includes('OR 1=1'), 'el valor no debe interpolarse');
  });
});

// ─── El grafo del esquema ────────────────────────────────────────────────────

/**
 * Una base inventada con relaciones: el usuario pertenece a una agencia y tiene un
 * perfil, y de él cuelgan movimientos. Lo que se prueba es que la causa se encuentre en
 * la agencia —una tabla que no nombra la cédula por ningún lado— y que el camino hasta
 * ella quede escrito.
 */
describe('grafo del esquema', () => {
  const ESQUEMA = [
    {
      name: 'usuarios',
      columns: [{ name: 'id' }, { name: 'cedula' }, { name: 'estado' }, { name: 'agencia_id' }, { name: 'perfil_id' }],
      foreignKeys: [
        { column: 'agencia_id', referencedTable: 'agencias', referencedColumn: 'id' },
        { column: 'perfil_id', referencedTable: 'perfiles', referencedColumn: 'id' },
      ],
    },
    { name: 'agencias', columns: [{ name: 'id' }, { name: 'activa' }, { name: 'zona' }], foreignKeys: [] },
    { name: 'perfiles', columns: [{ name: 'id' }, { name: 'vigente' }], foreignKeys: [] },
    {
      name: 'movimientos',
      columns: [{ name: 'id' }, { name: 'usuario_id' }, { name: 'tipo' }],
      foreignKeys: [{ column: 'usuario_id', referencedTable: 'usuarios', referencedColumn: 'id' }],
    },
  ];

  it('lee las relaciones en las dos direcciones', async () => {
    const { vecinas } = await import('./exploracion-generica.ts');
    const v = vecinas(ESQUEMA, 'usuarios');

    const padres = v.filter((a) => a.sentido === 'padre').map((a) => a.hacia).sort();
    assert.deepEqual(padres, ['agencias', 'perfiles'], 'a lo que apunta el usuario');

    const hijos = v.filter((a) => a.sentido === 'hijo').map((a) => a.hacia);
    assert.deepEqual(hijos, ['movimientos'], 'lo que apunta al usuario');
  });

  it('encuentra la causa en una tabla que no menciona el identificador', async () => {
    const { explorarVecindario } = await import('./exploracion-generica.ts');

    // La agencia 9 está inactiva mientras el 98 % de las agencias están activas. El
    // usuario en sí no tiene nada raro: la causa está un salto más allá.
    const ejecutar = async (sql: string) => {
      if (sql.includes('FROM "agencias" WHERE')) return [{ id: 9, activa: 0, zona: 'norte' }];
      if (sql.includes('FROM "perfiles" WHERE')) return [{ id: 4, vigente: 1 }];
      if (sql.includes('FROM "movimientos" WHERE')) return [{ id: 1, usuario_id: 7, tipo: 'A' }];
      if (sql.includes('COUNT(*) AS total')) return [{ total: 200 }];
      if (sql.includes('"activa"')) return [{ valor: 1, veces: 196 }, { valor: 0, veces: 4 }];
      if (sql.includes('"zona"')) return [{ valor: 'norte', veces: 180 }];
      if (sql.includes('"vigente"')) return [{ valor: 1, veces: 199 }];
      if (sql.includes('"tipo"')) return [{ valor: 'A', veces: 190 }];
      return [];
    };

    const raiz = {
      tabla: 'usuarios',
      columna: 'cedula',
      identificador: '0102030405',
      diferencias: [],
      poblacion: 200,
      fila: { id: 7, cedula: '0102030405', estado: 1, agencia_id: 9, perfil_id: 4 },
      camino: [],
    };

    const vecindario = await explorarVecindario(ejecutar, raiz, ESQUEMA);
    const enAgencias = vecindario.find((h) => h.tabla === 'agencias');

    assert.ok(enAgencias, 'deberia señalar la agencia');
    assert.deepEqual(enAgencias!.diferencias.map((d) => d.columna), ['activa']);
    assert.deepEqual(enAgencias!.camino, ['usuarios → agencias por agencia_id']);
    assert.ok(!vecindario.some((h) => h.tabla === 'perfiles'), 'el perfil está normal, no debe salir');
  });

  it('no vuelve sobre la tabla de partida ni se repite', async () => {
    const { explorarVecindario } = await import('./exploracion-generica.ts');
    const visitadas: string[] = [];
    const ejecutar = async (sql: string) => {
      const m = /FROM "([^"]+)" WHERE/.exec(sql);
      if (m) visitadas.push(m[1]);
      if (sql.includes('COUNT(*) AS total')) return [{ total: 200 }];
      if (m) return [{ id: 1, usuario_id: 7, agencia_id: 9, perfil_id: 4 }];
      return [];
    };

    await explorarVecindario(
      ejecutar,
      {
        tabla: 'usuarios',
        columna: 'cedula',
        identificador: '1',
        diferencias: [],
        poblacion: 200,
        fila: { id: 7, agencia_id: 9, perfil_id: 4 },
        camino: [],
      },
      ESQUEMA,
    );

    assert.ok(!visitadas.includes('usuarios'), 'no debe volver a la tabla de partida');
    assert.equal(new Set(visitadas).size, visitadas.length, 'ninguna tabla se visita dos veces');
  });

  it('sigue sin proponer ninguna escritura', async () => {
    const { explorarVecindario } = await import('./exploracion-generica.ts');
    const consultas: string[] = [];
    const ejecutar = async (sql: string) => {
      consultas.push(sql);
      if (sql.includes('COUNT(*) AS total')) return [{ total: 200 }];
      if (sql.includes('WHERE')) return [{ id: 9, activa: 0 }];
      if (sql.includes('"activa"')) return [{ valor: 1, veces: 196 }];
      return [];
    };

    const r = await explorarVecindario(
      ejecutar,
      {
        tabla: 'usuarios',
        columna: 'cedula',
        identificador: '1',
        diferencias: [],
        poblacion: 200,
        fila: { id: 7, agencia_id: 9 },
        camino: [],
      },
      ESQUEMA,
    );

    assert.ok(consultas.every((s) => /^\s*SELECT/i.test(s)), 'solo debe leer');
    assert.ok(!JSON.stringify(r).includes('UPDATE'), 'no debe redactar ninguna escritura');
  });
});

// ─── Precedentes ─────────────────────────────────────────────────────────────

describe('buscar precedentes', () => {
  it('descarta las palabras que aparecen en todos los tickets', async () => {
    // Sin esto, «Estimados, buenos dias, reporta un problema» casaba con todo y el
    // orden por relevancia dejaba de significar nada.
    const { aConsultaParaPruebas } = await import('./precedentes.ts');
    const q = aConsultaParaPruebas('Estimados buenos dias el usuario reporta un problema con el bloqueo del PIN');
    assert.ok(q, 'deberia producir consulta');
    for (const vacia of ['estimados', 'buenos', 'dias', 'reporta', 'usuario', 'problema']) {
      assert.ok(!q!.includes(vacia), `no deberia incluir «${vacia}»`);
    }
    assert.ok(q!.includes('bloqueo'), 'deberia conservar la palabra que distingue');
  });

  it('acepta ocultar los artículos que no son para todos', async () => {
    // El filtro tiene que existir ANTES de que haya pantalla de cliente: varios
    // artículos llevan SQL y nombres de tablas, y no son para cualquiera.
    const { buscarPrecedentes } = await import('./precedentes.ts');
    const firma = buscarPrecedentes.toString();
    assert.ok(firma.includes('soloPublicos'), 'debería soportar soloPublicos');
    assert.ok(firma.includes('visible_para'), 'debería filtrar por visible_para');
  });

  it('ignora las palabras demasiado cortas para distinguir', async () => {
    const { aConsultaParaPruebas } = await import('./precedentes.ts');
    assert.equal(aConsultaParaPruebas('de la el un'), null);
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
