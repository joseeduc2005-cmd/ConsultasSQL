import { config } from '../config.ts';
import type { NombreConsulta } from './catalogo.ts';
import type { EstadoFuente, Fila, FuenteDatos, Parametros } from './fuente.ts';

/**
 * Datos 100 % ficticios para probar la aplicación sin conexión a Oracle.
 * Cada escenario reproduce un patrón real ya documentado en los tickets:
 *   860000000000011  POS sano, con volumen alto de transacciones (límite del reporte)
 *   860000000000022  MAC duplicada con un POS inactivo de otro corresponsal (2026083700)
 *   demo.bloqueado   usuario con intentos de PIN agotados
 *   demo.inactivo    usuario desactivado
 *   0100000029       corresponsal con sucursal antigua en fisja.clientes (2026083744)
 */

/** Genera una cédula con dígito verificador válido a partir de 9 dígitos. */
function cedula(base: string): string {
  const suma = [...base].reduce((acc, d, i) => {
    const p = Number(d) * (i % 2 === 0 ? 2 : 1);
    return acc + (p > 9 ? p - 9 : p);
  }, 0);
  return base + ((10 - (suma % 10)) % 10);
}

const SUCURSALES: Record<number, string> = {
  1: 'Oficina Matriz',
  12: 'Oficina Norte',
  28: 'Oficina Sur',
  32: 'Oficina Centro Comercial',
};

const REPRESENTANTES = [
  { cr_id: 9001, cr_cod_socio: 700001, cr_identificacion: cedula('010000001'), codigo_sucursal: 1, cr_estado: 1 },
  { cr_id: 9002, cr_cod_socio: 700002, cr_identificacion: cedula('010000002'), codigo_sucursal: 32, cr_estado: 1 },
  { cr_id: 9003, cr_cod_socio: 700003, cr_identificacion: cedula('010000003'), codigo_sucursal: 12, cr_estado: 1 },
  { cr_id: 9004, cr_cod_socio: 700004, cr_identificacion: cedula('010000004'), codigo_sucursal: 28, cr_estado: 0 },
];

const ESTABLECIMIENTOS = [
  { ce_id: 8001, ce_representante: 9001, ce_razon_social: 'Tienda Demo Norte', ce_estado: 1 },
  { ce_id: 8002, ce_representante: 9002, ce_razon_social: 'Minimarket Demo Centro', ce_estado: 1 },
  { ce_id: 8003, ce_representante: 9003, ce_razon_social: 'Bazar Demo Sur', ce_estado: 1 },
  { ce_id: 8004, ce_representante: 9004, ce_razon_social: 'Farmacia Demo (cerrada)', ce_estado: 0 },
];

const POS = [
  { pos_id: 5001, pos_mac: '860000000000011', pos_estado_id: 1, establecimiento_ce_id: 8001 },
  { pos_id: 5002, pos_mac: '860000000000022', pos_estado_id: 1, establecimiento_ce_id: 8002 },
  { pos_id: 5003, pos_mac: '860000000000033', pos_estado_id: 1, establecimiento_ce_id: 8003 },
  { pos_id: 5004, pos_mac: '860000000000022', pos_estado_id: 2, establecimiento_ce_id: 8004 },
  { pos_id: 5005, pos_mac: '860000000000044', pos_estado_id: 1, establecimiento_ce_id: 8001 },
  { pos_id: 5006, pos_mac: '860000000000055', pos_estado_id: 2, establecimiento_ce_id: 8004 },
];

const USUARIOS = [
  { usr_codigo: 3001, usr_usuario: 'demo.activo', usr_cedula: REPRESENTANTES[0].cr_identificacion, usr_estado: 1, usr_intentos: 0, usr_intentos_pin: 0, usr_is_clave_temporal: 0, usr_is_pin_temporal: 0, codigo_sucursal: 1 },
  { usr_codigo: 3002, usr_usuario: 'demo.duplicado', usr_cedula: REPRESENTANTES[1].cr_identificacion, usr_estado: 1, usr_intentos: 0, usr_intentos_pin: 1, usr_is_clave_temporal: 0, usr_is_pin_temporal: 0, codigo_sucursal: 28 },
  { usr_codigo: 3003, usr_usuario: 'demo.bloqueado', usr_cedula: cedula('010000005'), usr_estado: 1, usr_intentos: 0, usr_intentos_pin: 3, usr_is_clave_temporal: 0, usr_is_pin_temporal: 1, codigo_sucursal: 1 },
  { usr_codigo: 3004, usr_usuario: 'demo.inactivo', usr_cedula: cedula('010000006'), usr_estado: 0, usr_intentos: 2, usr_intentos_pin: 0, usr_is_clave_temporal: 0, usr_is_pin_temporal: 0, codigo_sucursal: 28 },
  { usr_codigo: 3005, usr_usuario: 'demo.antiguo', usr_cedula: REPRESENTANTES[3].cr_identificacion, usr_estado: 0, usr_intentos: 0, usr_intentos_pin: 0, usr_is_clave_temporal: 0, usr_is_pin_temporal: 0, codigo_sucursal: 28 },
  { usr_codigo: 3006, usr_usuario: 'demo.sucursal', usr_cedula: REPRESENTANTES[2].cr_identificacion, usr_estado: 1, usr_intentos: 0, usr_intentos_pin: 0, usr_is_clave_temporal: 0, usr_is_pin_temporal: 0, codigo_sucursal: 12 },
];

const USUARIO_POS = [
  { up_id: 1, up_usuario: 3001, up_pos: 5001 },
  { up_id: 2, up_usuario: 3002, up_pos: 5002 },
  { up_id: 3, up_usuario: 3006, up_pos: 5003 },
  { up_id: 4, up_usuario: 3005, up_pos: 5004 },
  { up_id: 5, up_usuario: 3003, up_pos: 5005 },
  { up_id: 6, up_usuario: 3004, up_pos: 5006 },
];

const SUCURSAL_PERSONA: Record<number, number> = { 700001: 1, 700002: 32, 700003: 1, 700004: 28 };
const SUCURSAL_FISJA: Record<number, number> = { 700001: 1, 700002: 28, 700003: 12, 700004: 28 };

const TIPOS = [1, 2, 11, 13, 15, 17, 24];
const hoy = new Date();
const TRANSACCIONES = Array.from({ length: 420 }, (_, i) => {
  const pos = i < 340 ? 5001 : i < 400 ? 5002 : 5003;
  const fecha = new Date(hoy.getFullYear(), hoy.getMonth(), hoy.getDate() - (i % 28), 9 + (i % 9), (i * 7) % 60);
  const tipo = TIPOS[i % TIPOS.length];
  const sinTendero = pos === 5001 && tipo === 13 && i % 5 === 0;
  return {
    trans_id: 1_200_000 + i,
    trans_pos: pos,
    trans_fecha: fecha,
    trans_tipo: tipo,
    trans_monto: Number((5 + ((i * 13) % 180) + 0.25).toFixed(2)),
    trans_movimiento_tendero: sinTendero ? null : 440_000_000 + i,
  };
});

const dia = (d: unknown): string => (d instanceof Date ? d : new Date(String(d))).toISOString().slice(0, 10);
const posConContexto = (p: (typeof POS)[number]) => {
  const ce = ESTABLECIMIENTOS.find((e) => e.ce_id === p.establecimiento_ce_id);
  const cr = REPRESENTANTES.find((r) => r.cr_id === ce?.ce_representante);
  return {
    ...p,
    ce_razon_social: ce?.ce_razon_social ?? null,
    ce_estado: ce?.ce_estado ?? null,
    cr_id: cr?.cr_id ?? null,
    cr_cod_socio: cr?.cr_cod_socio ?? null,
    cr_identificacion: cr?.cr_identificacion ?? null,
    cr_estado: cr?.cr_estado ?? null,
    cr_sucursal: cr?.codigo_sucursal ?? null,
  };
};
const posView = () =>
  USUARIO_POS.map((up) => {
    const p = POS.find((x) => x.pos_id === up.up_pos)!;
    const u = USUARIOS.find((x) => x.usr_codigo === up.up_usuario)!;
    return { usr_usuario: u.usr_usuario, pos_mac: p.pos_mac, pos_estado_id: p.pos_estado_id };
  });
const enRango = (t: (typeof TRANSACCIONES)[number], p: Parametros) =>
  dia(t.trans_fecha) >= dia(p.desde) && dia(t.trans_fecha) <= dia(p.hasta);

const RESOLVEDORES: Record<NombreConsulta, (p: Parametros) => Fila[]> = {
  ping: () => [{ ok: 1 }],
  // El perfil de la poblacion sana necesita volumen real: con cinco usuarios
  // ficticios cualquier moda seria ruido. En demostracion no se explora.
  perfilUsrEstado: () => [],
  perfilUsrIntentos: () => [],
  perfilUsrIntentosPin: () => [],
  perfilUsrIsClaveTemporal: () => [],
  perfilUsrIsPinTemporal: () => [],
  usuariosActivosMuestra: () => [],
  usuarioPorUsuario: (p) => USUARIOS.filter((u) => u.usr_usuario === p.usuario),
  usuarioPorCedula: (p) => USUARIOS.filter((u) => u.usr_cedula === p.cedula),
  usuariosPorMac: (p) =>
    posView()
      .filter((v) => v.pos_mac === p.mac && v.pos_estado_id === 1)
      .map((v) => ({ ...USUARIOS.find((u) => u.usr_usuario === v.usr_usuario)!, pos_mac: v.pos_mac })),
  usuariosBloqueados: (p) =>
    USUARIOS.filter(
      (u) => u.usr_estado === 1 && (u.usr_intentos_pin >= Number(p.max_pin) || u.usr_intentos >= Number(p.max_clave)),
    ),
  posPorMac: (p) =>
    POS.filter((x) => x.pos_mac === p.mac)
      .sort((a, b) => a.pos_estado_id - b.pos_estado_id)
      .map(posConContexto),
  usuariosDePosPorMac: (p) =>
    USUARIO_POS.flatMap((up) => {
      const pos = POS.find((x) => x.pos_id === up.up_pos)!;
      if (pos.pos_mac !== p.mac) return [];
      const u = USUARIOS.find((x) => x.usr_codigo === up.up_usuario)!;
      return [{ ...up, usr_usuario: u.usr_usuario, usr_cedula: u.usr_cedula, usr_estado: u.usr_estado, pos_estado_id: pos.pos_estado_id }];
    }),
  posDeUsuario: (p) =>
    USUARIO_POS.filter((up) => up.up_usuario === Number(p.usr_codigo)).map((up) => {
      const pos = POS.find((x) => x.pos_id === up.up_pos)!;
      return { pos_id: pos.pos_id, pos_mac: pos.pos_mac, pos_estado_id: pos.pos_estado_id };
    }),
  macsDuplicadas: () => {
    const grupos = Map.groupBy(POS, (p) => p.pos_mac);
    return [...grupos]
      .filter(([, lista]) => lista.length > 1)
      .map(([mac, lista]) => ({ pos_mac: mac, total: lista.length, activos: lista.filter((x) => x.pos_estado_id === 1).length }));
  },
  representantePorCedula: (p) => REPRESENTANTES.filter((r) => r.cr_identificacion === p.cedula),
  sucursalesDeSocio: (p) => {
    const socio = Number(p.codigo_socio);
    const cr = REPRESENTANTES.filter((r) => r.cr_cod_socio === socio);
    const filas = [
      ...cr.map((r) => ({ fuente: 'corresponsales.corresponsalrepresentante', codigo_sucursal: r.codigo_sucursal })),
      ...(socio in SUCURSAL_PERSONA ? [{ fuente: 'jst_personas.vwt_cliente', codigo_sucursal: SUCURSAL_PERSONA[socio] }] : []),
      ...(socio in SUCURSAL_FISJA ? [{ fuente: 'fisja.clientes', codigo_sucursal: SUCURSAL_FISJA[socio] }] : []),
    ];
    return filas.map((f) => ({ ...f, nombre_sucursal: SUCURSALES[f.codigo_sucursal] ?? null }));
  },
  sucursalCorresponsalVsPersona: () =>
    REPRESENTANTES.filter((r) => r.cr_estado === 1 && SUCURSAL_PERSONA[r.cr_cod_socio] !== r.codigo_sucursal).map((r) => ({
      cr_id: r.cr_id,
      cr_cod_socio: r.cr_cod_socio,
      cr_identificacion: r.cr_identificacion,
      sucursal_corresponsal: r.codigo_sucursal,
      sucursal_persona: SUCURSAL_PERSONA[r.cr_cod_socio],
    })),
  sucursalCorresponsalVsFisja: () =>
    REPRESENTANTES.filter((r) => r.cr_estado === 1 && SUCURSAL_FISJA[r.cr_cod_socio] !== r.codigo_sucursal).map((r) => ({
      cr_id: r.cr_id,
      cr_cod_socio: r.cr_cod_socio,
      cr_identificacion: r.cr_identificacion,
      sucursal_corresponsal: r.codigo_sucursal,
      sucursal_fisja: SUCURSAL_FISJA[r.cr_cod_socio],
    })),
  resumenTransaccionesPorMac: (p) => {
    const ids = POS.filter((x) => x.pos_mac === p.mac).map((x) => x.pos_id);
    const lista = TRANSACCIONES.filter((t) => ids.includes(t.trans_pos) && enRango(t, p));
    return [{ total: lista.length, sin_movimiento_tendero: lista.filter((t) => t.trans_movimiento_tendero === null).length }];
  },
  transaccionesSinMovimientoTendero: (p) => {
    const ids = POS.filter((x) => x.pos_mac === p.mac).map((x) => x.pos_id);
    return TRANSACCIONES.filter((t) => ids.includes(t.trans_pos) && enRango(t, p) && t.trans_movimiento_tendero === null)
      .slice(0, 50)
      .map(({ trans_id, trans_fecha, trans_tipo, trans_monto }) => ({ trans_id, trans_fecha, trans_tipo, trans_monto }));
  },
  tenderoNuloPorTipo: (p) => {
    const limite = new Date(hoy.getFullYear(), hoy.getMonth(), hoy.getDate() - Number(p.dias));
    const nulas = TRANSACCIONES.filter((t) => t.trans_movimiento_tendero === null && t.trans_fecha >= limite);
    return [...Map.groupBy(nulas, (t) => t.trans_tipo)].map(([tipo, lista]) => ({
      trans_tipo: tipo,
      total: lista.length,
      primera: new Date(Math.min(...lista.map((t) => t.trans_fecha.getTime()))),
      ultima: new Date(Math.max(...lista.map((t) => t.trans_fecha.getTime()))),
    }));
  },
  nulosEnSubconsultasReporte: () => [{ fondeo_nulos: 1, reverso_nulos: 0 }],
};

export class FuenteDemo implements FuenteDatos {
  readonly modo = 'demo' as const;

  async consultar(nombre: NombreConsulta, parametros: Parametros = {}): Promise<Fila[]> {
    await new Promise((r) => setTimeout(r, 40 + Math.random() * 80));
    return structuredClone(RESOLVEDORES[nombre](parametros)).slice(0, config.oracle.maxFilas);
  }

  async estado(): Promise<EstadoFuente> {
    return { modo: 'demo', conectado: true, detalle: 'Datos ficticios de demostración' };
  }

  async cerrar(): Promise<void> {}
}
