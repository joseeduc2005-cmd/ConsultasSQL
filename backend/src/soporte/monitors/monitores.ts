import { config } from '../config.ts';
import type { AlertaDetectada } from '../knowledge/alertas.ts';
import type { FuenteDatos } from '../puertos/fuente-datos.ts';

export interface Monitor {
  id: string;
  nombre: string;
  descripcion: string;
  /** Cada cuántos minutos se ejecuta automáticamente. */
  intervaloMin: number;
  ticketsReferencia: string[];
  /** Playbook recomendado para investigar cada alerta. */
  playbook?: string;
  revisar(fuente: FuenteDatos): Promise<AlertaDetectada[]>;
}

const t = (v: unknown) => (v === null || v === undefined ? '' : String(v));
const fecha = (v: unknown) => (v instanceof Date ? v.toISOString().slice(0, 10) : t(v).slice(0, 10));

export const MONITORES: Monitor[] = [
  {
    id: 'macs-duplicadas',
    nombre: 'MACs duplicadas',
    descripcion: 'MACs registradas en más de un POS. Es la causa más frecuente de "usuario no encontrado" y "Unexpected Error" al ingresar.',
    intervaloMin: 60,
    ticketsReferencia: ['2026083700', 'SIN-NUMERO-1105681702'],
    playbook: 'mac-duplicada',
    async revisar(fuente) {
      const filas = await fuente.consultar('macsDuplicadas');
      return filas.map((f) => ({
        clave: t(f.pos_mac),
        titulo: `MAC ${t(f.pos_mac)} en ${t(f.total)} POS`,
        detalle: `${t(f.activos)} activo(s) de ${t(f.total)} registros.`,
        severidad: Number(f.activos) > 1 ? 'critica' : 'alta',
        datos: { mac: t(f.pos_mac) },
      }));
    },
  },
  {
    id: 'sucursal-vs-persona',
    nombre: 'Sucursal distinta a JST_PERSONAS',
    descripcion: 'Corresponsales activos cuya sucursal no coincide con la de JST_PERSONAS. Indica un cambio de oficina aplicado a medias.',
    intervaloMin: 360,
    ticketsReferencia: ['2026083744'],
    playbook: 'sucursal-corresponsal',
    async revisar(fuente) {
      const filas = await fuente.consultar('sucursalCorresponsalVsPersona');
      return filas.map((f) => ({
        clave: t(f.cr_id),
        titulo: `Corresponsal ${t(f.cr_identificacion)}: sucursal ${t(f.sucursal_corresponsal)} vs ${t(f.sucursal_persona)}`,
        detalle: `Corresponsales registra la sucursal ${t(f.sucursal_corresponsal)} y JST_PERSONAS la ${t(f.sucursal_persona)}.`,
        severidad: 'critica',
        datos: { cedula: t(f.cr_identificacion), codigo_socio: t(f.cr_cod_socio) },
      }));
    },
  },
  {
    id: 'sucursal-fisja',
    nombre: 'Sucursal antigua en fisja.clientes',
    descripcion: 'Corresponsales cuyas transacciones se contabilizan en una oficina anterior (descuadre en JASIT) hasta desplegar el fix del ticket 2026083744.',
    intervaloMin: 360,
    ticketsReferencia: ['2026083744'],
    playbook: 'sucursal-corresponsal',
    async revisar(fuente) {
      const filas = await fuente.consultar('sucursalCorresponsalVsFisja');
      return filas.map((f) => ({
        clave: t(f.cr_id),
        titulo: `Corresponsal ${t(f.cr_identificacion)} contabiliza en la sucursal ${t(f.sucursal_fisja)}`,
        detalle: `Su sucursal vigente es ${t(f.sucursal_corresponsal)}, pero fisja.clientes conserva la ${t(f.sucursal_fisja)}.`,
        severidad: 'alta',
        datos: { cedula: t(f.cr_identificacion), codigo_socio: t(f.cr_cod_socio) },
      }));
    },
  },
  {
    id: 'usuarios-bloqueados',
    nombre: 'Usuarios bloqueados',
    descripcion: 'Usuarios activos que alcanzaron el máximo de intentos fallidos de PIN o contraseña. Suelen convertirse en ticket el mismo día.',
    intervaloMin: 30,
    ticketsReferencia: [],
    playbook: 'acceso-corresponsal',
    async revisar(fuente) {
      const filas = await fuente.consultar('usuariosBloqueados', {
        max_pin: config.reglas.maxIntentosPin,
        max_clave: config.reglas.maxIntentosClave,
      });
      return filas.map((f) => ({
        clave: t(f.usr_codigo),
        titulo: `Usuario ${t(f.usr_usuario)} bloqueado`,
        detalle: `Intentos de PIN: ${t(f.usr_intentos_pin)} · intentos de contraseña: ${t(f.usr_intentos)}.`,
        severidad: 'media',
        datos: { usuario: t(f.usr_usuario), cedula: t(f.usr_cedula) },
      }));
    },
  },
  {
    id: 'tendero-nulo',
    nombre: 'Transacciones sin movimiento de tendero',
    descripcion: 'Transacciones de los últimos 7 días con trans_movimiento_tendero en NULL, que impiden generar el reporte del POS.',
    intervaloMin: 120,
    ticketsReferencia: ['2026091974'],
    playbook: 'reporte-transacciones',
    async revisar(fuente) {
      const filas = await fuente.consultar('tenderoNuloPorTipo', { dias: 7 });
      return filas.map((f) => ({
        clave: `tipo-${t(f.trans_tipo)}`,
        titulo: `${t(f.total)} transacción(es) de tipo ${t(f.trans_tipo)} sin movimiento de tendero`,
        detalle: `Entre ${fecha(f.primera)} y ${fecha(f.ultima)}.`,
        severidad: 'alta',
        datos: { tipo: t(f.trans_tipo) },
      }));
    },
  },
  {
    id: 'nulos-reporte',
    nombre: 'NULL en subconsultas del reporte',
    descripcion: 'Valores NULL en movimiento_fondeo o reverso que pueden dejar vacío el reporte de transacciones (NOT IN).',
    intervaloMin: 720,
    ticketsReferencia: [],
    playbook: 'reporte-transacciones',
    async revisar(fuente) {
      const [f] = await fuente.consultar('nulosEnSubconsultasReporte');
      const alertas: AlertaDetectada[] = [];
      if (Number(f?.fondeo_nulos) > 0) {
        alertas.push({
          clave: 'fondeo',
          titulo: `${t(f.fondeo_nulos)} registro(s) de movimiento_fondeo con id_transaccion_rev NULL`,
          detalle: 'Anula el filtro NOT IN del reporte de transacciones.',
          severidad: 'media',
        });
      }
      if (Number(f?.reverso_nulos) > 0) {
        alertas.push({
          clave: 'reverso',
          titulo: `${t(f.reverso_nulos)} registro(s) de reverso con rev_transaccion NULL`,
          detalle: 'Anula el filtro NOT IN del reporte de transacciones.',
          severidad: 'media',
        });
      }
      return alertas;
    },
  },
];

export const obtenerMonitor = (id: string) => MONITORES.find((m) => m.id === id);
