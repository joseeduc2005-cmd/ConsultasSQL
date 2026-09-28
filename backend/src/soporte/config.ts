/**
 * Configuración del módulo de soporte (tickets, diagnósticos y vigilancia).
 *
 * Reemplaza al antiguo `config/env.ts` de ticket v2.0: en lugar de tener su propio
 * archivo .env, lee el del backend y resuelve la conexión a Oracle desde el mismo
 * `multidb.databases.json` que usa el resto del sistema, para no duplicar credenciales.
 */
import { readFileSync } from 'node:fs';
import { isAbsolute, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export type ModoOracle = 'demo' | 'oracle';

/**
 * Los datos de demostración solo se usan si alguien los pide a propósito.
 *
 * Antes, quedarse sin Oracle configurada hacía caer al modo demostración en silencio,
 * y los diagnósticos respondían con hallazgos inventados —usuarios que no existen,
 * bloqueos que nadie tiene— con una etiqueta pequeña como única advertencia. En un
 * banco eso es peor que no responder.
 */
const demoPedido = (process.env.SOPORTE_MODO ?? '').trim().toLowerCase() === 'demo';

const raizSoporte = fileURLToPath(new URL('./', import.meta.url));

function numero(valor: string | undefined, porDefecto: number): number {
  const n = Number(valor);
  return Number.isFinite(n) && n > 0 ? n : porDefecto;
}

function texto(valor: string | undefined): string {
  return (valor ?? '').trim();
}

interface EntradaMultiDb {
  id?: string;
  type?: string;
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
  enabled?: boolean;
  /**
   * Marca de este módulo dentro de la entrada de multidb.
   *
   *   "soporte": { "tickets": true, "catalogo": "corresponsales" }
   *
   * `tickets` la habilita para investigar tickets. `catalogo` dice qué juego de
   * consultas entiende: las del módulo están escritas contra CORRESPONSALES.USUARIO,
   * FISJA.CLIENTES y demás, así que apuntarlas a una base sin esos esquemas no da un
   * mal diagnóstico — da un error. Declararlo deja que la pantalla ofrezca solo las
   * que sirven en vez de dejar que se elija cualquiera.
   */
  soporte?: { tickets?: boolean; catalogo?: string };
  /** Campos de organización del archivo; el registro del motor los ignora. */
  label?: string;
  grupo?: string;
  color?: string;
  nota?: string;
}

/** Catálogos de consultas que el módulo sabe ejecutar. */
export const CATALOGOS_SOPORTADOS = ['corresponsales'] as const;
export type CatalogoSoporte = (typeof CATALOGOS_SOPORTADOS)[number];

function leerMultiDb(): EntradaMultiDb[] {
  const archivo = texto(process.env.MULTI_DB_CONFIG_FILE) || './config/multidb.databases.json';
  const ruta = isAbsolute(archivo) ? archivo : resolve(process.cwd(), archivo);
  try {
    const { databases } = JSON.parse(readFileSync(ruta, 'utf8')) as { databases?: EntradaMultiDb[] };
    return (databases ?? []).filter((d) => d.enabled !== false);
  } catch {
    return [];
  }
}

/** Busca una base por id en el mismo JSON que carga MultiDbRegistry. */
function buscarEnMultiDb(id: string): EntradaMultiDb | null {
  return leerMultiDb().find((d) => d.id === id) ?? null;
}

export interface BaseTickets {
  id: string;
  etiqueta: string;
  /** produccion | pruebas | otras. Agrupa el selector del ticket. */
  grupo: string;
  /** rojo | ambar | verde | azul | gris. Rojo avisa de que toca datos reales. */
  color: string;
  nota: string | null;
  catalogo: CatalogoSoporte;
  usuario: string;
  clave: string;
  conexion: string;
  /** La que se usa cuando el ticket no dice contra cuál investigar. */
  porDefecto: boolean;
}

/**
 * Una base sirve para investigar tickets si está completa y su motor tiene catálogo.
 *
 * Hoy solo Oracle: las consultas de los diagnósticos están escritas en su dialecto y
 * contra esquemas concretos. La base de conocimiento —buscar, registrar, resolver,
 * publicar— sí funciona con cualquier motor; lo que está atado a Oracle es la mitad
 * que consulta datos ajenos, y para sumar otro motor hace falta su propio catálogo.
 */
function completa(e: EntradaMultiDb): boolean {
  return Boolean(e.type === 'oracle' && e.user && e.password && e.host && e.database);
}

/** Avisa de las bases marcadas que quedaron fuera, en vez de descartarlas en silencio. */
function avisarDescartadas(entradas: EntradaMultiDb[]): void {
  for (const e of entradas) {
    if (e.soporte?.tickets !== true || completa(e)) continue;
    const motivo =
      e.type !== 'oracle'
        ? `su motor es «${e.type}» y todavía no hay catálogo de consultas para él`
        : 'le faltan usuario, clave, host o base';
    console.warn(`⚠ [soporte] La base «${e.id}» está marcada para tickets pero no se usará: ${motivo}.`);
  }
}

function aBaseTickets(e: EntradaMultiDb, porDefecto: boolean): BaseTickets {
  const catalogo = (e.soporte?.catalogo ?? 'corresponsales') as CatalogoSoporte;
  return {
    id: e.id!,
    etiqueta: e.label ?? e.id!,
    grupo: e.grupo ?? 'otras',
    // Rojo por defecto en producción: si nadie lo declaró, más vale avisar de más.
    color: e.color ?? (e.grupo === 'pruebas' ? 'ambar' : 'rojo'),
    nota: e.nota ?? null,
    catalogo,
    usuario: e.user!,
    clave: e.password!,
    conexion: `${e.host}:${e.port ?? 1521}/${e.database}`,
    porDefecto,
  };
}

/**
 * Bases contra las que se puede investigar un ticket.
 *
 * Son las entradas de multidb marcadas con `soporte.tickets`, más la que resuelva
 * `SOPORTE_ORACLE_DB_ID` aunque no lleve la marca: si no, actualizar el módulo
 * dejaría sin diagnósticos a quien ya lo tenía andando.
 */
function resolverBasesTickets(idPorDefecto: string): BaseTickets[] {
  const todas = leerMultiDb();
  avisarDescartadas(todas);
  const entradas = todas.filter(completa);
  const marcadas = entradas.filter((e) => e.soporte?.tickets === true);
  const porDefecto = entradas.find((e) => e.id === idPorDefecto);

  const elegidas = [...marcadas];
  if (porDefecto && !elegidas.some((e) => e.id === porDefecto.id)) elegidas.unshift(porDefecto);

  return elegidas
    .filter((e) => CATALOGOS_SOPORTADOS.includes((e.soporte?.catalogo ?? 'corresponsales') as CatalogoSoporte))
    .map((e) => aBaseTickets(e, e.id === idPorDefecto));
}

/**
 * Credenciales de Oracle para los diagnósticos.
 *
 * Orden de resolución:
 *   1. SOPORTE_ORACLE_USUARIO / _CLAVE / _CONEXION  → usuario propio (lo ideal: solo SELECT).
 *   2. La entrada `SOPORTE_ORACLE_DB_ID` (por defecto "oracle") de multidb.databases.json.
 *   3. Modo demostración con datos ficticios.
 *
 * Con SOPORTE_MODO=demo se fuerza el modo demostración sin tocar nada más.
 */
function resolverOracle(): { modo: ModoOracle; usuario: string; clave: string; conexion: string; dbId: string | null } {
  const vacio = { modo: 'demo' as const, usuario: '', clave: '', conexion: '', dbId: null };
  if (demoPedido) return vacio;

  const usuario = texto(process.env.SOPORTE_ORACLE_USUARIO);
  const clave = texto(process.env.SOPORTE_ORACLE_CLAVE);
  const conexion = texto(process.env.SOPORTE_ORACLE_CONEXION);
  if (usuario && clave && conexion) return { modo: 'oracle', usuario, clave, conexion, dbId: null };

  const idBuscado = texto(process.env.SOPORTE_ORACLE_DB_ID) || 'oracle';
  const entrada = buscarEnMultiDb(idBuscado);
  if (entrada?.type === 'oracle' && entrada.user && entrada.password && entrada.host && entrada.database) {
    return {
      modo: 'oracle',
      usuario: entrada.user,
      clave: entrada.password,
      conexion: `${entrada.host}:${entrada.port ?? 1521}/${entrada.database}`,
      // Se conserva para que el frontend sepa contra qué base registrada corren
      // los diagnósticos y pueda ocultarlos en tickets de otra base.
      dbId: idBuscado,
    };
  }
  return vacio;
}

const oracle = resolverOracle();
const idPorDefecto = texto(process.env.SOPORTE_ORACLE_DB_ID) || 'oracle';
const basesTickets = oracle.modo === 'oracle' ? resolverBasesTickets(idPorDefecto) : [];

export const config = {
  version: '2.0.0',
  /** Prefijo de todas las rutas HTTP del módulo. */
  basePath: '/api/soporte',
  rutas: {
    semillas: resolve(raizSoporte, 'seed'),
  },
  /**
   * Bases contra las que se investiga un ticket. Vacío en modo demostración.
   * La pantalla del ticket solo ofrece estas; el resto quedan como contexto.
   */
  basesTickets,
  /** `true` solo si se pidió SOPORTE_MODO=demo. */
  demoPedido,
  oracle: {
    ...oracle,
    maxFilas: numero(process.env.SOPORTE_ORACLE_MAX_FILAS, 500),
    timeoutMs: numero(process.env.SOPORTE_ORACLE_TIMEOUT_MS, 20_000),
    // Segundos para dar por perdido el intento de conexión. Sin esto, con Oracle
    // inalcanzable el driver se queda esperando ~20 s y arrastra a /estado con él.
    conectarTimeoutS: numero(process.env.SOPORTE_ORACLE_CONECTAR_TIMEOUT_S, 6),
  },
  /**
   * Con `false` (el valor por defecto) los artículos de solución se publican como
   * documentación: guardan el SQL y sus campos, pero el panel no muestra el botón de
   * ejecutar. Encenderlo es una decisión aparte — hay que confirmar que el usuario de
   * Oracle tenga permiso de escritura y que el equipo acepte que la corrección se
   * aplique desde aquí. Un despliegue nuevo nunca debería poder escribir el día uno.
   */
  correccionesEjecutables: texto(process.env.SOPORTE_CORRECCIONES_EJECUTABLES).toLowerCase() === 'on',
  reglas: {
    maxIntentosPin: numero(process.env.MAX_INTENTOS_PIN, 3),
    maxIntentosClave: numero(process.env.MAX_INTENTOS_CLAVE, 3),
    limiteReporte: numero(process.env.LIMITE_REPORTE_TRANSACCIONES, 300),
  },
  vocabulario: {
    // Siembra automática del vocabulario de la Biblioteca en el motor de consultas
    // la primera vez. Con "off" solo se hace bajo demanda desde /api/soporte/vocabulario.
    automatico: texto(process.env.SOPORTE_VOCABULARIO).toLowerCase() !== 'off',
  },
  vigilancia: {
    // A diferencia de ticket v2.0, aquí la vigilancia arranca apagada:
    // hay que encenderla a propósito con VIGILANCIA=on.
    habilitada: texto(process.env.VIGILANCIA).toLowerCase() === 'on',
  },
} as const;
