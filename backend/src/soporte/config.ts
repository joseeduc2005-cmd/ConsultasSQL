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
}

/** Busca una base por id en el mismo JSON que carga MultiDbRegistry. */
function buscarEnMultiDb(id: string): EntradaMultiDb | null {
  const archivo = texto(process.env.MULTI_DB_CONFIG_FILE) || './config/multidb.databases.json';
  const ruta = isAbsolute(archivo) ? archivo : resolve(process.cwd(), archivo);
  try {
    const { databases } = JSON.parse(readFileSync(ruta, 'utf8')) as { databases?: EntradaMultiDb[] };
    return (databases ?? []).find((d) => d.id === id && d.enabled !== false) ?? null;
  } catch {
    return null;
  }
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
function resolverOracle(): { modo: ModoOracle; usuario: string; clave: string; conexion: string } {
  const vacio = { modo: 'demo' as const, usuario: '', clave: '', conexion: '' };
  if (texto(process.env.SOPORTE_MODO).toLowerCase() === 'demo') return vacio;

  const usuario = texto(process.env.SOPORTE_ORACLE_USUARIO);
  const clave = texto(process.env.SOPORTE_ORACLE_CLAVE);
  const conexion = texto(process.env.SOPORTE_ORACLE_CONEXION);
  if (usuario && clave && conexion) return { modo: 'oracle', usuario, clave, conexion };

  const entrada = buscarEnMultiDb(texto(process.env.SOPORTE_ORACLE_DB_ID) || 'oracle');
  if (entrada?.type === 'oracle' && entrada.user && entrada.password && entrada.host && entrada.database) {
    return {
      modo: 'oracle',
      usuario: entrada.user,
      clave: entrada.password,
      conexion: `${entrada.host}:${entrada.port ?? 1521}/${entrada.database}`,
    };
  }
  return vacio;
}

const oracle = resolverOracle();

export const config = {
  version: '2.0.0',
  /** Prefijo de todas las rutas HTTP del módulo. */
  basePath: '/api/soporte',
  rutas: {
    semillas: resolve(raizSoporte, 'seed'),
  },
  oracle: {
    ...oracle,
    maxFilas: numero(process.env.SOPORTE_ORACLE_MAX_FILAS, 500),
    timeoutMs: numero(process.env.SOPORTE_ORACLE_TIMEOUT_MS, 20_000),
  },
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
