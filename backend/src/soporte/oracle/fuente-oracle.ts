import oracledb from 'oracledb';
import { config } from '../config.ts';
import { CATALOGO, type NombreConsulta } from './catalogo.ts';
import { asegurarSoloLectura } from './guardia.ts';
import type { EstadoFuente, Fila, FuenteDatos, Parametros } from './fuente.ts';

/** Conserva solo los parámetros que la sentencia realmente usa (Oracle rechaza binds sobrantes). */
function parametrosUsados(sql: string, parametros: Parametros): Parametros {
  const usados = new Set([...sql.matchAll(/:([a-z_][a-z0-9_]*)/gi)].map((m) => m[1].toLowerCase()));
  return Object.fromEntries(Object.entries(parametros).filter(([k]) => usados.has(k.toLowerCase())));
}

function normalizarFila(fila: Record<string, unknown>): Fila {
  return Object.fromEntries(Object.entries(fila).map(([k, v]) => [k.toLowerCase(), v]));
}

/**
 * Conexión real a Oracle en modo "thin" (no requiere Oracle Instant Client).
 * Cada consulta corre dentro de una transacción READ ONLY que se descarta al terminar.
 */
export class FuenteOracle implements FuenteDatos {
  readonly modo = 'oracle' as const;
  #pool: oracledb.Pool | null = null;

  async #obtenerPool(): Promise<oracledb.Pool> {
    if (!this.#pool) {
      const { usuario, clave, conexion } = config.oracle;
      if (!usuario || !clave || !conexion) {
        throw new Error('Faltan ORACLE_USUARIO, ORACLE_CLAVE u ORACLE_CONEXION en el archivo .env.');
      }
      this.#pool = await oracledb.createPool({
        user: usuario,
        password: clave,
        connectString: conexion,
        poolMin: 0,
        poolMax: 4,
        poolIncrement: 1,
      });
    }
    return this.#pool;
  }

  async consultar(nombre: NombreConsulta, parametros: Parametros = {}): Promise<Fila[]> {
    const { sql } = CATALOGO[nombre];
    asegurarSoloLectura(sql);
    const pool = await this.#obtenerPool();
    const conexion = await pool.getConnection();
    try {
      conexion.callTimeout = config.oracle.timeoutMs;
      await conexion.execute('SET TRANSACTION READ ONLY');
      const resultado = await conexion.execute<Record<string, unknown>>(sql, parametrosUsados(sql, parametros), {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
        maxRows: config.oracle.maxFilas,
      });
      return (resultado.rows ?? []).map(normalizarFila);
    } finally {
      await conexion.rollback().catch(() => undefined);
      await conexion.close().catch(() => undefined);
    }
  }

  async estado(): Promise<EstadoFuente> {
    try {
      await this.consultar('ping');
      return { modo: 'oracle', conectado: true, detalle: `Conectado a ${config.oracle.conexion}` };
    } catch (error) {
      return { modo: 'oracle', conectado: false, detalle: (error as Error).message };
    }
  }

  async cerrar(): Promise<void> {
    await this.#pool?.close(5);
    this.#pool = null;
  }
}
