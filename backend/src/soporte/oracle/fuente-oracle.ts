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
 * Errores que significan «el pool ya no sirve», no «esta consulta falló».
 *
 * Cuando Oracle se reinicia, las conexiones que el pool tenía guardadas quedan
 * muertas: pedir una se encola y revienta por tiempo (NJS-040) una y otra vez,
 * aunque la base ya esté arriba. La única salida es tirar el pool y rehacerlo.
 */
const POOL_INSERVIBLE =
  /\b(NJS-040|NJS-500|NJS-501|NJS-503|NJS-510|ORA-03113|ORA-03114|ORA-12514|ORA-12537|ORA-12541|ORA-12571|ORA-01089|ORA-28547)\b/i;

/**
 * Conexión real a Oracle en modo "thin" (no requiere Oracle Instant Client).
 * Cada consulta corre dentro de una transacción READ ONLY que se descarta al terminar.
 */
export interface CredencialesOracle {
  usuario: string;
  clave: string;
  conexion: string;
}

export class FuenteOracle implements FuenteDatos {
  readonly modo = 'oracle' as const;
  #pool: oracledb.Pool | null = null;
  /** Evita que varias consultas simultáneas rehagan el pool a la vez. */
  #rehaciendo: Promise<void> | null = null;
  readonly #credenciales: CredencialesOracle;

  /**
   * Las credenciales se reciben en vez de leerse de la config global porque ahora hay
   * una fuente por base registrada: producción, contingencia y la de pruebas pueden
   * estar vivas a la vez, cada una con su pool.
   */
  constructor(credenciales: CredencialesOracle = config.oracle) {
    this.#credenciales = credenciales;
  }

  async #obtenerPool(): Promise<oracledb.Pool> {
    if (!this.#pool) {
      const { usuario, clave, conexion } = this.#credenciales;
      if (!usuario || !clave || !conexion) {
        throw new Error('Faltan usuario, clave o cadena de conexión para esta base Oracle.');
      }
      this.#pool = await oracledb.createPool({
        user: usuario,
        password: clave,
        connectString: conexion,
        poolMin: 0,
        poolMax: 4,
        poolIncrement: 1,
        // Con la base inalcanzable el driver esperaría ~20 s por intento. Acotarlo
        // evita que un Oracle caído bloquee /estado y los diagnósticos.
        connectTimeout: config.oracle.conectarTimeoutS,
        transportConnectTimeout: config.oracle.conectarTimeoutS,
        queueTimeout: config.oracle.conectarTimeoutS * 1000,
        // Una conexión que lleva más de 10 s guardada se comprueba antes de
        // entregarla. Es lo que descarta las que murieron mientras estaba ociosa.
        poolPingInterval: 10,
      });
    }
    return this.#pool;
  }

  /** Cierra el pool y deja que el siguiente uso lo cree de nuevo. */
  async #rehacerPool(): Promise<void> {
    if (this.#rehaciendo) return this.#rehaciendo;
    const anterior = this.#pool;
    this.#pool = null;
    this.#rehaciendo = (async () => {
      // `drainTime` 0 corta sin esperar: las conexiones ya están muertas.
      await anterior?.close(0).catch(() => undefined);
    })();
    try {
      await this.#rehaciendo;
    } finally {
      this.#rehaciendo = null;
    }
  }

  async #ejecutar(sql: string, parametros: Parametros): Promise<Fila[]> {
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

  /**
   * Ejecuta una consulta del catálogo, rehaciendo el pool si quedó inservible.
   *
   * Un reintento y no más: si la base sigue caída, el segundo falla igual y el
   * error llega a quien preguntó en vez de quedarse dando vueltas.
   */
  async consultar(nombre: NombreConsulta, parametros: Parametros = {}): Promise<Fila[]> {
    const { sql } = CATALOGO[nombre];
    asegurarSoloLectura(sql);
    try {
      return await this.#ejecutar(sql, parametros);
    } catch (error) {
      if (!POOL_INSERVIBLE.test((error as Error).message)) throw error;
      await this.#rehacerPool();
      return await this.#ejecutar(sql, parametros);
    }
  }

  async estado(): Promise<EstadoFuente> {
    try {
      await this.consultar('ping');
      return { modo: 'oracle', conectado: true, detalle: `Conectado a ${this.#credenciales.conexion}` };
    } catch (error) {
      return { modo: 'oracle', conectado: false, detalle: (error as Error).message };
    }
  }

  async cerrar(): Promise<void> {
    await this.#pool?.close(5);
    this.#pool = null;
  }
}
