/**
 * Exploración: proponer una corrección para un caso que el catálogo no reconoce.
 *
 * Los playbooks saben comprobar cuatro problemas concretos porque alguien los
 * escribió como reglas. Ante un caso nuevo devuelven «sin hallazgos» y ahí se acaba
 * el sistema. Esto es lo que sigue: en vez de preguntar «¿se cumple esta regla?»,
 * pregunta «¿en qué se diferencia este usuario de los que sí están operando?».
 *
 * Cómo funciona, en tres pasos:
 *
 *   1. Se define quién está sano: los usuarios que hicieron al menos una transacción
 *      en los últimos días. Esa es la población de referencia.
 *   2. Por cada columna corregible se mira con qué valor opera esa población. Si el
 *      97 % de los que operan tienen `usr_estado = 1` y este usuario tiene `0`, eso
 *      es una anomalía — y nadie tuvo que escribir la regla.
 *   3. Las anomalías de una misma fila se juntan en UNA sola sentencia.
 *
 * Ese último paso es lo que evita entregar cinco SQL para probar de uno en uno: como
 * todas las columnas viven en la misma fila de CORRESPONSALES.USUARIO, la corrección
 * es un único UPDATE que las arregla juntas.
 *
 * Tres límites deliberados:
 *
 *   - Solo se proponen columnas de LISTA_BLANCA, que son exactamente las que las
 *     correcciones ya aprobadas tocan. La exploración nunca propone escribir en una
 *     columna que nadie autorizó antes: no puede llegar a un monto ni a una fecha.
 *   - La sentencia siempre lleva WHERE por clave primaria, una sola fila.
 *   - Aquí no se ejecuta nada. Se redacta, se muestra con su nivel de confianza, y
 *     decide una persona. La exploración detecta y propone; validar sigue siendo
 *     trabajo de quien atiende.
 */
import type { Fila, FuenteDatos } from './oracle/fuente.ts';
import type { NombreConsulta } from './oracle/catalogo.ts';

/** Días hacia atrás que definen «usuario que está operando». */
const DIAS_SANOS = 30;
/** Por debajo de esto la población de referencia es demasiado chica para fiarse. */
const MINIMO_POBLACION = 8;
/** Cuota mínima del valor dominante para considerarlo «lo normal». */
const CUOTA_MINIMA = 0.75;

interface ColumnaCorregible {
  /** Nombre en Oracle, como va en el UPDATE. */
  columna: string;
  /** Nombre en la fila normalizada (minúsculas). */
  campo: string;
  etiqueta: string;
  /** Consulta del catálogo que resume la población sana para esta columna. */
  consulta: NombreConsulta;
}

/**
 * Las únicas columnas que la exploración puede proponer escribir.
 *
 * Arranca siendo exactamente el conjunto que las correcciones del catálogo ya tocan,
 * así la exploración nunca propone algo más atrevido que lo que el equipo ya aprobó.
 * `codigo_sucursal` queda fuera a propósito: su valor correcto no sale de la
 * población, sale del corresponsal, y para eso ya hay una regla.
 */
const LISTA_BLANCA: ColumnaCorregible[] = [
  { columna: 'USR_ESTADO', campo: 'usr_estado', etiqueta: 'Estado del usuario', consulta: 'perfilUsrEstado' },
  { columna: 'USR_INTENTOS', campo: 'usr_intentos', etiqueta: 'Intentos fallidos de clave', consulta: 'perfilUsrIntentos' },
  { columna: 'USR_INTENTOS_PIN', campo: 'usr_intentos_pin', etiqueta: 'Intentos fallidos de PIN', consulta: 'perfilUsrIntentosPin' },
  { columna: 'USR_IS_CLAVE_TEMPORAL', campo: 'usr_is_clave_temporal', etiqueta: 'Marca de clave temporal', consulta: 'perfilUsrIsClaveTemporal' },
  { columna: 'USR_IS_PIN_TEMPORAL', campo: 'usr_is_pin_temporal', etiqueta: 'Marca de PIN temporal', consulta: 'perfilUsrIsPinTemporal' },
];

const TABLA = 'CORRESPONSALES.USUARIO';
const CLAVE = { columna: 'USR_CODIGO', campo: 'usr_codigo' };
const SALTO = String.fromCharCode(10);

export interface Anomalia {
  columna: string;
  etiqueta: string;
  valorActual: number;
  valorSano: number;
  /** Proporción de la población sana que tiene `valorSano` (0..1). */
  cuota: number;
}

export interface PropuestaExploracion {
  /** Sentencia con marcador, para guardarla como plantilla reutilizable. */
  plantilla: string;
  /** La misma sentencia con el valor del caso, para leerla de un vistazo. */
  sql: string;
  tabla: string;
  clave: { campo: string; etiqueta: string; valor: number };
  anomalias: Anomalia[];
  /** 0..1. Es la cuota más floja de las anomalías incluidas: la cadena vale lo que su eslabón débil. */
  confianza: number;
  resumen: string;
  /** Cuántos usuarios sanos se usaron como referencia. */
  poblacion: number;
  dias: number;
}

export interface ResultadoExploracion {
  explorado: boolean;
  motivo?: string;
  propuesta?: PropuestaExploracion;
}

function aNumero(v: unknown): number | null {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/** Valor dominante de una columna entre la población sana, con su peso. */
async function valorNormal(
  fuente: FuenteDatos,
  col: ColumnaCorregible,
): Promise<{ valor: number; cuota: number; total: number } | null> {
  const filas = await fuente.consultar(col.consulta, { dias: DIAS_SANOS });
  if (filas.length === 0) return null;

  const primera = filas[0];
  const valor = aNumero(primera.valor);
  const veces = aNumero(primera.veces) ?? 0;
  const total = aNumero(primera.total) ?? 0;
  if (valor === null || total < MINIMO_POBLACION) return null;

  return { valor, cuota: veces / total, total };
}

function redactar(clave: number, anomalias: Anomalia[]): { plantilla: string; sql: string } {
  const asignaciones = anomalias.map((a) => `${a.columna} = ${a.valorSano}`).join(`,${SALTO}    `);
  const cuerpo = (donde: string) => `UPDATE ${TABLA}${SALTO}SET ${asignaciones}${SALTO}WHERE ${CLAVE.columna} = ${donde};${SALTO}COMMIT;`;
  return { plantilla: cuerpo(`:${CLAVE.campo}`), sql: cuerpo(String(clave)) };
}

/**
 * Compara un usuario contra los que sí están operando y redacta una sola corrección.
 *
 * Nunca lanza: la exploración es un extra, y un fallo suyo no debe tumbar el
 * diagnóstico que sí funcionó.
 */
export async function explorarUsuario(fuente: FuenteDatos, usuario: Fila): Promise<ResultadoExploracion> {
  try {
    const clave = aNumero(usuario[CLAVE.campo]);
    if (clave === null) return { explorado: false, motivo: 'El usuario no trae USR_CODIGO.' };

    const anomalias: Anomalia[] = [];
    let poblacion = 0;

    for (const col of LISTA_BLANCA) {
      const normal = await valorNormal(fuente, col);
      if (!normal) continue;
      poblacion = Math.max(poblacion, normal.total);

      const actual = aNumero(usuario[col.campo]);
      if (actual === null || actual === normal.valor) continue;
      if (normal.cuota < CUOTA_MINIMA) continue; // la población no es concluyente

      anomalias.push({
        columna: col.columna,
        etiqueta: col.etiqueta,
        valorActual: actual,
        valorSano: normal.valor,
        cuota: normal.cuota,
      });
    }

    if (poblacion === 0) {
      return {
        explorado: false,
        motivo: `Sin población de referencia: hacen falta al menos ${MINIMO_POBLACION} usuarios con transacciones en los últimos ${DIAS_SANOS} días.`,
      };
    }
    if (anomalias.length === 0) {
      return {
        explorado: true,
        motivo: `Este usuario no se diferencia de los ${poblacion} que están operando en ninguna columna corregible.`,
      };
    }

    const { plantilla, sql } = redactar(clave, anomalias);
    const confianza = Math.min(...anomalias.map((a) => a.cuota));
    const lista = anomalias.map((a) => a.etiqueta.toLowerCase()).join(', ');

    return {
      explorado: true,
      propuesta: {
        plantilla,
        sql,
        tabla: TABLA,
        clave: { campo: CLAVE.campo, etiqueta: `Código de usuario (${CLAVE.columna})`, valor: clave },
        anomalias,
        confianza,
        poblacion,
        dias: DIAS_SANOS,
        resumen:
          anomalias.length === 1
            ? `Difiere de los usuarios que operan en ${lista}.`
            : `Difiere de los usuarios que operan en ${anomalias.length} columnas: ${lista}.`,
      },
    };
  } catch (error) {
    return { explorado: false, motivo: (error as Error).message };
  }
}

/**
 * Explora a partir de lo que se sacó del texto del ticket.
 *
 * Resuelve el usuario por su propia cuenta en vez de reaprovechar el del playbook:
 * así la exploración corre igual cuando ningún diagnóstico reconoce el caso, que es
 * justo cuando hace falta.
 */
export async function explorarDesdeEntrada(
  fuente: FuenteDatos,
  entrada: { usuario?: string; cedula?: string; mac?: string },
): Promise<ResultadoExploracion> {
  try {
    const usuarios = entrada.usuario
      ? await fuente.consultar('usuarioPorUsuario', { usuario: entrada.usuario })
      : entrada.cedula
        ? await fuente.consultar('usuarioPorCedula', { cedula: entrada.cedula })
        : entrada.mac
          ? await fuente.consultar('usuariosPorMac', { mac: entrada.mac })
          : [];

    if (usuarios.length === 0) {
      return { explorado: false, motivo: 'No se pudo identificar al usuario para compararlo con los que operan.' };
    }
    return await explorarUsuario(fuente, usuarios[0]);
  } catch (error) {
    return { explorado: false, motivo: (error as Error).message };
  }
}
