/**
 * Exploración sin catálogo: señalar datos raros en una base que nadie estudió.
 *
 * La exploración de `exploracion.ts` está escrita contra CORRESPONSALES.USUARIO, con
 * sus columnas declaradas a mano. Sirve porque alguien se sentó a entender esa tabla.
 * Esto es lo otro: llega un ticket contra una base cualquiera —Postgres, MySQL,
 * Oracle— y hay que decir algo útil sin que nadie haya escrito nada sobre ella.
 *
 * Lo único que se necesita es el esquema que el motor ya introspecciona al arrancar.
 * Con eso:
 *
 *   1. Del texto del ticket sale un identificador: una cédula, un usuario, una MAC.
 *   2. Se busca en el esquema qué tabla tiene una columna que signifique eso. Es la
 *      misma técnica de `ErrorAnalysisEngine`: una lista de nombres candidatos, y se
 *      queda con la que aparezca.
 *   3. Se trae esa fila.
 *   4. Se compara cada columna contra el resto de la tabla: si el 97 % tiene un valor
 *      y esta fila tiene otro, eso es lo raro.
 *
 * Deliberadamente **no propone ningún UPDATE**. Adivinar a partir de un escaneo qué
 * columna es seguro escribir es exactamente lo que no se debe hacer en un banco: la
 * exploración del catálogo solo puede tocar cinco columnas porque un humano las
 * autorizó una por una. Esto señala; corregir sigue siendo decisión de una persona.
 */

/** Ejecuta una consulta de lectura contra la base que se esté investigando. */
export type Ejecutor = (sql: string, parametros: unknown[]) => Promise<Record<string, unknown>[]>;

export interface TablaEsquema {
  name: string;
  columns?: { name: string; type?: string }[];
}

/**
 * Nombres con los que cada dato suele aparecer.
 *
 * La lista es el conocimiento: no hace falta saber el esquema de nadie, basta con que
 * la columna se llame de alguna forma razonable. Cuando aparezca una base que use
 * otro nombre, se añade aquí y sirve para todas.
 */
const NOMBRES = {
  cedula: ['cedula', 'identificacion', 'documento', 'dni', 'nro_documento', 'num_documento', 'ruc'],
  usuario: ['usuario', 'username', 'user_name', 'login', 'nombre_usuario', 'cuenta'],
  mac: ['mac', 'pos_mac', 'direccion_mac', 'mac_address', 'imei', 'id_dispositivo'],
} as const;

export type TipoIdentificador = keyof typeof NOMBRES;

/** Columnas que nunca se comparan: cambian en cada fila y su rareza no significa nada. */
const IGNORADAS =
  /(^id$|_id$|^uuid|fecha|date|_at$|timestamp|hora|monto|valor|saldo|importe|clave|pass|pin|token|hash|secret)/i;

function normalizar(texto: string): string {
  return texto
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9]/g, '');
}

/** La columna de esta tabla que representa ese dato, si la hay. */
function columnaPara(tabla: TablaEsquema, tipo: TipoIdentificador): string | null {
  const candidatos = NOMBRES[tipo].map(normalizar);
  const columnas = tabla.columns ?? [];

  // Primero una coincidencia exacta; solo si no la hay se acepta que la contenga,
  // porque «codigo_usuario» y «usuario» no son lo mismo y el exacto debe ganar.
  const exacta = columnas.find((c) => candidatos.includes(normalizar(c.name)));
  if (exacta) return exacta.name;

  const parcial = columnas.find((c) => candidatos.some((n) => normalizar(c.name).includes(n)));
  return parcial?.name ?? null;
}

export interface Candidata {
  tabla: string;
  columna: string;
  /** Cuántas columnas tiene: entre dos tablas que sirven, gana la que más describe. */
  columnas: number;
}

/**
 * Tablas donde podría estar ese identificador, de la más prometedora a la menos.
 *
 * Se ordena por número de columnas porque una tabla de detalle suele tener más
 * información útil que una de relación con dos campos.
 */
export function tablasCandidatas(
  esquema: TablaEsquema[],
  tipo: TipoIdentificador,
  limite = 3,
): Candidata[] {
  return esquema
    .map((t) => {
      const columna = columnaPara(t, tipo);
      return columna ? { tabla: t.name, columna, columnas: (t.columns ?? []).length } : null;
    })
    .filter((c): c is Candidata => c !== null)
    .sort((a, b) => b.columnas - a.columnas)
    .slice(0, limite);
}

export interface DiferenciaGenerica {
  columna: string;
  valorActual: unknown;
  valorHabitual: unknown;
  /** Proporción de filas de la tabla que tienen el valor habitual (0..1). */
  cuota: number;
}

export interface HallazgoGenerico {
  tabla: string;
  columna: string;
  identificador: string;
  /** Columnas donde esta fila se sale de lo que hace el resto. */
  diferencias: DiferenciaGenerica[];
  /** Cuántas filas se compararon. */
  poblacion: number;
  /** La fila encontrada, para mostrarla tal cual. */
  fila: Record<string, unknown>;
}

const CUOTA_MINIMA = 0.8;
const MINIMO_POBLACION = 20;
/** Más valores distintos que esto y la columna no describe un estado, describe un dato. */
const MAX_VALORES_DISTINTOS = 12;

/** Cita un identificador para que un nombre con mayúsculas o reservado no rompa el SQL. */
function citar(nombre: string): string {
  return `"${nombre.replace(/"/g, '')}"`;
}

/**
 * Busca la fila y la compara contra el resto de su tabla.
 *
 * `ejecutar` recibe SQL ya armado con marcadores posicionales. Todo lo que se
 * interpola viene del esquema introspeccionado —nombres de tabla y de columna—, y el
 * valor del identificador viaja siempre como parámetro, nunca dentro del texto.
 *
 * Nunca lanza: esto acompaña a un diagnóstico y no debe tumbarlo.
 */
export async function explorarTabla(
  ejecutar: Ejecutor,
  candidata: Candidata,
  identificador: string,
  esquema: TablaEsquema[],
): Promise<HallazgoGenerico | null> {
  try {
    const tabla = esquema.find((t) => t.name === candidata.tabla);
    if (!tabla) return null;

    const filas = await ejecutar(
      `SELECT * FROM ${citar(candidata.tabla)} WHERE ${citar(candidata.columna)} = $1`,
      [identificador],
    );
    if (filas.length !== 1) return null; // ni encontrada, ni ambigua

    const fila = filas[0];
    const [conteo] = await ejecutar(`SELECT COUNT(*) AS total FROM ${citar(candidata.tabla)}`, []);
    const poblacion = Number(conteo?.total ?? 0);
    if (poblacion < MINIMO_POBLACION) return null;

    const diferencias: DiferenciaGenerica[] = [];
    for (const col of tabla.columns ?? []) {
      if (IGNORADAS.test(col.name)) continue;
      const actual = fila[col.name] ?? fila[col.name.toLowerCase()];
      if (actual === null || actual === undefined) continue;

      // Los valores más frecuentes de la columna. Si hay muchos distintos, la columna
      // guarda un dato propio de cada fila y compararla no dice nada.
      const reparto = await ejecutar(
        `SELECT ${citar(col.name)} AS valor, COUNT(*) AS veces
         FROM ${citar(candidata.tabla)}
         GROUP BY ${citar(col.name)}
         ORDER BY COUNT(*) DESC`,
        [],
      );
      if (reparto.length === 0 || reparto.length > MAX_VALORES_DISTINTOS) continue;

      const habitual = reparto[0];
      const cuota = Number(habitual.veces ?? 0) / poblacion;
      if (cuota < CUOTA_MINIMA) continue;
      if (String(habitual.valor) === String(actual)) continue;

      diferencias.push({
        columna: col.name,
        valorActual: actual,
        valorHabitual: habitual.valor,
        cuota,
      });
    }

    if (diferencias.length === 0) return null;
    return { tabla: candidata.tabla, columna: candidata.columna, identificador, diferencias, poblacion, fila };
  } catch {
    return null;
  }
}
