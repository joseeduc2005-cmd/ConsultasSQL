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
 *   5. Y se camina el grafo: el esquema trae sus claves foráneas, que son aristas entre
 *      tablas. Siguiéndolas se llega a filas que no nombran el identificador por ningún
 *      lado —la agencia del usuario, su perfil— y se repite la comparación allí, que es
 *      donde suele estar la causa de que algo no funcione.
 *
 * Deliberadamente **no propone ningún UPDATE**. Adivinar a partir de un escaneo qué
 * columna es seguro escribir es exactamente lo que no se debe hacer en un banco: la
 * exploración del catálogo solo puede tocar cinco columnas porque un humano las
 * autorizó una por una. Esto señala; corregir sigue siendo decisión de una persona.
 */

/** Ejecuta una consulta de lectura contra la base que se esté investigando. */
export type Ejecutor = (sql: string, parametros: unknown[]) => Promise<Record<string, unknown>[]>;

/** Una clave foránea, tal como la deja el registro del motor al introspeccionar. */
export interface ClaveForanea {
  column: string;
  referencedTable: string;
  referencedColumn: string;
  constraintName?: string;
}

export interface TablaEsquema {
  name: string;
  columns?: { name: string; type?: string }[];
  /** Las aristas del grafo. El motor ya las trae; durante mucho tiempo no se usaron. */
  foreignKeys?: ClaveForanea[];
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
  /**
   * Cómo se llegó hasta aquí desde la tabla donde estaba el identificador.
   *
   * Vacío en la tabla de partida. En las demás explica la ruta —«USUARIO → AGENCIA por
   * AGE_CODIGO»— porque un hallazgo en una tabla que el ticket ni menciona no se
   * entiende sin saber qué la ata al caso.
   */
  camino: string[];
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
    const fila = await traerUna(ejecutar, candidata.tabla, candidata.columna, identificador);
    if (!fila) return null;

    const comparada = await compararConLaPoblacion(ejecutar, candidata.tabla, fila, esquema);
    if (!comparada || comparada.diferencias.length === 0) return null;

    return {
      tabla: candidata.tabla,
      columna: candidata.columna,
      identificador,
      diferencias: comparada.diferencias,
      poblacion: comparada.poblacion,
      fila,
      camino: [],
    };
  } catch {
    return null;
  }
}

/** Una sola fila, o nada. Si hay varias el identificador es ambiguo y no sirve. */
async function traerUna(
  ejecutar: Ejecutor,
  tabla: string,
  columna: string,
  valor: unknown,
): Promise<Record<string, unknown> | null> {
  const filas = await ejecutar(`SELECT * FROM ${citar(tabla)} WHERE ${citar(columna)} = $1`, [valor]);
  return filas.length === 1 ? filas[0] : null;
}

/**
 * Compara una fila contra el resto de su tabla y devuelve en qué se sale.
 *
 * Separado de `explorarTabla` para poder aplicarlo también a las filas que se alcanzan
 * caminando por las claves foráneas: la pregunta es la misma —¿esta fila se parece a
 * sus iguales?— esté donde esté.
 */
async function compararConLaPoblacion(
  ejecutar: Ejecutor,
  nombreTabla: string,
  fila: Record<string, unknown>,
  esquema: TablaEsquema[],
): Promise<{ diferencias: DiferenciaGenerica[]; poblacion: number } | null> {
  const tabla = esquema.find((t) => t.name === nombreTabla);
  if (!tabla) return null;

  const [conteo] = await ejecutar(`SELECT COUNT(*) AS total FROM ${citar(nombreTabla)}`, []);
  const poblacion = Number(conteo?.total ?? 0);
  if (poblacion < MINIMO_POBLACION) return null;

  const diferencias: DiferenciaGenerica[] = [];
  for (const col of tabla.columns ?? []) {
    if (IGNORADAS.test(col.name)) continue;
    const actual = valorDe(fila, col.name);
    if (actual === null || actual === undefined) continue;

    // Los valores más frecuentes de la columna. Si hay muchos distintos, la columna
    // guarda un dato propio de cada fila y compararla no dice nada.
    const reparto = await ejecutar(
      `SELECT ${citar(col.name)} AS valor, COUNT(*) AS veces
       FROM ${citar(nombreTabla)}
       GROUP BY ${citar(col.name)}
       ORDER BY COUNT(*) DESC`,
      [],
    );
    if (reparto.length === 0 || reparto.length > MAX_VALORES_DISTINTOS) continue;

    const habitual = reparto[0];
    const cuota = Number(habitual.veces ?? 0) / poblacion;
    if (cuota < CUOTA_MINIMA) continue;
    if (String(habitual.valor) === String(actual)) continue;

    diferencias.push({ columna: col.name, valorActual: actual, valorHabitual: habitual.valor, cuota });
  }

  return { diferencias, poblacion };
}

/** Los motores devuelven las columnas en mayúsculas o minúsculas según les parece. */
function valorDe(fila: Record<string, unknown>, columna: string): unknown {
  if (columna in fila) return fila[columna];
  const minuscula = columna.toLowerCase();
  if (minuscula in fila) return fila[minuscula];
  const mayuscula = columna.toUpperCase();
  return mayuscula in fila ? fila[mayuscula] : undefined;
}

// ─── El grafo del esquema ────────────────────────────────────────────────────

/**
 * Una relación entre dos tablas, vista desde una de ellas.
 *
 * `padre` es la tabla a la que esta apunta —el usuario apunta a su agencia— y `hijo`
 * la que apunta a esta. Las dos direcciones importan: la causa de un problema puede
 * estar en lo que describe a la fila o en lo que cuelga de ella.
 */
export interface Arista {
  hacia: string;
  /** Columna de la tabla de partida que sostiene la relación. */
  columnaLocal: string;
  /** Columna de la tabla de destino con la que empareja. */
  columnaRemota: string;
  sentido: 'padre' | 'hijo';
}

function mismaTabla(a: string, b: string): boolean {
  // El motor normaliza los nombres, pero una FK puede venir cualificada con su esquema
  // («corresponsales.usuario») y la tabla no, o al revés.
  const sinEsquema = (n: string) => n.toLowerCase().split('.').pop() ?? n.toLowerCase();
  return sinEsquema(a) === sinEsquema(b);
}

/**
 * Las tablas vecinas de una, en las dos direcciones.
 *
 * Esto es el grafo: nodos las tablas, aristas las claves foráneas. El motor ya las
 * introspecciona al arrancar y hasta ahora nadie las leía — la exploración adivinaba
 * por el nombre de las columnas en vez de seguir las relaciones que la base declara.
 */
export function vecinas(esquema: TablaEsquema[], nombreTabla: string): Arista[] {
  const aristas: Arista[] = [];

  const tabla = esquema.find((t) => mismaTabla(t.name, nombreTabla));
  for (const fk of tabla?.foreignKeys ?? []) {
    if (!esquema.some((t) => mismaTabla(t.name, fk.referencedTable))) continue;
    aristas.push({
      hacia: esquema.find((t) => mismaTabla(t.name, fk.referencedTable))!.name,
      columnaLocal: fk.column,
      columnaRemota: fk.referencedColumn,
      sentido: 'padre',
    });
  }

  for (const otra of esquema) {
    if (mismaTabla(otra.name, nombreTabla)) continue;
    for (const fk of otra.foreignKeys ?? []) {
      if (!mismaTabla(fk.referencedTable, nombreTabla)) continue;
      aristas.push({
        hacia: otra.name,
        columnaLocal: fk.referencedColumn,
        columnaRemota: fk.column,
        sentido: 'hijo',
      });
    }
  }

  return aristas;
}

/** Hasta dónde se camina y cuántas tablas se tocan: esto corre dentro de un diagnóstico. */
const PROFUNDIDAD = 2;
const MAX_TABLAS = 8;
/** Más filas relacionadas que esto y la tabla es un detalle, no algo que describa la fila. */
const MAX_RELACIONADAS = 3;

/**
 * Camina el grafo desde la fila encontrada y compara también a sus vecinas.
 *
 * El salto que da esto: hasta ahora cada tabla se miraba suelta, y la única forma de
 * llegar a una era que tuviera una columna llamada «cedula» o «usuario». Siguiendo las
 * claves foráneas se llega a tablas que no nombran el identificador por ningún lado —la
 * agencia del usuario, su perfil— que es justo donde suele estar la causa.
 *
 * Nunca lanza, y nunca escribe: todas las consultas son SELECT y el valor por el que se
 * busca viaja como parámetro.
 */
export async function explorarVecindario(
  ejecutar: Ejecutor,
  raiz: HallazgoGenerico,
  esquema: TablaEsquema[],
): Promise<HallazgoGenerico[]> {
  const hallazgos: HallazgoGenerico[] = [];
  // La tabla de partida ya está mirada; marcarla evita volver sobre ella al rebotar.
  const vistas = new Set([raiz.tabla.toLowerCase()]);

  let frontera: { tabla: string; fila: Record<string, unknown>; camino: string[] }[] = [
    { tabla: raiz.tabla, fila: raiz.fila, camino: [] },
  ];

  for (let salto = 0; salto < PROFUNDIDAD && vistas.size < MAX_TABLAS; salto++) {
    const siguiente: typeof frontera = [];

    for (const actual of frontera) {
      for (const arista of vecinas(esquema, actual.tabla)) {
        if (vistas.size >= MAX_TABLAS) break;
        if (vistas.has(arista.hacia.toLowerCase())) continue;

        const valor = valorDe(actual.fila, arista.columnaLocal);
        if (valor === null || valor === undefined) continue;

        vistas.add(arista.hacia.toLowerCase());
        const camino = [...actual.camino, `${actual.tabla} → ${arista.hacia} por ${arista.columnaLocal}`];

        try {
          const filas = await ejecutar(
            `SELECT * FROM ${citar(arista.hacia)} WHERE ${citar(arista.columnaRemota)} = $1`,
            [valor],
          );
          // Si cuelgan muchas filas es una tabla de movimientos, no algo que describa a
          // esta fila: compararlas una a una diría poco y costaría mucho.
          if (filas.length === 0 || filas.length > MAX_RELACIONADAS) continue;

          for (const fila of filas) {
            const comparada = await compararConLaPoblacion(ejecutar, arista.hacia, fila, esquema);
            if (comparada && comparada.diferencias.length > 0) {
              hallazgos.push({
                tabla: arista.hacia,
                columna: arista.columnaRemota,
                identificador: String(valor),
                diferencias: comparada.diferencias,
                poblacion: comparada.poblacion,
                fila,
                camino,
              });
            }
            // Se sigue caminando aunque esta fila no tenga nada raro: la causa puede
            // estar un salto más allá, y pararse aquí la dejaría sin encontrar.
            if (filas.length === 1) siguiente.push({ tabla: arista.hacia, fila, camino });
          }
        } catch {
          // Una tabla que no se deja leer no tumba el recorrido: se sigue por las otras.
        }
      }
    }

    frontera = siguiente;
    if (frontera.length === 0) break;
  }

  return hallazgos;
}
