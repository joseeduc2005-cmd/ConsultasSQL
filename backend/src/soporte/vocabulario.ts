/**
 * Puente entre la Biblioteca SQL y el motor de consultas.
 *
 * Las 111 consultas de `soporte.consultas` son un corpus curado a mano: cada una
 * tiene un nombre en español, una categoría y notas de uso, junto al SQL que de
 * verdad funciona contra la base de producción. Eso es exactamente lo que le falta
 * a `semantic_learning`, la tabla donde el motor guarda qué palabra apunta a qué
 * tabla: hoy tiene un puñado de filas inferidas por él mismo y de baja calidad.
 *
 * Este módulo lee la Biblioteca, empareja los términos del nombre con las tablas y
 * columnas que aparecen en el SQL, y puntúa cada pareja por cuán exclusiva es.
 *
 * No interviene ningún modelo de lenguaje: todo es conteo y normalización.
 *
 * Sobre la seguridad del emparejamiento: el motor valida que la tabla exista en el
 * esquema que está consultando antes de usar un mapeo aprendido
 * (QueryIntelligenceEngine, comprobación `tableExists`). Por eso sembrar vocabulario
 * de Oracle no ensucia las consultas a PostgreSQL: allí esas tablas no existen y el
 * mapeo se descarta solo.
 */
import { enTransaccion, filas } from './knowledge/db.ts';

export interface ParejaVocabulario {
  termino: string;
  objetivo: string;
  tipo: 'table' | 'column';
  /** En cuántas consultas de la Biblioteca aparecen juntos. */
  apariciones: number;
  /** Qué proporción de las veces que sale el término, sale con este objetivo. */
  exclusividad: number;
  confianza: number;
  /** Códigos de la Biblioteca que respaldan la pareja, para poder auditarla. */
  ejemplos: string[];
}

/** Palabras que aparecen en casi cualquier consulta y no distinguen nada. */
const VACIAS = new Set([
  'para', 'pero', 'como', 'esta', 'este', 'estos', 'estas', 'porque', 'cuando', 'donde',
  'tiene', 'todos', 'todas', 'todo', 'cada', 'desde', 'hasta', 'sobre', 'entre', 'segun',
  'consulta', 'consultar', 'listar', 'listado', 'lista', 'buscar', 'obtener', 'mostrar',
  'revisar', 'verificar', 'validar', 'detalle', 'detalles', 'datos', 'dato', 'informacion',
  'general', 'generales', 'nuevo', 'nueva', 'actualizar', 'insertar', 'eliminar', 'borrar',
  'registro', 'registros', 'tabla', 'tablas', 'campo', 'campos', 'valor', 'valores',
  'total', 'totales', 'ultimo', 'ultima', 'ultimos', 'ultimas', 'otros', 'otras',
]);

/** Términos cortos del dominio que no deben caer por el filtro de longitud. */
const CORTOS_VALIDOS = new Set(['pos', 'mac', 'otp', 'sir', 'pin', 'ati', 'iva']);

/** Palabras reservadas de SQL que el extractor de tablas podría confundir. */
const RESERVADAS = new Set([
  'select', 'from', 'where', 'join', 'inner', 'left', 'right', 'full', 'outer', 'cross',
  'on', 'and', 'or', 'not', 'in', 'exists', 'group', 'order', 'by', 'having', 'union',
  'all', 'as', 'dual', 'values', 'set', 'into', 'table', 'lateral', 'using', 'with',
]);

function sinTildes(texto: string): string {
  return texto.normalize('NFD').replace(/[̀-ͯ]/g, '');
}

/** Reduce plurales simples para que "corresponsales" y "corresponsal" sean el mismo término. */
function singular(palabra: string): string {
  if (palabra.length > 5 && palabra.endsWith('es')) return palabra.slice(0, -2);
  if (palabra.length > 4 && palabra.endsWith('s')) return palabra.slice(0, -1);
  return palabra;
}

/** Extrae los términos de negocio de un texto en español. */
export function terminosDe(texto: string): string[] {
  const palabras = sinTildes(String(texto || '').toLowerCase())
    .split(/[^a-z0-9]+/)
    .filter(Boolean);
  const utiles = palabras
    .filter((p) => (p.length >= 4 || CORTOS_VALIDOS.has(p)) && !VACIAS.has(p) && !/^\d+$/.test(p))
    .map(singular)
    .filter((p) => p.length >= 3);
  return [...new Set(utiles)];
}

function limpiarSql(sql: string): string {
  return String(sql || '')
    .replace(/--[^\n]*/g, ' ')
    .replace(/\/\*[\s\S]*?\*\//g, ' ');
}

/**
 * Tablas referenciadas por una sentencia SQL, con su alias cuando lo declara.
 * Se queda con el nombre sin el prefijo del owner, en minúsculas: es como el
 * introspector de Oracle indexa el esquema (`OracleAdapter`).
 */
/** Corta la cláusula donde empieza la siguiente palabra clave del SQL. */
const FIN_DE_CLAUSULA = /\b(where|group|order|having|union|minus|intersect|connect|start|join|inner|left|right|full|cross|on|set|values|returning|fetch)\b|[;()]/i;

/** Interpreta un elemento "esquema.tabla [AS] alias" de una cláusula FROM. */
function leerReferencia(texto: string): { tabla: string; alias: string | null } | null {
  const partes = texto.trim().split(/\s+/).filter(Boolean);
  if (partes.length === 0) return null;
  const camino = partes[0].toLowerCase().split('.');
  const tabla = camino[camino.length - 1];
  if (!tabla || RESERVADAS.has(tabla) || !/^[a-z_][a-z0-9_$]*$/.test(tabla)) return null;
  const siguiente = (partes[1] || '').toLowerCase();
  const candidato = siguiente === 'as' ? (partes[2] || '').toLowerCase() : siguiente;
  const alias = candidato && /^[a-z][a-z0-9_$]*$/.test(candidato) && !RESERVADAS.has(candidato) ? candidato : null;
  return { tabla, alias };
}

export function tablasConAlias(sql: string): { tabla: string; alias: string | null }[] {
  const limpio = limpiarSql(sql);
  const encontradas: { tabla: string; alias: string | null }[] = [];

  // El corpus usa JOINs con coma al estilo Oracle antiguo ("FROM a u, b p"), así que
  // no basta con leer la tabla que sigue a FROM: hay que recorrer toda la cláusula.
  for (const m of limpio.matchAll(/\b(?:from|join|update|into)\b/gi)) {
    const resto = limpio.slice(m.index! + m[0].length);
    const corte = resto.search(FIN_DE_CLAUSULA);
    const clausula = corte === -1 ? resto : resto.slice(0, corte);
    for (const elemento of clausula.split(',')) {
      const ref = leerReferencia(elemento);
      if (ref) encontradas.push(ref);
    }
  }
  return encontradas;
}

export function tablasDe(sql: string): string[] {
  return [...new Set(tablasConAlias(sql).map((t) => t.tabla))];
}

/**
 * Peso de cada tabla dentro de una consulta.
 *
 * Aparecer en un FROM o un JOIN vale poco: muchas tablas entran solo de arrastre
 * para completar la relación. Lo que de verdad indica de qué trata la consulta son
 * las columnas que toca, y el alias del SQL dice exactamente a qué tabla pertenece
 * cada una. Sin esto, "mac" terminaba apuntando a `usuario` en lugar de a `pos`,
 * porque casi toda consulta de MAC hace un JOIN con usuario.
 */
export function pesoDeTablas(sql: string): Map<string, number> {
  const limpio = limpiarSql(sql);
  const refs = tablasConAlias(sql);
  const porAlias = new Map<string, string>();
  for (const { tabla, alias } of refs) {
    if (alias) porAlias.set(alias, tabla);
    porAlias.set(tabla, tabla); // también se puede calificar con el nombre completo
  }

  const pesos = new Map<string, number>();
  for (const { tabla } of refs) pesos.set(tabla, Math.max(pesos.get(tabla) ?? 0, 1));

  const vistas = new Set<string>();
  for (const m of limpio.matchAll(/\b([a-z][a-z0-9_$]*)\.([a-z][a-z0-9_$]*)\b/gi)) {
    const alias = m[1].toLowerCase();
    const columna = m[2].toLowerCase();
    const tabla = porAlias.get(alias);
    // `corresponsales.pos` es owner.tabla, no alias.columna: se descarta.
    if (!tabla || porAlias.has(columna) || vistas.has(`${tabla}.${columna}`)) continue;
    vistas.add(`${tabla}.${columna}`);
    pesos.set(tabla, (pesos.get(tabla) ?? 0) + 2);
  }
  return pesos;
}

/**
 * Columnas referenciadas. Se apoya en la convención de la base: los nombres llevan
 * un prefijo de tabla y un guion bajo (`pos_mac`, `usr_usuario`, `cr_identificacion`).
 */
export function columnasDe(sql: string): string[] {
  const limpio = String(sql || '')
    .replace(/--[^\n]*/g, ' ')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/'(?:[^']|'')*'/g, " '' ");
  const tablas = new Set(tablasDe(sql));
  const encontradas = [...limpio.matchAll(/\b([a-z][a-z0-9]*_[a-z0-9_]+)\b/gi)]
    .map((m) => m[1].toLowerCase())
    .filter((c) => !tablas.has(c) && !RESERVADAS.has(c));
  return [...new Set(encontradas)];
}

interface FilaConsulta {
  codigo: string;
  nombre: string;
  categoria: string | null;
  uso: string | null;
  sql: string;
}

interface Acumulado {
  /** peso acumulado y en cuántas consultas distintas aparece la pareja */
  porObjetivo: Map<string, { peso: number; consultas: number; ejemplos: string[] }>;
  /** peso total repartido por este término, para calcular la exclusividad */
  pesoTotal: number;
}

function acumular(
  mapa: Map<string, Acumulado>,
  terminos: string[],
  objetivos: Map<string, number>,
  codigo: string,
): void {
  const sumaPesos = [...objetivos.values()].reduce((a, b) => a + b, 0);
  if (sumaPesos === 0) return;
  for (const termino of terminos) {
    const entrada = mapa.get(termino) ?? { porObjetivo: new Map(), pesoTotal: 0 };
    entrada.pesoTotal += sumaPesos;
    for (const [objetivo, peso] of objetivos) {
      const actual = entrada.porObjetivo.get(objetivo) ?? { peso: 0, consultas: 0, ejemplos: [] };
      actual.peso += peso;
      actual.consultas += 1;
      if (actual.ejemplos.length < 4) actual.ejemplos.push(codigo);
      entrada.porObjetivo.set(objetivo, actual);
    }
    mapa.set(termino, entrada);
  }
}

function puntuar(
  mapa: Map<string, Acumulado>,
  tipo: 'table' | 'column',
  minApariciones: number,
  minExclusividad: number,
  /** En cuántas consultas aparece cada objetivo, en total. */
  frecuencia: Map<string, number>,
  totalConsultas: number,
): ParejaVocabulario[] {
  // Una tabla que sale en casi todas las consultas (usuario, por ejemplo) no
  // distingue nada: aparece junto a cualquier término por puro arrastre de JOINs.
  // Sin esto, "mac" terminaba apuntando a `usuario` en vez de a `pos`.
  const especificidad = (objetivo: string): number => {
    const df = frecuencia.get(objetivo) ?? 1;
    const idf = Math.log(totalConsultas / df) / Math.log(totalConsultas);
    return 0.35 + 0.65 * Math.max(0, Math.min(1, idf));
  };

  const parejas: ParejaVocabulario[] = [];
  for (const [termino, entrada] of mapa) {
    for (const [objetivo, { peso, consultas, ejemplos }] of entrada.porObjetivo) {
      const exclusividad = peso / entrada.pesoTotal;
      if (consultas < minApariciones || exclusividad < minExclusividad) continue;
      const relevancia = exclusividad * especificidad(objetivo);
      // Se topa en 0.95 para que nunca gane a un mapeo explícito de una persona.
      const confianza = Math.min(0.95, 0.7 + relevancia * 0.25 + Math.min(consultas, 8) * 0.005);
      parejas.push({
        termino,
        objetivo,
        tipo,
        apariciones: consultas,
        exclusividad: Number(exclusividad.toFixed(3)),
        confianza: Number(confianza.toFixed(3)),
        ejemplos,
      });
    }
  }
  return parejas.sort((a, b) => b.confianza - a.confianza || b.apariciones - a.apariciones);
}

export interface OpcionesVocabulario {
  /** Mínimo de consultas en que la pareja debe aparecer. */
  minApariciones?: number;
  /** Mínima proporción de exclusividad término → objetivo. */
  minExclusividad?: number;
}

/**
 * Deriva el vocabulario de la Biblioteca SQL. No escribe nada: devolver el resultado
 * permite revisarlo antes de sembrarlo.
 */
export async function construirVocabulario(
  opciones: OpcionesVocabulario = {},
): Promise<{ tablas: ParejaVocabulario[]; columnas: ParejaVocabulario[]; consultas: number }> {
  const minApariciones = opciones.minApariciones ?? 2;
  const minExclusividad = opciones.minExclusividad ?? 0.18;

  const consultas = await filas<FilaConsulta>(
    'SELECT codigo, nombre, categoria, uso, sql FROM soporte.consultas',
  );

  const porTabla = new Map<string, Acumulado>();
  const porColumna = new Map<string, Acumulado>();
  const frecTablas = new Map<string, number>();
  const frecColumnas = new Map<string, number>();
  const contar = (mapa: Map<string, number>, claves: string[]) => {
    for (const k of claves) mapa.set(k, (mapa.get(k) ?? 0) + 1);
  };

  for (const c of consultas) {
    // El nombre y la categoría describen la intención; `uso` suele ser prosa larga
    // y mete demasiado ruido, así que no entra.
    const terminos = terminosDe(`${c.nombre} ${c.categoria ?? ''}`);
    const tablas = pesoDeTablas(c.sql);
    const columnas = new Map(columnasDe(c.sql).map((col) => [col, 1]));
    contar(frecTablas, [...tablas.keys()]);
    contar(frecColumnas, [...columnas.keys()]);
    if (terminos.length === 0) continue;
    acumular(porTabla, terminos, tablas, c.codigo);
    acumular(porColumna, terminos, columnas, c.codigo);
  }

  const n = Math.max(2, consultas.length);
  return {
    consultas: consultas.length,
    tablas: puntuar(porTabla, 'table', minApariciones, minExclusividad, frecTablas, n),
    // Las columnas son mucho más ruidosas: se exige más exclusividad.
    columnas: puntuar(porColumna, 'column', minApariciones, Math.max(minExclusividad, 0.25), frecColumnas, n),
  };
}

/**
 * Marca de origen en `semantic_learning`.
 *
 * La tabla es del motor, no de este módulo, así que se añade una columna aditiva
 * —con el mismo patrón `ADD COLUMN IF NOT EXISTS` que usa app.js— para poder
 * distinguir lo que siembra la Biblioteca de lo que aprende el motor por su cuenta,
 * y regenerarlo sin tocar lo demás.
 */
export const ORIGEN = 'biblioteca';

export interface ResultadoSiembra {
  escritas: number;
  tablas: number;
  columnas: number;
  consultas: number;
  eliminadas: number;
}

/**
 * Escribe el vocabulario derivado en `semantic_learning`.
 *
 * `huella` debe ser la misma que usa el motor al leer
 * (`schemaDetector.getConnectionFingerprint()`): la carga filtra por ella y, si no
 * coincide, las filas quedarían escritas pero nunca se leerían.
 */
export async function sembrarVocabulario(
  huella: string,
  opciones: OpcionesVocabulario = {},
): Promise<ResultadoSiembra> {
  if (!huella) throw new Error('Falta la huella de conexión del motor de consultas.');
  const vocabulario = await construirVocabulario(opciones);
  const parejas = [...vocabulario.tablas, ...vocabulario.columnas];

  return enTransaccion(async (cliente) => {
    await cliente.query(
      `ALTER TABLE semantic_learning ADD COLUMN IF NOT EXISTS origen text NOT NULL DEFAULT 'motor'`,
    );
    const borradas = await cliente.query(
      'DELETE FROM semantic_learning WHERE origen = $1 AND db_fingerprint = $2',
      [ORIGEN, huella],
    );

    for (const p of parejas) {
      await cliente.query(
        `INSERT INTO semantic_learning
           (db_fingerprint, normalized_term, original_term, target_kind, target_name,
            table_name, confidence, hit_count, origen)
         VALUES ($1, $2, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (db_fingerprint, normalized_term, target_kind, target_name, table_name)
         DO UPDATE SET confidence = EXCLUDED.confidence,
                       hit_count  = EXCLUDED.hit_count,
                       origen     = EXCLUDED.origen,
                       last_seen  = now()`,
        [
          huella,
          p.termino,
          p.tipo,
          p.objetivo,
          // El motor usa `table_name` como la tabla a la que resolver el término.
          p.tipo === 'table' ? p.objetivo : '',
          p.confianza,
          p.apariciones,
          ORIGEN,
        ],
      );
    }

    return {
      escritas: parejas.length,
      tablas: vocabulario.tablas.length,
      columnas: vocabulario.columnas.length,
      consultas: vocabulario.consultas,
      eliminadas: borradas.rowCount ?? 0,
    };
  });
}

/** Cuántas parejas sembradas por la Biblioteca hay ya para esta huella. */
export async function vocabularioSembrado(huella: string): Promise<number> {
  const r = await filas<{ n: string }>(
    `SELECT count(*) AS n FROM semantic_learning
     WHERE origen = $1 AND db_fingerprint = $2`,
    [ORIGEN, huella],
  ).catch(() => []); // la columna `origen` puede no existir todavía
  return Number(r[0]?.n ?? 0);
}
