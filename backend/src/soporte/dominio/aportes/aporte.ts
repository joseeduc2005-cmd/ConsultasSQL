/**
 * Lo que es un aporte y qué reglas lo gobiernan.
 *
 * Dominio puro: ni base de datos ni HTTP. Aquí viven las decisiones que definen qué
 * cuenta como un aporte válido y cómo se redacta cuando se publica — lo que seguiría
 * siendo cierto aunque el sistema guardara los datos en otro sitio.
 */

export type EstadoAporte = 'pendiente' | 'aceptado' | 'rechazado';

/**
 * Un paso de lo que alguien hizo para resolverlo.
 *
 * `efecto` lo declara quien lo escribe, y no se adivina del SQL: una consulta puede
 * llamar a algo que escribe, y un `UPDATE` puede estar dentro de un bloque que al final
 * hace rollback. Quien lo vivió sabe qué pasó; el sistema no, y fingir que sí sería
 * peor que preguntarlo.
 */
export interface PasoAporte {
  descripcion: string;
  script: string | null;
  /**
   * Solo 'consulta', o null si el paso no trae script.
   *
   * Antes tambien podia ser 'cambio', y quien escribia el aporte elegia si su script
   * miraba o modificaba. El sistema ya no ejecuta escrituras, asi que preguntarlo
   * seria ofrecer algo que no se puede hacer.
   */
  efecto: 'consulta' | null;
}

export interface Aporte {
  id: string;
  autor: string;
  titulo: string;
  problema: string | null;
  /** Los pasos ya redactados, que es lo que indexa la búsqueda. */
  pasos: string;
  /** Los mismos pasos, uno a uno, para poder mostrarlos y editarlos. */
  pasos_json: PasoAporte[] | null;
  sql_sugerido: string | null;
  base: string | null;
  caso: string | null;
  estado: EstadoAporte;
  nota: string | null;
  revisado_por: string | null;
  revisado_en: string | null;
  articulo: string | null;
  creado_en: string;
}

/** Lo mínimo que tiene que contar alguien para que su aporte le sirva a otro. */
export const MINIMO_DESCRITO = 20;

/**
 * Normaliza los pasos que llegan del navegador.
 *
 * Se aceptan las dos formas: la lista de pasos de ahora y el texto corrido de antes.
 * Los pasos vacíos se descartan en vez de rechazar el envío entero — quien añadió una
 * casilla de más y no la llenó no tiene por qué perder lo que escribió.
 */
export function normalizarPasos(valor: unknown): PasoAporte[] {
  if (typeof valor === 'string') {
    const texto = valor.trim();
    return texto ? [{ descripcion: texto, script: null, efecto: null }] : [];
  }
  if (!Array.isArray(valor)) return [];

  return valor
    .map((bruto) => {
      const p = (bruto ?? {}) as Record<string, unknown>;
      const descripcion = typeof p.descripcion === 'string' ? p.descripcion.trim() : '';
      const script = typeof p.script === 'string' && p.script.trim() ? p.script.trim() : null;
        // Si hay script es una consulta; si no, no hay efecto. No se lee lo que venga
        // en `efecto`: un aporte viejo puede traer 'cambio' y ya no significa nada.
        return { descripcion, script, efecto: script ? 'consulta' : null };
    })
    .filter((p): p is PasoAporte => Boolean(p.descripcion || p.script));
}

/** Los pasos como texto, que es lo que lee la búsqueda y lo que queda en el artículo. */
export function redactarPasos(pasos: PasoAporte[]): string {
  return pasos
    .map((p, i) => {
      const cabecera = `${i + 1}. ${p.descripcion || '(sin descripción)'}`;
      if (!p.script) return cabecera;
      return `${cabecera}\n\n   _Solo consulta_\n\n\`\`\`sql\n${p.script}\n\`\`\``;
    })
    .join('\n\n');
}

/**
 * Cuánto se escribió de verdad, sin contar el SQL pegado.
 *
 * La medida es sobre lo descrito: un script sin una línea que explique cuándo aplicarlo
 * no le sirve al que venga después.
 */
export function loEscrito(pasos: PasoAporte[]): string {
  return pasos.map((p) => p.descripcion).join(' ').trim();
}

/**
 * El script que vale como «la sentencia» del aporte: el primero que haya.
 *
 * Antes buscaba primero el que cambiara algo, porque era el que habia que mirar con
 * cuidado. Ya no hay scripts que cambien nada, asi que el criterio es el orden.
 */
export function sentenciaPrincipal(pasos: PasoAporte[]): string | null {
  return pasos.find((p) => p.script)?.script ?? null;
}

/** El artículo tal como queda: lo que escribió quien lo vivió, con su nombre. */
export function contenidoDelAporte(a: Aporte): string {
  const partes = [`# ${a.titulo}`, ''];
  if (a.problema) partes.push('## El problema', '', a.problema, '');

  // Los pasos ya vienen redactados con sus scripts dentro, en orden: así se lee como
  // lo que es —lo que alguien hizo, una cosa después de otra— y no como un texto y
  // aparte un bloque de SQL suelto al final que no se sabe en qué momento va.
  partes.push('## Cómo se resolvió', '', a.pasos, '');
  if (a.base) partes.push(`Sobre la base **${a.base}**.`, '');
  if (a.sql_sugerido) {
    partes.push('> Los scripts están aquí para consultarlos. Quien los ejecuta es el equipo de soporte.', '');
  }
  partes.push('---', '', `Lo aportó **${a.autor}**${a.caso ? ` a partir del caso ${a.caso}` : ''}.`);

  return partes.filter((linea) => linea !== '').join('\n');
}

/** Un resumen corto para la tarjeta del tema. */
export function recorte(texto: string): string {
  const limpio = texto.trim().replace(/\s+/g, ' ');
  return limpio.length > 200 ? `${limpio.slice(0, 197)}…` : limpio;
}
