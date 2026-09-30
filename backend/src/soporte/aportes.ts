/**
 * Soluciones que propone quien reporta el problema.
 *
 * La base de conocimiento se llenaba solo desde el panel: alguien de soporte resolvía
 * un ticket y publicaba el artículo. Pero muchas veces quien tiene el problema termina
 * encontrando la salida por su cuenta, y eso se perdía — el siguiente que tropieza con
 * lo mismo vuelve a empezar de cero.
 *
 * Aquí puede escribirlo. No entra directo: queda como aporte pendiente, alguien de
 * soporte lo lee, y recién al aceptarlo se convierte en artículo. La razón es simple —
 * estos textos llevan pasos que otros van a seguir, y a veces SQL; publicar sin que
 * nadie lo mire sería poner en la base de conocimiento algo que nadie revisó.
 *
 * El SQL propuesto se guarda como texto, no como script ejecutable. Un aporte aceptado
 * nace como artículo de lectura; convertirlo en algo que se ejecuta con un botón es
 * otra decisión, y se toma desde el editor de la biblioteca, que es donde están las
 * validaciones para eso.
 */
import { solicitudInvalida, textoOpcional, textoRequerido } from './http/errores.ts';
import { fila, filas } from './knowledge/db.ts';

export type EstadoAporte = 'pendiente' | 'aceptado' | 'rechazado';

export interface Aporte {
  id: string;
  autor: string;
  titulo: string;
  problema: string | null;
  pasos: string;
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

const COLUMNAS = `id::text, autor, titulo, problema, pasos, sql_sugerido, base, caso,
                  estado, nota, revisado_por, revisado_en, articulo, creado_en`;

export interface AporteNuevo {
  titulo?: unknown;
  problema?: unknown;
  pasos?: unknown;
  sql?: unknown;
  base?: unknown;
  caso?: unknown;
}

/** Registra un aporte. El autor sale de la sesión, nunca del cuerpo de la petición. */
export async function crearAporte(datos: AporteNuevo, autor: string): Promise<Aporte> {
  const titulo = textoRequerido(datos.titulo, 'titulo');
  const pasos = textoRequerido(datos.pasos, 'pasos');
  if (pasos.trim().length < 20) {
    throw solicitudInvalida('Cuente con un poco más de detalle qué hizo para resolverlo.');
  }

  const creado = await fila<Aporte>(
    `INSERT INTO soporte.aportes (autor, titulo, problema, pasos, sql_sugerido, base, caso)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     RETURNING ${COLUMNAS}`,
    [
      autor,
      titulo,
      textoOpcional(datos.problema),
      pasos,
      textoOpcional(datos.sql),
      textoOpcional(datos.base),
      textoOpcional(datos.caso),
    ],
  );
  return creado!;
}

/** Los aportes de una persona, para que vea en qué quedó lo que mandó. */
export function aportesDe(autor: string): Promise<Aporte[]> {
  return filas<Aporte>(
    `SELECT ${COLUMNAS} FROM soporte.aportes WHERE autor = $1 ORDER BY id DESC LIMIT 50`,
    [autor],
  );
}

/** La bandeja de revisión. Sin filtro trae los pendientes, que es lo que hay que mirar. */
export function listarAportes(estado?: string | null): Promise<Aporte[]> {
  const filtro = !estado || estado === 'pendiente' ? 'pendiente' : estado === 'todos' ? null : estado;
  return filas<Aporte>(
    `SELECT ${COLUMNAS} FROM soporte.aportes
     WHERE ($1::text IS NULL OR estado = $1)
     ORDER BY (estado = 'pendiente') DESC, id DESC
     LIMIT 100`,
    [filtro],
  );
}

export function obtenerAporte(id: string): Promise<Aporte | null> {
  return fila<Aporte>(`SELECT ${COLUMNAS} FROM soporte.aportes WHERE id::text = $1`, [id]);
}

export async function contarAportesPendientes(): Promise<number> {
  const [f] = await filas<{ total: number }>(
    "SELECT COUNT(*)::int AS total FROM soporte.aportes WHERE estado = 'pendiente'",
  );
  return f?.total ?? 0;
}

/** El artículo tal como queda: lo que escribió quien lo vivió, con su nombre. */
function contenidoDelAporte(a: Aporte): string {
  const partes = [`# ${a.titulo}`, ''];
  if (a.problema) partes.push('## El problema', '', a.problema, '');
  partes.push('## Cómo se resolvió', '', a.pasos, '');
  if (a.sql_sugerido) {
    partes.push(
      '## Sentencia usada',
      '',
      '```sql',
      a.sql_sugerido,
      '```',
      '',
      a.base ? `Sobre la base **${a.base}**.` : '',
      '> Está aquí para consultarla. Quien la ejecuta es el equipo de soporte.',
      '',
    );
  }
  partes.push('---', '', `Lo aportó **${a.autor}**${a.caso ? ` a partir del caso ${a.caso}` : ''}.`);
  return partes.filter((linea) => linea !== '').join('\n');
}

export interface RevisionAporte {
  /** El revisor puede corregir lo que escribió el autor antes de publicarlo. */
  titulo?: unknown;
  pasos?: unknown;
  categoria?: unknown;
  /** Quién lo puede leer una vez publicado. Por defecto, cualquiera. */
  visible?: unknown;
  nota?: unknown;
}

/**
 * Acepta el aporte y lo publica como tema de ayuda.
 *
 * Se publica como artículo de lectura, no ejecutable: el SQL queda dentro del texto.
 * Si después hay que convertirlo en algo que se ejecuta con un botón, se hace desde el
 * editor de la biblioteca, que es donde están las validaciones para eso.
 */
export async function aceptarAporte(
  id: string,
  revision: RevisionAporte,
  revisor: { id: string | null; nombre: string },
): Promise<{ aporte: Aporte; articulo: string }> {
  const aporte = await obtenerAporte(id);
  if (!aporte) throw solicitudInvalida('No existe ese aporte.');
  if (aporte.estado === 'aceptado') throw solicitudInvalida('Ese aporte ya se publicó.');
  // `knowledge_base.creado_por` apunta a users: sin revisor identificado no hay a quién
  // atribuir la publicación, y es mejor decirlo que fallar con un error de base.
  if (!revisor.id) throw solicitudInvalida('No se pudo identificar a quién está revisando.');

  // Lo que corrija el revisor manda sobre lo que escribió el autor.
  const listo: Aporte = {
    ...aporte,
    titulo: textoOpcional(revision.titulo) ?? aporte.titulo,
    pasos: textoOpcional(revision.pasos) ?? aporte.pasos,
  };
  const categoria = textoOpcional(revision.categoria) ?? 'Aportes del equipo';
  const visible = textoOpcional(revision.visible) === 'admin' ? 'admin' : 'todos';

  // `creado_por` es el revisor, que es quien publica; `creado_por_username` queda con
  // el nombre de quien lo aportó, que es la autoría que importa cuando alguien lo lee.
  const creado = await fila<{ id: string }>(
    `INSERT INTO knowledge_base
       (titulo, contenido, contenido_md, descripcion, categoria, tipo_solucion,
        visible_para, creado_por, creado_por_username, actualizado_por, tags)
     VALUES ($1, $2, $2, $3, $4, 'lectura', $5, $6, $7, $8, $9)
     RETURNING id::text`,
    [
      listo.titulo,
      contenidoDelAporte(listo),
      recorte(listo.problema ?? listo.pasos),
      categoria,
      visible,
      revisor.id,
      listo.autor,
      revisor.nombre,
      ['aporte'],
    ],
  );

  const actualizado = await fila<Aporte>(
    `UPDATE soporte.aportes
        SET estado = 'aceptado', articulo = $2, nota = $3,
            revisado_por = $4, revisado_en = now()
      WHERE id::text = $1
      RETURNING ${COLUMNAS}`,
    [id, creado!.id, textoOpcional(revision.nota), revisor.nombre],
  );

  return { aporte: actualizado!, articulo: creado!.id };
}

/** Rechaza el aporte. La nota se le muestra a quien lo mandó: no se rechaza en silencio. */
export async function rechazarAporte(id: string, nota: unknown, revisor: string): Promise<Aporte> {
  const motivo = textoRequerido(nota, 'nota');
  const actualizado = await fila<Aporte>(
    `UPDATE soporte.aportes
        SET estado = 'rechazado', nota = $2, revisado_por = $3, revisado_en = now()
      WHERE id::text = $1
      RETURNING ${COLUMNAS}`,
    [id, motivo, revisor],
  );
  if (!actualizado) throw solicitudInvalida('No existe ese aporte.');
  return actualizado;
}

function recorte(texto: string): string {
  const limpio = texto.trim().replace(/\s+/g, ' ');
  return limpio.length > 200 ? `${limpio.slice(0, 197)}…` : limpio;
}
