/**
 * Soluciones que propone quien reporta el problema.
 *
 * La base de conocimiento se llenaba solo desde el panel: alguien de soporte resolvía
 * un ticket y publicaba el tema. Pero muchas veces quien tiene el problema termina
 * encontrando la salida por su cuenta, y eso se perdía — el siguiente que tropieza con
 * lo mismo vuelve a empezar de cero.
 *
 * Aquí puede escribirlo. No entra directo: queda como aporte pendiente, alguien de
 * soporte lo lee, y recién al aceptarlo se convierte en tema. La razón es simple —
 * estos textos llevan pasos que otros van a seguir, y a veces SQL; publicar sin que
 * nadie lo mire sería poner en la base de conocimiento algo que nadie revisó.
 *
 * Estos son los casos de uso. Solo conocen el dominio y el puerto: no saben si debajo
 * hay PostgreSQL, ni les importa.
 */
import { solicitudInvalida, textoOpcional, textoRequerido } from '../http/errores.ts';
import {
  MINIMO_DESCRITO,
  contenidoDelAporte,
  loEscrito,
  normalizarPasos,
  recorte,
  redactarPasos,
  sentenciaPrincipal,
  type Aporte,
  type EstadoAporte,
} from '../dominio/aportes/aporte.ts';
import type { RepositorioAportes } from '../puertos/repositorio-aportes.ts';

export interface AporteNuevo {
  titulo?: unknown;
  problema?: unknown;
  /** Una lista de pasos. Se acepta también un texto suelto, de cuando no los había. */
  pasos?: unknown;
  base?: unknown;
  caso?: unknown;
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

/** Registra un aporte. El autor sale de la sesión, nunca del cuerpo de la petición. */
export async function crearAporte(
  repo: RepositorioAportes,
  datos: AporteNuevo,
  autor: string,
): Promise<Aporte> {
  const titulo = textoRequerido(datos.titulo, 'titulo');
  const pasos = normalizarPasos(datos.pasos);
  if (pasos.length === 0) throw solicitudInvalida('Falta contar qué hizo para resolverlo.');
  if (loEscrito(pasos).length < MINIMO_DESCRITO) {
    throw solicitudInvalida('Describa los pasos con un poco más de detalle.');
  }

  return repo.guardar({
    autor,
    titulo,
    problema: textoOpcional(datos.problema),
    pasos: redactarPasos(pasos),
    pasosJson: pasos,
    sqlSugerido: sentenciaPrincipal(pasos),
    base: textoOpcional(datos.base),
    caso: textoOpcional(datos.caso),
  });
}

export function aportesDe(repo: RepositorioAportes, autor: string): Promise<Aporte[]> {
  return repo.deAutor(autor);
}

export function listarAportes(repo: RepositorioAportes, estado?: string | null): Promise<Aporte[]> {
  const filtro = !estado || estado === 'pendiente' ? 'pendiente' : estado === 'todos' ? 'todos' : estado;
  return repo.listar(filtro as EstadoAporte | 'todos');
}

export function obtenerAporte(repo: RepositorioAportes, id: string): Promise<Aporte | null> {
  return repo.obtener(id);
}

export function contarAportesPendientes(repo: RepositorioAportes): Promise<number> {
  return repo.contarPendientes();
}

/**
 * Acepta el aporte y lo publica como tema de ayuda.
 *
 * Se publica como tema de lectura, no ejecutable: el SQL queda dentro del texto. Si
 * después hay que convertirlo en algo que se ejecuta con un botón, se hace desde el
 * editor de la biblioteca, que es donde están las validaciones para eso.
 */
export async function aceptarAporte(
  repo: RepositorioAportes,
  id: string,
  revision: RevisionAporte,
  revisor: { id: string | null; nombre: string },
): Promise<{ aporte: Aporte; articulo: string }> {
  const aporte = await repo.obtener(id);
  if (!aporte) throw solicitudInvalida('No existe ese aporte.');
  if (aporte.estado === 'aceptado') throw solicitudInvalida('Ese aporte ya se publicó.');
  // El tema publicado se atribuye a quien lo revisó: sin revisor identificado no hay a
  // quién atribuirlo, y es mejor decirlo que fallar con un error de base.
  if (!revisor.id) throw solicitudInvalida('No se pudo identificar a quién está revisando.');

  // Lo que corrija el revisor manda sobre lo que escribió el autor.
  const listo: Aporte = {
    ...aporte,
    titulo: textoOpcional(revision.titulo) ?? aporte.titulo,
    pasos: textoOpcional(revision.pasos) ?? aporte.pasos,
  };

  const articulo = await repo.publicarTema({
    titulo: listo.titulo,
    contenido: contenidoDelAporte(listo),
    descripcion: recorte(listo.problema ?? listo.pasos),
    categoria: textoOpcional(revision.categoria) ?? 'Aportes del equipo',
    visible: textoOpcional(revision.visible) === 'admin' ? 'admin' : 'todos',
    revisorId: revisor.id,
    autor: listo.autor,
    revisorNombre: revisor.nombre,
  });

  const actualizado = await repo.marcarAceptado(id, articulo, textoOpcional(revision.nota), revisor.nombre);
  return { aporte: actualizado!, articulo };
}

/** Rechaza el aporte. La nota se le muestra a quien lo mandó: no se rechaza en silencio. */
export async function rechazarAporte(
  repo: RepositorioAportes,
  id: string,
  nota: unknown,
  revisor: string,
): Promise<Aporte> {
  const motivo = textoRequerido(nota, 'nota');
  const actualizado = await repo.marcarRechazado(id, motivo, revisor);
  if (!actualizado) throw solicitudInvalida('No existe ese aporte.');
  return actualizado;
}
