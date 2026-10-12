/**
 * Lo que los casos de uso de aportes necesitan de fuera.
 *
 * Dos almacenes distintos y conviene que se note en el puerto: los aportes, que son
 * borradores a la espera de revisión, y los temas publicados. Publicar es moverse del
 * primero al segundo, y es lo único que escribe en la base de conocimiento.
 */
import type { Aporte, EstadoAporte } from '../dominio/aportes/aporte.ts';

/** Lo que hay que guardar para dejar constancia de un aporte nuevo. */
export interface AporteParaGuardar {
  autor: string;
  titulo: string;
  problema: string | null;
  /** Los pasos ya redactados como texto. */
  pasos: string;
  pasosJson: unknown;
  sqlSugerido: string | null;
  base: string | null;
  caso: string | null;
}

/** Lo que hace falta para publicar un aporte aceptado como tema de ayuda. */
export interface TemaParaPublicar {
  titulo: string;
  contenido: string;
  descripcion: string;
  categoria: string;
  visible: 'todos' | 'admin';
  /** Quién publica. Es a quien la base atribuye la creación del registro. */
  revisorId: string;
  /** Quién lo aportó. Es la autoría que importa cuando alguien lo lee. */
  autor: string;
  revisorNombre: string;
}

export interface RepositorioAportes {
  guardar(aporte: AporteParaGuardar): Promise<Aporte>;
  obtener(id: string): Promise<Aporte | null>;
  /** Los de una persona, para que vea en qué quedó lo que mandó. */
  deAutor(autor: string): Promise<Aporte[]>;
  /** La bandeja de revisión. Sin estado, los pendientes. */
  listar(estado: EstadoAporte | 'todos' | null): Promise<Aporte[]>;
  contarPendientes(): Promise<number>;

  /** Publica el tema y devuelve su id. Es lo único que escribe en la base de conocimiento. */
  publicarTema(tema: TemaParaPublicar): Promise<string>;

  marcarAceptado(id: string, tema: string, nota: string | null, revisor: string): Promise<Aporte | null>;
  marcarRechazado(id: string, motivo: string, revisor: string): Promise<Aporte | null>;
}
