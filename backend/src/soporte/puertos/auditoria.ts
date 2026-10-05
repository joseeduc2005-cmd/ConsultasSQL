/**
 * Por dónde deja rastro el módulo de soporte.
 *
 * El módulo no sabe que detrás hay PostgreSQL ni cómo son las tablas: solo que hay un
 * sitio donde anotar lo que hace. Quien lo arranca decide cuál es. Si no se le pasa
 * ninguno, no se cae — se queda sin auditar, y eso es visible en el arranque.
 */
export interface Auditoria {
  registrarEvento(evento: {
    evento: string;
    usuario?: string;
    rol?: string;
    detalle?: unknown;
  }): Promise<void>;

  registrarError(error: {
    mensaje: string;
    stack?: string | null;
    capa?: string;
    contexto?: string;
  }): Promise<void>;
}
