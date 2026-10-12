/**
 * Lo que el caso de uso de búsqueda necesita de fuera, dicho sin nombrar a nadie.
 *
 * Esto es un puerto: declara qué hace falta, no cómo se consigue. El adaptador de
 * PostgreSQL lo cumple hoy; mañana podría cumplirlo otro motor, o uno falso en una
 * prueba, sin que el dominio se entere.
 *
 * Fíjese en lo que NO aparece aquí: ni SQL, ni tablas, ni columnas. Si en la firma de
 * un método se cuela una palabra de PostgreSQL, el puerto está mal hecho — el dominio
 * habría quedado atado a esa base.
 */

/** Un tema de ayuda tal como lo devuelve la búsqueda, antes de puntuarlo. */
export interface TemaEncontrado {
  id: string;
  titulo: string;
  descripcion: string | null;
  categoria: string | null;
  sql: string | null;
  /** Lo que el buscador del motor considera densidad de coincidencia. */
  relevancia: number;
  /** Qué parte del peso de la pregunta cubre este tema, de 0 a 1. */
  cobertura: number;
}

/** Un ticket resuelto que dejó escrito cómo se resolvió. */
export interface TicketEncontrado {
  numero: string;
  titulo: string;
  causaRaiz: string | null;
  solucion: string | null;
  sistema: string | null;
  relevancia: number;
  cobertura: number;
}

export interface Pregunta {
  /** Los términos ya filtrados, sin los que no distinguen nada. */
  terminos: string[];
  /** El peso de cada término, en el mismo orden. */
  pesos: number[];
  limite: number;
}

export interface BusquedaRegistrada {
  texto: string;
  veces: number;
  /** Cuántos resultados dio. Cero significa que falta ese tema por escribir. */
  resultados: number;
}

/**
 * El catálogo de conocimiento: dónde viven los temas, los tickets y lo aprendido.
 */
export interface Catalogo {
  /** Cuántos temas hay en total. Hace falta para pesar los términos. */
  contarTemas(): Promise<number>;

  /** En cuántos temas aparece cada término, contando por raíz y no por letra. */
  frecuenciaDe(terminos: string[]): Promise<{ termino: string; apareceEn: number }[]>;

  buscarTemas(pregunta: Pregunta, soloPublicos: boolean): Promise<TemaEncontrado[]>;

  buscarTickets(pregunta: Pregunta, excluir: string | null): Promise<TicketEncontrado[]>;

  /** Deja constancia de una búsqueda, para saber después si sirvió de algo. */
  anotarBusqueda(texto: string, autor: string | null, resultados: number, confianza: string | null): Promise<void>;

  /**
   * Apunta que, buscando estos términos, alguien abrió ese tema — y da por servida la
   * búsqueda que lo originó.
   */
  anotarAcierto(terminos: string[], texto: string, tema: string): Promise<void>;

  /** Lo que se buscó y no acabó en nada, agrupado por lo que se escribió. */
  busquedasSinRespuesta(dias: number, limite: number): Promise<BusquedaRegistrada[]>;
}
