/**
 * Un error con su código HTTP.
 *
 * El código va en dos propiedades con el mismo valor, y no es un descuido. Hay dos
 * manejadores que lo leen: el del módulo de soporte, que mira `estado`, y el general de
 * la aplicación, que mira `status`. Cuando solo existía `estado`, todo lo que no pasaba
 * por el primero —el router de cliente entero, que no tiene manejador propio— terminaba
 * en 500, y el manejador general oculta el mensaje de los 500 a propósito: así que
 * buscar con el cuadro vacío respondía «Internal Server Error» en vez de decir qué
 * falta. Un fallo de quien usa la pantalla presentado como una avería del servidor.
 *
 * Teniéndolo en los dos nombres, cualquier ruta nueva sale bien sin tener que acordarse
 * de añadirle un manejador.
 */
export class ErrorHttp extends Error {
  readonly estado: number;

  /** El mismo código, con el nombre que lee el manejador general. */
  readonly status: number;

  constructor(estado: number, mensaje: string) {
    super(mensaje);
    this.estado = estado;
    this.status = estado;
  }
}

export const noEncontrado = (mensaje: string) => new ErrorHttp(404, mensaje);
export const solicitudInvalida = (mensaje: string) => new ErrorHttp(400, mensaje);

export function textoRequerido(valor: unknown, campo: string): string {
  if (typeof valor !== 'string' || valor.trim() === '') {
    throw solicitudInvalida(`El campo "${campo}" es obligatorio.`);
  }
  return valor.trim();
}

export function textoOpcional(valor: unknown): string | null {
  return typeof valor === 'string' && valor.trim() !== '' ? valor.trim() : null;
}
