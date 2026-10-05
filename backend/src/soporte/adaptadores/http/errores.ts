export class ErrorHttp extends Error {
  readonly estado: number;

  constructor(estado: number, mensaje: string) {
    super(mensaje);
    this.estado = estado;
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
