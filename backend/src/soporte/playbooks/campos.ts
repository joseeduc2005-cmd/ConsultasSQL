import type { CampoEntrada } from './tipos.ts';

export const CAMPO_MAC: CampoEntrada = {
  clave: 'mac',
  etiqueta: 'MAC / IMEI del POS',
  tipo: 'texto',
  patron: '^\\d{15}$',
  ejemplo: '860718041153392',
  ayuda: '15 dígitos, como aparece en la etiqueta del equipo.',
};

export const CAMPO_USUARIO: CampoEntrada = {
  clave: 'usuario',
  etiqueta: 'Usuario',
  tipo: 'texto',
  patron: '^[A-Za-z0-9._-]{3,40}$',
  ejemplo: 'nombre.apellido',
};

export const CAMPO_CEDULA: CampoEntrada = {
  clave: 'cedula',
  etiqueta: 'Cédula',
  tipo: 'texto',
  patron: '^\\d{10}$',
  ejemplo: '0102030405',
};

export const campoFecha = (clave: string, etiqueta: string): CampoEntrada => ({
  clave,
  etiqueta,
  tipo: 'fecha',
  patron: '^\\d{4}-\\d{2}-\\d{2}$',
  ejemplo: '2026-09-01',
  requerido: true,
});
