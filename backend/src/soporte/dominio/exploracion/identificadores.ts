/**
 * Deducir dónde vive un dato en una base que nadie estudió.
 *
 * No hace falta saber el esquema de nadie: basta con que la columna se llame de alguna
 * forma razonable. Cuando aparezca una base que use otro nombre, se añade a la lista y
 * sirve para todas.
 *
 * Dominio puro: trabaja sobre el esquema introspeccionado, sin tocar la base.
 */
import type { TablaEsquema } from './grafo.ts';

const NOMBRES = {
  cedula: ['cedula', 'identificacion', 'documento', 'dni', 'nro_documento', 'num_documento', 'ruc'],
  usuario: ['usuario', 'username', 'user_name', 'login', 'nombre_usuario', 'cuenta'],
  mac: ['mac', 'pos_mac', 'direccion_mac', 'mac_address', 'imei', 'id_dispositivo'],
} as const;

export type TipoIdentificador = keyof typeof NOMBRES;


function normalizar(texto: string): string {
  return texto
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9]/g, '');
}

/** La columna de esta tabla que representa ese dato, si la hay. */
function columnaPara(tabla: TablaEsquema, tipo: TipoIdentificador): string | null {
  const candidatos = NOMBRES[tipo].map(normalizar);
  const columnas = tabla.columns ?? [];

  // Primero una coincidencia exacta; solo si no la hay se acepta que la contenga,
  // porque «codigo_usuario» y «usuario» no son lo mismo y el exacto debe ganar.
  const exacta = columnas.find((c) => candidatos.includes(normalizar(c.name)));
  if (exacta) return exacta.name;

  const parcial = columnas.find((c) => candidatos.some((n) => normalizar(c.name).includes(n)));
  return parcial?.name ?? null;
}

export interface Candidata {
  tabla: string;
  columna: string;
  /** Cuántas columnas tiene: entre dos tablas que sirven, gana la que más describe. */
  columnas: number;
}

/**
 * Tablas donde podría estar ese identificador, de la más prometedora a la menos.
 *
 * Se ordena por número de columnas porque una tabla de detalle suele tener más
 * información útil que una de relación con dos campos.
 */
export function tablasCandidatas(
  esquema: TablaEsquema[],
  tipo: TipoIdentificador,
  limite = 3,
): Candidata[] {
  return esquema
    .map((t) => {
      const columna = columnaPara(t, tipo);
      return columna ? { tabla: t.name, columna, columnas: (t.columns ?? []).length } : null;
    })
    .filter((c): c is Candidata => c !== null)
    .sort((a, b) => b.columnas - a.columnas)
    .slice(0, limite);
}
