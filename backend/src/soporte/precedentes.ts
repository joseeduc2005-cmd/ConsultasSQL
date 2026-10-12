/**
 * La búsqueda, cableada contra PostgreSQL.
 *
 * Este archivo ya no tiene lógica: elige qué adaptador cumple el puerto y se lo pasa a
 * los casos de uso. Todo lo que decide algo vive en otro sitio:
 *
 *   dominio/busqueda/terminos.ts   trocear la frase            (puro)
 *   dominio/busqueda/pesaje.ts     pesos, cobertura, confianza (puro)
 *   aplicacion/buscar-precedentes  el caso de uso              (solo conoce el puerto)
 *   puertos/catalogo.ts            el contrato
 *   adaptadores/postgres/…         el SQL
 *
 * Se conserva como punto de entrada, con las mismas firmas de siempre, para que quien
 * lo importa —las rutas, las herramientas, las pruebas— no tenga que cambiar.
 */
import {
  busquedasFallidas as buscarFallidas,
  buscarPrecedentes as buscar,
  registrarAcierto as anotarAcierto,
  registrarBusqueda as anotarBusqueda,
  type OpcionesBusqueda,
  type Precedente,
} from './aplicacion/buscar-precedentes.ts';
import { catalogoPostgres } from './adaptadores/postgres/catalogo-postgres.ts';
import { aConsulta } from './dominio/busqueda/terminos.ts';

export type { Precedente, OpcionesBusqueda };

export function buscarPrecedentes(texto: string, opciones: OpcionesBusqueda = {}): Promise<Precedente[]> {
  return buscar(catalogoPostgres, texto, opciones);
}

export function registrarAcierto(texto: string, articulo: string): Promise<void> {
  return anotarAcierto(catalogoPostgres, texto, articulo);
}

export function registrarBusqueda(
  texto: string,
  autor: string | null,
  resultados: number,
  confianza: string | null,
): Promise<void> {
  return anotarBusqueda(catalogoPostgres, texto, autor, resultados, confianza);
}

export function busquedasFallidas(dias = 30, limite = 50) {
  return buscarFallidas(catalogoPostgres, dias, limite);
}

/** Expuesto solo para las pruebas: es la pieza que decide qué se busca. */
export const aConsultaParaPruebas = aConsulta;
