/**
 * Los aportes, cableados contra PostgreSQL.
 *
 * Sin lógica: elige el adaptador y se lo pasa a los casos de uso. Lo que decide algo
 * está en `dominio/aportes/` (qué es un aporte válido, cómo se redacta el tema) y en
 * `aplicacion/aportes.ts` (los casos de uso). El SQL, en `adaptadores/postgres/`.
 *
 * Se conserva como punto de entrada con las mismas firmas de siempre, para que las
 * rutas y las herramientas no tengan que cambiar.
 */
import { aportesPostgres } from './adaptadores/postgres/aportes-postgres.ts';
import * as casos from './aplicacion/aportes.ts';
import type { Aporte, EstadoAporte, PasoAporte } from './dominio/aportes/aporte.ts';
import type { AporteNuevo, RevisionAporte } from './aplicacion/aportes.ts';

export type { Aporte, EstadoAporte, PasoAporte, AporteNuevo, RevisionAporte };

export const crearAporte = (datos: AporteNuevo, autor: string) =>
  casos.crearAporte(aportesPostgres, datos, autor);

export const aportesDe = (autor: string) => casos.aportesDe(aportesPostgres, autor);

export const listarAportes = (estado?: string | null) => casos.listarAportes(aportesPostgres, estado);

export const obtenerAporte = (id: string) => casos.obtenerAporte(aportesPostgres, id);

export const contarAportesPendientes = () => casos.contarAportesPendientes(aportesPostgres);

export const aceptarAporte = (
  id: string,
  revision: RevisionAporte,
  revisor: { id: string | null; nombre: string },
) => casos.aceptarAporte(aportesPostgres, id, revision, revisor);

export const rechazarAporte = (id: string, nota: unknown, revisor: string) =>
  casos.rechazarAporte(aportesPostgres, id, nota, revisor);
