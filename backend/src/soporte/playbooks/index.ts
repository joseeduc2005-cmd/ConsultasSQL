import { accesoCorresponsal } from './acceso-corresponsal.ts';
import { comprobanteRecaudacion } from './comprobante-recaudacion.ts';
import { macDuplicada } from './mac-duplicada.ts';
import { politicaClaves } from './politica-claves.ts';
import { reporteTransacciones } from './reporte-transacciones.ts';
import { servicioExterno } from './servicio-externo.ts';
import { sucursalCorresponsal } from './sucursal-corresponsal.ts';
import type { Playbook } from './tipos.ts';

/** Para agregar un diagnóstico nuevo: crear el archivo del playbook y registrarlo aquí. */
export const PLAYBOOKS: Playbook[] = [
  accesoCorresponsal,
  macDuplicada,
  sucursalCorresponsal,
  reporteTransacciones,
  politicaClaves,
  comprobanteRecaudacion,
  servicioExterno,
];

export const obtenerPlaybook = (id: string): Playbook | undefined => PLAYBOOKS.find((p) => p.id === id);

/** Versión serializable para el frontend (sin funciones ni expresiones regulares). */
export const describirPlaybook = (p: Playbook) => ({
  id: p.id,
  nombre: p.nombre,
  descripcion: p.descripcion,
  sistema: p.sistema,
  categoria: p.categoria,
  ticketsReferencia: p.ticketsReferencia,
  campos: p.campos,
  requiereAlguno: p.requiereAlguno ?? [],
});
