/**
 * Cliente de la API de quien reporta.
 *
 * Aparte del cliente de `/soporte`, que es el del panel de administración y exige rol
 * admin. Aquí solo hay cuatro cosas: buscar, reportar, ver lo propio, y proponer una
 * solución para que soporte la revise.
 */
export interface PrecedentePublico {
  tipo: 'ticket' | 'articulo';
  id: string;
  titulo: string;
  detalle: string;
  solucion?: string | null;
  relevancia: number;
  categoria: string | null;
  enlace: string;
}

export interface CasoPropio {
  numero: string;
  titulo: string;
  descripcion?: string | null;
  estado: string;
  sistema?: string | null;
  creado_en?: string | null;
  resuelto_en?: string | null;
  causa_raiz?: string | null;
  solucion?: string | null;
  con_solucion?: boolean;
}

function cabeceras(): Record<string, string> {
  if (typeof window === 'undefined') return {};
  try {
    const token = localStorage.getItem('token');
    return token ? { Authorization: `Bearer ${token}` } : {};
  } catch {
    return {};
  }
}

async function pedir<T>(ruta: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`/api/soporte-cliente${ruta}`, {
    ...init,
    headers: {
      ...cabeceras(),
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...init?.headers,
    },
  });
  const cuerpo = await r.json().catch(() => null);
  if (!r.ok) throw new Error(cuerpo?.error ?? `Error ${r.status}`);
  return cuerpo as T;
}

export interface TemaArticulo {
  id: string;
  titulo: string;
  descripcion: string | null;
  categoria: string | null;
  subcategoria: string | null;
}

export interface Tema {
  categoria: string;
  articulos: TemaArticulo[];
}

export interface AporteNuevo {
  titulo: string;
  problema?: string;
  pasos: string;
  sql?: string;
  base?: string;
  caso?: string;
}

export interface Aporte extends AporteNuevo {
  id: string;
  autor: string;
  sql_sugerido: string | null;
  estado: 'pendiente' | 'aceptado' | 'rechazado';
  /** Por qué se rechazó, o la observación con que se aceptó. */
  nota: string | null;
  revisado_por: string | null;
  revisado_en: string | null;
  /** El tema que se creó a partir de esto, si se aceptó. */
  articulo: string | null;
  creado_en: string;
}

export const clienteApi = {
  solucionar: (texto: string) =>
    pedir<{ consulta: string; precedentes: PrecedentePublico[] }>('/solucionar', {
      method: 'POST',
      body: JSON.stringify({ texto }),
    }),

  /** El autor no viaja: lo pone el backend desde la sesión. */
  reportar: (datos: { titulo: string; descripcion?: string; sistema?: string }) =>
    pedir<CasoPropio>('/casos', { method: 'POST', body: JSON.stringify(datos) }),

  temas: () => pedir<Tema[]>('/temas'),
  tema: (id: string) =>
    pedir<{
      id: string;
      titulo: string;
      contenido: string | null;
      categoria: string | null;
      /** Viene para leerla. Ejecutarla es cosa del panel: aquí no hay con qué. */
      sql: string | null;
      base: string | null;
    }>(`/temas/${encodeURIComponent(id)}`),

  misCasos: () => pedir<CasoPropio[]>('/casos'),
  caso: (numero: string) => pedir<CasoPropio>(`/casos/${encodeURIComponent(numero)}`),

  /** No publica: queda pendiente de que soporte lo revise. */
  aportar: (datos: AporteNuevo) =>
    pedir<Aporte>('/aportes', { method: 'POST', body: JSON.stringify(datos) }),
  misAportes: () => pedir<Aporte[]>('/aportes'),
};
