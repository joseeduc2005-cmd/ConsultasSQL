/**
 * Cliente de la API de quien reporta.
 *
 * Aparte del cliente de `/soporte`, que es el del panel de administración y exige rol
 * admin. Aquí solo hay tres cosas: buscar, reportar, y ver lo propio.
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

export const clienteApi = {
  solucionar: (texto: string) =>
    pedir<{ consulta: string; precedentes: PrecedentePublico[] }>('/solucionar', {
      method: 'POST',
      body: JSON.stringify({ texto }),
    }),

  /** El autor no viaja: lo pone el backend desde la sesión. */
  reportar: (datos: { titulo: string; descripcion?: string; sistema?: string }) =>
    pedir<CasoPropio>('/casos', { method: 'POST', body: JSON.stringify(datos) }),

  misCasos: () => pedir<CasoPropio[]>('/casos'),
  caso: (numero: string) => pedir<CasoPropio>(`/casos/${encodeURIComponent(numero)}`),
};
