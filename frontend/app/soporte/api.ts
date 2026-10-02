/**
 * Cliente de la API de quien reporta.
 *
 * Aparte del cliente de `/soporte`, que es el del panel de administración y exige rol
 * admin. Aquí solo hay dos cosas: buscar en lo que ya se resolvió, y aportar la
 * solución propia para que soporte la revise.
 */
export interface PrecedentePublico {
  tipo: 'ticket' | 'articulo';
  id: string;
  titulo: string;
  detalle: string;
  solucion?: string | null;
  relevancia: number;
  /** Cuánto fiarse. Se mide sobre la puntuación real, no sobre el primer puesto. */
  confianza: 'alta' | 'media' | 'baja';
  categoria: string | null;
  enlace: string;
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

/** Un paso de lo que se hizo. El script es opcional; el efecto solo aplica si lo hay. */
export interface PasoAporte {
  descripcion: string;
  script: string | null;
  efecto: 'consulta' | 'cambio' | null;
}

export interface AporteNuevo {
  titulo: string;
  problema?: string;
  pasos: PasoAporte[];
  base?: string;
  caso?: string;
}

export interface Aporte extends Omit<AporteNuevo, 'pasos'> {
  id: string;
  autor: string;
  pasos: string;
  pasos_json: PasoAporte[] | null;
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

  temas: () => pedir<Tema[]>('/temas'),
  tema: (id: string) =>
    pedir<{
      id: string;
      titulo: string;
      contenido: string | null;
      categoria: string | null;
      /**
       * Viene ya formateada para leerla —SQL, la instrucción de Mongo, o JSON si es
       * una forma que todavía no se reconoce—. Ejecutarla es cosa del panel: aquí no
       * hay con qué.
       */
      sentencia: { lenguaje: string; texto: string } | null;
      base: string | null;
    }>(`/temas/${encodeURIComponent(id)}`),

  /** No publica: queda pendiente de que soporte lo revise. */
  aportar: (datos: AporteNuevo) =>
    pedir<Aporte>('/aportes', { method: 'POST', body: JSON.stringify(datos) }),
  misAportes: () => pedir<Aporte[]>('/aportes'),

  /** Solo las bases a las que el backend llegó de verdad, para elegir sin escribir. */
  bases: () => pedir<{ id: string; etiqueta: string; motor: string }[]>('/bases'),
};
