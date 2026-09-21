'use client';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useRouter } from 'next/navigation';
import { useEffect, useState, type ReactNode } from 'react';
import CabeceraPanel from '@/app/components/CabeceraPanel';
import NavegacionSoporte from '@/app/components/NavegacionSoporte';
import { withAuth } from '@/app/components/ProtectedRoute';
import { useTheme } from '@/app/components/ThemeProvider';
import { AvisosProvider } from '@/soporte/components/ui/toast';

/**
 * El Centro de Soporte es una sección del panel de administración, no una
 * aplicación aparte: reutiliza su cabecera (`CabeceraPanel`) y su misma disposición
 * de barra lateral y contenido, para que al cambiar de sección no cambie el marco.
 *
 * `.soporte-raiz` se aplica solo al área de contenido. Es lo que activa los tokens
 * de diseño del módulo, y acotarlo aquí evita que repinte la cabecera o la barra.
 *
 * Exige rol admin, igual que el resto del panel. Es la primera de dos barreras:
 * `withAuth` confirma el rol contra /api/auth/me; la segunda vive en el backend,
 * que lo resuelve desde la sesión del token.
 */
interface Usuario {
  id: string;
  username: string;
  role: string;
}

function LayoutSoporte({ children }: { children: ReactNode }) {
  // Un cliente por montaje: crearlo en el módulo lo compartiría entre peticiones en SSR.
  const [cliente] = useState(
    () =>
      new QueryClient({
        defaultOptions: { queries: { staleTime: 15_000, retry: 1, refetchOnWindowFocus: false } },
      }),
  );
  const router = useRouter();
  const { theme } = useTheme();
  const [usuario, setUsuario] = useState<Usuario | null>(null);

  useEffect(() => {
    try {
      const guardado = localStorage.getItem('user');
      if (guardado) setUsuario(JSON.parse(guardado));
    } catch {
      /* almacenamiento no disponible */
    }
  }, []);

  const cerrarSesion = async () => {
    try {
      const token = localStorage.getItem('token');
      await fetch('/api/auth/logout', {
        method: 'POST',
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
    } catch {
      /* el backend puede estar caído; la sesión local se limpia igual */
    }
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    router.push('/login');
  };

  return (
    <QueryClientProvider client={cliente}>
      <AvisosProvider>
        <div className="h-screen flex flex-col overflow-hidden px-2 py-2">
          <CabeceraPanel
            subtitulo="Tickets, diagnósticos automáticos, biblioteca SQL y vigilancia."
            currentUser={usuario}
            isLightTheme={theme === 'light'}
            onLogout={cerrarSesion}
            primaryActionLabel="Ir a Panel SQL Automático"
            onPrimaryAction={() => router.push('/dashboard')}
          />

          <div className="flex-1 min-h-0 w-full py-4">
            <div className="flex gap-3 h-full min-w-0">
              <aside className="glass-panel glass-panel-hover rounded-2xl p-2 h-full min-h-0 overflow-y-auto shrink-0 w-[240px] xl:w-[280px]">
                <NavegacionSoporte />
                <div className="px-2 pt-1">
                  <a
                    href="/admin"
                    className="block w-full rounded-lg px-3 py-2 text-sm text-(--ink-700) transition-colors hover:bg-white/75 hover:text-(--ink-900)"
                  >
                    ← Artículos y workflows
                  </a>
                </div>
              </aside>

              <section className="glass-panel-strong glass-panel-hover flex-1 min-w-0 rounded-2xl h-full min-h-0 overflow-hidden">
                <div className="soporte-raiz h-full overflow-y-auto scroll-fino rounded-2xl px-5 py-5">{children}</div>
              </section>
            </div>
          </div>
        </div>
      </AvisosProvider>
    </QueryClientProvider>
  );
}

export default withAuth(LayoutSoporte, 'admin');
