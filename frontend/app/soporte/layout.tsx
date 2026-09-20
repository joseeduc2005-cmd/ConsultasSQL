'use client';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { usePathname } from 'next/navigation';
import { useEffect, useState, type ReactNode } from 'react';
import { withAuth } from '@/app/components/ProtectedRoute';
import { BarraLateral } from '@/soporte/components/layout/barra-lateral';
import { Cabecera } from '@/soporte/components/layout/cabecera';
import { AvisosProvider } from '@/soporte/components/ui/toast';

/**
 * Marco del módulo de soporte: barra lateral, cabecera y área de contenido.
 *
 * `.soporte-raiz` es lo que activa los tokens de diseño del módulo (ver el bloque
 * final de globals.css). Están acotados a este subárbol para que ninguna pantalla
 * de ConsultasSQL cambie de aspecto.
 *
 * Todo el módulo exige rol admin. Esta es la primera de dos barreras: `withAuth`
 * confirma el rol contra /api/auth/me, así que no basta con manipular localStorage.
 * La segunda vive en el backend, que resuelve el rol desde la sesión del token y
 * rechaza cualquier petición a /api/soporte que no sea de un admin.
 */
function LayoutSoporte({ children }: { children: ReactNode }) {
  // Un cliente por montaje: crearlo en el módulo lo compartiría entre peticiones en SSR.
  const [cliente] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: { staleTime: 15_000, retry: 1, refetchOnWindowFocus: false },
        },
      }),
  );
  const ruta = usePathname();
  const [menuAbierto, setMenuAbierto] = useState(false);

  // Al navegar en móvil, el menú desplegable se cierra solo.
  useEffect(() => setMenuAbierto(false), [ruta]);

  return (
    <QueryClientProvider client={cliente}>
      <AvisosProvider>
        <div className="soporte-raiz flex h-dvh overflow-hidden">
          <div className="hidden lg:block">
            <BarraLateral />
          </div>

          {menuAbierto && (
            <div className="fixed inset-0 z-40 lg:hidden">
              <div className="absolute inset-0 bg-black/40" onClick={() => setMenuAbierto(false)} />
              <div className="animate-entrada relative h-full w-64">
                <BarraLateral onNavegar={() => setMenuAbierto(false)} />
              </div>
            </div>
          )}

          <div className="flex min-w-0 flex-1 flex-col">
            <Cabecera menuAbierto={menuAbierto} onAlternarMenu={() => setMenuAbierto((v) => !v)} />
            <main className="scroll-fino flex-1 overflow-y-auto">
              <div key={ruta} className="animate-entrada mx-auto w-full max-w-[1400px] px-4 py-6 sm:px-6 lg:px-8">
                {children}
              </div>
            </main>
          </div>
        </div>
      </AvisosProvider>
    </QueryClientProvider>
  );
}

export default withAuth(LayoutSoporte, 'admin');
