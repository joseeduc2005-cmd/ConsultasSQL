'use client';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useState, type ReactNode } from 'react';
import { withAuth } from '@/app/components/ProtectedRoute';
import { AvisosProvider } from '@/soporte/components/ui/toast';

/**
 * Contenedor del módulo de soporte.
 *
 * `.soporte-raiz` es lo que activa los tokens de diseño del módulo (ver el bloque
 * final de globals.css). Están acotados a este subárbol para que ninguna pantalla
 * de ConsultasSQL cambie de aspecto.
 *
 * La navegación todavía es provisional: en el paso final las pantallas se cuelgan
 * de la barra lateral de ConsultasSQL y esta cabecera desaparece.
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

  return (
    <QueryClientProvider client={cliente}>
      <AvisosProvider>
        <div className="soporte-raiz min-h-screen">
          <div className="mx-auto w-full max-w-[1400px] px-4 py-6 sm:px-6 lg:px-8">{children}</div>
        </div>
      </AvisosProvider>
    </QueryClientProvider>
  );
}

export default withAuth(LayoutSoporte, 'admin');
