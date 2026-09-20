'use client';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useState, type ReactNode } from 'react';
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
 */
export default function LayoutSoporte({ children }: { children: ReactNode }) {
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
