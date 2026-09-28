'use client';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useState, type ReactNode } from 'react';

/**
 * Contexto de datos para la pantalla de quien reporta.
 *
 * Tiene el suyo porque vive fuera de `/admin`, que es donde estaba el único
 * `QueryClientProvider`. Sin esto la página revienta al montar.
 *
 * El cliente se crea con `useState` y no como constante de módulo: así cada pestaña
 * tiene su caché y no se comparte entre sesiones distintas del mismo navegador.
 */
export default function LayoutSoporteCliente({ children }: { children: ReactNode }) {
  const [cliente] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            // Quien reporta abre la pantalla, mira y se va: no hace falta refrescar
            // cada vez que vuelve a la ventana.
            refetchOnWindowFocus: false,
            staleTime: 30_000,
            retry: 1,
          },
        },
      }),
  );

  return <QueryClientProvider client={cliente}>{children}</QueryClientProvider>;
}
