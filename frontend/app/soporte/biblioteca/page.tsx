'use client';

import { Suspense } from 'react';
import { PaginaBiblioteca } from '@/soporte/features/biblioteca/pagina';
import { Cargando } from '@/soporte/components/ui/estados';

export default function Biblioteca() {
  // useSearchParams obliga a un límite de Suspense para que Next pueda prerenderizar.
  return (
    <Suspense fallback={<Cargando />}>
      <PaginaBiblioteca />
    </Suspense>
  );
}
