'use client';

import { Suspense } from 'react';
import { Cargando } from '@/soporte/components/ui/estados';
import { PaginaTickets } from '@/soporte/features/tickets/lista';

export default function Tickets() {
  // Los filtros viven en la URL, y useSearchParams exige un límite de Suspense.
  return (
    <Suspense fallback={<Cargando />}>
      <PaginaTickets />
    </Suspense>
  );
}
