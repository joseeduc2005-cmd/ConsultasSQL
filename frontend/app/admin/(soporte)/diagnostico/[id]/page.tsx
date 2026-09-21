'use client';

import { Suspense } from 'react';
import { Cargando } from '@/soporte/components/ui/estados';
import { PaginaEjecutarDiagnostico } from '@/soporte/features/diagnostico/paginas';

export default function EjecutarDiagnostico() {
  // Los datos de entrada pueden llegar prellenados por la URL (enlace "Investigar").
  return (
    <Suspense fallback={<Cargando />}>
      <PaginaEjecutarDiagnostico />
    </Suspense>
  );
}
