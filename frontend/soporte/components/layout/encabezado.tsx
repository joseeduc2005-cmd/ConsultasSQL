import type { ReactNode } from 'react';

/**
 * Encabezado común de las pantallas de soporte.
 *
 * Venía de `components/layout/shell.tsx` en ticket v2.0. El resto de aquel archivo
 * (el Shell con su barra lateral) no se trajo: la navegación la pone ConsultasSQL.
 */
export function EncabezadoPagina({
  titulo,
  descripcion,
  acciones,
}: {
  titulo: string;
  descripcion?: string;
  acciones?: ReactNode;
}) {
  return (
    <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
      <div>
        <h2 className="text-xl font-semibold tracking-tight">{titulo}</h2>
        {descripcion && <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{descripcion}</p>}
      </div>
      {acciones && <div className="flex items-center gap-2">{acciones}</div>}
    </div>
  );
}
