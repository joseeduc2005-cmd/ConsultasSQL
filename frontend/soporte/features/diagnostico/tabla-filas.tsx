'use client';

import type { Fila } from '@/soporte/lib/types';
import { valorCelda } from '@/soporte/lib/formato';
import { cn } from '@/soporte/lib/utils';

export function TablaFilas({ filas, max = 50 }: { filas: Fila[]; max?: number }) {
  if (filas.length === 0) {
    return <p className="rounded-lg border border-dashed px-3 py-2.5 text-xs text-muted-foreground">La consulta no devolvió filas.</p>;
  }
  const columnas = Object.keys(filas[0]);
  const visibles = filas.slice(0, max);
  return (
    <div className="scroll-fino overflow-x-auto rounded-lg border">
      <table className="w-full text-left text-xs">
        <thead className="bg-surface-muted text-muted-foreground">
          <tr>
            {columnas.map((c) => (
              <th key={c} className="px-3 py-2 font-mono text-[11px] font-medium whitespace-nowrap">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y">
          {visibles.map((fila, i) => (
            <tr key={i} className="hover:bg-surface-muted/60">
              {columnas.map((c) => {
                const v = fila[c];
                return (
                  <td key={c} className={cn('tabular px-3 py-1.5 font-mono whitespace-nowrap', v === null || v === undefined ? 'text-muted-foreground/60 italic' : '')}>
                    {valorCelda(v)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {filas.length > max && <p className="border-t px-3 py-1.5 text-[11px] text-muted-foreground">Mostrando {max} de {filas.length} filas.</p>}
    </div>
  );
}
