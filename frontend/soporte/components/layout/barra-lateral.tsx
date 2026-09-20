'use client';

import { useQuery } from '@tanstack/react-query';
import { ArrowLeft, BookOpen, LayoutDashboard, MessageSquareText, Radar, ScanSearch, Ticket } from 'lucide-react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { api } from '@/soporte/lib/api';
import { claves, useEstado } from '@/soporte/lib/consultas';
import { cn } from '@/soporte/lib/utils';
import { Marca } from './marca';

/**
 * Navegación del módulo de soporte.
 *
 * Viene de `components/layout/sidebar.tsx` de ticket v2.0. Cambia el `NavLink` de
 * react-router por `next/link` más `usePathname` para saber cuál está activa, y se
 * quitó la fila de estado del modelo de lenguaje, que quedó fuera de esta fase.
 */
const NAVEGACION = [
  { a: '/soporte', etiqueta: 'Panel', icono: LayoutDashboard, exacta: true },
  { a: '/soporte/diagnostico', etiqueta: 'Diagnósticos', icono: ScanSearch },
  { a: '/soporte/tickets', etiqueta: 'Tickets', icono: Ticket },
  { a: '/soporte/biblioteca', etiqueta: 'Biblioteca SQL', icono: BookOpen },
  { a: '/soporte/vigilancia', etiqueta: 'Vigilancia', icono: Radar },
  { a: '/soporte/interpretar', etiqueta: 'Interpretar mensaje', icono: MessageSquareText, pendiente: true },
];

export function BarraLateral({ onNavegar }: { onNavegar?: () => void }) {
  const ruta = usePathname();
  const { data: estado } = useEstado();
  const { data: resumen } = useQuery({ queryKey: claves.resumen, queryFn: api.resumen, refetchInterval: 60_000 });

  const insignias: Record<string, number | undefined> = {
    '/soporte/vigilancia': resumen?.alertas.total || undefined,
    '/soporte/tickets': (resumen ? resumen.tickets.abiertos + resumen.tickets.investigacion : 0) || undefined,
  };

  return (
    <aside className="flex h-full w-64 flex-col bg-sidebar text-sidebar-foreground">
      <div className="flex h-16 items-center gap-3 border-b border-sidebar-border px-5">
        <Marca />
        <div className="leading-tight">
          <p className="text-[14px] font-semibold tracking-tight">Centro de Soporte</p>
          <p className="text-[11.5px] text-sidebar-muted">Sistemas · Corresponsales</p>
        </div>
      </div>

      <nav className="flex-1 space-y-0.5 overflow-y-auto px-3 py-4">
        <p className="px-3 pb-2 text-[11px] font-semibold tracking-wider text-sidebar-muted/80 uppercase">Operación</p>
        {NAVEGACION.map(({ a, etiqueta, icono: Icono, exacta, pendiente }) => {
          const activa = exacta ? ruta === a : ruta.startsWith(a);
          return (
            <Link
              key={a}
              href={a}
              onClick={onNavegar}
              className={cn(
                'group relative flex h-9 items-center gap-3 rounded-lg px-3 text-[13.5px] font-medium transition-colors',
                activa
                  ? 'bg-sidebar-active text-white'
                  : 'text-sidebar-foreground/75 hover:bg-sidebar-hover hover:text-sidebar-foreground',
              )}
            >
              {activa && <span className="absolute top-2 bottom-2 left-0 w-[3px] rounded-r-full bg-[#5eead4]" />}
              <Icono
                className={cn(
                  'size-4 shrink-0',
                  activa ? 'text-[#5eead4]' : 'text-sidebar-muted group-hover:text-sidebar-foreground',
                )}
              />
              <span className="flex-1 truncate">{etiqueta}</span>
              {pendiente && (
                <span className="rounded-md bg-white/10 px-1.5 py-px text-[10px] font-medium text-sidebar-muted">
                  pendiente
                </span>
              )}
              {insignias[a] !== undefined && (
                <span
                  className={cn(
                    'tabular rounded-md px-1.5 py-px text-[11px] font-semibold',
                    a === '/soporte/vigilancia' ? 'bg-danger/90 text-white' : 'bg-white/10 text-sidebar-foreground',
                  )}
                >
                  {insignias[a]}
                </span>
              )}
            </Link>
          );
        })}
      </nav>

      <div className="space-y-2.5 border-t border-sidebar-border px-5 py-4 text-[12px]">
        <div className="flex items-center justify-between gap-2" title={estado?.oracle.detalle}>
          <span className="text-sidebar-muted">Oracle</span>
          <span className="flex items-center gap-1.5 font-medium">
            <span
              className={cn(
                'size-1.5 rounded-full',
                !estado
                  ? 'bg-sidebar-muted'
                  : estado.oracle.modo === 'demo'
                    ? 'bg-amber-400'
                    : estado.oracle.conectado
                      ? 'bg-emerald-400'
                      : 'bg-rose-400',
              )}
            />
            {!estado
              ? '…'
              : estado.oracle.modo === 'demo'
                ? 'Demostración'
                : estado.oracle.conectado
                  ? 'Conectado'
                  : 'Sin conexión'}
          </span>
        </div>
        <div className="flex items-center justify-between gap-2">
          <span className="text-sidebar-muted">Vigilancia</span>
          <span className="flex items-center gap-1.5 font-medium">
            <span className={cn('size-1.5 rounded-full', estado?.vigilancia ? 'bg-emerald-400' : 'bg-sidebar-muted')} />
            {estado?.vigilancia ? 'Activa' : 'Apagada'}
          </span>
        </div>
        <Link
          href="/admin"
          className="mt-1 flex items-center gap-1.5 pt-1 text-[11.5px] text-sidebar-muted transition-colors hover:text-sidebar-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Volver a ConsultasSQL
        </Link>
      </div>
    </aside>
  );
}
