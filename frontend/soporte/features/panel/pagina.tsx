'use client';

import { useQuery } from '@tanstack/react-query';
import { CircleCheck, Lightbulb, MessageSquareText, Search, Ticket } from 'lucide-react';
import type { ReactNode } from 'react';
import Link from 'next/link';
import { Badge } from '@/soporte/components/ui/badge';
import { Card, CardHeader } from '@/soporte/components/ui/card';
import { Skeleton, Vacio } from '@/soporte/components/ui/estados';
import { api } from '@/soporte/lib/api';
import { claves } from '@/soporte/lib/consultas';
import { ESTADO_TICKET } from '@/soporte/lib/formato';
import { cn } from '@/soporte/lib/utils';

/**
 * El panel de entrada.
 *
 * Antes mostraba alertas de la vigilancia, un acceso rápido a los playbooks de
 * diagnóstico y el historial de diagnósticos ejecutados. Esas tres pantallas se
 * retiraron, y con ellas sus rutas: el panel seguía pidiéndolas y habría quedado
 * llamando a cuatro endpoints que devuelven 404.
 *
 * Lo que queda es lo que de verdad hay que mirar cada día: cuántos aportes esperan
 * revisión —esa es la bandeja que, si nadie la ve, se queda sin ver— y en qué están
 * los tickets.
 */
function saludo(): string {
  const h = new Date().getHours();
  return h < 12 ? 'Buenos días' : h < 19 ? 'Buenas tardes' : 'Buenas noches';
}

export function PaginaPanel() {
  const { data: resumen } = useQuery({ queryKey: claves.resumen, queryFn: api.resumen, refetchInterval: 60_000 });
  const { data: tickets } = useQuery({ queryKey: claves.tickets(), queryFn: () => api.tickets() });

  const activos = tickets?.filter((t) => t.estado !== 'resuelto') ?? [];
  const fecha = new Intl.DateTimeFormat('es-EC', { weekday: 'long', day: 'numeric', month: 'long' }).format(new Date());

  return (
    <>
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-[13px] text-muted-foreground first-letter:uppercase">{fecha}</p>
          <h2 className="mt-0.5 text-2xl font-semibold tracking-tight">{saludo()}</h2>
        </div>
        <Link
          href="/admin/solucionar"
          className="inline-flex h-9 items-center gap-2 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary-hover"
        >
          <MessageSquareText className="size-4" />
          Buscar una solución
        </Link>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Indicador
          etiqueta="Aportes por revisar"
          valor={resumen?.aportesPendientes}
          detalle="Esperando que alguien los apruebe"
          acento={resumen?.aportesPendientes ? 'warning' : 'success'}
          icono={<Lightbulb />}
          a="/admin/aportes"
        />
        <Indicador
          etiqueta="Temas de ayuda"
          valor={resumen?.temas}
          detalle="En la base de conocimiento"
          acento="primary"
          icono={<Search />}
          a="/admin/solucionar"
        />
        <Indicador
          etiqueta="Tickets en investigación"
          valor={resumen?.tickets.investigacion}
          detalle={resumen ? `${resumen.tickets.abiertos} abiertos sin investigar` : undefined}
          acento="warning"
          icono={<Ticket />}
          a="/admin/tickets?estado=investigacion"
        />
        <Indicador
          etiqueta="Tickets resueltos"
          valor={resumen?.tickets.resueltos}
          detalle="Ya con su causa y su solución"
          acento="success"
          icono={<CircleCheck />}
          a="/admin/tickets?estado=resuelto"
        />
      </div>

      <div className="mt-6">
        <Card>
          <CardHeader
            titulo="Tickets en curso"
            icono={<Ticket />}
            acciones={
              <Link href="/admin/tickets" className="text-[13px] font-medium text-primary hover:underline">
                Ver tickets
              </Link>
            }
          />
          {activos.length === 0 ? (
            <Vacio titulo="No hay tickets activos" />
          ) : (
            <ul className="divide-y">
              {activos.slice(0, 8).map((t) => (
                <li key={t.numero}>
                  <Link
                    href={`/admin/tickets/${t.numero}`}
                    className="group flex items-center gap-4 px-5 py-3 hover:bg-surface-muted/50"
                  >
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-[13.5px] font-medium group-hover:text-primary">{t.titulo}</p>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        <span className="font-mono">{t.numero}</span> · {t.pasos} paso{t.pasos === 1 ? '' : 's'}
                      </p>
                    </div>
                    <Badge tono={ESTADO_TICKET[t.estado].tono} punto>
                      {ESTADO_TICKET[t.estado].etiqueta}
                    </Badge>
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>
    </>
  );
}

const ACENTOS = {
  danger: 'bg-danger-soft text-danger',
  warning: 'bg-warning-soft text-warning',
  primary: 'bg-primary-soft text-primary-soft-foreground',
  success: 'bg-success-soft text-success',
} as const;

function Indicador({
  etiqueta, valor, detalle, acento, icono, a,
}: { etiqueta: string; valor?: number; detalle?: string; acento: keyof typeof ACENTOS; icono: ReactNode; a: string }) {
  return (
    <Link href={a} className="group">
      <Card className="p-5 transition-all group-hover:-translate-y-0.5 group-hover:shadow-pop">
        <div className="flex items-start justify-between">
          <p className="text-[13px] font-medium text-muted-foreground">{etiqueta}</p>
          <span className={cn('grid size-8 place-items-center rounded-lg [&_svg]:size-4', ACENTOS[acento])}>{icono}</span>
        </div>
        {valor === undefined ? <Skeleton className="mt-2 h-9 w-16" /> : <p className="tabular mt-1 text-3xl font-semibold tracking-tight">{valor}</p>}
        <p className="mt-1 h-4 text-xs text-muted-foreground">{detalle}</p>
      </Card>
    </Link>
  );
}
