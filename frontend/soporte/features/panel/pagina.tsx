'use client';

import { useQuery } from '@tanstack/react-query';
import { ArrowRight, CircleCheck, History, MessageSquareText, Radar, ScanSearch, Ticket } from 'lucide-react';
import type { ReactNode } from 'react';
import Link from 'next/link';
import { Badge } from '@/soporte/components/ui/badge';
import { Card, CardHeader } from '@/soporte/components/ui/card';
import { Skeleton, Vacio } from '@/soporte/components/ui/estados';
import { api } from '@/soporte/lib/api';
import { claves, usePlaybooks } from '@/soporte/lib/consultas';
import { ESTADO_TICKET, SEVERIDAD, VEREDICTO, haceCuanto } from '@/soporte/lib/formato';
import { cn } from '@/soporte/lib/utils';
import { ICONO_PLAYBOOK } from '@/soporte/lib/iconos';
import { enlaceInvestigar } from '@/soporte/features/vigilancia/pagina';

function saludo(): string {
  const h = new Date().getHours();
  return h < 12 ? 'Buenos días' : h < 19 ? 'Buenas tardes' : 'Buenas noches';
}

export function PaginaPanel() {
  const { data: resumen } = useQuery({ queryKey: claves.resumen, queryFn: api.resumen, refetchInterval: 60_000 });
  const { data: alertas } = useQuery({ queryKey: claves.alertas('abierta'), queryFn: () => api.alertas('abierta'), refetchInterval: 60_000 });
  const { data: monitores } = useQuery({ queryKey: claves.monitores, queryFn: api.monitores });
  const { data: tickets } = useQuery({ queryKey: claves.tickets(), queryFn: () => api.tickets() });
  const { data: diagnosticos } = useQuery({ queryKey: claves.diagnosticos, queryFn: () => api.diagnosticos(15) });
  const { data: playbooks } = usePlaybooks();

  const activos = tickets?.filter((t) => t.estado !== 'resuelto') ?? [];
  const fecha = new Intl.DateTimeFormat('es-EC', { weekday: 'long', day: 'numeric', month: 'long' }).format(new Date());

  return (
    <>
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-[13px] text-muted-foreground first-letter:uppercase">{fecha}</p>
          <h2 className="mt-0.5 text-2xl font-semibold tracking-tight">{saludo()}</h2>
        </div>
        <Link href="/admin/soporte/interpretar" className="inline-flex h-9 items-center gap-2 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary-hover">
          <MessageSquareText className="size-4" />
          Interpretar un mensaje
        </Link>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Indicador
          etiqueta="Alertas abiertas"
          valor={resumen?.alertas.total}
          detalle={resumen ? `${resumen.alertas.criticas} de severidad alta o crítica` : undefined}
          acento={resumen?.alertas.criticas ? 'danger' : 'success'}
          icono={<Radar />}
          a="/admin/soporte/vigilancia"
        />
        <Indicador etiqueta="Tickets en investigación" valor={resumen?.tickets.investigacion} detalle={resumen ? `${resumen.tickets.abiertos} abiertos sin investigar` : undefined} acento="warning" icono={<Ticket />} a="/admin/soporte/tickets?estado=investigacion" />
        <Indicador etiqueta="Diagnósticos (7 días)" valor={resumen?.diagnosticos.ultimos7dias} detalle="Ejecutados por el equipo" acento="primary" icono={<ScanSearch />} a="/admin/soporte/diagnostico" />
        <Indicador etiqueta="Tickets resueltos" valor={resumen?.tickets.resueltos} detalle={resumen ? `${resumen.consultas} consultas en la Biblioteca` : undefined} acento="success" icono={<CircleCheck />} a="/admin/soporte/tickets?estado=resuelto" />
      </div>

      <div className="mt-6 grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_400px]">
        <div className="min-w-0 space-y-6">
          <Card>
            <CardHeader
              titulo="Alertas que requieren atención"
              descripcion="Detectadas por la vigilancia automática."
              icono={<Radar />}
              acciones={
                <Link href="/admin/soporte/vigilancia" className="text-[13px] font-medium text-primary hover:underline">
                  Ver todas
                </Link>
              }
            />
            {!alertas ? (
              <div className="space-y-3 p-5">
                <Skeleton className="h-10" />
                <Skeleton className="h-10" />
              </div>
            ) : alertas.length === 0 ? (
              <Vacio icono={<CircleCheck />} titulo="Todo en orden" descripcion="No hay alertas abiertas en este momento." />
            ) : (
              <ul className="divide-y">
                {alertas.slice(0, 6).map((a) => {
                  const enlace = enlaceInvestigar(a, monitores?.find((m) => m.id === a.monitor));
                  return (
                    <li key={a.id} className="flex items-center gap-3 px-5 py-3">
                      <span className={cn('h-8 w-1 shrink-0 rounded-full', { critica: 'bg-danger', alta: 'bg-orange', media: 'bg-warning', info: 'bg-info' }[a.severidad])} />
                      <div className="min-w-0 flex-1">
                        <p className="truncate text-[13.5px] font-medium">{a.titulo}</p>
                        <p className="truncate text-xs text-muted-foreground">
                          {SEVERIDAD[a.severidad].etiqueta} · {haceCuanto(a.ultima_vez)}
                        </p>
                      </div>
                      {enlace && (
                        <Link href={enlace} className="inline-flex shrink-0 items-center gap-1 text-[13px] font-medium text-primary hover:underline">
                          Investigar
                          <ArrowRight className="size-3.5" />
                        </Link>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}
          </Card>

          <Card>
            <CardHeader
              titulo="Tickets en curso"
              icono={<Ticket />}
              acciones={
                <Link href="/admin/soporte/tickets" className="text-[13px] font-medium text-primary hover:underline">
                  Ver tickets
                </Link>
              }
            />
            {activos.length === 0 ? (
              <Vacio titulo="No hay tickets activos" />
            ) : (
              <ul className="divide-y">
                {activos.slice(0, 6).map((t) => (
                  <li key={t.numero}>
                    <Link href={`/admin/soporte/tickets/${t.numero}`} className="group flex items-center gap-4 px-5 py-3 hover:bg-surface-muted/50">
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

        <div className="space-y-6">
          <Card>
            <CardHeader titulo="Diagnóstico rápido" descripcion="Ingrese directo al diagnóstico que necesita." icono={<ScanSearch />} />
            <div className="grid grid-cols-2 gap-2 p-3">
              {playbooks?.map((p) => (
                <Link key={p.id} href={`/admin/soporte/diagnostico/${p.id}`} className="group flex flex-col gap-2 rounded-lg border p-3 transition-all hover:border-primary/40 hover:bg-primary-soft/30">
                  <span className="grid size-8 place-items-center rounded-lg bg-primary-soft text-primary-soft-foreground [&_svg]:size-4">{ICONO_PLAYBOOK[p.id]}</span>
                  <span className="text-[13px] leading-tight font-medium">{p.nombre}</span>
                </Link>
              ))}
            </div>
          </Card>

          <Card>
            <CardHeader
              titulo="Actividad reciente"
              icono={<History />}
              acciones={
                <Link href="/admin/soporte/diagnostico" className="text-[13px] font-medium text-primary hover:underline">
                  Historial
                </Link>
              }
            />
            {!diagnosticos?.length ? (
              <p className="px-5 py-6 text-[13px] text-muted-foreground">Aún no se han ejecutado diagnósticos.</p>
            ) : (
              <ul className="divide-y">
                {diagnosticos.slice(0, 6).map((d) => (
                  <li key={d.id}>
                    <Link href={`/admin/soporte/diagnostico/historial/${d.id}`} className="block px-5 py-3 hover:bg-surface-muted/50">
                      <div className="flex items-center justify-between gap-2">
                        <span className="text-[13px] font-medium">{playbooks?.find((p) => p.id === d.playbook)?.nombre ?? d.playbook}</span>
                        <Badge tono={VEREDICTO[d.veredicto].tono} className="h-5 px-1.5 text-[11px]">
                          {VEREDICTO[d.veredicto].etiqueta}
                        </Badge>
                      </div>
                      <p className="mt-0.5 truncate text-xs text-muted-foreground">
                        {d.resumen} · {haceCuanto(d.creado_en)}
                      </p>
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </Card>
        </div>
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

function Indicador({ etiqueta, valor, detalle, acento, icono, a }: { etiqueta: string; valor?: number; detalle?: string; acento: keyof typeof ACENTOS; icono: ReactNode; a: string }) {
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
