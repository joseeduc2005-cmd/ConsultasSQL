'use client';

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowRight, CircleCheck, Play, Radar, RotateCcw } from 'lucide-react';
import { useState } from 'react';
import Link from 'next/link';
import { EncabezadoPagina } from '@/soporte/components/layout/encabezado';
import { Badge, Punto } from '@/soporte/components/ui/badge';
import { Button } from '@/soporte/components/ui/button';
import { Card, CardHeader } from '@/soporte/components/ui/card';
import { Cargando, ErrorCarga, Vacio } from '@/soporte/components/ui/estados';
import { Segmented } from '@/soporte/components/ui/segmented';
import { useAvisos } from '@/soporte/components/ui/toast';
import { api } from '@/soporte/lib/api';
import { claves } from '@/soporte/lib/consultas';
import { ESTADO_ALERTA, SEVERIDAD, haceCuanto, intervalo } from '@/soporte/lib/formato';
import type { Alerta, EstadoAlerta, MonitorInfo } from '@/soporte/lib/types';
import { cn } from '@/soporte/lib/utils';

export function PaginaVigilancia() {
  const [filtro, setFiltro] = useState<EstadoAlerta | 'todas'>('abierta');
  const { data: monitores, isLoading, error } = useQuery({ queryKey: claves.monitores, queryFn: api.monitores, refetchInterval: 30_000 });
  const { data: alertas } = useQuery({ queryKey: claves.alertas(), queryFn: () => api.alertas(), refetchInterval: 30_000 });

  const visibles = alertas?.filter((a) => filtro === 'todas' || a.estado === filtro) ?? [];
  const cuenta = (e: EstadoAlerta) => alertas?.filter((a) => a.estado === e).length ?? 0;
  const nombreMonitor = (id: string) => monitores?.find((m) => m.id === id);

  return (
    <>
      <EncabezadoPagina
        titulo="Vigilancia"
        descripcion="Revisiones automáticas de solo lectura que detectan problemas conocidos antes de que lleguen como ticket. Las alertas se cierran solas cuando el problema desaparece."
      />
      {error && <ErrorCarga error={error} />}
      {isLoading && <Cargando />}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {monitores?.map((m) => <TarjetaMonitor key={m.id} monitor={m} />)}
      </div>

      <Card className="mt-6">
        <CardHeader
          titulo="Alertas"
          descripcion="Ordenadas por estado y severidad."
          icono={<Radar />}
          acciones={
            <Segmented<EstadoAlerta | 'todas'>
              valor={filtro}
              onChange={setFiltro}
              opciones={[
                { valor: 'abierta', etiqueta: 'Abiertas', conteo: cuenta('abierta') },
                { valor: 'atendida', etiqueta: 'Atendidas', conteo: cuenta('atendida') },
                { valor: 'resuelta', etiqueta: 'Resueltas', conteo: cuenta('resuelta') },
                { valor: 'todas', etiqueta: 'Todas' },
              ]}
            />
          }
        />
        {visibles.length === 0 ? (
          <Vacio icono={<CircleCheck />} titulo={filtro === 'abierta' ? 'Sin alertas abiertas' : 'Sin alertas en este estado'} descripcion="La vigilancia seguirá revisando en segundo plano." />
        ) : (
          <ul className="divide-y">
            {visibles.map((a) => (
              <FilaAlerta key={a.id} alerta={a} monitor={nombreMonitor(a.monitor)} />
            ))}
          </ul>
        )}
      </Card>
    </>
  );
}

function TarjetaMonitor({ monitor: m }: { monitor: MonitorInfo }) {
  const qc = useQueryClient();
  const avisar = useAvisos();
  const ejecutar = useMutation({
    mutationFn: () => api.ejecutarMonitor(m.id),
    onSuccess: (r) => {
      qc.invalidateQueries({ queryKey: claves.monitores });
      qc.invalidateQueries({ queryKey: ['alertas'] });
      qc.invalidateQueries({ queryKey: claves.resumen });
      avisar(r.estado === 'error' ? 'error' : 'exito', m.nombre, r.estado === 'error' ? (r.detalle ?? 'Error') : `${r.hallazgos} hallazgo(s)`);
    },
    onError: (e) => avisar('error', 'No se pudo ejecutar', (e as Error).message),
  });
  const u = m.ultimaEjecucion;
  const tono = !u ? 'neutral' : u.estado === 'ok' ? 'success' : u.estado === 'alerta' ? 'orange' : 'danger';

  return (
    <Card className="flex flex-col p-5">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2">
          <Punto tono={tono} pulso={ejecutar.isPending || m.enCurso} />
          <h3 className="font-semibold tracking-tight">{m.nombre}</h3>
        </div>
        <span className="text-xs whitespace-nowrap text-muted-foreground">{intervalo(m.intervaloMin)}</span>
      </div>
      <p className="mt-2 flex-1 text-[13px] leading-relaxed text-muted-foreground">{m.descripcion}</p>
      <div className="mt-4 flex items-center justify-between gap-2 border-t pt-3">
        <div className="text-xs">
          {u ? (
            <>
              <span className={cn('font-medium', u.estado === 'error' ? 'text-danger' : u.hallazgos ? 'text-orange' : 'text-success')}>
                {u.estado === 'error' ? 'Error' : u.hallazgos ? `${u.hallazgos} hallazgo${u.hallazgos === 1 ? '' : 's'}` : 'Sin hallazgos'}
              </span>
              <span className="text-muted-foreground"> · {haceCuanto(u.creado_en)}</span>
            </>
          ) : (
            <span className="text-muted-foreground">Aún no se ejecuta</span>
          )}
        </div>
        <Button variante="borde" tamano="sm" onClick={() => ejecutar.mutate()} cargando={ejecutar.isPending}>
          {!ejecutar.isPending && <Play />}
          Revisar ahora
        </Button>
      </div>
      {u?.estado === 'error' && u.detalle && <p className="mt-2 rounded-md bg-danger-soft px-2.5 py-1.5 text-xs text-danger">{u.detalle}</p>}
    </Card>
  );
}

export function enlaceInvestigar(alerta: Alerta, monitor?: MonitorInfo): string | null {
  if (!monitor?.playbook) return null;
  const datos = Object.fromEntries(Object.entries(alerta.datos ?? {}).filter(([k]) => ['mac', 'usuario', 'cedula', 'codigo_socio'].includes(k)));
  if (Object.keys(datos).length === 0) return `/soporte/diagnostico/${monitor.playbook}`;
  return `/soporte/diagnostico/${monitor.playbook}?${new URLSearchParams({ ...datos, auto: '1' })}`;
}

function FilaAlerta({ alerta: a, monitor }: { alerta: Alerta; monitor?: MonitorInfo }) {
  const qc = useQueryClient();
  const cambiar = useMutation({
    mutationFn: (estado: EstadoAlerta) => api.cambiarAlerta(a.id, estado),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['alertas'] });
      qc.invalidateQueries({ queryKey: claves.resumen });
    },
  });
  const s = SEVERIDAD[a.severidad];
  const investigar = enlaceInvestigar(a, monitor);

  return (
    <li className="flex flex-wrap items-start gap-4 px-5 py-4">
      <Badge tono={s.tono} punto className="mt-0.5 w-24 justify-start">
        {s.etiqueta}
      </Badge>
      <div className="min-w-0 flex-1">
        <p className={cn('text-[14px] font-medium', a.estado === 'resuelta' && 'text-muted-foreground line-through decoration-muted-foreground/40')}>{a.titulo}</p>
        {a.detalle && <p className="mt-0.5 text-[13px] text-muted-foreground">{a.detalle}</p>}
        <p className="mt-1.5 text-xs text-muted-foreground">
          {monitor?.nombre ?? a.monitor} · detectada {haceCuanto(a.primera_vez)} · vista {a.veces} {a.veces === 1 ? 'vez' : 'veces'}
          {a.estado !== 'abierta' && (
            <>
              {' · '}
              <span className="font-medium">{ESTADO_ALERTA[a.estado].etiqueta}</span>
            </>
          )}
        </p>
      </div>
      <div className="flex items-center gap-2">
        {investigar && a.estado !== 'resuelta' && (
          <Link href={investigar} className="inline-flex h-8 items-center gap-1.5 rounded-lg bg-primary px-3 text-[13px] font-medium text-primary-foreground hover:bg-primary-hover">
            Investigar
            <ArrowRight className="size-3.5" />
          </Link>
        )}
        {a.estado === 'abierta' && (
          <Button variante="borde" tamano="sm" onClick={() => cambiar.mutate('atendida')} cargando={cambiar.isPending}>
            Marcar atendida
          </Button>
        )}
        {a.estado === 'atendida' && (
          <Button variante="fantasma" tamano="sm" onClick={() => cambiar.mutate('abierta')} cargando={cambiar.isPending}>
            <RotateCcw />
            Reabrir
          </Button>
        )}
      </div>
    </li>
  );
}
