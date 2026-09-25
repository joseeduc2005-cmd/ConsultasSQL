'use client';

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowLeft, ArrowRight, ChevronDown, ExternalLink, FileText, ScanSearch, UserRound } from 'lucide-react';
import { useState, type ReactNode } from 'react';
import Link from 'next/link';
import { useParams } from 'next/navigation';
import { Badge } from '@/soporte/components/ui/badge';
import { Button } from '@/soporte/components/ui/button';
import { Card, CardContent, CardHeader } from '@/soporte/components/ui/card';
import { CodeBlock } from '@/soporte/components/ui/code-block';
import { Dialog } from '@/soporte/components/ui/dialog';
import { Cargando, ErrorCarga, Vacio } from '@/soporte/components/ui/estados';
import { Campo, Input, Select, Textarea } from '@/soporte/components/ui/form';
import { PrecedentesVista } from '../diagnostico/precedentes';
import { Resolucion } from './resolucion';
import { ResultadoDiagnosticoVista } from '../diagnostico/resultado';
import { useAvisos } from '@/soporte/components/ui/toast';
import { api } from '@/soporte/lib/api';
import { claves, useEstado, usePlaybooks } from '@/soporte/lib/consultas';
import { ESTADO_TICKET, fechaCorta } from '@/soporte/lib/formato';
import type {
  EstadoTicket,
  PasoTicket,
  ResultadoDiagnostico,
  Precedente,
  ResultadoExploracion,
  TicketDetalle,
} from '@/soporte/lib/types';
import { cn } from '@/soporte/lib/utils';

export function PaginaTicket() {
  const { numero = '' } = useParams<{ numero: string }>();
  const { data: t, isLoading, error } = useQuery({ queryKey: claves.ticket(numero), queryFn: () => api.ticket(numero) });

  if (isLoading) return <Cargando />;
  if (error || !t) return <ErrorCarga error={error ?? new Error('Ticket no encontrado.')} />;

  return (
    <>
      <Link href="/admin/tickets" className="mb-4 inline-flex items-center gap-1.5 text-[13px] text-muted-foreground hover:text-foreground">
        <ArrowLeft className="size-3.5" />
        Tickets
      </Link>

      <div className="mb-6 flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2 text-[13px] text-muted-foreground">
            <span className="font-mono">{t.numero}</span>
            {t.sistema && <span>· {t.sistema}</span>}
            {t.modulo && <span>· {t.modulo}</span>}
          </div>
          <h2 className="mt-1 max-w-4xl text-xl font-semibold tracking-tight">{t.titulo}</h2>
        </div>
        <div className="flex items-center gap-2">
          {t.url_sir && (
            <a href={t.url_sir} target="_blank" rel="noreferrer" className="inline-flex h-9 items-center gap-2 rounded-lg border px-3 text-[13px] font-medium shadow-card hover:bg-surface-muted">
              <ExternalLink className="size-4" />
              Abrir en SIR
            </a>
          )}
          <CambiarEstado ticket={t} />
        </div>
      </div>

      <div className="grid items-start gap-6 lg:grid-cols-[minmax(0,1fr)_320px]">
        <div className="min-w-0 space-y-6">
          <Card>
            <CardContent className="space-y-5">
              <Seccion titulo="Descripción" texto={t.descripcion} />

              {t.observaciones && <Seccion titulo="Observaciones" texto={t.observaciones} />}
            </CardContent>
          </Card>

          <Resolucion ticket={t} />

          <Investigacion ticket={t} />
        </div>

        <aside className="space-y-6 lg:sticky lg:top-0">
          <Card>
            <CardContent className="space-y-3 text-[13px]">
              <Dato etiqueta="Estado">
                <Badge tono={ESTADO_TICKET[t.estado].tono} punto>
                  {ESTADO_TICKET[t.estado].etiqueta}
                </Badge>
              </Dato>
              <Dato etiqueta="Prioridad">{t.prioridad ?? '—'}</Dato>
              <Dato etiqueta="Base de datos">
                {t.base_datos ? <span className="font-mono text-[12.5px]">{t.base_datos}</span> : '—'}
              </Dato>
              <Dato etiqueta="Categoría">{t.categoria ?? '—'}</Dato>
              <Dato etiqueta="Autor">
                <span className="inline-flex items-center gap-1.5">
                  <UserRound className="size-3.5 text-muted-foreground" />
                  {t.autor ?? '—'}
                </span>
              </Dato>
              <Dato etiqueta="Creado">{fechaCorta(t.creado_en)}</Dato>
              <Dato etiqueta="Resuelto">{fechaCorta(t.resuelto_en)}</Dato>
            </CardContent>
          </Card>

          <DiagnosticarDesdeTicket numero={t.numero} base={t.base_datos} />

          <Card>
            <CardHeader titulo="Casos similares" />
            {t.similares.length === 0 ? (
              <p className="px-5 py-4 text-[13px] text-muted-foreground">Sin casos parecidos.</p>
            ) : (
              <ul className="divide-y">
                {t.similares.map((s) => (
                  <li key={s.numero}>
                    <Link href={`/admin/tickets/${s.numero}`} className="group block px-5 py-3 hover:bg-surface-muted/60">
                      <p className="line-clamp-2 text-[13px] font-medium group-hover:text-primary">{s.titulo}</p>
                      <p className="mt-0.5 font-mono text-xs text-muted-foreground">{s.numero}</p>
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </Card>
        </aside>
      </div>
    </>
  );
}

function Seccion({ titulo, texto, vacio }: { titulo: string; texto: string | null; vacio?: string }) {
  if (!texto && !vacio) return null;
  return (
    <section>
      <h3 className="mb-1.5 text-xs font-semibold tracking-wide text-muted-foreground uppercase">{titulo}</h3>
      {texto ? <p className="text-[14px] leading-relaxed whitespace-pre-line">{texto}</p> : <p className="text-[13.5px] text-muted-foreground italic">{vacio}</p>}
    </section>
  );
}

function Dato({ etiqueta, children }: { etiqueta: string; children: ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-muted-foreground">{etiqueta}</span>
      <span className="text-right font-medium">{children}</span>
    </div>
  );
}

function CambiarEstado({ ticket }: { ticket: TicketDetalle }) {
  const qc = useQueryClient();
  const avisar = useAvisos();
  const mutar = useMutation({
    mutationFn: (estado: EstadoTicket) => api.actualizarTicket(ticket.numero, { estado }),
    onSuccess: (t) => {
      qc.invalidateQueries({ queryKey: claves.ticket(ticket.numero) });
      qc.invalidateQueries({ queryKey: ['tickets'] });
      qc.invalidateQueries({ queryKey: claves.resumen });
      avisar('exito', `Estado actualizado: ${ESTADO_TICKET[t.estado].etiqueta}`);
    },
    onError: (e) => avisar('error', 'No se pudo actualizar', (e as Error).message),
  });
  return (
    <div className="w-44">
      <Select value={ticket.estado} onChange={(e) => mutar.mutate(e.target.value as EstadoTicket)} disabled={mutar.isPending} aria-label="Estado del ticket">
        {(Object.keys(ESTADO_TICKET) as EstadoTicket[]).map((e) => (
          <option key={e} value={e}>
            {ESTADO_TICKET[e].etiqueta}
          </option>
        ))}
      </Select>
    </div>
  );
}

function DiagnosticarDesdeTicket({ numero, base }: { numero: string; base: string | null }) {
  const { data: playbooks } = usePlaybooks();
  const { data: estado } = useEstado();

  // Los cuatro diagnósticos tienen SQL escrito contra el esquema de Corresponsales:
  // ofrecerlos en un ticket de otra base sería prometer algo que no funciona.
  const baseDiagnosticos = estado?.baseDiagnosticos ?? null;
  const otraBase = Boolean(base && baseDiagnosticos && base !== baseDiagnosticos);

  if (otraBase) {
    return (
      <Card>
        <CardHeader titulo="Diagnosticar" icono={<ScanSearch />} />
        <p className="px-5 py-4 text-[13px] text-muted-foreground">
          Este caso se investiga contra <span className="font-mono">{base}</span>, y los diagnósticos
          automáticos están escritos contra <span className="font-mono">{baseDiagnosticos}</span>. Use la
          Biblioteca SQL o el panel de consultas para esta base.
        </p>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader titulo="Diagnosticar" descripcion="El resultado se puede registrar como paso de este ticket." icono={<ScanSearch />} />
      <ul className="divide-y">
        {playbooks?.map((p) => (
          <li key={p.id}>
            <Link href={`/admin/diagnostico/${p.id}?ticket=${encodeURIComponent(numero)}`} className="group flex items-center justify-between gap-2 px-5 py-2.5 text-[13px] hover:bg-surface-muted/60">
              {p.nombre}
              <ArrowRight className="size-3.5 text-muted-foreground transition-transform group-hover:translate-x-0.5 group-hover:text-primary" />
            </Link>
          </li>
        ))}
      </ul>
    </Card>
  );
}

function Investigacion({ ticket }: { ticket: TicketDetalle }) {
  const qc = useQueryClient();
  const avisar = useAvisos();
  const [revision, setRevision] = useState<{
    ejecutado: boolean;
    motivo?: string;
    resultado?: ResultadoDiagnostico;
    exploracion?: ResultadoExploracion;
    precedentes?: Precedente[];
  } | null>(null);

  // Un solo botón: el backend deduce qué comprobar del texto del ticket, lo ejecuta
  // y deja el paso registrado. El resultado se muestra aquí mismo.
  // Sin base configurada el botón no tiene contra qué investigar: se apaga y lo dice,
  // en vez de dejar que alguien lo pulse para recibir un error.
  const { data: estado } = useQuery({ queryKey: ['estado'], queryFn: api.estado, staleTime: 60_000 });
  const sinBase = estado !== undefined && !estado.baseDiagnosticos;

  const revisar = useMutation({
    mutationFn: () => api.revisarTicket(ticket.numero),
    onSuccess: (r) => {
      setRevision(r);
      qc.invalidateQueries({ queryKey: claves.ticket(ticket.numero) });
      qc.invalidateQueries({ queryKey: ['tickets'] });
      if (r.ejecutado) avisar('exito', 'Revisión terminada', r.resultado?.resumen ?? '');
      else avisar('error', 'No se pudo revisar', r.motivo ?? '');
    },
    onError: (e) => avisar('error', 'No se pudo revisar', (e as Error).message),
  });

  return (
    <Card>
      <CardHeader
        titulo="Investigación"
        descripcion={`${ticket.pasos.length} paso${ticket.pasos.length === 1 ? '' : 's'} registrados, en orden cronológico.`}
        icono={<FileText />}
        acciones={
          <Button
            tamano="sm"
            onClick={() => revisar.mutate()}
            cargando={revisar.isPending}
            title={sinBase ? 'Sin base conectada: buscará casos parecidos en la base de conocimiento' : undefined}
          >
            {!revisar.isPending && <ScanSearch />}
            Revisar error
          </Button>
        }
      />

      {sinBase && (
        <p className="mx-5 mb-4 rounded-lg border px-4 py-3 text-[13px] text-muted-foreground">
          Sin base de datos conectada, «Revisar error» busca casos parecidos ya resueltos en la base de
          conocimiento. Cuando se configure la conexión, además comprobará el caso en Oracle.
        </p>
      )}

      {revision && !revision.ejecutado && (
        <p className="mx-5 mb-4 rounded-lg border border-warning/25 bg-warning-soft/60 px-4 py-3 text-[13px]">
          {revision.motivo}
        </p>
      )}
      {revision?.resultado && (
        <div className="border-b px-5 pb-5">
          <ResultadoDiagnosticoVista resultado={revision.resultado} exploracion={revision.exploracion} />
        </div>
      )}

      {revision?.precedentes && revision.precedentes.length > 0 && (
        <div className="border-b px-5 pb-5">
          <PrecedentesVista precedentes={revision.precedentes} />
        </div>
      )}

      {ticket.pasos.length === 0 && !revision?.resultado ? (
        <Vacio
          icono={<ScanSearch />}
          titulo="Sin pasos de investigación"
          descripcion="Pulse «Revisar error» para que el sistema compruebe el caso, o agregue un paso manual."
        />
      ) : ticket.pasos.length === 0 ? null : (
        <ol className="px-5 py-2">
          {ticket.pasos.map((p, i) => (
            <PasoItem key={p.id} paso={p} ultimo={i === ticket.pasos.length - 1} />
          ))}
        </ol>
      )}
    </Card>
  );
}

const ORIGEN: Record<PasoTicket['origen'], { etiqueta: string; tono: 'primary' | 'neutral' | 'info' }> = {
  diagnostico: { etiqueta: 'Diagnóstico automático', tono: 'primary' },
  manual: { etiqueta: 'Manual', tono: 'neutral' },
  importado: { etiqueta: 'Importado', tono: 'info' },
};

function PasoItem({ paso, ultimo }: { paso: PasoTicket; ultimo: boolean }) {
  const [abierto, setAbierto] = useState(ultimo);
  const tieneDetalle = Boolean(paso.resultado || paso.sql || paso.evidencia || paso.conclusion);
  return (
    <li className="relative flex gap-4 py-3">
      <div className="relative flex flex-col items-center">
        <span className={cn('tabular z-10 grid size-7 place-items-center rounded-full text-xs font-semibold', paso.origen === 'diagnostico' ? 'bg-primary text-primary-foreground' : 'bg-surface-muted text-foreground ring-1 ring-border')}>
          {paso.numero}
        </span>
        {!ultimo && <span className="absolute top-8 -bottom-3 w-px bg-border" />}
      </div>
      <div className="min-w-0 flex-1 pb-1">
        <button type="button" onClick={() => setAbierto((v) => !v)} className="flex w-full items-start justify-between gap-3 text-left" disabled={!tieneDetalle}>
          <div className="min-w-0">
            <p className="text-[14px] leading-snug font-medium">{paso.titulo}</p>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
              {paso.fecha && <span>{paso.fecha}</span>}
              <Badge tono={ORIGEN[paso.origen].tono} className="h-5 px-1.5 text-[11px]">
                {ORIGEN[paso.origen].etiqueta}
              </Badge>
              {paso.diagnostico_id && (
                <Link href={`/admin/diagnostico/historial/${paso.diagnostico_id}`} onClick={(e) => e.stopPropagation()} className="text-primary hover:underline">
                  Ver diagnóstico
                </Link>
              )}
            </div>
          </div>
          {tieneDetalle && <ChevronDown className={cn('mt-1 size-4 shrink-0 text-muted-foreground transition-transform', abierto && 'rotate-180')} />}
        </button>
        {abierto && tieneDetalle && (
          <div className="mt-3 animate-entrada space-y-3">
            {paso.resultado && <Bloque etiqueta="Resultado" texto={paso.resultado} />}
            {paso.sql && <CodeBlock codigo={paso.sql} maxAlto="max-h-72" />}
            {paso.evidencia && <Bloque etiqueta="Evidencia" texto={paso.evidencia} />}
            {paso.conclusion && (
              <div className="rounded-lg border-l-2 border-primary bg-primary-soft/40 px-3 py-2">
                <p className="text-[11px] font-semibold tracking-wide text-primary-soft-foreground uppercase">Conclusión</p>
                <p className="mt-0.5 text-[13.5px] leading-relaxed whitespace-pre-line">{paso.conclusion}</p>
              </div>
            )}
          </div>
        )}
      </div>
    </li>
  );
}

function Bloque({ etiqueta, texto }: { etiqueta: string; texto: string }) {
  return (
    <div>
      <p className="text-[11px] font-semibold tracking-wide text-muted-foreground uppercase">{etiqueta}</p>
      <p className="mt-0.5 text-[13.5px] leading-relaxed whitespace-pre-line">{texto}</p>
    </div>
  );
}

