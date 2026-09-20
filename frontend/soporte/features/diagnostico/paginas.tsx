'use client';

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowLeft, ArrowRight, History, ListChecks, ScanSearch } from 'lucide-react';
import { useEffect, useRef, type ReactNode } from 'react';
import Link from 'next/link';
import { useParams, usePathname, useRouter, useSearchParams } from 'next/navigation';
import { EncabezadoPagina } from '@/soporte/components/layout/encabezado';
import { Badge } from '@/soporte/components/ui/badge';
import { Card, CardContent, CardHeader } from '@/soporte/components/ui/card';
import { Cargando, ErrorCarga, Vacio } from '@/soporte/components/ui/estados';
import { useAvisos } from '@/soporte/components/ui/toast';
import { api } from '@/soporte/lib/api';
import { claves, usePlaybooks } from '@/soporte/lib/consultas';
import { ETIQUETA_CAMPO, VEREDICTO, haceCuanto } from '@/soporte/lib/formato';
import type { DiagnosticoResumen, PlaybookInfo } from '@/soporte/lib/types';
import { ICONO_PLAYBOOK } from '@/soporte/lib/iconos';
import { FormularioPlaybook } from './formulario';
import { ResultadoDiagnosticoVista } from './resultado';

export function PaginaDiagnosticos() {
  const { data: playbooks, isLoading, error } = usePlaybooks();
  const { data: historial } = useQuery({ queryKey: claves.diagnosticos, queryFn: () => api.diagnosticos(15) });
  const nombre = (id: string) => playbooks?.find((p) => p.id === id)?.nombre ?? id;

  return (
    <>
      <EncabezadoPagina
        titulo="Diagnósticos automáticos"
        descripcion="Cada diagnóstico ejecuta verificaciones de solo lectura sobre Oracle, aplica las reglas aprendidas de tickets anteriores y propone la corrección. Nada se modifica sin aprobación."
      />
      {error && <ErrorCarga error={error} />}
      {isLoading && <Cargando />}
      <div className="grid gap-4 md:grid-cols-2">
        {playbooks?.map((p) => (
          <Link key={p.id} href={`/soporte/diagnostico/${p.id}`} className="group">
            <Card className="h-full p-5 transition-all group-hover:-translate-y-0.5 group-hover:border-primary/40 group-hover:shadow-pop">
              <div className="flex items-start gap-4">
                <div className="grid size-11 shrink-0 place-items-center rounded-xl bg-primary-soft text-primary-soft-foreground [&_svg]:size-5">
                  {ICONO_PLAYBOOK[p.id] ?? <ScanSearch />}
                </div>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center justify-between gap-2">
                    <h3 className="font-semibold tracking-tight">{p.nombre}</h3>
                    <ArrowRight className="size-4 text-muted-foreground transition-transform group-hover:translate-x-0.5 group-hover:text-primary" />
                  </div>
                  <p className="mt-1 text-[13px] leading-relaxed text-muted-foreground">{p.descripcion}</p>
                  <div className="mt-3 flex flex-wrap items-center gap-1.5">
                    <Badge>{p.categoria}</Badge>
                    {p.campos.map((c) => (
                      <Badge key={c.clave} tono="primary">
                        {ETIQUETA_CAMPO[c.clave] ?? c.etiqueta}
                      </Badge>
                    ))}
                  </div>
                </div>
              </div>
            </Card>
          </Link>
        ))}
      </div>

      <Card className="mt-6">
        <CardHeader titulo="Historial reciente" descripcion="Últimos diagnósticos ejecutados por el equipo." icono={<History />} />
        {!historial?.length ? (
          <Vacio icono={<ListChecks />} titulo="Aún no hay diagnósticos" descripcion="Los diagnósticos ejecutados aparecerán aquí." />
        ) : (
          <TablaHistorial filas={historial} nombre={nombre} />
        )}
      </Card>
    </>
  );
}

export function TablaHistorial({ filas, nombre }: { filas: DiagnosticoResumen[]; nombre: (id: string) => string }) {
  const router = useRouter();
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="text-left text-xs text-muted-foreground">
          <tr className="border-b">
            <th className="px-5 py-2.5 font-medium">Diagnóstico</th>
            <th className="px-3 py-2.5 font-medium">Datos</th>
            <th className="px-3 py-2.5 font-medium">Resultado</th>
            <th className="px-3 py-2.5 font-medium">Ticket</th>
            <th className="px-5 py-2.5 text-right font-medium">Cuándo</th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {filas.map((d) => (
            <tr key={d.id} onClick={() => router.push(`/soporte/diagnostico/historial/${d.id}`)} className="cursor-pointer transition-colors hover:bg-surface-muted/60">
              <td className="px-5 py-3 font-medium whitespace-nowrap">{nombre(d.playbook)}</td>
              <td className="px-3 py-3 font-mono text-xs text-muted-foreground">
                {Object.values(d.entrada).join(' · ')}
              </td>
              <td className="max-w-md px-3 py-3">
                <div className="flex items-center gap-2">
                  <Badge tono={VEREDICTO[d.veredicto].tono} punto>
                    {VEREDICTO[d.veredicto].etiqueta}
                  </Badge>
                  <span className="truncate text-[13px] text-muted-foreground">{d.resumen}</span>
                </div>
              </td>
              <td className="px-3 py-3 font-mono text-xs">{d.ticket ?? <span className="text-muted-foreground">—</span>}</td>
              <td className="px-5 py-3 text-right text-xs whitespace-nowrap text-muted-foreground">{haceCuanto(d.creado_en)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function PaginaEjecutarDiagnostico() {
  const { id = '' } = useParams<{ id: string }>();
  const params = useSearchParams();
  const { data: playbooks, isLoading } = usePlaybooks();
  const playbook = playbooks?.find((p) => p.id === id);

  if (isLoading) return <Cargando />;
  if (!playbook) return <Vacio titulo="Diagnóstico no encontrado" accion={<Link href="/soporte/diagnostico" className="text-sm text-primary hover:underline">Volver a diagnósticos</Link>} />;

  const inicial = Object.fromEntries(playbook.campos.map((c) => [c.clave, params.get(c.clave) ?? '']).filter(([, v]) => v));
  return <EjecutarDiagnostico key={`${id}-${params.toString()}`} playbook={playbook} inicial={inicial} automatico={params.get('auto') === '1'} ticket={params.get('ticket') ?? undefined} />;
}

function EjecutarDiagnostico({ playbook, inicial, automatico, ticket }: { playbook: PlaybookInfo; inicial: Record<string, string>; automatico: boolean; ticket?: string }) {
  const qc = useQueryClient();
  const avisar = useAvisos();
  const ejecutar = useMutation({
    mutationFn: (entrada: Record<string, string>) => api.ejecutarPlaybook(playbook.id, entrada, ticket),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: claves.diagnosticos });
      qc.invalidateQueries({ queryKey: claves.resumen });
    },
    onError: (e) => avisar('error', 'No se pudo ejecutar el diagnóstico', (e as Error).message),
  });

  const yaEjecutado = useRef(false);
  useEffect(() => {
    if (automatico && !yaEjecutado.current) {
      yaEjecutado.current = true;
      ejecutar.mutate(inicial);
    }
  }, [automatico, inicial, ejecutar]);

  return (
    <>
      <Link href="/soporte/diagnostico" className="mb-4 inline-flex items-center gap-1.5 text-[13px] text-muted-foreground hover:text-foreground">
        <ArrowLeft className="size-3.5" />
        Diagnósticos
      </Link>
      <EncabezadoPagina titulo={playbook.nombre} descripcion={playbook.descripcion} />
      <div className="grid items-start gap-6 lg:grid-cols-[340px_minmax(0,1fr)]">
        <Card className="lg:sticky lg:top-0">
          <CardHeader titulo="Datos del caso" icono={ICONO_PLAYBOOK[playbook.id] ?? <ScanSearch />} />
          <CardContent>
            <FormularioPlaybook playbook={playbook} inicial={inicial} ejecutando={ejecutar.isPending} onEjecutar={(e) => ejecutar.mutate(e)} />
            {playbook.ticketsReferencia.length > 0 && (
              <div className="mt-5 border-t pt-4">
                <p className="text-xs font-medium text-muted-foreground">Construido a partir de</p>
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {playbook.ticketsReferencia.map((t) => (
                    <Link key={t} href={`/soporte/tickets/${t}`} className="rounded-md bg-surface-muted px-2 py-0.5 font-mono text-xs hover:bg-primary-soft hover:text-primary-soft-foreground">
                      {t}
                    </Link>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <div className="min-w-0">
          {ejecutar.data ? (
            <ResultadoDiagnosticoVista resultado={ejecutar.data} />
          ) : ejecutar.isPending ? (
            <Card>
              <Cargando texto="Ejecutando verificaciones de solo lectura…" className="py-24" />
            </Card>
          ) : (
            <Card className="border-dashed">
              <Vacio
                icono={<ScanSearch />}
                titulo="Ingrese los datos y ejecute el diagnóstico"
                descripcion="Verá cada verificación con la consulta usada, las filas devueltas, los hallazgos y la corrección sugerida."
                className="py-24"
              />
            </Card>
          )}
        </div>
      </div>
    </>
  );
}

export function PaginaResultadoGuardado() {
  const { id } = useParams<{ id: string }>();
  const { data, isLoading, error } = useQuery({ queryKey: claves.diagnostico(Number(id)), queryFn: () => api.diagnostico(Number(id)) });
  return (
    <>
      <Link href="/soporte/diagnostico" className="mb-4 inline-flex items-center gap-1.5 text-[13px] text-muted-foreground hover:text-foreground">
        <ArrowLeft className="size-3.5" />
        Diagnósticos
      </Link>
      {isLoading && <Cargando />}
      {error && <ErrorCarga error={error} />}
      {data && (
        <>
          <EncabezadoPagina
            titulo={data.nombrePlaybook}
            descripcion={`Diagnóstico #${data.id} · ${Object.entries(data.entrada).map(([k, v]) => `${ETIQUETA_CAMPO[k] ?? k} ${v}`).join(' · ')}`}
            acciones={
              <Link href={`/soporte/diagnostico/${data.playbook}?${new URLSearchParams(data.entrada)}`} className="text-[13px] font-medium text-primary hover:underline">
                Volver a ejecutar
              </Link>
            }
          />
          <ResultadoDiagnosticoVista resultado={data} />
        </>
      )}
    </>
  );
}
