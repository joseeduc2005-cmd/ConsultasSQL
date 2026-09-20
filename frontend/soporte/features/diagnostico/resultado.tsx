'use client';

import {
  ChevronDown,
  CircleAlert,
  CircleCheck,
  CircleMinus,
  CircleX,
  Clock,
  Copy,
  Database,
  ShieldAlert,
  ShieldCheck,
  TriangleAlert,
} from 'lucide-react';
import { useState, type ReactNode } from 'react';
import Link from 'next/link';
import { Badge } from '@/soporte/components/ui/badge';
import { Button } from '@/soporte/components/ui/button';
import { Card } from '@/soporte/components/ui/card';
import { CodeBlock, useCopiar } from '@/soporte/components/ui/code-block';
import { ESTADO_PASO, ETIQUETA_CAMPO, SEVERIDAD, VEREDICTO, fechaHora } from '@/soporte/lib/formato';
import type { EstadoPaso, Hallazgo, PasoDiagnostico, ResultadoDiagnostico, Severidad } from '@/soporte/lib/types';
import { cn } from '@/soporte/lib/utils';
import { RegistrarEnTicket } from './registrar-en-ticket';
import { TablaFilas } from './tabla-filas';

const BORDE_SEVERIDAD: Record<Severidad, string> = {
  critica: 'before:bg-danger',
  alta: 'before:bg-orange',
  media: 'before:bg-warning',
  info: 'before:bg-info',
};

const ICONO_PASO: Record<EstadoPaso, { icono: ReactNode; clase: string }> = {
  ok: { icono: <CircleCheck />, clase: 'text-success bg-success-soft' },
  alerta: { icono: <TriangleAlert />, clase: 'text-warning bg-warning-soft' },
  falla: { icono: <CircleX />, clase: 'text-danger bg-danger-soft' },
  omitido: { icono: <CircleMinus />, clase: 'text-muted-foreground bg-surface-muted' },
  error: { icono: <CircleAlert />, clase: 'text-danger bg-danger-soft' },
};

function textoResumen(r: ResultadoDiagnostico): string {
  const lineas = [
    `${r.nombrePlaybook} — ${VEREDICTO[r.veredicto].etiqueta}`,
    `Datos: ${Object.entries(r.entrada).map(([k, v]) => `${ETIQUETA_CAMPO[k] ?? k} ${v}`).join(', ')}`,
    '',
    r.resumen,
    ...r.hallazgos.map((h) => `- [${SEVERIDAD[h.severidad].etiqueta}] ${h.titulo}`),
    '',
    'Verificaciones:',
    ...r.pasos.map((p) => `- ${p.titulo}: ${p.detalle}`),
  ];
  return lineas.join('\n');
}

export function ResultadoDiagnosticoVista({ resultado }: { resultado: ResultadoDiagnostico }) {
  const { copiado, copiar } = useCopiar();
  const v = VEREDICTO[resultado.veredicto];
  const Icono = resultado.veredicto === 'problema' ? CircleAlert : resultado.veredicto === 'incompleto' ? TriangleAlert : CircleCheck;
  const consultas = resultado.pasos.reduce((n, p) => n + p.consultas.length, 0);

  return (
    <div className="space-y-5">
      <Card className="overflow-hidden">
        <div
          className={cn(
            'flex flex-wrap items-start gap-4 px-5 py-4',
            resultado.veredicto === 'problema' ? 'bg-danger-soft/60' : resultado.veredicto === 'incompleto' ? 'bg-warning-soft/60' : 'bg-success-soft/60',
          )}
        >
          <div
            className={cn(
              'grid size-10 shrink-0 place-items-center rounded-xl [&_svg]:size-5',
              resultado.veredicto === 'problema' ? 'bg-danger text-white' : resultado.veredicto === 'incompleto' ? 'bg-warning text-white' : 'bg-success text-white',
            )}
          >
            <Icono />
          </div>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <Badge tono={v.tono}>{v.etiqueta}</Badge>
              <span className="text-[13px] text-muted-foreground">{resultado.nombrePlaybook}</span>
            </div>
            <p className="mt-1.5 text-[15px] font-semibold">{resultado.resumen}</p>
          </div>
          <div className="flex items-center gap-2">
            <Button variante="fantasma" tamano="sm" onClick={() => copiar(textoResumen(resultado))}>
              <Copy />
              {copiado ? 'Copiado' : 'Copiar resumen'}
            </Button>
            {resultado.id !== undefined && <RegistrarEnTicket diagnosticoId={resultado.id} ticketActual={resultado.ticket} />}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 border-t px-5 py-2.5 text-[12.5px] text-muted-foreground">
          {Object.entries(resultado.entrada).map(([k, valor]) => (
            <span key={k}>
              {ETIQUETA_CAMPO[k] ?? k} <span className="font-mono text-foreground">{valor}</span>
            </span>
          ))}
          <span className="ml-auto flex items-center gap-4">
            <span className="flex items-center gap-1.5">
              <Database className="size-3.5" />
              {resultado.origenDatos === 'demo' ? 'Demostración' : 'Oracle'} · {consultas} consultas
            </span>
            <span className="flex items-center gap-1.5">
              <Clock className="size-3.5" />
              {fechaHora(resultado.inicio)} · {resultado.duracionMs} ms
            </span>
          </span>
        </div>
      </Card>

      {resultado.hallazgos.length > 0 && (
        <section className="space-y-3">
          <h3 className="text-[13px] font-semibold tracking-wide text-muted-foreground uppercase">Hallazgos</h3>
          {resultado.hallazgos.map((h, i) => (
            <HallazgoCard key={i} hallazgo={h} />
          ))}
        </section>
      )}

      <section>
        <h3 className="mb-3 text-[13px] font-semibold tracking-wide text-muted-foreground uppercase">Verificaciones</h3>
        <Card>
          <ol className="divide-y">
            {resultado.pasos.map((p, i) => (
              <PasoFila key={p.id} paso={p} ultimo={i === resultado.pasos.length - 1} />
            ))}
          </ol>
        </Card>
      </section>
    </div>
  );
}

function HallazgoCard({ hallazgo: h }: { hallazgo: Hallazgo }) {
  const s = SEVERIDAD[h.severidad];
  return (
    <Card className={cn('relative overflow-hidden p-4 pl-5 before:absolute before:inset-y-0 before:left-0 before:w-1', BORDE_SEVERIDAD[h.severidad])}>
      <div className="flex flex-wrap items-start justify-between gap-2">
        <p className="font-medium">{h.titulo}</p>
        <Badge tono={s.tono} punto>
          {s.etiqueta}
        </Badge>
      </div>
      <p className="mt-1.5 text-[13.5px] leading-relaxed whitespace-pre-line text-muted-foreground">{h.detalle}</p>

      {h.ticketsRelacionados && h.ticketsRelacionados.length > 0 && (
        <div className="mt-3 flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
          Casos relacionados:
          {h.ticketsRelacionados.map((t) => (
            <Link key={t} href={`/soporte/tickets/${t}`} className="rounded-md bg-surface-muted px-1.5 py-0.5 font-mono text-foreground hover:bg-primary-soft hover:text-primary-soft-foreground">
              {t}
            </Link>
          ))}
        </div>
      )}

      {h.acciones?.map((a, i) => (
        <div key={i} className="mt-3.5 rounded-lg border bg-surface-muted/40 p-3">
          <div className="flex flex-wrap items-center gap-2">
            {a.riesgo === 'escritura' ? (
              <Badge tono="danger">
                <ShieldAlert className="size-3.5" />
                Escritura · requiere aprobación
              </Badge>
            ) : (
              <Badge tono="success">
                <ShieldCheck className="size-3.5" />
                Solo lectura
              </Badge>
            )}
            <span className="text-xs text-muted-foreground">Responsable: {a.responsable}</span>
            {a.biblioteca && (
              <Link href={`/soporte/biblioteca?codigo=${a.biblioteca}`} className="ml-auto font-mono text-xs font-medium text-primary hover:underline">
                {a.biblioteca} en Biblioteca
              </Link>
            )}
          </div>
          <p className="mt-2 text-[13px]">{a.descripcion}</p>
          {a.sql && (
            <>
              <CodeBlock codigo={a.sql} className="mt-2.5" etiqueta={a.riesgo === 'escritura' ? 'SQL de corrección' : 'SQL'} tono={a.riesgo === 'escritura' ? 'peligro' : 'normal'} />
              {a.riesgo === 'escritura' && (
                <p className="mt-2 text-[11.5px] text-muted-foreground">
                  Este sistema no ejecuta sentencias de escritura. Revise los valores y envíe la corrección al responsable.
                </p>
              )}
            </>
          )}
        </div>
      ))}
    </Card>
  );
}

function PasoFila({ paso, ultimo }: { paso: PasoDiagnostico; ultimo: boolean }) {
  const [abierto, setAbierto] = useState(false);
  const icono = ICONO_PASO[paso.estado];
  return (
    <li className="relative">
      <button type="button" onClick={() => setAbierto((v) => !v)} className="flex w-full items-start gap-3.5 px-5 py-3.5 text-left transition-colors hover:bg-surface-muted/50" aria-expanded={abierto}>
        <span className={cn('relative z-10 mt-px grid size-7 shrink-0 place-items-center rounded-full [&_svg]:size-4', icono.clase)}>{icono.icono}</span>
        {!ultimo && <span className="absolute top-11 bottom-0 left-[33.5px] w-px bg-border" />}
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-medium">{paso.titulo}</span>
            <span className="text-[11px] text-muted-foreground">{ESTADO_PASO[paso.estado].etiqueta}</span>
          </div>
          <p className="mt-0.5 text-[13px] text-muted-foreground">{paso.detalle}</p>
        </div>
        <span className="tabular hidden shrink-0 text-xs text-muted-foreground sm:block">
          {paso.consultas.length} consulta{paso.consultas.length === 1 ? '' : 's'} · {paso.duracionMs} ms
        </span>
        <ChevronDown className={cn('mt-1 size-4 shrink-0 text-muted-foreground transition-transform', abierto && 'rotate-180')} />
      </button>
      {abierto && (
        <div className="animate-entrada space-y-4 border-t bg-surface-muted/30 px-5 py-4 pl-[62px]">
          {paso.consultas.length === 0 && <p className="text-xs text-muted-foreground">Este paso no ejecutó consultas.</p>}
          {paso.consultas.map((c, i) => (
            <div key={i} className="space-y-2">
              <div className="flex flex-wrap items-center gap-2 text-[13px]">
                <span className="font-medium">{c.descripcion}</span>
                {c.biblioteca && (
                  <Link href={`/soporte/biblioteca?codigo=${c.biblioteca}`} className="font-mono text-xs text-primary hover:underline">
                    {c.biblioteca}
                  </Link>
                )}
                <span className="tabular text-xs text-muted-foreground">
                  {c.filas.length} fila{c.filas.length === 1 ? '' : 's'} · {c.duracionMs} ms
                </span>
              </div>
              <CodeBlock codigo={c.sql} maxAlto="max-h-48" />
              {Object.keys(c.parametros).length > 0 && (
                <p className="font-mono text-[11.5px] text-muted-foreground">
                  {Object.entries(c.parametros)
                    .map(([k, v]) => `:${k} = ${String(v).slice(0, 24)}`)
                    .join('   ')}
                </p>
              )}
              <TablaFilas filas={c.filas} />
            </div>
          ))}
        </div>
      )}
    </li>
  );
}
