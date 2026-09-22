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
  BookPlus,
  Sparkle,
} from 'lucide-react';
import { useMutation } from '@tanstack/react-query';
import { useMemo, useState, type ReactNode } from 'react';
import Link from 'next/link';
import { api } from '@/soporte/lib/api';
import { Badge } from '@/soporte/components/ui/badge';
import { Button } from '@/soporte/components/ui/button';
import { Card } from '@/soporte/components/ui/card';
import { Dialog } from '@/soporte/components/ui/dialog';
import { CodeBlock, useCopiar } from '@/soporte/components/ui/code-block';
import { useAvisos } from '@/soporte/components/ui/toast';
import { ESTADO_PASO, ETIQUETA_CAMPO, SEVERIDAD, VEREDICTO, fechaHora } from '@/soporte/lib/formato';
import type {
  EstadoPaso,
  Hallazgo,
  PasoDiagnostico,
  ResultadoDiagnostico,
  PropuestaExploracion,
  ResultadoExploracion,
  Severidad,
  SolucionPublicada,
} from '@/soporte/lib/types';
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

export function ResultadoDiagnosticoVista({
  resultado,
  exploracion,
}: {
  resultado: ResultadoDiagnostico;
  exploracion?: ResultadoExploracion;
}) {
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
            {resultado.id !== undefined && resultado.hallazgos.length > 0 && (
              <PublicarSolucion resultado={resultado} exploracion={exploracion?.propuesta} />
            )}
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

      {exploracion?.propuesta && <PropuestaExploracionVista propuesta={exploracion.propuesta} />}

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
            <Link key={t} href={`/admin/tickets/${t}`} className="rounded-md bg-surface-muted px-1.5 py-0.5 font-mono text-foreground hover:bg-primary-soft hover:text-primary-soft-foreground">
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
              <Link href={`/admin/biblioteca?codigo=${a.biblioteca}`} className="ml-auto font-mono text-xs font-medium text-primary hover:underline">
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
                  <Link href={`/admin/biblioteca?codigo=${c.biblioteca}`} className="font-mono text-xs text-primary hover:underline">
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

/**
 * La propuesta que sale de comparar al usuario contra los que si estan operando.
 *
 * Va arriba de los hallazgos a proposito: cuando existe, es lo primero que hay que
 * mirar, y cuando ningun playbook reconocio el caso es lo unico que hay.
 *
 * Es una sola sentencia aunque las diferencias sean varias, porque todas las columnas
 * viven en la misma fila. Eso es lo que evita entregar cinco SQL para probar de uno
 * en uno.
 */
export function PropuestaExploracionVista({ propuesta: p }: { propuesta: PropuestaExploracion }) {
  const { copiado, copiar } = useCopiar();
  const porcentaje = Math.round(p.confianza * 100);
  const tono = porcentaje >= 90 ? 'success' : porcentaje >= 75 ? 'warning' : 'danger';

  return (
    <Card className="overflow-hidden border-primary/30">
      <div className="flex flex-wrap items-start gap-4 bg-primary-soft/40 px-5 py-4">
        <div className="grid size-10 shrink-0 place-items-center rounded-xl bg-primary text-white [&_svg]:size-5">
          <Sparkle />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <Badge tono="primary">Propuesta del sistema</Badge>
            <Badge tono={tono} punto>
              {porcentaje} % de confianza
            </Badge>
          </div>
          <p className="mt-1.5 text-[15px] font-semibold">{p.resumen}</p>
          <p className="mt-0.5 text-[12.5px] text-muted-foreground">
            Comparado contra {p.poblacion} usuarios que operaron en los ultimos {p.dias} dias.
          </p>
        </div>
        <Button variante="fantasma" tamano="sm" onClick={() => copiar(p.sql)}>
          <Copy />
          {copiado ? 'Copiado' : 'Copiar SQL'}
        </Button>
      </div>

      <div className="space-y-4 px-5 py-4">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-[13px]">
            <thead>
              <tr className="text-[11.5px] tracking-wide text-muted-foreground uppercase">
                <th className="pb-1.5 font-medium">Columna</th>
                <th className="pb-1.5 font-medium">Tiene</th>
                <th className="pb-1.5 font-medium">Lo normal</th>
                <th className="pb-1.5 font-medium">Cuota</th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {p.anomalias.map((a) => (
                <tr key={a.columna}>
                  <td className="py-1.5 font-mono text-[12.5px]">{a.columna}</td>
                  <td className="py-1.5 font-mono text-danger">{a.valorActual}</td>
                  <td className="py-1.5 font-mono text-success">{a.valorSano}</td>
                  <td className="tabular py-1.5 text-muted-foreground">{Math.round(a.cuota * 100)} %</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <CodeBlock codigo={p.sql} etiqueta="Correccion propuesta" tono="peligro" />

        <p className="text-[11.5px] leading-relaxed text-muted-foreground">
          Esta sentencia no viene del catalogo de correcciones aprobadas: la redacto el sistema comparando este caso
          con la poblacion. Afecta a una sola fila, por clave primaria. Confirmela antes de aplicarla; publicarla la
          deja lista en categorias, no la ejecuta.
        </p>
      </div>
    </Card>
  );
}

interface CorreccionElegible {
  id: string;
  descripcion: string;
  hallazgo: string;
  severidad: Severidad;
  parametros: Record<string, string | number | null>;
}

/**
 * Correcciones de escritura que propone el diagnóstico, sin repetir.
 *
 * Es la misma lista que arma el backend al publicar; aquí se deriva de los hallazgos
 * que ya están en pantalla para no pedirla otra vez. Vienen en orden de severidad,
 * porque el motor ordena los hallazgos antes de devolverlos.
 */
function correccionesElegibles(r: ResultadoDiagnostico): CorreccionElegible[] {
  const vistas = new Set<string>();
  const salida: CorreccionElegible[] = [];
  for (const h of r.hallazgos) {
    for (const a of h.acciones ?? []) {
      if (a.riesgo !== 'escritura' || !a.correccion || vistas.has(a.correccion)) continue;
      vistas.add(a.correccion);
      salida.push({
        id: a.correccion,
        descripcion: a.descripcion,
        hallazgo: h.titulo,
        severidad: h.severidad,
        parametros: a.parametros ?? {},
      });
    }
  }
  return salida;
}

/**
 * Publica el diagnostico en el arbol de categorias: un articulo con el caso y uno
 * ejecutable por cada correccion que se marque.
 *
 * El selector existe porque un diagnostico puede proponer varias correcciones y no
 * todas valen para el caso. La propuesta de la exploracion va aparte y marcada,
 * porque nadie la reviso antes de que el sistema la redactara.
 *
 * Marcar no ejecuta nada: solo deja los articulos listos con su formulario.
 */
function PublicarSolucion({
  resultado,
  exploracion,
}: {
  resultado: ResultadoDiagnostico;
  exploracion?: PropuestaExploracion;
}) {
  const avisar = useAvisos();
  const diagnosticoId = resultado.id!;
  const elegibles = useMemo(() => correccionesElegibles(resultado), [resultado]);
  const [abierto, setAbierto] = useState(false);
  const [marcadas, setMarcadas] = useState<string[]>(() => elegibles.map((c) => c.id));
  // Si el catalogo cubre el caso, sus correcciones mandan y la explorada queda
  // desmarcada. Si no cubre nada, la explorada es lo unico que hay.
  const [conExploracion, setConExploracion] = useState(() => elegibles.length === 0);
  const [publicado, setPublicado] = useState<SolucionPublicada | null>(null);

  const publicar = useMutation({
    mutationFn: (v: { ids?: string[]; exploracion?: boolean }) =>
      api.publicarSolucion(diagnosticoId, v.ids, v.exploracion),
    onSuccess: (r) => {
      setPublicado(r);
      setAbierto(false);
      const n = r.scripts.length;
      avisar(
        'exito',
        r.articulo.creado ? 'Solucion publicada' : 'Solucion actualizada',
        n === 0
          ? r.articulo.titulo
          : `${r.articulo.titulo} · ${n} correccion${n === 1 ? '' : 'es'} lista${n === 1 ? '' : 's'} para ejecutar`,
      );
    },
    onError: (e) => avisar('error', 'No se pudo publicar', (e as Error).message),
  });

  if (publicado) {
    return (
      <Link
        href={`/admin?articulo=${encodeURIComponent(publicado.articulo.id)}`}
        className="text-[13px] font-medium text-primary hover:underline"
      >
        Ver en categorias
      </Link>
    );
  }

  // Sin nada que elegir se publica el caso y ya.
  if (elegibles.length === 0 && !exploracion) {
    return (
      <Button variante="fantasma" tamano="sm" onClick={() => publicar.mutate({})} cargando={publicar.isPending}>
        <BookPlus />
        Publicar como solucion
      </Button>
    );
  }

  const alternar = (id: string) =>
    setMarcadas((previas) => (previas.includes(id) ? previas.filter((x) => x !== id) : [...previas, id]));
  const total = marcadas.length + (conExploracion ? 1 : 0);

  return (
    <>
      <Button variante="fantasma" tamano="sm" onClick={() => setAbierto(true)}>
        <BookPlus />
        Publicar como solucion
      </Button>

      <Dialog
        abierto={abierto}
        onCerrar={() => setAbierto(false)}
        titulo="Publicar la solucion"
        descripcion="Marque las correcciones que valen para este caso: cada una queda como articulo ejecutable en categorias."
        ancho="max-w-2xl"
        pie={
          <>
            <Button variante="fantasma" tamano="sm" onClick={() => setAbierto(false)}>
              Cancelar
            </Button>
            <Button
              tamano="sm"
              onClick={() => publicar.mutate({ ids: marcadas, exploracion: conExploracion })}
              cargando={publicar.isPending}
            >
              <BookPlus />
              Publicar {total > 0 && `(${total})`}
            </Button>
          </>
        }
      >
        <ul className="space-y-2">
          {exploracion && (
            <li>
              <label
                className={cn(
                  'flex cursor-pointer gap-3 rounded-lg border p-3 transition-colors',
                  conExploracion ? 'border-primary/40 bg-primary-soft/30' : 'hover:bg-surface-muted/50',
                )}
              >
                <input
                  type="checkbox"
                  checked={conExploracion}
                  onChange={() => setConExploracion((v) => !v)}
                  className="mt-0.5 size-4 shrink-0 accent-[var(--color-primary)]"
                />
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-[13.5px] font-medium">{exploracion.resumen}</span>
                    <Badge tono="primary">
                      <Sparkle className="size-3.5" />
                      Propuesta del sistema
                    </Badge>
                  </div>
                  <p className="mt-1 text-[12.5px] text-muted-foreground">
                    {Math.round(exploracion.confianza * 100)} % de confianza · corrige{' '}
                    {exploracion.anomalias.length} columna{exploracion.anomalias.length === 1 ? '' : 's'} en una sola
                    sentencia. No viene del catalogo aprobado.
                  </p>
                </div>
              </label>
            </li>
          )}

          {elegibles.map((c) => {
            const marcada = marcadas.includes(c.id);
            const sev = SEVERIDAD[c.severidad];
            return (
              <li key={c.id}>
                <label
                  className={cn(
                    'flex cursor-pointer gap-3 rounded-lg border p-3 transition-colors',
                    marcada ? 'border-primary/40 bg-primary-soft/30' : 'hover:bg-surface-muted/50',
                  )}
                >
                  <input
                    type="checkbox"
                    checked={marcada}
                    onChange={() => alternar(c.id)}
                    className="mt-0.5 size-4 shrink-0 accent-[var(--color-primary)]"
                  />
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-[13.5px] font-medium">{c.descripcion}</span>
                      <Badge tono={sev.tono} punto>
                        {sev.etiqueta}
                      </Badge>
                    </div>
                    <p className="mt-1 text-[12.5px] text-muted-foreground">
                      Hallazgo: {c.hallazgo}
                      {Object.keys(c.parametros).length > 0 && (
                        <span className="ml-2 font-mono">
                          {Object.entries(c.parametros)
                            .map(([k, v]) => `${k} = ${v ?? '?'}`)
                            .join('  ·  ')}
                        </span>
                      )}
                    </p>
                  </div>
                </label>
              </li>
            );
          })}
        </ul>
        <p className="mt-3 text-[12px] text-muted-foreground">
          El articulo guarda la plantilla con sus marcadores, no el SQL ya resuelto: los valores del caso quedan como
          sugerencia del formulario y la correccion sirve para el proximo. Publicar no ejecuta nada.
        </p>
      </Dialog>
    </>
  );
}
