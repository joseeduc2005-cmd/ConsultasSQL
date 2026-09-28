'use client';

import { BookOpen, CheckCircle2, Clock, History, Search, Send } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { withAuth } from '../components/ProtectedRoute';
import ThemeToggle from '../components/ThemeToggle';
import UserIdentityBadge from '../components/UserIdentityBadge';
import { useTheme } from '../components/ThemeProvider';
import { clienteApi, type CasoPropio, type PrecedentePublico } from './api';

/**
 * Lo que ve quien tiene el problema, no quien lo resuelve.
 *
 * El orden importa: primero se busca, y solo si no aparece nada se reporta. Buena
 * parte de lo que llega a soporte ya está resuelto en algún sitio, y hacer que la
 * persona lo encuentre sola es más rápido para ella y más barato para el equipo.
 *
 * Deliberadamente no hay barra lateral ni secciones: una caja, un botón, y los casos
 * propios abajo. Quien entra aquí viene con un problema, no a navegar.
 */
function SoportePage() {
  const [texto, setTexto] = useState('');
  const [resultados, setResultados] = useState<PrecedentePublico[] | null>(null);
  const [reportado, setReportado] = useState<CasoPropio | null>(null);
  const qc = useQueryClient();

  const { data: misCasos } = useQuery({ queryKey: ['mis-casos'], queryFn: clienteApi.misCasos });

  const buscar = useMutation({
    mutationFn: () => clienteApi.solucionar(texto),
    onSuccess: (r) => setResultados(r.precedentes),
  });

  const reportar = useMutation({
    mutationFn: () => clienteApi.reportar({ titulo: primeraLinea(texto), descripcion: texto }),
    onSuccess: (caso) => {
      setReportado(caso);
      setResultados(null);
      qc.invalidateQueries({ queryKey: ['mis-casos'] });
    },
  });

  const listo = texto.trim().length >= 10;

  const empezarDeNuevo = () => {
    setTexto('');
    setResultados(null);
    setReportado(null);
  };

  return (
    <div className="app-shell min-h-screen">
      <div className="mx-auto w-full max-w-3xl px-4 py-6 sm:px-6">
        <Cabecera />

        {reportado ? (
          <Confirmacion caso={reportado} onNuevo={empezarDeNuevo} />
        ) : (
          <>
            <section className="mt-6">
              <h1 className="text-xl font-semibold tracking-tight text-(--ink-900)">¿Qué problema tiene?</h1>
              <p className="mt-1 text-[13.5px] text-(--ink-700)">
                Descríbalo con sus palabras. Primero buscamos si ya se resolvió antes.
              </p>

              <textarea
                value={texto}
                onChange={(e) => setTexto(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && listo) buscar.mutate();
                }}
                rows={5}
                autoFocus
                placeholder="Por ejemplo: entro al módulo de aprobaciones y la tabla se queda vacía, no carga nada desde ayer."
                className="mt-4 w-full resize-none rounded-xl border border-(--line) bg-white/80 px-4 py-3.5 text-[15px] leading-relaxed outline-none focus:border-(--accent)"
              />

              <div className="mt-3 flex flex-wrap items-center gap-3">
                <button
                  type="button"
                  onClick={() => buscar.mutate()}
                  disabled={!listo || buscar.isPending}
                  className="inline-flex h-10 items-center gap-2 rounded-lg bg-(--accent) px-4 text-sm font-semibold text-white transition hover:opacity-90 disabled:opacity-40"
                >
                  <Search className="size-4" />
                  {buscar.isPending ? 'Buscando…' : 'Buscar solución'}
                </button>
                <span className="text-[12px] text-(--ink-500)">
                  {listo ? 'Ctrl + Enter para buscar' : 'Escriba un poco más'}
                </span>
              </div>
            </section>

            {resultados !== null && (
              <section className="mt-8">
                {resultados.length > 0 ? (
                  <>
                    <h2 className="text-[13px] font-semibold tracking-wide text-(--ink-500) uppercase">
                      Esto se parece a lo suyo
                    </h2>
                    <div className="mt-3 space-y-2">
                      {resultados.map((p) => (
                        <Resultado key={`${p.tipo}-${p.id}`} precedente={p} />
                      ))}
                    </div>
                  </>
                ) : (
                  <p className="rounded-xl border border-(--line) bg-white/70 px-4 py-5 text-center text-[14px] text-(--ink-700)">
                    No encontramos nada parecido todavía.
                  </p>
                )}

                {/* El reporte va SIEMPRE después de buscar, también cuando hubo
                    resultados: que aparezcan no significa que alguno sirva. */}
                <div className="mt-5 rounded-xl border border-(--line) bg-white/70 px-4 py-4">
                  <p className="text-[13.5px] font-medium text-(--ink-900)">
                    {resultados.length > 0 ? '¿Ninguna le sirve?' : '¿Lo reportamos al equipo?'}
                  </p>
                  <p className="mt-0.5 text-[12.5px] text-(--ink-700)">
                    Se crea un caso con lo que escribió y soporte lo revisa. Verá la respuesta aquí mismo.
                  </p>
                  <button
                    type="button"
                    onClick={() => reportar.mutate()}
                    disabled={reportar.isPending}
                    className="mt-3 inline-flex h-9 items-center gap-2 rounded-lg border border-(--line) bg-white px-3.5 text-[13px] font-semibold text-(--ink-900) transition hover:bg-white disabled:opacity-40"
                  >
                    <Send className="size-4" />
                    {reportar.isPending ? 'Enviando…' : 'Reportar el problema'}
                  </button>
                  {reportar.isError && (
                    <p className="mt-2 text-[12.5px] text-rose-600">
                      No se pudo enviar: {(reportar.error as Error).message}
                    </p>
                  )}
                </div>
              </section>
            )}
          </>
        )}

        {misCasos && misCasos.length > 0 && <MisCasos casos={misCasos} />}
      </div>
    </div>
  );
}

/**
 * Cabecera propia, más liviana que la del panel.
 *
 * No hereda la del administrador porque aquella trae acciones que aquí no existen
 * —subir artículos, ir al panel SQL— y una barra con botones que no llevan a ningún
 * sitio es peor que no tenerla.
 */
function Cabecera() {
  const { theme } = useTheme();
  const claro = theme === 'light';
  const usuario = leerUsuario();

  const salir = () => {
    try {
      localStorage.removeItem('token');
      localStorage.removeItem('user');
      localStorage.removeItem('userRole');
    } catch {
      /* almacenamiento no disponible */
    }
    document.cookie = 'token=; Path=/; Max-Age=0; SameSite=Lax';
    window.location.href = '/login';
  };

  return (
    <div className="glass-panel-strong flex items-center gap-4 rounded-2xl border px-4 py-3 sm:px-5">
      <div className="flex size-10 shrink-0 items-center justify-center rounded-xl bg-linear-to-br from-[#2363eb] to-[#1aa0c8] text-[15px] font-bold text-white shadow-sm">
        S
      </div>
      <div className="min-w-0">
        <p className="truncate text-[16px] leading-tight font-semibold tracking-tight text-(--ink-900)">
          Soporte
        </p>
        <p className="truncate text-[12px] leading-tight text-(--ink-700)">
          Busque su problema o repórtelo al equipo.
        </p>
      </div>
      <div className="ml-auto flex shrink-0 items-center gap-2">
        <ThemeToggle />
        <span className="mx-0.5 hidden h-6 w-px bg-(--line) sm:block" />
        <UserIdentityBadge
          username={usuario?.username ?? 'usuario'}
          role={usuario?.role ?? 'user'}
          isLightTheme={claro}
          onLogout={salir}
        />
      </div>
    </div>
  );
}

function leerUsuario(): { username?: string; role?: string } | null {
  if (typeof window === 'undefined') return null;
  try {
    return JSON.parse(localStorage.getItem('user') ?? 'null');
  } catch {
    return null;
  }
}

/** El título del caso sale de la primera frase, para no pedir dos veces lo mismo. */
function primeraLinea(texto: string): string {
  const limpio = texto.trim().replace(/\s+/g, ' ');
  const corte = limpio.search(/[.\n]/);
  const titulo = corte > 15 ? limpio.slice(0, corte) : limpio;
  return titulo.length > 120 ? `${titulo.slice(0, 117)}…` : titulo;
}

function Resultado({ precedente: p }: { precedente: PrecedentePublico }) {
  const [abierto, setAbierto] = useState(false);
  const cuerpo = p.solucion || p.detalle;

  return (
    <div className="rounded-xl border border-(--line) bg-white/80">
      <button
        type="button"
        onClick={() => setAbierto((v) => !v)}
        className="flex w-full items-start gap-3 px-4 py-3 text-left"
        aria-expanded={abierto}
      >
        <span className="mt-0.5 grid size-8 shrink-0 place-items-center rounded-lg bg-(--accent)/10 text-(--accent)">
          {p.tipo === 'ticket' ? <History className="size-4" /> : <BookOpen className="size-4" />}
        </span>
        <span className="min-w-0 flex-1">
          <span className="block text-[14px] font-medium text-(--ink-900)">{p.titulo}</span>
          {!abierto && cuerpo && (
            <span className="mt-0.5 line-clamp-2 block text-[12.5px] text-(--ink-700)">{cuerpo}</span>
          )}
        </span>
      </button>

      {abierto && (
        <div className="border-t border-(--line) px-4 py-3">
          {cuerpo ? (
            <p className="text-[13.5px] leading-relaxed whitespace-pre-line text-(--ink-800)">{cuerpo}</p>
          ) : (
            <p className="text-[13px] text-(--ink-700)">
              Este caso no dejó una explicación escrita. Repórtelo y soporte lo revisa.
            </p>
          )}
        </div>
      )}
    </div>
  );
}

function Confirmacion({ caso, onNuevo }: { caso: CasoPropio; onNuevo: () => void }) {
  return (
    <section className="mt-8 rounded-xl border border-(--line) bg-white/80 px-5 py-6 text-center">
      <CheckCircle2 className="mx-auto size-8 text-emerald-600" />
      <h1 className="mt-3 text-lg font-semibold text-(--ink-900)">Su caso quedó registrado</h1>
      <p className="mt-1 font-mono text-[13px] text-(--ink-700)">{caso.numero}</p>
      <p className="mx-auto mt-3 max-w-md text-[13.5px] text-(--ink-700)">
        Soporte lo revisa y la respuesta aparece aquí mismo, en sus casos. No hace falta que escriba a nadie más.
      </p>
      <button
        type="button"
        onClick={onNuevo}
        className="mt-5 inline-flex h-9 items-center rounded-lg border border-(--line) bg-white px-4 text-[13px] font-semibold text-(--ink-900)"
      >
        Reportar otro problema
      </button>
    </section>
  );
}

function MisCasos({ casos }: { casos: CasoPropio[] }) {
  return (
    <section className="mt-10">
      <h2 className="text-[13px] font-semibold tracking-wide text-(--ink-500) uppercase">Sus casos</h2>
      <div className="mt-3 space-y-2">
        {casos.map((c) => (
          <details key={c.numero} className="rounded-xl border border-(--line) bg-white/70">
            <summary className="flex cursor-pointer items-center gap-3 px-4 py-3">
              <span
                className={`grid size-7 shrink-0 place-items-center rounded-lg ${
                  c.con_solucion || c.estado === 'resuelto'
                    ? 'bg-emerald-50 text-emerald-600'
                    : 'bg-amber-50 text-amber-600'
                }`}
              >
                {c.con_solucion || c.estado === 'resuelto' ? (
                  <CheckCircle2 className="size-4" />
                ) : (
                  <Clock className="size-4" />
                )}
              </span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[13.5px] font-medium text-(--ink-900)">{c.titulo}</span>
                <span className="block text-[11.5px] text-(--ink-500)">
                  {c.numero} · {c.estado}
                </span>
              </span>
            </summary>
            <DetalleCaso numero={c.numero} />
          </details>
        ))}
      </div>
    </section>
  );
}

/** El detalle se pide al abrir: la mayoría de los casos no se despliegan nunca. */
function DetalleCaso({ numero }: { numero: string }) {
  const { data, isLoading } = useQuery({
    queryKey: ['mi-caso', numero],
    queryFn: () => clienteApi.caso(numero),
  });

  if (isLoading) return <p className="px-4 pb-3 text-[13px] text-(--ink-500)">Cargando…</p>;
  if (!data) return null;

  return (
    <div className="space-y-3 border-t border-(--line) px-4 py-3">
      {data.solucion ? (
        <div>
          <p className="text-[11.5px] font-semibold tracking-wide text-(--ink-500) uppercase">Solución</p>
          <p className="mt-1 text-[13.5px] leading-relaxed whitespace-pre-line text-(--ink-800)">{data.solucion}</p>
        </div>
      ) : (
        <p className="text-[13px] text-(--ink-700)">
          Todavía en revisión. Cuando soporte lo resuelva, la respuesta aparece aquí.
        </p>
      )}
      {data.causa_raiz && (
        <div>
          <p className="text-[11.5px] font-semibold tracking-wide text-(--ink-500) uppercase">Por qué pasó</p>
          <p className="mt-1 text-[13.5px] leading-relaxed whitespace-pre-line text-(--ink-800)">{data.causa_raiz}</p>
        </div>
      )}
    </div>
  );
}

export default withAuth(SoportePage, ['user', 'admin', 'superadmin']);
