'use client';

import { BookOpen, CheckCircle2, ChevronDown, Clock, History, Search, Send } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import ReactMarkdown from 'react-markdown';
import { withAuth } from '../components/ProtectedRoute';
import ThemeToggle from '../components/ThemeToggle';
import UserIdentityBadge from '../components/UserIdentityBadge';
import { useTheme } from '../components/ThemeProvider';
import { clienteApi, type CasoPropio, type PrecedentePublico, type Tema } from './api';

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
      <div className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6">
        <Cabecera />

        <div className="mt-6 grid items-start gap-6 lg:grid-cols-[minmax(0,1fr)_340px]">
          <div className="min-w-0 space-y-6">
        {reportado ? (
          <Confirmacion caso={reportado} onNuevo={empezarDeNuevo} />
        ) : (
          <>
            <section>
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
              <section>
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

          </div>

          <aside className="space-y-4 lg:sticky lg:top-6">
            {!reportado && <Temas />}
            {misCasos && misCasos.length > 0 && <MisCasos casos={misCasos} />}
          </aside>
        </div>
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

/**
 * Explorar por tema, para quien no sabe qué escribir.
 *
 * La búsqueda sirve cuando uno puede poner el problema en palabras; a veces no, y
 * entonces hace falta poder mirar qué hay. Viene plegado porque lo primero que se
 * ofrece sigue siendo buscar: es más rápido cuando funciona.
 */
function Temas() {
  const [abierto, setAbierto] = useState(false);
  const { data: temas } = useQuery({
    queryKey: ['temas'],
    queryFn: clienteApi.temas,
    // No se pide hasta que alguien despliega: la mayoría busca y se va.
    enabled: abierto,
  });

  return (
    <section>
      <button
        type="button"
        onClick={() => setAbierto((v) => !v)}
        className="flex w-full items-center gap-2 rounded-xl border border-(--line) bg-white/70 px-4 py-3 text-left"
        aria-expanded={abierto}
      >
        <ChevronDown
          className={`size-4 text-(--ink-500) transition-transform ${abierto ? 'rotate-180' : ''}`}
        />
        <span className="text-[13.5px] font-medium text-(--ink-900)">Explorar por tema</span>
        <span className="ml-auto text-[12px] text-(--ink-500)">si no sabe cómo describirlo</span>
      </button>

      {abierto && (
        <div className="mt-2 space-y-2">
          {temas === undefined && <p className="px-4 text-[13px] text-(--ink-500)">Cargando…</p>}
          {temas?.length === 0 && (
            <p className="px-4 text-[13px] text-(--ink-700)">Todavía no hay temas publicados.</p>
          )}
          {temas?.map((t) => (
            <Categoria key={t.categoria} tema={t} />
          ))}
        </div>
      )}
    </section>
  );
}

function Categoria({ tema }: { tema: Tema }) {
  return (
    <details className="rounded-xl border border-(--line) bg-white/70">
      <summary className="flex cursor-pointer items-center gap-3 px-4 py-2.5">
        <BookOpen className="size-4 shrink-0 text-(--ink-500)" />
        <span className="text-[13.5px] font-medium text-(--ink-900)">{tema.categoria}</span>
        <span className="ml-auto text-[12px] text-(--ink-500)">{tema.articulos.length}</span>
      </summary>
      <div className="border-t border-(--line)">
        {tema.articulos.map((a) => (
          <Articulo key={a.id} id={a.id} titulo={a.titulo} descripcion={a.descripcion} />
        ))}
      </div>
    </details>
  );
}

function Articulo({ id, titulo, descripcion }: { id: string; titulo: string; descripcion: string | null }) {
  const [abierto, setAbierto] = useState(false);
  const { data, isLoading } = useQuery({
    queryKey: ['tema', id],
    queryFn: () => clienteApi.tema(id),
    enabled: abierto,
  });

  return (
    <div className="border-b border-(--line) last:border-b-0">
      <button
        type="button"
        onClick={() => setAbierto((v) => !v)}
        className="w-full px-4 py-2.5 text-left"
        aria-expanded={abierto}
      >
        <span className="block text-[13.5px] text-(--ink-900)">{titulo}</span>
        {!abierto && descripcion && (
          <span className="mt-0.5 line-clamp-1 block text-[12px] text-(--ink-700)">{resumenPlano(descripcion)}</span>
        )}
      </button>
      {abierto && (
        <div className="px-4 pb-3">
          {isLoading && <p className="text-[13px] text-(--ink-500)">Cargando…</p>}
          {data &&
            (data.contenido ? (
              <Texto contenido={data.contenido} />
            ) : (
              <p className="text-[13px] text-(--ink-700)">Este tema no tiene contenido escrito.</p>
            ))}
        </div>
      )}
    </div>
  );
}

/**
 * Contenido de un artículo, que viene en Markdown.
 *
 * Antes se pintaba tal cual y los `##` de los títulos salían en pantalla. Se
 * estilan aquí y no con una hoja global porque esta vista tiene su propia escala
 * tipográfica, más chica que la del panel.
 */
function Texto({ contenido }: { contenido: string }) {
  return (
    <div className="text-[13.5px] leading-relaxed text-(--ink-800)">
      <ReactMarkdown
        components={{
          h1: ({ children }) => (
            <h3 className="mt-3 mb-1.5 text-[14.5px] font-semibold text-(--ink-900) first:mt-0">{children}</h3>
          ),
          h2: ({ children }) => (
            <h4 className="mt-3 mb-1 text-[13.5px] font-semibold text-(--ink-900) first:mt-0">{children}</h4>
          ),
          h3: ({ children }) => (
            <h5 className="mt-2.5 mb-1 text-[13px] font-semibold text-(--ink-900) first:mt-0">{children}</h5>
          ),
          p: ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
          ul: ({ children }) => <ul className="mb-2 ml-4 list-disc space-y-0.5">{children}</ul>,
          ol: ({ children }) => <ol className="mb-2 ml-4 list-decimal space-y-0.5">{children}</ol>,
          strong: ({ children }) => <strong className="font-semibold text-(--ink-900)">{children}</strong>,
          a: ({ children, href }) => (
            <a href={href} className="text-(--accent) underline">
              {children}
            </a>
          ),
          code: ({ children }) => (
            <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-[12px]">{children}</code>
          ),
          pre: ({ children }) => (
            <pre className="mb-2 overflow-x-auto rounded-lg bg-slate-900 p-3 font-mono text-[12px] text-slate-100">
              {children}
            </pre>
          ),
          blockquote: ({ children }) => (
            <blockquote className="mb-2 border-l-2 border-(--line) pl-3 text-(--ink-700)">{children}</blockquote>
          ),
          hr: () => <hr className="my-3 border-(--line)" />,
          table: ({ children }) => (
            <div className="mb-2 overflow-x-auto">
              <table className="w-full text-left text-[12.5px]">{children}</table>
            </div>
          ),
          th: ({ children }) => <th className="border-b border-(--line) py-1 pr-3 font-medium">{children}</th>,
          td: ({ children }) => <td className="border-b border-(--line) py-1 pr-3">{children}</td>,
        }}
      >
        {contenido}
      </ReactMarkdown>
    </div>
  );
}

/**
 * Deja un resumen de una línea, sin marcas de Markdown.
 *
 * Varios artículos traen la descripción en Markdown —hay uno cuyo resumen empieza
 * con «## Ranking salarial»— y en una vista previa de una línea esas marcas solo
 * estorban. Renderizarlas tampoco sirve: un título dentro de una línea de resumen
 * se ve peor todavía.
 */
function resumenPlano(texto: string): string {
  return texto
    .replace(/^#{1,6}\s*/gm, '')
    .replace(/\*\*|__|\*|_/g, '')
    .replace(new RegExp('`', 'g'), '')
    .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
    .replace(/^>\s*/gm, '')
    .replace(/\s+/g, ' ')
    .trim();
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
            <span className="mt-0.5 line-clamp-2 block text-[12.5px] text-(--ink-700)">{resumenPlano(cuerpo)}</span>
          )}
        </span>
      </button>

      {abierto && (
        <div className="border-t border-(--line) px-4 py-3">
          {cuerpo ? (
            <Texto contenido={cuerpo} />
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
    <section className="rounded-xl border border-(--line) bg-white/80 px-5 py-6 text-center">
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
    <section>
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
