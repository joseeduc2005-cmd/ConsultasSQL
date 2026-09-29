'use client';

import {
  ArrowLeft,
  BookOpen,
  CheckCircle2,
  Clock,
  FolderOpen,
  History,
  MessageSquarePlus,
  Search,
  Send,
} from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState, type ReactNode } from 'react';
import { withAuth } from '../components/ProtectedRoute';
import ThemeToggle from '../components/ThemeToggle';
import UserIdentityBadge from '../components/UserIdentityBadge';
import { useTheme } from '../components/ThemeProvider';
import { Markdown, resumenPlano } from './markdown';
import { clienteApi, type CasoPropio, type PrecedentePublico, type Tema } from './api';

type Seccion = 'buscar' | 'temas' | 'casos';

/**
 * Lo que ve quien tiene el problema, no quien lo resuelve.
 *
 * Tres secciones, una a la vez y a pantalla completa. La versión anterior las metía
 * todas en la misma vista, en columnas, y el resultado era una barra lateral apretada
 * donde el contenido de un artículo no se podía leer. Quien entra aquí hace una cosa:
 * o describe su problema, o mira temas, o revisa sus casos.
 */
function SoportePage() {
  const [seccion, setSeccion] = useState<Seccion>('buscar');
  const { data: misCasos } = useQuery({ queryKey: ['mis-casos'], queryFn: clienteApi.misCasos });
  const pendientes = (misCasos ?? []).filter((c) => c.estado !== 'resuelto').length;

  return (
    <div className="app-shell min-h-screen">
      <div className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6">
        <Cabecera />

        <nav className="mt-5 flex gap-1 rounded-xl border border-(--line) bg-white/60 p-1">
          <Pestana activa={seccion === 'buscar'} onClick={() => setSeccion('buscar')} icono={<MessageSquarePlus />}>
            Mi problema
          </Pestana>
          <Pestana activa={seccion === 'temas'} onClick={() => setSeccion('temas')} icono={<FolderOpen />}>
            Temas de ayuda
          </Pestana>
          <Pestana
            activa={seccion === 'casos'}
            onClick={() => setSeccion('casos')}
            icono={<Clock />}
            insignia={pendientes > 0 ? pendientes : undefined}
          >
            Mis casos
          </Pestana>
        </nav>

        <main className="mt-6">
          {seccion === 'buscar' && <Buscar />}
          {seccion === 'temas' && <Temas />}
          {seccion === 'casos' && <MisCasos casos={misCasos ?? []} onBuscar={() => setSeccion('buscar')} />}
        </main>
      </div>
    </div>
  );
}

function Pestana({
  activa,
  onClick,
  icono,
  insignia,
  children,
}: {
  activa: boolean;
  onClick: () => void;
  icono: ReactNode;
  insignia?: number;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-current={activa ? 'page' : undefined}
      className={`flex flex-1 items-center justify-center gap-2 rounded-lg px-3 py-2.5 text-[13.5px] font-medium transition-colors ${
        activa ? 'bg-(--accent) text-white shadow-sm' : 'text-(--ink-700) hover:bg-white/70'
      } [&_svg]:size-4`}
    >
      {icono}
      <span className="hidden sm:inline">{children}</span>
      {insignia !== undefined && (
        <span
          className={`rounded-full px-1.5 text-[11px] font-semibold ${
            activa ? 'bg-white/25' : 'bg-amber-100 text-amber-700'
          }`}
        >
          {insignia}
        </span>
      )}
    </button>
  );
}

// ─── Mi problema ─────────────────────────────────────────────────────────────

function Buscar() {
  const [texto, setTexto] = useState('');
  const [resultados, setResultados] = useState<PrecedentePublico[] | null>(null);
  const [reportado, setReportado] = useState<CasoPropio | null>(null);
  const qc = useQueryClient();

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

  if (reportado) {
    return (
      <section className="rounded-2xl border border-(--line) bg-white/80 px-6 py-10 text-center">
        <CheckCircle2 className="mx-auto size-10 text-emerald-600" />
        <h2 className="mt-4 text-lg font-semibold text-(--ink-900)">Su caso quedó registrado</h2>
        <p className="mt-1 font-mono text-[13px] text-(--ink-700)">{reportado.numero}</p>
        <p className="mx-auto mt-3 max-w-md text-[14px] text-(--ink-700)">
          Soporte lo revisa y la respuesta aparece en «Mis casos». No hace falta que escriba a nadie más.
        </p>
        <button
          type="button"
          onClick={() => {
            setTexto('');
            setReportado(null);
          }}
          className="mt-6 inline-flex h-10 items-center rounded-lg border border-(--line) bg-white px-4 text-[13.5px] font-semibold text-(--ink-900)"
        >
          Reportar otro problema
        </button>
      </section>
    );
  }

  return (
    <div className="space-y-6">
      <section>
        <h1 className="text-xl font-semibold tracking-tight text-(--ink-900)">¿Qué problema tiene?</h1>
        <p className="mt-1 text-[14px] text-(--ink-700)">
          Descríbalo con sus palabras. Primero buscamos si ya se resolvió antes.
        </p>

        <textarea
          value={texto}
          onChange={(e) => setTexto(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && listo) buscar.mutate();
          }}
          rows={4}
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
          <span className="text-[12.5px] text-(--ink-500)">
            {listo ? 'Ctrl + Enter para buscar' : 'Escriba un poco más'}
          </span>
        </div>
      </section>

      {resultados !== null && (
        <section className="space-y-3">
          {resultados.length > 0 ? (
            <>
              <h2 className="text-[13px] font-semibold tracking-wide text-(--ink-500) uppercase">
                Esto se parece a lo suyo
              </h2>
              {resultados.map((p) => (
                <Desplegable
                  key={`${p.tipo}-${p.id}`}
                  icono={p.tipo === 'ticket' ? <History /> : <BookOpen />}
                  titulo={p.titulo}
                  resumen={p.solucion || p.detalle}
                >
                  {p.solucion || p.detalle ? (
                    <Markdown contenido={p.solucion || p.detalle} />
                  ) : (
                    <p className="text-[13.5px] text-(--ink-700)">
                      Este caso no dejó una explicación escrita. Repórtelo y soporte lo revisa.
                    </p>
                  )}
                </Desplegable>
              ))}
            </>
          ) : (
            <p className="rounded-xl border border-(--line) bg-white/70 px-4 py-6 text-center text-[14px] text-(--ink-700)">
              No encontramos nada parecido todavía.
            </p>
          )}

          {/* Aparece también cuando hubo resultados: que salgan no significa que alguno sirva. */}
          <div className="rounded-xl border border-(--line) bg-white/70 px-4 py-4">
            <p className="text-[14px] font-medium text-(--ink-900)">
              {resultados.length > 0 ? '¿Ninguna le sirve?' : '¿Lo reportamos al equipo?'}
            </p>
            <p className="mt-0.5 text-[13px] text-(--ink-700)">
              Se crea un caso con lo que escribió y soporte lo revisa.
            </p>
            <button
              type="button"
              onClick={() => reportar.mutate()}
              disabled={reportar.isPending}
              className="mt-3 inline-flex h-9 items-center gap-2 rounded-lg border border-(--line) bg-white px-3.5 text-[13px] font-semibold text-(--ink-900) disabled:opacity-40"
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
    </div>
  );
}

// ─── Temas de ayuda ──────────────────────────────────────────────────────────

/**
 * El árbol de temas, a pantalla completa.
 *
 * Dos pasos y no uno: primero las categorías, y al elegir una se entra a sus
 * artículos. Anidar categoría dentro de categoría dentro de artículo dejaba el texto
 * en una columna de cuatro centímetros.
 */
function Temas() {
  const [abierta, setAbierta] = useState<Tema | null>(null);
  const { data: temas, isLoading } = useQuery({ queryKey: ['temas'], queryFn: clienteApi.temas });

  if (isLoading) return <p className="text-[14px] text-(--ink-500)">Cargando temas…</p>;
  if (!temas?.length) return <p className="text-[14px] text-(--ink-700)">Todavía no hay temas publicados.</p>;

  if (abierta) {
    return (
      <div>
        <button
          type="button"
          onClick={() => setAbierta(null)}
          className="inline-flex items-center gap-1.5 text-[13.5px] font-medium text-(--accent)"
        >
          <ArrowLeft className="size-4" />
          Todos los temas
        </button>
        <h1 className="mt-3 text-xl font-semibold tracking-tight text-(--ink-900)">{abierta.categoria}</h1>
        <p className="mt-1 text-[13.5px] text-(--ink-700)">
          {abierta.articulos.length} tema{abierta.articulos.length === 1 ? '' : 's'}
        </p>
        <div className="mt-4 space-y-3">
          {abierta.articulos.map((a) => (
            <Articulo key={a.id} id={a.id} titulo={a.titulo} descripcion={a.descripcion} />
          ))}
        </div>
      </div>
    );
  }

  return (
    <div>
      <h1 className="text-xl font-semibold tracking-tight text-(--ink-900)">Temas de ayuda</h1>
      <p className="mt-1 text-[14px] text-(--ink-700)">Elija un tema si no sabe cómo describir su problema.</p>
      <div className="mt-4 grid gap-3 sm:grid-cols-2">
        {temas.map((t) => (
          <button
            key={t.categoria}
            type="button"
            onClick={() => setAbierta(t)}
            className="flex items-center gap-3 rounded-xl border border-(--line) bg-white/70 px-4 py-3.5 text-left transition hover:border-(--accent)/40 hover:bg-white"
          >
            <span className="grid size-9 shrink-0 place-items-center rounded-lg bg-(--accent)/10 text-(--accent)">
              <BookOpen className="size-4" />
            </span>
            <span className="min-w-0 flex-1">
              <span className="block truncate text-[14px] font-medium text-(--ink-900)">{t.categoria}</span>
              <span className="block text-[12.5px] text-(--ink-500)">
                {t.articulos.length} tema{t.articulos.length === 1 ? '' : 's'}
              </span>
            </span>
          </button>
        ))}
      </div>
    </div>
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
    <Desplegable
      icono={<BookOpen />}
      titulo={titulo}
      resumen={descripcion ?? ''}
      abierto={abierto}
      onAlternar={() => setAbierto((v) => !v)}
    >
      {isLoading && <p className="text-[13.5px] text-(--ink-500)">Cargando…</p>}
      {data &&
        (data.contenido ? (
          <Markdown contenido={data.contenido} />
        ) : (
          <p className="text-[13.5px] text-(--ink-700)">Este tema no tiene contenido escrito.</p>
        ))}
    </Desplegable>
  );
}

// ─── Mis casos ───────────────────────────────────────────────────────────────

function MisCasos({ casos, onBuscar }: { casos: CasoPropio[]; onBuscar: () => void }) {
  if (casos.length === 0) {
    return (
      <section className="rounded-2xl border border-(--line) bg-white/70 px-6 py-10 text-center">
        <Clock className="mx-auto size-8 text-(--ink-500)" />
        <h2 className="mt-3 text-[15px] font-semibold text-(--ink-900)">Todavía no ha reportado nada</h2>
        <p className="mx-auto mt-1 max-w-sm text-[13.5px] text-(--ink-700)">
          Cuando reporte un problema, aparecerá aquí con su respuesta.
        </p>
        <button
          type="button"
          onClick={onBuscar}
          className="mt-5 inline-flex h-10 items-center gap-2 rounded-lg bg-(--accent) px-4 text-[13.5px] font-semibold text-white"
        >
          <MessageSquarePlus className="size-4" />
          Reportar un problema
        </button>
      </section>
    );
  }

  return (
    <div>
      <h1 className="text-xl font-semibold tracking-tight text-(--ink-900)">Mis casos</h1>
      <div className="mt-4 space-y-3">
        {casos.map((c) => (
          <Caso key={c.numero} caso={c} />
        ))}
      </div>
    </div>
  );
}

function Caso({ caso }: { caso: CasoPropio }) {
  const [abierto, setAbierto] = useState(false);
  const resuelto = caso.con_solucion || caso.estado === 'resuelto';
  const { data } = useQuery({
    queryKey: ['mi-caso', caso.numero],
    queryFn: () => clienteApi.caso(caso.numero),
    enabled: abierto,
  });

  return (
    <div className="overflow-hidden rounded-xl border border-(--line) bg-white/80">
      <button
        type="button"
        onClick={() => setAbierto((v) => !v)}
        className="flex w-full items-start gap-3 px-4 py-3.5 text-left"
        aria-expanded={abierto}
      >
        <span
          className={`mt-0.5 grid size-8 shrink-0 place-items-center rounded-lg ${
            resuelto ? 'bg-emerald-50 text-emerald-600' : 'bg-amber-50 text-amber-600'
          }`}
        >
          {resuelto ? <CheckCircle2 className="size-4" /> : <Clock className="size-4" />}
        </span>
        <span className="min-w-0 flex-1">
          <span className="block text-[14px] font-medium text-(--ink-900)">{caso.titulo}</span>
          <span className="block text-[12px] text-(--ink-500)">
            {caso.numero} · {resuelto ? 'resuelto' : 'en revisión'}
          </span>
        </span>
      </button>

      {abierto && (
        <div className="space-y-4 border-t border-(--line) px-4 py-4">
          {data?.solucion ? (
            <Bloque titulo="Solución" texto={data.solucion} />
          ) : (
            <p className="text-[13.5px] text-(--ink-700)">
              Todavía en revisión. Cuando soporte lo resuelva, la respuesta aparece aquí.
            </p>
          )}
          {data?.causa_raiz && <Bloque titulo="Por qué pasó" texto={data.causa_raiz} />}
        </div>
      )}
    </div>
  );
}

function Bloque({ titulo, texto }: { titulo: string; texto: string }) {
  return (
    <div>
      <p className="mb-1 text-[11.5px] font-semibold tracking-wide text-(--ink-500) uppercase">{titulo}</p>
      <Markdown contenido={texto} />
    </div>
  );
}

// ─── Piezas compartidas ──────────────────────────────────────────────────────

/** Tarjeta que se abre para mostrar su contenido. La usan resultados y artículos. */
function Desplegable({
  icono,
  titulo,
  resumen,
  children,
  abierto: abiertoExterno,
  onAlternar,
}: {
  icono: ReactNode;
  titulo: string;
  resumen: string;
  children: ReactNode;
  abierto?: boolean;
  onAlternar?: () => void;
}) {
  const [propio, setPropio] = useState(false);
  const abierto = abiertoExterno ?? propio;
  const alternar = onAlternar ?? (() => setPropio((v) => !v));

  return (
    <div className="overflow-hidden rounded-xl border border-(--line) bg-white/80">
      <button
        type="button"
        onClick={alternar}
        className="flex w-full items-start gap-3 px-4 py-3.5 text-left"
        aria-expanded={abierto}
      >
        <span className="mt-0.5 grid size-8 shrink-0 place-items-center rounded-lg bg-(--accent)/10 text-(--accent) [&_svg]:size-4">
          {icono}
        </span>
        <span className="min-w-0 flex-1">
          <span className="block text-[14px] font-medium text-(--ink-900)">{titulo}</span>
          {!abierto && resumen && (
            <span className="mt-0.5 line-clamp-2 block text-[12.5px] text-(--ink-700)">{resumenPlano(resumen)}</span>
          )}
        </span>
      </button>
      {abierto && <div className="border-t border-(--line) px-4 py-4">{children}</div>}
    </div>
  );
}

/**
 * Cabecera propia, más liviana que la del panel.
 *
 * No hereda la del administrador porque aquella trae acciones que aquí no existen, y
 * una barra con botones que no llevan a ningún sitio es peor que no tenerla.
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
        <p className="truncate text-[16px] leading-tight font-semibold tracking-tight text-(--ink-900)">Soporte</p>
        <p className="truncate text-[12px] leading-tight text-(--ink-700)">Busque su problema o repórtelo al equipo.</p>
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

export default withAuth(SoportePage, ['user', 'admin', 'superadmin']);
