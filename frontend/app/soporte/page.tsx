'use client';

import {
  BookOpen,
  CheckCircle2,
  ChevronDown,
  Clock,
  Database,
  History,
  Lightbulb,
  MessageSquarePlus,
  Search,
  Send,
  XCircle,
} from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState, type ReactNode } from 'react';
import CommentsSection from '../components/CommentsSection';
import { withAuth } from '../components/ProtectedRoute';
import ThemeToggle from '../components/ThemeToggle';
import UserIdentityBadge from '../components/UserIdentityBadge';
import { useTheme } from '../components/ThemeProvider';
import { Markdown, resumenPlano } from './markdown';
import { clienteApi, type Aporte, type CasoPropio, type PrecedentePublico } from './api';

/** Qué se está mirando en el panel de la derecha. */
type Vista =
  | { tipo: 'buscar' }
  | { tipo: 'articulo'; id: string }
  | { tipo: 'caso'; numero: string }
  /** Compartir una solución propia. Puede venir desde un caso, y entonces lo arrastra. */
  | { tipo: 'aportar'; caso?: string; titulo?: string };

/**
 * Lo que ve quien tiene el problema.
 *
 * Misma forma que el panel de administración —índice a la izquierda, contenido a la
 * derecha— porque es la que ya conoce quien usa el sistema, y porque el contenido de
 * un artículo necesita ancho para leerse.
 */
function SoportePage() {
  const [vista, setVista] = useState<Vista>({ tipo: 'buscar' });
  const [comentarios, setComentarios] = useState(false);

  return (
    <div className="app-shell min-h-screen">
      <div className="mx-auto w-full max-w-7xl px-4 py-5 sm:px-6">
        <Cabecera />

        <div className="mt-5 grid items-start gap-5 lg:grid-cols-[264px_minmax(0,1fr)]">
          <BarraLateral vista={vista} onElegir={setVista} />

          <div className="flex min-w-0 gap-4">
            <main className="min-w-0 flex-1 glass-panel-strong rounded-2xl px-5 py-5 sm:px-6">
              {vista.tipo === 'buscar' && <Buscar onVerCaso={(numero) => setVista({ tipo: 'caso', numero })} />}
              {vista.tipo === 'articulo' && <Articulo id={vista.id} />}
              {vista.tipo === 'caso' && <Caso numero={vista.numero} onAportar={setVista} />}
              {vista.tipo === 'aportar' && <Aportar caso={vista.caso} titulo={vista.titulo} />}
            </main>

            {/*
              Los comentarios van en su propia columna, como en el panel: dentro del
              artículo quedaban al final de un texto largo, y el botón de plegar se
              veía suelto en medio de la nada.
            */}
            {vista.tipo === 'articulo' && (
              <section
                className={`hidden shrink-0 glass-panel-strong rounded-2xl p-3 transition-[width] duration-300 lg:block ${
                  comentarios ? 'w-[300px]' : 'w-[52px]'
                }`}
              >
                <PanelComentarios
                  id={vista.id}
                  plegado={!comentarios}
                  onAlternar={() => setComentarios((v) => !v)}
                />
              </section>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Barra lateral ───────────────────────────────────────────────────────────

function BarraLateral({ vista, onElegir }: { vista: Vista; onElegir: (v: Vista) => void }) {
  const { data: temas } = useQuery({ queryKey: ['temas'], queryFn: clienteApi.temas });
  const { data: casos } = useQuery({ queryKey: ['mis-casos'], queryFn: clienteApi.misCasos });
  const pendientes = (casos ?? []).filter((c) => c.estado !== 'resuelto').length;

  return (
    <aside className="space-y-4 glass-panel-strong rounded-2xl p-3 lg:sticky lg:top-5">
      <button
        type="button"
        onClick={() => onElegir({ tipo: 'buscar' })}
        className={`flex w-full items-center gap-2.5 rounded-lg px-3 py-2.5 text-[13.5px] font-semibold transition-colors ${
          vista.tipo === 'buscar' ? 'bg-(--accent) text-white' : 'text-(--ink-900) hover:bg-(--surface-2)'
        }`}
      >
        <MessageSquarePlus className="size-4" />
        Mi problema
      </button>

      {/*
        Segundo botón y no una pestaña dentro del primero: son dos intenciones
        distintas —vengo a que me ayuden, vengo a contar cómo lo arreglé— y quien
        entra a lo segundo ya sabe lo que busca.
      */}
      <button
        type="button"
        onClick={() => onElegir({ tipo: 'aportar' })}
        className={`-mt-2 flex w-full items-center gap-2.5 rounded-lg px-3 py-2.5 text-[13.5px] font-semibold transition-colors ${
          vista.tipo === 'aportar' ? 'bg-(--accent) text-white' : 'text-(--ink-900) hover:bg-(--surface-2)'
        }`}
      >
        <Lightbulb className="size-4" />
        Yo lo resolví
      </button>

      <Grupo titulo="Temas de ayuda">
        {temas?.length ? (
          temas.map((t) => (
            <Categoria
              key={t.categoria}
              nombre={t.categoria}
              articulos={t.articulos}
              activo={vista.tipo === 'articulo' ? vista.id : null}
              onElegir={(id) => onElegir({ tipo: 'articulo', id })}
            />
          ))
        ) : (
          <p className="px-3 py-1 text-[12.5px] text-(--ink-500)">Sin temas todavía.</p>
        )}
      </Grupo>

      <Grupo titulo="Mis casos" insignia={pendientes || undefined}>
        {casos?.length ? (
          casos.map((c) => {
            const activo = vista.tipo === 'caso' && vista.numero === c.numero;
            const resuelto = c.con_solucion || c.estado === 'resuelto';
            return (
              <button
                key={c.numero}
                type="button"
                onClick={() => onElegir({ tipo: 'caso', numero: c.numero })}
                className={`flex w-full items-start gap-2 rounded-lg px-3 py-2 text-left text-[12.5px] transition-colors ${
                  activo ? 'bg-(--accent)/10 font-medium text-(--accent)' : 'text-(--ink-700) hover:bg-(--surface-2)'
                }`}
              >
                {resuelto ? (
                  <CheckCircle2 className="mt-0.5 size-3.5 shrink-0 text-emerald-600" />
                ) : (
                  <Clock className="mt-0.5 size-3.5 shrink-0 text-amber-600" />
                )}
                <span className="line-clamp-2">{c.titulo}</span>
              </button>
            );
          })
        ) : (
          <p className="px-3 py-1 text-[12.5px] text-(--ink-500)">Aún no ha reportado nada.</p>
        )}
      </Grupo>
    </aside>
  );
}

function Grupo({ titulo, insignia, children }: { titulo: string; insignia?: number; children: ReactNode }) {
  return (
    <div>
      <p className="flex items-center gap-2 px-3 pt-1 pb-1.5 text-[10.5px] font-semibold tracking-[0.14em] text-(--ink-500) uppercase">
        {titulo}
        {insignia !== undefined && (
          <span className="rounded-full bg-amber-100 px-1.5 text-[10px] font-semibold text-amber-700">{insignia}</span>
        )}
      </p>
      <div className="space-y-0.5">{children}</div>
    </div>
  );
}

function Categoria({
  nombre,
  articulos,
  activo,
  onElegir,
}: {
  nombre: string;
  articulos: { id: string; titulo: string }[];
  activo: string | null;
  onElegir: (id: string) => void;
}) {
  // Abierta si el artículo que se está viendo es suyo: al entrar por un enlace, la
  // rama correspondiente aparece desplegada en vez de haber que buscarla.
  const [abierta, setAbierta] = useState(() => articulos.some((a) => a.id === activo));

  return (
    <div>
      <button
        type="button"
        onClick={() => setAbierta((v) => !v)}
        className="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[13px] text-(--ink-900) transition-colors hover:bg-(--surface-2)"
        aria-expanded={abierta}
      >
        <ChevronDown className={`size-3.5 shrink-0 text-(--ink-500) transition-transform ${abierta ? '' : '-rotate-90'}`} />
        <span className="min-w-0 flex-1 truncate">{nombre}</span>
        <span className="text-[11px] text-(--ink-500)">{articulos.length}</span>
      </button>
      {abierta && (
        <div className="ml-4 space-y-0.5 border-l border-(--line) pl-2">
          {articulos.map((a) => (
            <button
              key={a.id}
              type="button"
              onClick={() => onElegir(a.id)}
              className={`block w-full rounded-lg px-2.5 py-1.5 text-left text-[12.5px] transition-colors ${
                activo === a.id ? 'bg-(--accent)/10 font-medium text-(--accent)' : 'text-(--ink-700) hover:bg-(--surface-2)'
              }`}
            >
              <span className="line-clamp-2">{a.titulo}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Mi problema ─────────────────────────────────────────────────────────────

function Buscar({ onVerCaso }: { onVerCaso: (numero: string) => void }) {
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
      <div className="py-8 text-center">
        <CheckCircle2 className="mx-auto size-10 text-emerald-600" />
        <h2 className="mt-4 text-lg font-semibold text-(--ink-900)">Su caso quedó registrado</h2>
        <p className="mt-1 font-mono text-[13px] text-(--ink-700)">{reportado.numero}</p>
        <p className="mx-auto mt-3 max-w-md text-[14px] text-(--ink-700)">
          Soporte lo revisa y la respuesta aparece en «Mis casos», aquí al lado.
        </p>
        <div className="mt-6 flex flex-wrap justify-center gap-2">
          <button
            type="button"
            onClick={() => onVerCaso(reportado.numero)}
            className="inline-flex h-10 items-center rounded-lg bg-(--accent) px-4 text-[13.5px] font-semibold text-white"
          >
            Ver mi caso
          </button>
          <button
            type="button"
            onClick={() => {
              setTexto('');
              setReportado(null);
            }}
            className="inline-flex h-10 items-center rounded-lg border border-(--line) bg-(--surface-2) px-4 text-[13.5px] font-semibold text-(--ink-900)"
          >
            Reportar otro
          </button>
        </div>
      </div>
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
          className="mt-4 w-full resize-none rounded-xl border border-(--line) bg-(--surface-2) px-4 py-3.5 text-[15px] leading-relaxed outline-none focus:border-(--accent)"
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
                <Resultado key={`${p.tipo}-${p.id}`} precedente={p} />
              ))}
            </>
          ) : (
            <p className="rounded-xl border border-(--line) bg-(--surface-2) px-4 py-6 text-center text-[14px] text-(--ink-700)">
              No encontramos nada parecido todavía.
            </p>
          )}

          {/* Aparece también cuando hubo resultados: que salgan no significa que sirvan. */}
          <div className="rounded-xl border border-(--line) bg-(--surface-2) px-4 py-4">
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
              className="mt-3 inline-flex h-9 items-center gap-2 rounded-lg border border-(--line) bg-(--surface-2) px-3.5 text-[13px] font-semibold text-(--ink-900) disabled:opacity-40"
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

function Resultado({ precedente: p }: { precedente: PrecedentePublico }) {
  const [abierto, setAbierto] = useState(false);
  const cuerpo = p.solucion || p.detalle;

  return (
    <div className="overflow-hidden rounded-xl border border-(--line) bg-(--surface-2)">
      <button
        type="button"
        onClick={() => setAbierto((v) => !v)}
        className="flex w-full items-start gap-3 px-4 py-3.5 text-left"
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
        <div className="border-t border-(--line) px-4 py-4">
          {cuerpo ? (
            <Markdown contenido={cuerpo} />
          ) : (
            <p className="text-[13.5px] text-(--ink-700)">
              Este caso no dejó una explicación escrita. Repórtelo y soporte lo revisa.
            </p>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Un tema ─────────────────────────────────────────────────────────────────

function Articulo({ id }: { id: string }) {
  const { data, isLoading } = useQuery({ queryKey: ['tema', id], queryFn: () => clienteApi.tema(id) });

  if (isLoading) return <p className="text-[14px] text-(--ink-500)">Cargando…</p>;
  if (!data) return <p className="text-[14px] text-(--ink-700)">No se encontró ese tema.</p>;

  return (
    <article>
      {data.categoria && (
        <p className="text-[11.5px] font-semibold tracking-wide text-(--ink-500) uppercase">{data.categoria}</p>
      )}
      <h1 className="mt-1 text-xl font-semibold tracking-tight text-(--ink-900)">{data.titulo}</h1>

      <div className="mt-4">
        {data.contenido ? (
          <Markdown contenido={data.contenido} />
        ) : (
          <p className="text-[13.5px] text-(--ink-700)">Este tema no tiene contenido escrito.</p>
        )}
      </div>

      {/*
        La sentencia del tema, cuando lo tiene.
        Que el tema esté marcado para todos ya es la autorización: alguien decidió
        que su contenido se puede mostrar, y ocultar justo la parte que explica qué
        se hace dejaba esa decisión sin efecto. Se muestra para leerla — copiarla y
        llevarla a soporte — no para correrla: aquí no hay con qué.
      */}
      {data.sql && (
        <section className="mt-6 border-t border-(--line) pt-5">
          <div className="flex flex-wrap items-center gap-2">
            <p className="text-[11.5px] font-semibold tracking-wide text-(--ink-500) uppercase">Sentencia</p>
            {data.base && (
              <span className="inline-flex items-center gap-1 rounded-full bg-(--surface-2) px-2 py-0.5 text-[11px] text-(--ink-700)">
                <Database className="size-3" />
                {data.base}
              </span>
            )}
          </div>
          <pre className="mt-2 overflow-x-auto rounded-xl border border-(--line) bg-(--surface-2) px-4 py-3 text-[12.5px] leading-relaxed text-(--ink-900)">
            <code>{data.sql}</code>
          </pre>
          <p className="mt-2 text-[12.5px] text-(--ink-500)">
            Solo para consultarla. Quien la ejecuta es el equipo de soporte.
          </p>
        </section>
      )}
    </article>
  );
}

/** Los comentarios del tema abierto, en la columna de la derecha. */
function PanelComentarios({ id, plegado, onAlternar }: { id: string; plegado: boolean; onAlternar: () => void }) {
  const usuario = leerUsuario();
  const { data } = useQuery({ queryKey: ['tema', id], queryFn: () => clienteApi.tema(id) });
  if (!data) return null;

  return (
    <CommentsSection
      articleId={data.id}
      articleTitle={data.titulo}
      currentUser={usuario ? { username: usuario.username ?? '', role: usuario.role ?? 'user' } : null}
      collapsed={plegado}
      onToggleCollapse={onAlternar}
    />
  );
}

// ─── Un caso propio ──────────────────────────────────────────────────────────

function Caso({ numero, onAportar }: { numero: string; onAportar: (v: Vista) => void }) {
  const { data, isLoading } = useQuery({ queryKey: ['mi-caso', numero], queryFn: () => clienteApi.caso(numero) });

  if (isLoading) return <p className="text-[14px] text-(--ink-500)">Cargando…</p>;
  if (!data) return <p className="text-[14px] text-(--ink-700)">No se encontró ese caso.</p>;

  const resuelto = Boolean(data.solucion) || data.estado === 'resuelto';

  return (
    <article>
      <div className="flex flex-wrap items-center gap-2">
        <span
          className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[12px] font-medium ${
            resuelto ? 'bg-emerald-50 text-emerald-700' : 'bg-amber-50 text-amber-700'
          }`}
        >
          {resuelto ? <CheckCircle2 className="size-3.5" /> : <Clock className="size-3.5" />}
          {resuelto ? 'Resuelto' : 'En revisión'}
        </span>
        <span className="font-mono text-[12px] text-(--ink-500)">{data.numero}</span>
      </div>

      <h1 className="mt-3 text-xl font-semibold tracking-tight text-(--ink-900)">{data.titulo}</h1>

      {data.descripcion && (
        <p className="mt-2 text-[13.5px] leading-relaxed whitespace-pre-line text-(--ink-700)">{data.descripcion}</p>
      )}

      <div className="mt-6 space-y-5 border-t border-(--line) pt-5">
        {data.solucion ? (
          <Bloque titulo="Solución" texto={data.solucion} />
        ) : (
          <p className="text-[13.5px] text-(--ink-700)">
            Todavía en revisión. Cuando soporte lo resuelva, la respuesta aparece aquí.
          </p>
        )}
        {data.causa_raiz && <Bloque titulo="Por qué pasó" texto={data.causa_raiz} />}
      </div>

      {/*
        Aparece aunque el caso ya esté resuelto: a veces la salida la encuentra uno
        antes de que llegue la respuesta, y esa es justamente la que se pierde.
      */}
      <button
        type="button"
        onClick={() => onAportar({ tipo: 'aportar', caso: data.numero, titulo: data.titulo })}
        className="mt-6 inline-flex h-9 items-center gap-2 rounded-lg border border-(--line) bg-(--surface-2) px-3.5 text-[13px] font-semibold text-(--ink-900) transition-colors hover:border-(--accent)"
      >
        <Lightbulb className="size-4" />
        Lo resolví yo — contar cómo
      </button>
    </article>
  );
}

function Bloque({ titulo, texto }: { titulo: string; texto: string }) {
  return (
    <div>
      <p className="mb-1.5 text-[11.5px] font-semibold tracking-wide text-(--ink-500) uppercase">{titulo}</p>
      <Markdown contenido={texto} />
    </div>
  );
}

// ─── Compartir una solución ──────────────────────────────────────────────────

/**
 * Lo que uno resolvió por su cuenta, para que no se pierda.
 *
 * No se publica al enviarlo. Queda pendiente hasta que alguien de soporte lo lea, y el
 * formulario lo dice antes de mandar, no después: prometer que quedó publicado y que
 * no aparezca por ningún lado sería peor que avisar.
 */
function Aportar({ caso, titulo: tituloInicial }: { caso?: string; titulo?: string }) {
  const [titulo, setTitulo] = useState(tituloInicial ?? '');
  const [problema, setProblema] = useState('');
  const [pasos, setPasos] = useState('');
  const [sql, setSql] = useState('');
  const [base, setBase] = useState('');
  const [enviado, setEnviado] = useState(false);
  const qc = useQueryClient();

  const { data: mios } = useQuery({ queryKey: ['mis-aportes'], queryFn: clienteApi.misAportes });

  const enviar = useMutation({
    mutationFn: () =>
      clienteApi.aportar({
        titulo: titulo.trim(),
        problema: problema.trim() || undefined,
        pasos: pasos.trim(),
        sql: sql.trim() || undefined,
        base: base.trim() || undefined,
        caso,
      }),
    onSuccess: () => {
      setEnviado(true);
      setTitulo('');
      setProblema('');
      setPasos('');
      setSql('');
      setBase('');
      qc.invalidateQueries({ queryKey: ['mis-aportes'] });
    },
  });

  const listo = titulo.trim().length >= 5 && pasos.trim().length >= 20;

  return (
    <div className="space-y-8">
      <section>
        <h1 className="text-xl font-semibold tracking-tight text-(--ink-900)">¿Lo resolvió usted?</h1>
        <p className="mt-1 text-[14px] text-(--ink-700)">
          Cuéntelo y soporte lo revisa. Si está bien, queda como tema de ayuda para el que tropiece con lo mismo.
        </p>

        {enviado && (
          <div className="mt-4 flex items-start gap-2.5 rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-3">
            <CheckCircle2 className="mt-0.5 size-4 shrink-0 text-emerald-600" />
            <p className="text-[13.5px] text-(--ink-900)">
              Enviado. Queda pendiente de revisión — abajo puede ver en qué va.
            </p>
          </div>
        )}

        <div className="mt-5 space-y-4">
          {caso && (
            <p className="text-[12.5px] text-(--ink-500)">
              Queda ligado a su caso <span className="font-mono">{caso}</span>.
            </p>
          )}

          <Campo etiqueta="¿Qué problema era?" ayuda="En una línea, como lo buscaría otro.">
            <input
              value={titulo}
              onChange={(e) => setTitulo(e.target.value)}
              placeholder="La tabla de aprobaciones sale vacía al filtrar por fecha"
              className="w-full rounded-xl border border-(--line) bg-(--surface-2) px-4 py-2.5 text-[14px] outline-none focus:border-(--accent)"
            />
          </Campo>

          <Campo etiqueta="Detalle del problema" ayuda="Opcional. Cuándo pasaba, qué se veía.">
            <textarea
              value={problema}
              onChange={(e) => setProblema(e.target.value)}
              rows={3}
              className="w-full resize-none rounded-xl border border-(--line) bg-(--surface-2) px-4 py-2.5 text-[14px] leading-relaxed outline-none focus:border-(--accent)"
            />
          </Campo>

          <Campo etiqueta="Qué hizo para resolverlo" ayuda="Los pasos, en orden. Es lo que va a seguir el siguiente.">
            <textarea
              value={pasos}
              onChange={(e) => setPasos(e.target.value)}
              rows={6}
              placeholder={'1. Entré a Configuración → Periodos\n2. El periodo del mes estaba cerrado\n3. Lo reabrí y la tabla volvió a cargar'}
              className="w-full resize-none rounded-xl border border-(--line) bg-(--surface-2) px-4 py-3 text-[14px] leading-relaxed outline-none focus:border-(--accent)"
            />
          </Campo>

          <Campo
            etiqueta="Sentencia que usó"
            ayuda="Opcional. Va como texto: nadie la ejecuta desde aquí, soporte la revisa antes."
          >
            <textarea
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              rows={3}
              spellCheck={false}
              className="w-full resize-none rounded-xl border border-(--line) bg-(--surface-2) px-4 py-2.5 font-mono text-[12.5px] leading-relaxed outline-none focus:border-(--accent)"
            />
            {sql.trim() && (
              <input
                value={base}
                onChange={(e) => setBase(e.target.value)}
                placeholder="¿Sobre qué base? (opcional)"
                className="mt-2 w-full rounded-xl border border-(--line) bg-(--surface-2) px-4 py-2 text-[13px] outline-none focus:border-(--accent)"
              />
            )}
          </Campo>

          <div className="flex flex-wrap items-center gap-3">
            <button
              type="button"
              onClick={() => enviar.mutate()}
              disabled={!listo || enviar.isPending}
              className="inline-flex h-10 items-center gap-2 rounded-lg bg-(--accent) px-4 text-sm font-semibold text-white transition hover:opacity-90 disabled:opacity-40"
            >
              <Send className="size-4" />
              {enviar.isPending ? 'Enviando…' : 'Enviar para revisión'}
            </button>
            <span className="text-[12.5px] text-(--ink-500)">
              {listo ? 'No se publica hasta que soporte lo apruebe.' : 'Falta el título o los pasos.'}
            </span>
          </div>

          {enviar.isError && (
            <p className="text-[12.5px] text-rose-600">No se pudo enviar: {(enviar.error as Error).message}</p>
          )}
        </div>
      </section>

      {mios && mios.length > 0 && (
        <section className="border-t border-(--line) pt-6">
          <h2 className="text-[13px] font-semibold tracking-wide text-(--ink-500) uppercase">Lo que ha enviado</h2>
          <div className="mt-3 space-y-2">
            {mios.map((a) => (
              <MiAporte key={a.id} aporte={a} />
            ))}
          </div>
        </section>
      )}
    </div>
  );
}

function Campo({ etiqueta, ayuda, children }: { etiqueta: string; ayuda?: string; children: ReactNode }) {
  return (
    <label className="block">
      <span className="text-[13px] font-medium text-(--ink-900)">{etiqueta}</span>
      {ayuda && <span className="mt-0.5 block text-[12px] text-(--ink-500)">{ayuda}</span>}
      <div className="mt-1.5">{children}</div>
    </label>
  );
}

/** En qué quedó un aporte. El motivo del rechazo se muestra: no se rechaza en silencio. */
function MiAporte({ aporte }: { aporte: Aporte }) {
  const estados = {
    pendiente: { icono: Clock, color: 'text-amber-600', texto: 'En revisión' },
    aceptado: { icono: CheckCircle2, color: 'text-emerald-600', texto: 'Publicado' },
    rechazado: { icono: XCircle, color: 'text-rose-600', texto: 'No se publicó' },
  } as const;
  const { icono: Icono, color, texto } = estados[aporte.estado] ?? estados.pendiente;

  return (
    <div className="rounded-xl border border-(--line) bg-(--surface-2) px-4 py-3">
      <div className="flex items-start gap-2.5">
        <Icono className={`mt-0.5 size-4 shrink-0 ${color}`} />
        <div className="min-w-0 flex-1">
          <p className="text-[13.5px] font-medium text-(--ink-900)">{aporte.titulo}</p>
          <p className="mt-0.5 text-[12px] text-(--ink-500)">
            {texto}
            {aporte.revisado_por ? ` · revisó ${aporte.revisado_por}` : ''}
          </p>
          {aporte.nota && <p className="mt-1.5 text-[12.5px] text-(--ink-700)">{aporte.nota}</p>}
        </div>
      </div>
    </div>
  );
}

// ─── Cabecera ────────────────────────────────────────────────────────────────

/**
 * Más liviana que la del panel: aquella trae acciones que aquí no existen, y una
 * barra con botones que no llevan a ningún sitio es peor que no tenerla.
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
