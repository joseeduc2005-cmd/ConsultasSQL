'use client';

import {
  BookOpen,
  CheckCircle2,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Clock,
  Copy,
  Database,
  History,
  Lightbulb,
  Mic,
  MicOff,
  MessageSquarePlus,
  Plus,
  Search,
  Send,
  Trash2,
  XCircle,
} from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect, useRef, useState, type ReactNode } from 'react';
import CommentsSection from '../components/CommentsSection';
import { withAuth } from '../components/ProtectedRoute';
import ThemeToggle from '../components/ThemeToggle';
import UserIdentityBadge from '../components/UserIdentityBadge';
import { useTheme } from '../components/ThemeProvider';
import { Markdown, resumenPlano } from './markdown';
import { clienteApi, type Aporte, type PasoAporte, type PrecedentePublico } from './api';

/** Qué se está mirando en el panel de la derecha. */
type Vista =
  | { tipo: 'buscar' }
  | { tipo: 'articulo'; id: string }
  /** Compartir una solución propia, con el título que se venía escribiendo. */
  | { tipo: 'aportar'; titulo?: string };

/**
 * Lo que ve quien tiene el problema.
 *
 * Dos cosas, y nada más: buscar si ya se resolvió antes, y contar cómo lo resolvió uno
 * para que el siguiente lo encuentre. Antes había una tercera —abrir un caso cuando la
 * búsqueda no daba nada— y se quitó a propósito: lo que mantiene viva una base de
 * conocimiento es lo que la gente escribe en ella, no una cola de tickets.
 *
 * Misma forma que el panel de administración —índice a la izquierda, contenido a la
 * derecha— porque es la que ya conoce quien usa el sistema, y porque el contenido de
 * un artículo necesita ancho para leerse.
 */
function SoportePage() {
  const [vista, setVista] = useState<Vista>({ tipo: 'buscar' });
  // Abiertos de entrada. Plegados había que descubrirlos —una flecha suelta en el
  // borde— y la mitad de lo que aporta un tema está en lo que comentó el que ya pasó
  // por ahí. Quien no los quiera, los cierra y se queda cerrado mientras navega.
  const [comentarios, setComentarios] = useState(true);
  // La búsqueda vive aquí y no dentro de `Buscar`: al abrir un tema desde un resultado
  // ese componente se desmonta, y volver obligaba a reescribir todo. Lo normal es mirar
  // un resultado, ver que no sirve, y volver a la lista.
  const [consulta, setConsulta] = useState('');
  const [resultados, setResultados] = useState<PrecedentePublico[] | null>(null);

  return (
    // Alto fijo con scroll por columna, como el panel: así la página no crece por
    // debajo del pliegue dejando una franja vacía cuando el contenido es corto, y el
    // índice de la izquierda no se va de la pantalla al bajar por un artículo largo.
    <div className="app-shell flex h-screen flex-col overflow-hidden px-3 py-3 sm:px-4 sm:py-4">
      <Cabecera />

      <div className="mt-3 grid min-h-0 flex-1 gap-3 lg:grid-cols-[264px_minmax(0,1fr)]">
        <BarraLateral vista={vista} onElegir={setVista} />

        <div className="flex min-h-0 min-w-0 gap-3">
          <main className="glass-panel-strong min-w-0 flex-1 overflow-y-auto scroll-fino rounded-2xl px-5 py-5 sm:px-7">
            {/* El contenido no se estira a lo ancho sin límite: una línea de texto de
                punta a punta de un monitor ancho no hay quien la lea. */}
            <div className="mx-auto w-full max-w-3xl">
              {vista.tipo === 'buscar' && (
                <Buscar
                  onAbrir={setVista}
                  texto={consulta}
                  onTexto={setConsulta}
                  resultados={resultados}
                  onResultados={setResultados}
                />
              )}
              {vista.tipo === 'articulo' && (
                <Articulo
                  id={vista.id}
                  onVolver={resultados ? () => setVista({ tipo: 'buscar' }) : undefined}
                />
              )}
              {vista.tipo === 'aportar' && <Aportar titulo={vista.titulo} />}
            </div>
          </main>

          {/*
            Los comentarios van en su propia columna, como en el panel: dentro del
            artículo quedaban al final de un texto largo, y el botón de plegar se
            veía suelto en medio de la nada.
          */}
          {vista.tipo === 'articulo' && (
            <section
              className={`glass-panel-strong hidden shrink-0 overflow-y-auto scroll-fino rounded-2xl p-3 transition-[width] duration-300 lg:block ${
                comentarios ? 'w-[320px]' : 'w-[52px]'
              }`}
            >
              <PanelComentarios id={vista.id} plegado={!comentarios} onAlternar={() => setComentarios((v) => !v)} />
            </section>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── Barra lateral ───────────────────────────────────────────────────────────

function BarraLateral({ vista, onElegir }: { vista: Vista; onElegir: (v: Vista) => void }) {
  const { data: temas } = useQuery({ queryKey: ['temas'], queryFn: clienteApi.temas });
  const { data: aportes } = useQuery({ queryKey: ['mis-aportes'], queryFn: clienteApi.misAportes });
  const enRevision = (aportes ?? []).filter((a) => a.estado === 'pendiente').length;

  return (
    <aside className="glass-panel-strong min-h-0 space-y-4 overflow-y-auto scroll-fino rounded-2xl p-3">
      {/*
        Dos botones y no uno con pestañas: son dos intenciones distintas —vengo a que
        me ayuden, vengo a contar cómo lo arreglé— y quien entra a la segunda ya sabe
        lo que busca. Van juntos y arriba del todo, antes del árbol de temas.
      */}
      <div className="space-y-0.5">
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

        <button
          type="button"
          onClick={() => onElegir({ tipo: 'aportar' })}
          className={`flex w-full items-center gap-2.5 rounded-lg px-3 py-2.5 text-[13.5px] font-semibold transition-colors ${
            vista.tipo === 'aportar' ? 'bg-(--accent) text-white' : 'text-(--ink-900) hover:bg-(--surface-2)'
          }`}
        >
          <Lightbulb className="size-4" />
          Yo lo resolví
          {enRevision > 0 && (
            <span
              title={`${enRevision} esperando revisión`}
              className={`ml-auto rounded-full px-1.5 text-[10.5px] font-semibold ${
                vista.tipo === 'aportar' ? 'bg-white/25 text-white' : 'bg-amber-500/20 text-amber-600'
              }`}
            >
              {enRevision}
            </span>
          )}
        </button>
      </div>

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
    </aside>
  );
}

function Grupo({ titulo, children }: { titulo: string; children: ReactNode }) {
  return (
    <div>
      <p className="px-3 pt-1 pb-1.5 text-[10.5px] font-semibold tracking-[0.14em] text-(--ink-500) uppercase">
        {titulo}
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

// ─── Dictado ─────────────────────────────────────────────────────────────────

/**
 * Dictar el problema en vez de escribirlo.
 *
 * Usa el reconocimiento de voz del navegador, no una librería ni un servicio: ya viene
 * con Chrome y Edge, no hay nada que instalar y el audio no pasa por nuestro backend.
 * Donde no exista —Firefox, por ejemplo— el botón no se muestra en vez de salir roto.
 *
 * Lo dictado se añade a lo que ya hubiera escrito, no lo reemplaza: quien empieza a
 * teclear y termina hablando no pierde lo del principio.
 */
function useDictado(alTexto: (fragmento: string) => void) {
  const [grabando, setGrabando] = useState(false);
  const [disponible, setDisponible] = useState(false);
  const motor = useRef<any>(null);
  // El callback cambia en cada render; la referencia evita rearmar el reconocedor.
  const entregar = useRef(alTexto);
  entregar.current = alTexto;

  useEffect(() => {
    const Reconocimiento =
      (window as any).SpeechRecognition ?? (window as any).webkitSpeechRecognition;
    if (!Reconocimiento) return;

    const r = new Reconocimiento();
    r.lang = 'es-EC';
    r.continuous = true;
    // Sin resultados intermedios la caja se queda muerta mientras uno habla, y parece
    // que no está escuchando.
    r.interimResults = false;
    r.onresult = (e: any) => {
      for (let i = e.resultIndex; i < e.results.length; i++) {
        if (e.results[i].isFinal) entregar.current(e.results[i][0].transcript.trim());
      }
    };
    r.onend = () => setGrabando(false);
    r.onerror = () => setGrabando(false);

    motor.current = r;
    setDisponible(true);
    return () => {
      try {
        r.stop();
      } catch {
        /* ya estaba parado */
      }
    };
  }, []);

  const alternar = () => {
    const r = motor.current;
    if (!r) return;
    try {
      if (grabando) r.stop();
      else {
        r.start();
        setGrabando(true);
      }
    } catch {
      setGrabando(false);
    }
  };

  return { grabando, disponible, alternar };
}

/**
 * El botón se muestra siempre, también donde el navegador no sabe dictar.
 *
 * Antes se ocultaba, y eso dejaba a quien lo buscaba sin saber si el sistema no lo
 * tenía o si estaba roto. Mejor que esté y diga por qué no puede.
 */
function BotonDictado({
  grabando,
  disponible,
  alternar,
}: {
  grabando: boolean;
  disponible: boolean;
  alternar: () => void;
}) {
  if (!disponible) {
    return (
      <span
        title="Dictar necesita Chrome o Edge; su navegador no lo permite"
        className="inline-flex h-10 cursor-not-allowed items-center gap-2 rounded-lg border border-(--line) px-3.5 text-[13px] font-semibold text-(--ink-500) opacity-60"
      >
        <MicOff className="size-4" />
        Dictar (no disponible)
      </span>
    );
  }

  return (
    <button
      type="button"
      onClick={alternar}
      title={grabando ? 'Dejar de dictar' : 'Dictar el problema'}
      aria-pressed={grabando}
      className={`inline-flex h-10 items-center gap-2 rounded-lg border px-3.5 text-[13px] font-semibold transition-colors ${
        grabando
          ? 'border-rose-500/40 bg-rose-500/10 text-rose-600'
          : 'border-(--line) bg-(--surface-2) text-(--ink-900) hover:border-(--accent)'
      }`}
    >
      {grabando ? <MicOff className="size-4 animate-pulse" /> : <Mic className="size-4" />}
      {grabando ? 'Escuchando…' : 'Dictar'}
    </button>
  );
}

// ─── Mi problema ─────────────────────────────────────────────────────────────

function Buscar({
  onAbrir,
  texto,
  onTexto,
  resultados,
  onResultados,
}: {
  onAbrir: (v: Vista) => void;
  texto: string;
  onTexto: (v: string) => void;
  resultados: PrecedentePublico[] | null;
  onResultados: (v: PrecedentePublico[]) => void;
}) {
  const buscar = useMutation({
    mutationFn: () => clienteApi.solucionar(texto),
    onSuccess: (r) => onResultados(r.precedentes),
  });

  // Lo dictado se añade a lo que haya, separado por un espacio.
  const dictado = useDictado((fragmento) => onTexto(texto ? `${texto} ${fragmento}` : fragmento));

  const listo = texto.trim().length >= 10;

  return (
    <div className="space-y-6">
      <section>
        <h1 className="text-xl font-semibold tracking-tight text-(--ink-900)">¿Qué problema tiene?</h1>
        <p className="mt-1 text-[14px] text-(--ink-700)">
          Descríbalo con sus palabras. Buscamos si ya se resolvió antes.
        </p>

        <textarea
          value={texto}
          onChange={(e) => onTexto(e.target.value)}
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
          <BotonDictado grabando={dictado.grabando} disponible={dictado.disponible} alternar={dictado.alternar} />
          <span className="text-[12.5px] text-(--ink-500)">
            {dictado.grabando
              ? 'Hable con naturalidad; pulse otra vez para terminar'
              : !dictado.disponible
                ? 'El dictado necesita Chrome o Edge'
                : listo
                  ? 'Ctrl + Enter para buscar'
                  : 'Escriba o dicte su problema'}
          </span>
        </div>
      </section>

      {resultados !== null && (
        <section className="space-y-3">
          {resultados.length > 0 ? (
            <>
              {/*
                El encabezado dice la verdad en vez de prometer siempre lo mismo: si lo
                mejor que hay es un parecido flojo, anunciarlo como «esto es lo suyo»
                hace perder más tiempo que no devolver nada.
              */}
              <h2 className="text-[13px] font-semibold tracking-wide text-(--ink-500) uppercase">
                {resultados[0].confianza === 'baja' ? 'Quizá tenga que ver, revíselo' : 'Esto se parece a lo suyo'}
              </h2>
              {resultados.map((p) => (
                <Resultado
                  key={`${p.tipo}-${p.id}`}
                  precedente={p}
                  onAbrir={
                    p.tipo === 'articulo'
                      ? () => {
                          // Abrir un resultado es la mejor respuesta que hay a «esta
                          // forma de decirlo, ¿con qué tema se resuelve?». Se apunta
                          // antes de navegar, sin esperar: si falla, no se nota.
                          void clienteApi.temaAbierto(p.id, texto);
                          onAbrir({ tipo: 'articulo', id: p.id });
                        }
                      : undefined
                  }
                />
              ))}
            </>
          ) : (
            <p className="rounded-xl border border-(--line) bg-(--surface-2) px-4 py-6 text-center text-[14px] text-(--ink-700)">
              No encontramos nada parecido todavía.
            </p>
          )}

          {/*
            Aparece también cuando hubo resultados: que salgan no significa que sirvan.
            Y lleva a aportar, no a abrir un caso — si lo acaba resolviendo, eso es lo
            que falta en la base de conocimiento, y el texto que ya escribió se arrastra
            como título para no pedírselo dos veces.
          */}
          <div className="rounded-xl border border-(--line) bg-(--surface-2) px-4 py-4">
            <p className="text-[14px] font-medium text-(--ink-900)">
              {resultados.length > 0 ? '¿Ninguna le sirve?' : '¿Lo resolvió por su cuenta?'}
            </p>
            <p className="mt-0.5 text-[13px] text-(--ink-700)">
              Si da con la solución, cuéntela aquí y queda como tema de ayuda para el que venga detrás.
            </p>
            <button
              type="button"
              onClick={() => onAbrir({ tipo: 'aportar', titulo: primeraLinea(texto) })}
              className="mt-3 inline-flex h-9 items-center gap-2 rounded-lg border border-(--line) bg-(--surface-2) px-3.5 text-[13px] font-semibold text-(--ink-900) transition-colors hover:border-(--accent)"
            >
              <Lightbulb className="size-4" />
              Aportar mi solución
            </button>
          </div>
        </section>
      )}
    </div>
  );
}

/**
 * Un resultado de la búsqueda.
 *
 * Si es un tema de ayuda, al pulsarlo se abre el tema entero: la tarjeta solo enseña un
 * recorte, y lo que hace falta para resolver —los pasos completos, la sentencia con su
 * botón de copiar, los comentarios de quien ya pasó por ahí— está en el tema.
 *
 * Si es un ticket viejo no hay a dónde ir desde aquí, así que se despliega en el sitio.
 */
function Resultado({ precedente: p, onAbrir }: { precedente: PrecedentePublico; onAbrir?: () => void }) {
  const [abierto, setAbierto] = useState(false);
  const cuerpo = p.solucion || p.detalle;
  const navegable = Boolean(onAbrir);

  return (
    <div
      className={`overflow-hidden rounded-xl border border-(--line) bg-(--surface-2) transition-colors ${
        navegable ? 'hover:border-(--accent)' : ''
      }`}
    >
      <button
        type="button"
        onClick={() => (onAbrir ? onAbrir() : setAbierto((v) => !v))}
        className="flex w-full items-start gap-3 px-4 py-3.5 text-left"
        aria-expanded={navegable ? undefined : abierto}
      >
        <span className="mt-0.5 grid size-8 shrink-0 place-items-center rounded-lg bg-(--accent)/10 text-(--accent)">
          {p.tipo === 'ticket' ? <History className="size-4" /> : <BookOpen className="size-4" />}
        </span>
        <span className="min-w-0 flex-1">
          <span className="block text-[14px] font-medium text-(--ink-900)">{p.titulo}</span>
          {!abierto && cuerpo && (
            <span className="mt-0.5 line-clamp-2 block text-[12.5px] text-(--ink-700)">{resumenPlano(cuerpo)}</span>
          )}
          {navegable && (
            <span className="mt-1.5 inline-flex items-center gap-1 text-[12px] font-medium text-(--accent)">
              Ver el tema completo
              <ChevronRight className="size-3.5" />
            </span>
          )}
        </span>
      </button>
      {abierto && !navegable && (
        <div className="border-t border-(--line) px-4 py-4">
          {cuerpo ? (
            <Markdown contenido={cuerpo} />
          ) : (
            <p className="text-[13.5px] text-(--ink-700)">Este caso no dejó una explicación escrita.</p>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Un tema ─────────────────────────────────────────────────────────────────

function Articulo({ id, onVolver }: { id: string; onVolver?: () => void }) {
  const { data, isLoading } = useQuery({ queryKey: ['tema', id], queryFn: () => clienteApi.tema(id) });

  if (isLoading) return <p className="text-[14px] text-(--ink-500)">Cargando…</p>;
  if (!data) return <p className="text-[14px] text-(--ink-700)">No se encontró ese tema.</p>;

  return (
    <article>
      {/* Solo cuando se llegó desde una búsqueda: si no, no hay a dónde volver. */}
      {onVolver && (
        <button
          type="button"
          onClick={onVolver}
          className="mb-3 inline-flex items-center gap-1 text-[12.5px] font-medium text-(--ink-500) transition-colors hover:text-(--ink-900)"
        >
          <ChevronLeft className="size-3.5" />
          Volver a los resultados
        </button>
      )}
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
      {data.sentencia && (
        <section className="mt-6 border-t border-(--line) pt-5">
          <div className="flex flex-wrap items-center gap-2">
            <p className="text-[11.5px] font-semibold tracking-wide text-(--ink-500) uppercase">
              {data.sentencia.lenguaje === 'sql' ? 'Sentencia' : 'Instrucción'}
            </p>
            {data.base && (
              <span className="inline-flex items-center gap-1 rounded-full bg-(--surface-2) px-2 py-0.5 text-[11px] text-(--ink-700)">
                <Database className="size-3" />
                {data.base}
              </span>
            )}
            <button
              type="button"
              onClick={() => navigator.clipboard?.writeText(data.sentencia!.texto)}
              className="ml-auto inline-flex items-center gap-1.5 rounded-lg border border-(--line) px-2.5 py-1 text-[11.5px] text-(--ink-700) transition-colors hover:text-(--ink-900)"
            >
              <Copy className="size-3" />
              Copiar
            </button>
          </div>
          <pre className="mt-2 overflow-x-auto rounded-xl border border-(--line) bg-(--surface-2) px-4 py-3 text-[12.5px] leading-relaxed text-(--ink-900)">
            <code>{data.sentencia.texto}</code>
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

// ─── Compartir una solución ──────────────────────────────────────────────────

const PASO_VACIO: PasoAporte = { descripcion: '', script: null, efecto: null };

/**
 * Lo que uno resolvió por su cuenta, para que no se pierda.
 *
 * Va por pasos y no como un texto corrido porque así es como se arregla algo: una cosa
 * después de otra, y a veces con un script en medio. Cada paso dice si su script solo
 * mira o si cambia datos — lo declara quien lo escribe, no se adivina del SQL, porque
 * una consulta puede llamar a algo que escribe y un `UPDATE` puede acabar en rollback.
 *
 * No se publica al enviarlo. Queda pendiente hasta que alguien de soporte lo lea, y el
 * formulario lo dice antes de mandar, no después.
 */
function Aportar({ titulo: tituloInicial }: { titulo?: string }) {
  const [titulo, setTitulo] = useState(tituloInicial ?? '');
  const [problema, setProblema] = useState('');
  const [pasos, setPasos] = useState<PasoAporte[]>([{ ...PASO_VACIO }]);
  const [base, setBase] = useState('');
  const [enviado, setEnviado] = useState(false);
  const qc = useQueryClient();

  const { data: mios } = useQuery({ queryKey: ['mis-aportes'], queryFn: clienteApi.misAportes });
  const { data: bases } = useQuery({ queryKey: ['bases'], queryFn: clienteApi.bases });

  const conScript = pasos.some((p) => p.script?.trim());

  const cambiar = (i: number, cambios: Partial<PasoAporte>) =>
    setPasos((previos) => previos.map((p, j) => (j === i ? { ...p, ...cambios } : p)));

  const enviar = useMutation({
    mutationFn: () =>
      clienteApi.aportar({
        titulo: titulo.trim(),
        problema: problema.trim() || undefined,
        pasos: pasos
          .map((p) => ({
            descripcion: p.descripcion.trim(),
            script: p.script?.trim() || null,
            efecto: p.script?.trim() ? (p.efecto ?? 'consulta') : null,
          }))
          .filter((p) => p.descripcion || p.script),
        base: conScript ? base || undefined : undefined,
      }),
    onSuccess: () => {
      setEnviado(true);
      setTitulo('');
      setProblema('');
      setPasos([{ ...PASO_VACIO }]);
      setBase('');
      qc.invalidateQueries({ queryKey: ['mis-aportes'] });
    },
  });

  const escrito = pasos.map((p) => p.descripcion).join(' ').trim();
  const listo = titulo.trim().length >= 5 && escrito.length >= 20;

  return (
    <div className="space-y-8">
      <section>
        <h1 className="text-xl font-semibold tracking-tight text-(--ink-900)">¿Lo resolvió usted?</h1>
        <p className="mt-1 text-[14px] text-(--ink-700)">
          Cuéntelo paso a paso y soporte lo revisa. Si está bien, queda como tema de ayuda para el que
          tropiece con lo mismo.
        </p>

        {enviado && (
          <div className="mt-4 flex items-start gap-2.5 rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-3">
            <CheckCircle2 className="mt-0.5 size-4 shrink-0 text-emerald-600" />
            <p className="text-[13.5px] text-(--ink-900)">
              Enviado. Queda pendiente de revisión — abajo puede ver en qué va.
            </p>
          </div>
        )}

        <div className="mt-5 space-y-5">
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

          <div>
            <p className="text-[13px] font-medium text-(--ink-900)">Qué hizo, paso a paso</p>
            <p className="mt-0.5 text-[12px] text-(--ink-500)">
              Añada un paso por cada cosa que hizo. Si en alguno usó un script, péguelo ahí mismo.
            </p>

            <div className="mt-3 space-y-3">
              {pasos.map((paso, i) => (
                <Paso
                  key={i}
                  numero={i + 1}
                  paso={paso}
                  sePuedeQuitar={pasos.length > 1}
                  onCambiar={(cambios) => cambiar(i, cambios)}
                  onQuitar={() => setPasos((previos) => previos.filter((_, j) => j !== i))}
                />
              ))}
            </div>

            <button
              type="button"
              onClick={() => setPasos((previos) => [...previos, { ...PASO_VACIO }])}
              className="mt-3 inline-flex h-9 items-center gap-2 rounded-lg border border-dashed border-(--line) px-3.5 text-[13px] font-semibold text-(--ink-700) transition-colors hover:border-(--accent) hover:text-(--ink-900)"
            >
              <Plus className="size-4" />
              Agregar paso
            </button>
          </div>

          {/*
            La base solo se pregunta si hay algún script: sin script no hay base contra
            la que se hizo nada. Y se elige de una lista, no se escribe — quien reporta
            muchas veces no sabe cómo se llama por dentro.
          */}
          {conScript && (
            <Campo etiqueta="¿Sobre qué base?" ayuda="Solo salen las bases a las que el sistema llegó.">
              {bases?.length ? (
                <select
                  value={base}
                  onChange={(e) => setBase(e.target.value)}
                  className="w-full rounded-xl border border-(--line) bg-(--surface-2) px-4 py-2.5 text-[14px] outline-none focus:border-(--accent) sm:w-80"
                >
                  <option value="">Sin especificar</option>
                  {bases.map((b) => (
                    <option key={b.id} value={b.id}>
                      {b.etiqueta}
                      {b.motor ? ` — ${b.motor}` : ''}
                    </option>
                  ))}
                </select>
              ) : (
                <p className="text-[12.5px] text-(--ink-500)">
                  Ahora mismo el sistema no alcanza ninguna base. Mándelo igual y soporte lo completa.
                </p>
              )}
            </Campo>
          )}

          <div className="flex flex-wrap items-center gap-3 border-t border-(--line) pt-4">
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
              {listo ? 'No se publica hasta que soporte lo apruebe.' : 'Falta el título o describir los pasos.'}
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

function Paso({
  numero,
  paso,
  sePuedeQuitar,
  onCambiar,
  onQuitar,
}: {
  numero: number;
  paso: PasoAporte;
  sePuedeQuitar: boolean;
  onCambiar: (cambios: Partial<PasoAporte>) => void;
  onQuitar: () => void;
}) {
  const tieneScript = paso.script !== null;

  return (
    <div className="rounded-xl border border-(--line) bg-(--surface-2) px-4 py-3.5">
      <div className="flex items-center gap-2">
        <span className="grid size-6 shrink-0 place-items-center rounded-full bg-(--accent)/10 text-[11.5px] font-semibold text-(--accent)">
          {numero}
        </span>
        <span className="text-[12px] text-(--ink-500)">Paso {numero}</span>
        {sePuedeQuitar && (
          <button
            type="button"
            onClick={onQuitar}
            title="Quitar este paso"
            className="ml-auto rounded-lg p-1 text-(--ink-500) transition-colors hover:text-rose-600"
          >
            <Trash2 className="size-3.5" />
          </button>
        )}
      </div>

      <textarea
        value={paso.descripcion}
        onChange={(e) => onCambiar({ descripcion: e.target.value })}
        rows={2}
        placeholder="Qué hizo en este paso"
        className="mt-2 w-full resize-none rounded-lg border border-(--line) bg-(--surface) px-3 py-2 text-[13.5px] leading-relaxed outline-none focus:border-(--accent)"
      />

      {!tieneScript ? (
        <button
          type="button"
          onClick={() => onCambiar({ script: '', efecto: 'consulta' })}
          className="mt-2 text-[12.5px] font-medium text-(--accent) hover:underline"
        >
          + Añadir un script a este paso
        </button>
      ) : (
        <div className="mt-3 space-y-2">
          <textarea
            value={paso.script ?? ''}
            onChange={(e) => onCambiar({ script: e.target.value })}
            rows={3}
            spellCheck={false}
            placeholder="SELECT … / UPDATE …"
            className="w-full resize-none rounded-lg border border-(--line) bg-(--surface) px-3 py-2 font-mono text-[12.5px] leading-relaxed outline-none focus:border-(--accent)"
          />

          <div className="flex flex-wrap items-center gap-1.5">
            <span className="mr-1 text-[12px] text-(--ink-500)">Este script</span>
            <Opcion activo={paso.efecto !== 'cambio'} onClick={() => onCambiar({ efecto: 'consulta' })}>
              solo consulta
            </Opcion>
            <Opcion activo={paso.efecto === 'cambio'} onClick={() => onCambiar({ efecto: 'cambio' })} peligro>
              cambia datos
            </Opcion>
            <button
              type="button"
              onClick={() => onCambiar({ script: null, efecto: null })}
              className="ml-auto text-[12px] text-(--ink-500) hover:text-rose-600"
            >
              Quitar script
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function Opcion({
  activo,
  peligro,
  onClick,
  children,
}: {
  activo: boolean;
  peligro?: boolean;
  onClick: () => void;
  children: ReactNode;
}) {
  const encendido = peligro ? 'border-amber-500/40 bg-amber-500/15 text-amber-600' : 'border-(--accent)/40 bg-(--accent)/10 text-(--accent)';
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={activo}
      className={`rounded-full border px-2.5 py-0.5 text-[12px] font-medium transition-colors ${
        activo ? encendido : 'border-(--line) text-(--ink-500) hover:text-(--ink-700)'
      }`}
    >
      {children}
    </button>
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
  const pasos = aporte.pasos_json?.length ?? 0;

  return (
    <div className="rounded-xl border border-(--line) bg-(--surface-2) px-4 py-3">
      <div className="flex items-start gap-2.5">
        <Icono className={`mt-0.5 size-4 shrink-0 ${color}`} />
        <div className="min-w-0 flex-1">
          <p className="text-[13.5px] font-medium text-(--ink-900)">{aporte.titulo}</p>
          <p className="mt-0.5 text-[12px] text-(--ink-500)">
            {texto}
            {pasos > 0 ? ` · ${pasos} paso${pasos === 1 ? '' : 's'}` : ''}
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
    // `relative z-30` no es decorativo: `glass-panel-strong` lleva backdrop-filter, que
    // convierte esta cabecera en un contexto de apilado propio. El `z-80` del menú de
    // usuario queda encerrado dentro, así que sin elevar la cabecera entera el panel de
    // comentarios —que va después en el DOM— le pasaba por encima al desplegarlo.
    <div className="glass-panel-strong relative z-30 flex items-center gap-4 rounded-2xl border px-4 py-3 sm:px-5">
      <div className="flex size-10 shrink-0 items-center justify-center rounded-xl bg-linear-to-br from-[#2363eb] to-[#1aa0c8] text-[15px] font-bold text-white shadow-sm">
        S
      </div>
      <div className="min-w-0">
        <p className="truncate text-[16px] leading-tight font-semibold tracking-tight text-(--ink-900)">Soporte</p>
        <p className="truncate text-[12px] leading-tight text-(--ink-700)">
          Busque su problema, o cuente cómo lo resolvió.
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

/** El título sale de la primera frase de lo que ya escribió, para no pedirlo dos veces. */
function primeraLinea(texto: string): string {
  const limpio = texto.trim().replace(/\s+/g, ' ');
  const corte = limpio.search(/[.\n]/);
  const titulo = corte > 15 ? limpio.slice(0, corte) : limpio;
  return titulo.length > 120 ? `${titulo.slice(0, 117)}…` : titulo;
}

export default withAuth(SoportePage, ['user', 'admin', 'superadmin']);
