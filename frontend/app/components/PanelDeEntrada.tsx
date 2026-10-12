'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Inbox, Search, TrendingUp } from 'lucide-react';
import type { KnowledgeArticle } from '../types';

/**
 * Lo que se ve al entrar al panel, antes de elegir un artículo.
 *
 * Aquí había un icono gris y la frase «Selecciona un artículo del menú»: media pantalla
 * para repetir lo que ya se ve, porque el menú está justo al lado. El sitio más visible
 * del panel, ocupado por una instrucción que nadie necesita.
 *
 * Lo que sí hace falta saber al entrar son tres cosas, y las tres se pueden hacer clic:
 *
 *   · qué espera a alguien — los aportes sin revisar son la bandeja que, si nadie la
 *     mira, se queda sin mirar;
 *   · qué usa el equipo de verdad — los temas más ejecutados, que además es la forma más
 *     rápida de llegar a uno;
 *   · qué falta por escribir — lo que la gente buscó y no encontró. Ese es el dato que
 *     dice qué tema hay que crear, y hasta ahora vivía enterrado en una pantalla de
 *     aportes que casi nadie abre.
 */
interface Resumen {
  aportesPendientes: number;
  temas: number;
  tickets: { abiertos: number; investigacion: number; resueltos: number };
}

interface Ejecucion {
  article_id: string;
  article_titulo: string;
}

interface Fallida {
  texto: string;
  veces: number;
  resultados: number;
}

async function pedir<T>(ruta: string): Promise<T | null> {
  try {
    const token = localStorage.getItem('token');
    const rol = localStorage.getItem('userRole') || 'user';
    const res = await fetch(ruta, {
      headers: { 'x-user-role': rol, ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    });
    if (!res.ok) return null;
    const datos = await res.json();
    return (datos?.data ?? datos) as T;
  } catch {
    return null;
  }
}

export default function PanelDeEntrada({
  articulos,
  onAbrir,
}: {
  articulos: KnowledgeArticle[];
  onAbrir: (a: KnowledgeArticle) => void;
}) {
  const router = useRouter();
  const [resumen, setResumen] = useState<Resumen | null>(null);
  const [masUsados, setMasUsados] = useState<{ id: string; titulo: string; veces: number }[]>([]);
  const [fallidas, setFallidas] = useState<Fallida[]>([]);

  useEffect(() => {
    let vigente = true;
    (async () => {
      const [r, ejecuciones, sinRespuesta] = await Promise.all([
        pedir<Resumen>('/api/soporte/resumen'),
        pedir<Ejecucion[]>('/api/article-executions?limit=400'),
        pedir<Fallida[]>('/api/soporte/busquedas-fallidas?dias=30'),
      ]);
      if (!vigente) return;
      if (r) setResumen(r);

      // Las ejecuciones llegan una por una; lo que interesa es cuál se usa, no cuándo.
      if (Array.isArray(ejecuciones)) {
        const cuenta = new Map<string, { titulo: string; veces: number }>();
        for (const e of ejecuciones) {
          if (!e.article_id) continue;
          const previo = cuenta.get(e.article_id);
          cuenta.set(e.article_id, {
            titulo: e.article_titulo || previo?.titulo || 'Sin título',
            veces: (previo?.veces ?? 0) + 1,
          });
        }
        setMasUsados(
          [...cuenta.entries()]
            .map(([id, v]) => ({ id, ...v }))
            .sort((a, b) => b.veces - a.veces)
            .slice(0, 5),
        );
      }
      if (Array.isArray(sinRespuesta)) setFallidas(sinRespuesta.slice(0, 5));
    })();
    return () => { vigente = false; };
  }, []);

  const abrirPorId = (id: string) => {
    const a = articulos.find((x) => String(x.id) === String(id));
    if (a) onAbrir(a);
  };

  return (
    <section className="glass-panel-strong flex-1 min-w-0 overflow-y-auto rounded-2xl p-6 xl:p-8">
      <div className="mx-auto w-full max-w-3xl space-y-8">
        <div>
          <h2 className="text-[22px] font-semibold tracking-[-0.021em] text-(--ink-900)">
            Qué hay hoy
          </h2>
          <p className="mt-1 text-[14px] text-(--ink-600)">
            Elija un tema del menú, o empiece por aquí.
          </p>
        </div>

        {/* Lo que espera a alguien. Si nadie mira la bandeja, se queda sin mirar. */}
        <Bloque icono={<Inbox className="size-4" />} titulo="Esperando a alguien">
          <div className="grid gap-2 sm:grid-cols-3">
            <Dato
              valor={resumen?.aportesPendientes}
              etiqueta="aportes por revisar"
              urgente={Boolean(resumen?.aportesPendientes)}
              onClick={() => router.push('/admin/aportes')}
            />
            <Dato
              valor={resumen?.tickets.abiertos}
              etiqueta="tickets sin investigar"
              urgente={Boolean(resumen?.tickets.abiertos)}
              onClick={() => router.push('/admin/tickets?estado=abierto')}
            />
            <Dato
              valor={resumen?.tickets.investigacion}
              etiqueta="en investigación"
              onClick={() => router.push('/admin/tickets?estado=investigacion')}
            />
          </div>
        </Bloque>

        {/* La forma más rápida de llegar a un tema es el que ya se usa. */}
        <Bloque icono={<TrendingUp className="size-4" />} titulo="Lo que más se consulta">
          {masUsados.length === 0 ? (
            <Vacio>
              Todavía no se ha ejecutado ningún tema. En cuanto el equipo empiece a usarlos,
              los de siempre aparecen aquí.
            </Vacio>
          ) : (
            <ul className="divide-y divide-(--line)">
              {masUsados.map((t) => (
                <li key={t.id}>
                  <button
                    type="button"
                    onClick={() => abrirPorId(t.id)}
                    className="flex w-full items-center gap-3 py-2.5 text-left transition-colors hover:text-(--accent)"
                  >
                    <span className="min-w-0 flex-1 truncate text-[14px] text-(--ink-900)">{t.titulo}</span>
                    <span className="shrink-0 text-[13px] text-(--ink-500)">
                      {t.veces} {t.veces === 1 ? 'vez' : 'veces'}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </Bloque>

        {/*
          Lo que falta por escribir.
          Si algo se repite y no devuelve resultados, es un tema que no existe. Si
          devuelve resultados y nadie los abre, es una palabra que falta en el título de
          un tema que sí existe. Las dos cosas se arreglan escribiendo, y hasta ahora el
          dato vivía enterrado en una pantalla que casi nadie abre.
        */}
        <Bloque icono={<Search className="size-4" />} titulo="Buscaron y se fueron sin nada">
          {fallidas.length === 0 ? (
            <Vacio>
              Nadie se ha ido con las manos vacías en los últimos treinta días.
            </Vacio>
          ) : (
            <ul className="divide-y divide-(--line)">
              {fallidas.map((f) => (
                <li key={f.texto} className="flex items-baseline gap-3 py-2.5">
                  <span className="min-w-0 flex-1 text-[14px] text-(--ink-900)">«{f.texto}»</span>
                  <span className="shrink-0 text-[13px] text-(--ink-500)">
                    {f.veces}× · {f.resultados === 0 ? 'sin resultados' : 'nadie los abrió'}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </Bloque>
      </div>
    </section>
  );
}

function Bloque({ icono, titulo, children }: { icono: React.ReactNode; titulo: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="mb-2.5 flex items-center gap-2 text-(--ink-600)">
        {icono}
        <h3 className="text-[13px] font-semibold tracking-[0.02em] text-(--ink-600) uppercase">{titulo}</h3>
      </div>
      {children}
    </div>
  );
}

/**
 * Un número que se puede pulsar.
 *
 * Mientras no llega, el hueco se queda vacío en vez de poner un cero: un cero dice «no
 * hay nada que revisar», y eso puede ser falso todavía.
 */
function Dato({
  valor, etiqueta, urgente, onClick,
}: { valor?: number; etiqueta: string; urgente?: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="rounded-xl border border-(--line) px-4 py-3 text-left transition-colors hover:border-(--line-fuerte)"
    >
      <p className={`text-[26px] leading-none font-semibold ${urgente ? 'text-(--aviso)' : 'text-(--ink-900)'}`}>
        {valor === undefined ? <span className="text-(--ink-500)">—</span> : valor}
      </p>
      <p className="mt-1.5 text-[13px] text-(--ink-600)">{etiqueta}</p>
    </button>
  );
}

function Vacio({ children }: { children: React.ReactNode }) {
  return <p className="py-1 text-[14px] text-(--ink-500)">{children}</p>;
}
