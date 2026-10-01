'use client';

import { CheckCircle2, Clock, Database, Lightbulb, XCircle } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { api } from '@/soporte/lib/api';
import { Card } from '@/soporte/components/ui/card';
import type { Aporte, EstadoAporte } from '@/soporte/lib/types';

/**
 * Lo que proponen los usuarios, antes de ser tema de ayuda.
 *
 * La base de conocimiento se llenaba solo desde aquí dentro. Pero el que tiene el
 * problema muchas veces lo resuelve antes de que llegue la respuesta, y eso se perdía.
 * Ahora puede escribirlo, y esta pantalla es el filtro: nada que mande un usuario
 * aparece como tema hasta que alguien lo lea.
 *
 * Aceptar publica. Por eso el texto se puede corregir aquí mismo —el título con el que
 * lo van a buscar rara vez es el que escribió quien lo vivió— y por eso la categoría se
 * elige al aceptar y no se hereda de nada.
 *
 * Rechazar exige motivo. El autor lo ve: rechazar en silencio es la forma más rápida de
 * que nadie vuelva a aportar.
 */
export default function AportesPage() {
  const [filtro, setFiltro] = useState<EstadoAporte | 'todos'>('pendiente');
  const { data: aportes, isLoading } = useQuery({
    queryKey: ['aportes', filtro],
    queryFn: () => api.aportes(filtro),
  });

  return (
    <div className="mx-auto max-w-4xl space-y-5 py-2">
      <header>
        <h1 className="flex items-center gap-2 text-xl font-semibold tracking-tight">
          <Lightbulb className="size-5 text-(--accent)" />
          Aportes de los usuarios
        </h1>
        <p className="mt-1 text-[13.5px] text-muted-foreground">
          Soluciones que mandó quien tuvo el problema. Nada de esto está publicado: al aceptarlo se
          crea el tema de ayuda y pasa a la base de conocimiento.
        </p>
      </header>

      <div className="flex flex-wrap gap-1.5">
        {(['pendiente', 'aceptado', 'rechazado', 'todos'] as const).map((e) => (
          <button
            key={e}
            type="button"
            onClick={() => setFiltro(e)}
            className={`h-8 rounded-lg px-3 text-[12.5px] font-medium capitalize transition-colors ${
              filtro === e
                ? 'bg-(--accent) text-white'
                : 'border border-(--line) bg-(--surface-2) text-(--ink-700) hover:text-(--ink-900)'
            }`}
          >
            {e === 'todos' ? 'Todos' : `${e}s`}
          </button>
        ))}
      </div>

      {isLoading && <p className="text-[13.5px] text-muted-foreground">Cargando…</p>}

      {!isLoading && !aportes?.length && (
        <Card className="px-4 py-8 text-center text-[13.5px] text-muted-foreground">
          {filtro === 'pendiente' ? 'No hay nada por revisar.' : 'No hay aportes con ese estado.'}
        </Card>
      )}

      <div className="space-y-3">
        {aportes?.map((a) => (
          <Ficha key={a.id} aporte={a} />
        ))}
      </div>
    </div>
  );
}

function Ficha({ aporte }: { aporte: Aporte }) {
  const pendiente = aporte.estado === 'pendiente';
  const [abierto, setAbierto] = useState(pendiente);
  const [titulo, setTitulo] = useState(aporte.titulo);
  const [categoria, setCategoria] = useState('Aportes del equipo');
  const [visible, setVisible] = useState<'todos' | 'admin'>('todos');
  const [motivo, setMotivo] = useState('');
  const qc = useQueryClient();

  const refrescar = () => qc.invalidateQueries({ queryKey: ['aportes'] });

  const aceptar = useMutation({
    mutationFn: () => api.aceptarAporte(aporte.id, { titulo: titulo.trim(), categoria: categoria.trim(), visible }),
    onSuccess: refrescar,
  });

  const rechazar = useMutation({
    mutationFn: () => api.rechazarAporte(aporte.id, motivo.trim()),
    onSuccess: refrescar,
  });

  const estados = {
    pendiente: { Icono: Clock, color: 'text-amber-500', texto: 'Por revisar' },
    aceptado: { Icono: CheckCircle2, color: 'text-emerald-500', texto: 'Publicado' },
    rechazado: { Icono: XCircle, color: 'text-rose-500', texto: 'Rechazado' },
  } as const;
  const { Icono, color, texto } = estados[aporte.estado] ?? estados.pendiente;

  return (
    <Card className="overflow-hidden">
      <button
        type="button"
        onClick={() => setAbierto((v) => !v)}
        className="flex w-full items-start gap-3 px-4 py-3.5 text-left"
        aria-expanded={abierto}
      >
        <Icono className={`mt-0.5 size-4 shrink-0 ${color}`} />
        <span className="min-w-0 flex-1">
          <span className="block text-[14px] font-medium">{aporte.titulo}</span>
          <span className="mt-0.5 block text-[12px] text-muted-foreground">
            {texto} · lo mandó {aporte.autor}
            {aporte.caso ? ` · caso ${aporte.caso}` : ''}
            {aporte.pasos_json?.length ? ` · ${aporte.pasos_json.length} pasos` : ''}
            {aporte.sql_sugerido ? ' · trae script' : ''}
          </span>
        </span>
      </button>

      {abierto && (
        <div className="space-y-4 border-t border-(--line) px-4 py-4">
          {aporte.problema && (
            <Seccion titulo="El problema">
              <p className="text-[13.5px] leading-relaxed whitespace-pre-line">{aporte.problema}</p>
            </Seccion>
          )}

          <Seccion titulo="Qué hizo">
            {/*
              Paso a paso, con el script de cada uno donde iba. Los scripts se muestran,
              no se ofrecen para ejecutar: esto lo escribió alguien de fuera del equipo
              y nadie lo ha revisado todavía — leerlo es justamente el trabajo de esta
              pantalla. Si al final hay que dejar alguno ejecutable, se arma en la
              Biblioteca SQL, que es donde están las validaciones.
            */}
            {aporte.pasos_json?.length ? (
              <div className="space-y-2.5">
                {aporte.base && (
                  <span className="inline-flex items-center gap-1 rounded-full bg-(--surface-2) px-2 py-0.5 text-[11px] text-(--ink-700)">
                    <Database className="size-3" />
                    {aporte.base}
                  </span>
                )}
                {aporte.pasos_json.map((paso, i) => (
                  <div key={i} className="flex gap-2.5">
                    <span className="mt-0.5 grid size-5 shrink-0 place-items-center rounded-full bg-(--accent)/10 text-[11px] font-semibold text-(--accent)">
                      {i + 1}
                    </span>
                    <div className="min-w-0 flex-1">
                      <p className="text-[13.5px] leading-relaxed whitespace-pre-line">{paso.descripcion}</p>
                      {paso.script && (
                        <>
                          <span
                            className={`mt-1.5 inline-block rounded-full px-2 py-0.5 text-[11px] font-medium ${
                              paso.efecto === 'cambio'
                                ? 'bg-amber-500/15 text-amber-600'
                                : 'bg-(--surface-2) text-(--ink-700)'
                            }`}
                          >
                            {paso.efecto === 'cambio' ? 'Cambia datos' : 'Solo consulta'}
                          </span>
                          <pre className="mt-1.5 overflow-x-auto rounded-lg border border-(--line) bg-(--surface-2) px-3 py-2.5 text-[12.5px] leading-relaxed">
                            <code>{paso.script}</code>
                          </pre>
                        </>
                      )}
                    </div>
                  </div>
                ))}
                <p className="text-[12px] text-amber-600">
                  Revise los scripts antes de publicar. No se ejecutan desde aquí.
                </p>
              </div>
            ) : (
              <p className="text-[13.5px] leading-relaxed whitespace-pre-line">{aporte.pasos}</p>
            )}
          </Seccion>

          {!pendiente && aporte.nota && (
            <p className="rounded-lg border border-(--line) bg-(--surface-2) px-3 py-2 text-[12.5px]">
              <span className="text-muted-foreground">Nota de {aporte.revisado_por}:</span> {aporte.nota}
            </p>
          )}

          {pendiente && (
            <div className="space-y-3 border-t border-(--line) pt-4">
              <div className="grid gap-3 sm:grid-cols-2">
                <Etiqueta texto="Título con el que se publica">
                  <input
                    value={titulo}
                    onChange={(e) => setTitulo(e.target.value)}
                    className="w-full rounded-lg border border-(--line) bg-(--surface-2) px-3 py-2 text-[13px] outline-none focus:border-(--accent)"
                  />
                </Etiqueta>
                <Etiqueta texto="Categoría">
                  <input
                    value={categoria}
                    onChange={(e) => setCategoria(e.target.value)}
                    className="w-full rounded-lg border border-(--line) bg-(--surface-2) px-3 py-2 text-[13px] outline-none focus:border-(--accent)"
                  />
                </Etiqueta>
              </div>

              <Etiqueta texto="Quién lo puede ver">
                <select
                  value={visible}
                  onChange={(e) => setVisible(e.target.value as 'todos' | 'admin')}
                  className="w-full rounded-lg border border-(--line) bg-(--surface-2) px-3 py-2 text-[13px] outline-none focus:border-(--accent) sm:w-64"
                >
                  <option value="todos">Todos — sale como tema de ayuda</option>
                  <option value="admin">Solo el equipo de soporte</option>
                </select>
              </Etiqueta>

              <div className="flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={() => aceptar.mutate()}
                  disabled={aceptar.isPending || titulo.trim().length < 5}
                  className="inline-flex h-9 items-center gap-2 rounded-lg bg-(--accent) px-3.5 text-[13px] font-semibold text-white disabled:opacity-40"
                >
                  <CheckCircle2 className="size-4" />
                  {aceptar.isPending ? 'Publicando…' : 'Aceptar y publicar'}
                </button>

                <input
                  value={motivo}
                  onChange={(e) => setMotivo(e.target.value)}
                  placeholder="Motivo, si lo va a rechazar"
                  className="h-9 min-w-[200px] flex-1 rounded-lg border border-(--line) bg-(--surface-2) px-3 text-[13px] outline-none focus:border-(--accent)"
                />
                <button
                  type="button"
                  onClick={() => rechazar.mutate()}
                  disabled={rechazar.isPending || motivo.trim().length < 5}
                  className="inline-flex h-9 items-center gap-2 rounded-lg border border-(--line) bg-(--surface-2) px-3.5 text-[13px] font-semibold text-rose-600 disabled:opacity-40"
                  title="Hace falta un motivo: el autor lo va a ver"
                >
                  <XCircle className="size-4" />
                  Rechazar
                </button>
              </div>

              {(aceptar.isError || rechazar.isError) && (
                <p className="text-[12.5px] text-rose-600">
                  {((aceptar.error ?? rechazar.error) as Error)?.message}
                </p>
              )}
            </div>
          )}
        </div>
      )}
    </Card>
  );
}

function Seccion({ titulo, children }: { titulo: string; children: React.ReactNode }) {
  return (
    <div>
      <p className="mb-1.5 text-[11.5px] font-semibold tracking-wide text-muted-foreground uppercase">{titulo}</p>
      {children}
    </div>
  );
}

function Etiqueta({ texto, children }: { texto: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <span className="mb-1 block text-[12px] text-muted-foreground">{texto}</span>
      {children}
    </label>
  );
}
