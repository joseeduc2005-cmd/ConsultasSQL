'use client';

import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { BookOpen, Search, ShieldAlert, ShieldCheck } from 'lucide-react';
import { useDeferredValue, useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { EncabezadoPagina } from '@/soporte/components/layout/encabezado';
import { Badge } from '@/soporte/components/ui/badge';
import { Card } from '@/soporte/components/ui/card';
import { CodeBlock } from '@/soporte/components/ui/code-block';
import { Cargando, ErrorCarga, Vacio } from '@/soporte/components/ui/estados';
import { Input, Select } from '@/soporte/components/ui/form';
import { Segmented } from '@/soporte/components/ui/segmented';
import { api } from '@/soporte/lib/api';
import { claves } from '@/soporte/lib/consultas';
import type { ConsultaBiblioteca } from '@/soporte/lib/types';
import { cn } from '@/soporte/lib/utils';

type FiltroRiesgo = 'todas' | 'lectura' | 'escritura';

export function PaginaBiblioteca() {
  const params = useSearchParams();
  const router = useRouter();
  const rutaActual = usePathname();
  const [texto, setTexto] = useState('');
  const [categoria, setCategoria] = useState('');
  const [riesgo, setRiesgo] = useState<FiltroRiesgo>('todas');
  const busqueda = useDeferredValue(texto);
  const seleccion = params.get('codigo');

  const { data, isLoading, error } = useQuery({
    queryKey: claves.consultas({ busqueda, categoria, riesgo }),
    queryFn: () => api.consultas({ q: busqueda, categoria, riesgo: riesgo === 'todas' ? '' : riesgo }),
    placeholderData: keepPreviousData,
  });
  const { data: todas } = useQuery({ queryKey: claves.consultas(), queryFn: () => api.consultas() });

  const lista = data?.consultas ?? [];
  const activa = todas?.consultas.find((c) => c.codigo === seleccion) ?? lista[0];
  // replace en vez de push: elegir consultas no debe llenar el historial del navegador.
  const seleccionar = (codigo: string) =>
    router.replace(`${rutaActual}?codigo=${encodeURIComponent(codigo)}`, { scroll: false });

  const elementoActivo = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    elementoActivo.current?.scrollIntoView({ block: 'nearest' });
  }, [seleccion, todas]);

  return (
    <>
      <EncabezadoPagina
        titulo="Biblioteca SQL"
        descripcion="Consultas catalogadas por el equipo. Las de escritura se ejecutan solo con aprobación y por el responsable indicado."
      />

      <div className="mb-4 flex flex-wrap items-center gap-3">
        <div className="relative w-full sm:w-80">
          <Search className="absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input value={texto} onChange={(e) => setTexto(e.target.value)} placeholder="Buscar por código, nombre, tabla o ticket" className="pl-9" />
        </div>
        <div className="w-52">
          <Select value={categoria} onChange={(e) => setCategoria(e.target.value)} aria-label="Categoría">
            <option value="">Todas las categorías</option>
            {todas?.categorias.map((c) => (
              <option key={c.categoria} value={c.categoria}>
                {c.categoria} ({c.total})
              </option>
            ))}
          </Select>
        </div>
        <Segmented<FiltroRiesgo>
          valor={riesgo}
          onChange={setRiesgo}
          opciones={[
            { valor: 'todas', etiqueta: 'Todas' },
            { valor: 'lectura', etiqueta: 'Lectura' },
            { valor: 'escritura', etiqueta: 'Escritura' },
          ]}
        />
        <span className="tabular ml-auto text-[13px] text-muted-foreground">{lista.length} consultas</span>
      </div>

      {error && <ErrorCarga error={error} />}
      <div className="grid items-start gap-6 lg:grid-cols-[380px_minmax(0,1fr)]">
        <Card className="scroll-fino overflow-hidden lg:max-h-[calc(100dvh-15rem)] lg:overflow-y-auto">
          {isLoading ? (
            <Cargando />
          ) : lista.length === 0 ? (
            <Vacio icono={<BookOpen />} titulo="Sin resultados" descripcion="Pruebe con otro término o categoría." />
          ) : (
            <ul className="divide-y">
              {lista.map((c) => {
                const esActiva = activa?.codigo === c.codigo;
                return (
                  <li key={c.codigo}>
                    <button
                      ref={esActiva ? elementoActivo : undefined}
                      type="button"
                      onClick={() => seleccionar(c.codigo)}
                      className={cn('flex w-full items-start gap-3 px-4 py-3 text-left transition-colors', esActiva ? 'bg-primary-soft/60' : 'hover:bg-surface-muted/60')}
                    >
                      <span
                        className={cn(
                          'mt-0.5 w-11 shrink-0 rounded-md py-0.5 text-center font-mono text-[11px] font-semibold',
                          c.riesgo === 'escritura' ? 'bg-danger-soft text-danger' : 'bg-surface-muted text-muted-foreground',
                        )}
                      >
                        {c.codigo}
                      </span>
                      <span className="min-w-0 flex-1">
                        <span className={cn('line-clamp-2 text-[13.5px] leading-snug', esActiva ? 'font-semibold' : 'font-medium')}>{c.nombre}</span>
                        <span className="mt-0.5 block text-xs text-muted-foreground">{c.categoria}</span>
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </Card>

        {activa ? <DetalleConsulta consulta={activa} /> : null}
      </div>
    </>
  );
}

function DetalleConsulta({ consulta: c }: { consulta: ConsultaBiblioteca }) {
  const tickets = c.tickets?.split(/[,\s]+/).filter(Boolean) ?? [];
  return (
    <Card className="animate-entrada p-6" key={c.codigo}>
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-mono text-sm font-semibold text-primary">{c.codigo}</span>
        {c.categoria && <Badge>{c.categoria}</Badge>}
        {c.tipo && <Badge tono="info">{c.tipo}</Badge>}
        {c.riesgo === 'escritura' ? (
          <Badge tono="danger">
            <ShieldAlert className="size-3.5" />
            Escritura
          </Badge>
        ) : (
          <Badge tono="success">
            <ShieldCheck className="size-3.5" />
            Lectura
          </Badge>
        )}
        {c.sistema && <Badge tono="primary">{c.sistema}</Badge>}
      </div>
      <h3 className="mt-3 text-lg font-semibold tracking-tight">{c.nombre}</h3>

      {c.riesgo === 'escritura' && (
        <div className="mt-4 flex gap-3 rounded-lg border border-danger/25 bg-danger-soft/60 p-3 text-[13px]">
          <ShieldAlert className="mt-0.5 size-4 shrink-0 text-danger" />
          <p>Modifica datos. Verifique primero con la consulta de lectura correspondiente y solicite la ejecución a Administración de Aplicaciones con el ticket asociado.</p>
        </div>
      )}

      <CodeBlock codigo={c.sql} className="mt-4" maxAlto="max-h-[28rem]" tono={c.riesgo === 'escritura' ? 'peligro' : 'normal'} />

      <dl className="mt-5 grid gap-4 text-[13.5px] sm:grid-cols-2">
        {c.parametros && (
          <div>
            <dt className="text-xs font-semibold tracking-wide text-muted-foreground uppercase">Parámetros</dt>
            <dd className="mt-1 leading-relaxed whitespace-pre-line">{c.parametros}</dd>
          </div>
        )}
        {c.uso && (
          <div>
            <dt className="text-xs font-semibold tracking-wide text-muted-foreground uppercase">Uso</dt>
            <dd className="mt-1 leading-relaxed whitespace-pre-line">{c.uso}</dd>
          </div>
        )}
        {tickets.length > 0 && (
          <div className="sm:col-span-2">
            <dt className="text-xs font-semibold tracking-wide text-muted-foreground uppercase">Tickets relacionados</dt>
            <dd className="mt-1.5 flex flex-wrap gap-1.5">
              {tickets.map((t) => (
                <Link key={t} href={`/soporte/tickets/${t}`} className="rounded-md bg-surface-muted px-2 py-0.5 font-mono text-xs hover:bg-primary-soft hover:text-primary-soft-foreground">
                  {t}
                </Link>
              ))}
            </dd>
          </div>
        )}
      </dl>
    </Card>
  );
}
