'use client';

import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ChevronDown, Plus, Search, Ticket as IconoTicket } from 'lucide-react';
import { useDeferredValue, useState } from 'react';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { EncabezadoPagina } from '@/soporte/components/layout/encabezado';
import { Badge } from '@/soporte/components/ui/badge';
import { Button } from '@/soporte/components/ui/button';
import { Card } from '@/soporte/components/ui/card';
import { cn } from '@/soporte/lib/utils';
import { Dialog } from '@/soporte/components/ui/dialog';
import { Cargando, ErrorCarga, Vacio } from '@/soporte/components/ui/estados';
import { Campo, Input, Select, Textarea } from '@/soporte/components/ui/form';
import { Segmented } from '@/soporte/components/ui/segmented';
import { useAvisos } from '@/soporte/components/ui/toast';
import { api } from '@/soporte/lib/api';
import { claves } from '@/soporte/lib/consultas';
import { ESTADO_TICKET, fechaCorta } from '@/soporte/lib/formato';
import type { EstadoTicket } from '@/soporte/lib/types';

// Los sistemas salen de los datos, no de una lista escrita a mano: el sistema sirve
// para cualquier aplicación, y una lista fija obligaba a tocar código para añadir una.
/**
 * Enlace al caso en el sistema de tickets de la organización, si hay uno configurado.
 *
 * Antes se armaba siempre contra el SIR: cualquier referencia numérica producía un
 * enlace a una URL de la cooperativa, aunque el caso fuera de otro sistema o de otra
 * organización. Sin `NEXT_PUBLIC_TICKETS_URL` no se inventa ningún enlace.
 */
function enlaceExterno(numero: string): string {
  const base = process.env.NEXT_PUBLIC_TICKETS_URL;
  return base && /^\d+$/.test(numero) ? `${base}${numero}` : '';
}

const SISTEMAS_BASE = ['Corresponsales', 'Ja Movil', 'Ja Web', 'Ja Pagos'];

/** Los de la base, más los de siempre, sin repetir y en orden alfabético. */
function useSistemas(): string[] {
  const { data } = useQuery({ queryKey: ['sistemas'], queryFn: api.sistemas, staleTime: 5 * 60_000 });
  const deLaBase = (data ?? []).map((s) => s.nombre);
  return [...new Set([...deLaBase, ...SISTEMAS_BASE])].sort((a, b) => a.localeCompare(b, 'es'));
}
type FiltroEstado = 'activos' | EstadoTicket | 'todos';

/**
 * Una fila de la bandeja, venga de donde venga.
 *
 * Las dos tablas siguen separadas por debajo: un ticket tiene numero, sistema, modulo,
 * prioridad y pasos de investigacion; un aporte tiene autor, problema y los pasos que esa
 * persona siguio. Meterlos en una tabla comun habria obligado a inventar columnas vacias
 * para la mitad de las filas. Lo que se une es lo que se ve.
 */
interface Fila {
  clave: string;
  origen: 'ticket' | 'aporte';
  titulo: string;
  referencia: string;
  detalle: string | null;
  sistema: string | null;
  estado: EstadoTicket;
  /** Lo que se escribe en la pastilla: un aporte no esta «abierto», esta «por revisar». */
  etiquetaEstado: string;
  prioridad: string | null;
  pasos: number;
  creado: string | null;
  destino: string;
}

function deTicket(t: {
  numero: string; titulo: string; modulo?: string | null; sistema?: string | null;
  estado: EstadoTicket; prioridad?: string | null; pasos: number; creado_en?: string | null;
}): Fila {
  return {
    clave: `t:${t.numero}`,
    origen: 'ticket',
    titulo: t.titulo,
    referencia: t.numero,
    detalle: t.modulo ?? null,
    sistema: t.sistema ?? null,
    estado: t.estado,
    etiquetaEstado: ESTADO_TICKET[t.estado].etiqueta,
    prioridad: t.prioridad ?? null,
    pasos: t.pasos,
    creado: t.creado_en ?? null,
    destino: `/admin/tickets/${t.numero}`,
  };
}

/**
 * Un aporte, como fila de la bandeja.
 *
 * El estado se traduce al del ticket para que la lista se pueda filtrar entera con los
 * mismos botones, pero la palabra que se ve es la del aporte: uno «por revisar» no esta
 * abierto, esta esperando una decision, y «publicado» no es lo mismo que «resuelto».
 */
function deAporte(a: {
  id: string; titulo: string; autor: string; estado: string; base?: string | null;
  pasos_json?: unknown[] | null; creado_en?: string | null;
}): Fila {
  const comoTicket: EstadoTicket =
    a.estado === 'pendiente' ? 'investigacion' : a.estado === 'aceptado' ? 'resuelto' : 'pendiente';
  const etiqueta =
    a.estado === 'pendiente' ? 'Por revisar' : a.estado === 'aceptado' ? 'Publicado' : 'Rechazado';
  return {
    clave: `a:${a.id}`,
    origen: 'aporte',
    titulo: a.titulo,
    referencia: `Aporte de ${a.autor}`,
    detalle: a.base ?? null,
    sistema: null,
    estado: comoTicket,
    etiquetaEstado: etiqueta,
    prioridad: null,
    pasos: Array.isArray(a.pasos_json) ? a.pasos_json.length : 0,
    creado: a.creado_en ?? null,
    destino: '/admin/aportes',
  };
}

function encaja(f: Fila, filtro: FiltroEstado): boolean {
  if (filtro === 'todos') return true;
  if (filtro === 'activos') return f.estado !== 'resuelto';
  return f.estado === filtro;
}

/** El filtro de texto y de sistema para lo que no filtra el backend. */
function coincide(f: Fila, texto: string, sistema: string): boolean {
  if (sistema) return false; // un aporte no tiene sistema: filtrar por uno lo excluye
  const aguja = texto.trim().toLowerCase();
  if (!aguja) return true;
  return `${f.titulo} ${f.referencia}`.toLowerCase().includes(aguja);
}

export function PaginaTickets() {
  const sistemas = useSistemas();
  const params = useSearchParams();
  const router = useRouter();
  const rutaActual = usePathname();
  const [texto, setTexto] = useState(params.get('q') ?? '');
  const [estado, setEstado] = useState<FiltroEstado>((params.get('estado') as FiltroEstado) ?? 'activos');
  const [sistema, setSistema] = useState(params.get('sistema') ?? '');
  const [nuevo, setNuevo] = useState(false);
  const busqueda = useDeferredValue(texto);

  const { data: todos } = useQuery({ queryKey: claves.tickets(), queryFn: () => api.tickets() });
  const { data, isLoading, error } = useQuery({
    queryKey: claves.tickets({ busqueda, sistema }),
    queryFn: () => api.tickets({ q: busqueda, sistema }),
    placeholderData: keepPreviousData,
  });

  // Los aportes entran en esta lista. Lo que llega de otra persona llega por un sitio, y
  // tener dos pantallas obligaba a acordarse de mirar la segunda.
  const { data: aportes } = useQuery({ queryKey: ['aportes', 'todos'], queryFn: () => api.aportes('todos') });

  const filasTodas = [...(todos ?? []).map(deTicket), ...(aportes ?? []).map(deAporte)];
  const filasVisibles = [...(data ?? []).map(deTicket), ...(aportes ?? []).map(deAporte)]
    // El filtro de texto lo aplica el backend a los tickets; los aportes se filtran aqui,
    // que es donde estan.
    .filter((f) => (f.origen === 'ticket' ? true : coincide(f, busqueda, sistema)))
    .sort((a, b) => String(b.creado ?? '').localeCompare(String(a.creado ?? '')));

  const cuenta = (e: FiltroEstado) => filasTodas.filter((f) => encaja(f, e)).length || undefined;
  const lista = filasVisibles.filter((f) => encaja(f, estado));

  const actualizarParams = (cambios: Record<string, string>) => {
    const p = new URLSearchParams(params.toString());
    Object.entries(cambios).forEach(([k, v]) => (v ? p.set(k, v) : p.delete(k)));
    router.replace(`${rutaActual}?${p}`, { scroll: false });
  };

  return (
    <>
      <EncabezadoPagina
        titulo="Casos y aportes"
        descripcion="Todo lo que llega: los casos que hay que investigar y las soluciones que manda el equipo, esperando revisión."
        acciones={
          <Button onClick={() => setNuevo(true)}>
            <Plus />
            Nuevo ticket
          </Button>
        }
      />

      <div className="mb-4 flex flex-wrap items-center gap-3">
        <Segmented<FiltroEstado>
          valor={estado}
          onChange={(v) => {
            setEstado(v);
            actualizarParams({ estado: v === 'activos' ? '' : v });
          }}
          opciones={[
            { valor: 'activos', etiqueta: 'Activos', conteo: cuenta('activos') },
            { valor: 'investigacion', etiqueta: 'En investigación', conteo: cuenta('investigacion') },
            { valor: 'resuelto', etiqueta: 'Resueltos', conteo: cuenta('resuelto') },
            { valor: 'todos', etiqueta: 'Todos', conteo: cuenta('todos') },
          ]}
        />
        <div className="w-44">
          <Select
            value={sistema}
            onChange={(e) => {
              setSistema(e.target.value);
              actualizarParams({ sistema: e.target.value });
            }}
            aria-label="Sistema"
          >
            <option value="">Todos los sistemas</option>
            {sistemas.map((s) => (
              <option key={s}>{s}</option>
            ))}
          </Select>
        </div>
        <div className="relative ml-auto w-full sm:w-72">
          <Search className="absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={texto}
            onChange={(e) => {
              setTexto(e.target.value);
              actualizarParams({ q: e.target.value });
            }}
            placeholder="Buscar por número, título o módulo"
            className="pl-9"
          />
        </div>
      </div>

      {error && <ErrorCarga error={error} />}
      <Card className="overflow-hidden">
        {isLoading ? (
          <Cargando />
        ) : !lista?.length ? (
          <Vacio icono={<IconoTicket />} titulo="Nada con estos filtros" descripcion="Cambie los filtros, o registre un caso nuevo." />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-surface-muted/60 text-left text-xs text-muted-foreground">
                <tr>
                  <th className="px-5 py-2.5 font-medium">Ticket</th>
                  <th className="px-3 py-2.5 font-medium">Sistema</th>
                  <th className="px-3 py-2.5 font-medium">Estado</th>
                  <th className="px-3 py-2.5 font-medium">Prioridad</th>
                  <th className="px-3 py-2.5 text-right font-medium">Pasos</th>
                  <th className="px-5 py-2.5 text-right font-medium">Creado</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {lista.map((f) => (
                  <tr key={f.clave} onClick={() => router.push(f.destino)} className="group cursor-pointer transition-colors hover:bg-surface-muted/50">
                    <td className="max-w-xl px-5 py-3">
                      <div className="flex items-center gap-2">
                        {/* De dónde viene. Sin esto, un aporte y un ticket se leerían igual,
                            y lo que hay que hacer con cada uno no se parece. */}
                        {f.origen === 'aporte' && (
                          <span className="shrink-0 rounded-md bg-surface-muted px-1.5 py-0.5 text-[11px] font-medium text-muted-foreground">
                            Aporte
                          </span>
                        )}
                        <p className="truncate font-medium group-hover:text-primary">{f.titulo}</p>
                      </div>
                      <p className="mt-0.5 truncate text-xs text-muted-foreground">
                        <span className={f.origen === 'ticket' ? 'font-mono' : ''}>{f.referencia}</span>
                        {f.detalle && <> · {f.detalle}</>}
                      </p>
                    </td>
                    <td className="px-3 py-3 whitespace-nowrap text-muted-foreground">{f.sistema ?? '—'}</td>
                    <td className="px-3 py-3">
                      <Badge tono={ESTADO_TICKET[f.estado].tono} punto>
                        {f.etiquetaEstado}
                      </Badge>
                    </td>
                    <td className="px-3 py-3 whitespace-nowrap">
                      {f.prioridad ? <Badge tono={f.prioridad === 'Alta' ? 'danger' : f.prioridad === 'Media' ? 'warning' : 'neutral'}>{f.prioridad}</Badge> : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="tabular px-3 py-3 text-right text-muted-foreground">{f.pasos}</td>
                    <td className="px-5 py-3 text-right whitespace-nowrap text-muted-foreground">{fechaCorta(f.creado)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <NuevoTicket abierto={nuevo} onCerrar={() => setNuevo(false)} />
    </>
  );
}

function NuevoTicket({ abierto, onCerrar }: { abierto: boolean; onCerrar: () => void }) {
  const sistemas = useSistemas();
  const [masDetalles, setMasDetalles] = useState(false);
  const vacio = { numero: '', titulo: '', descripcion: '', sistema: '', modulo: '', prioridad: '', autor: '', url_sir: '', base_datos: '' };
  const [datos, setDatos] = useState(vacio);

  // Los pasos, igual que al subir una solución: una cosa después de otra, y a veces
  // con una consulta en medio. Un ticket resuelto y un aporte acaban siendo lo mismo
  // —alguien cuenta qué pasó y cómo lo comprobó—, así que se registran igual.
  //
  // Solo consultas: el sistema no ejecuta escrituras, y el backend rechaza el ticket
  // entero si algún paso trae un UPDATE o un DELETE.
  const [pasos, setPasos] = useState<{ descripcion: string; script: string | null }[]>([]);
  const cambiarPaso = (i: number, cambios: Partial<{ descripcion: string; script: string | null }>) =>
    setPasos((previos) => previos.map((p, j) => (j === i ? { ...p, ...cambios } : p)));
  // Las bases registradas en el motor: sirven de contexto de investigación del caso.
  // Solo las bases habilitadas para tickets: las consultas de los diagnosticos estan
  // escritas contra los esquemas de Corresponsales, y apuntar a otra da un error, no
  // un mal diagnostico. Antes se ofrecian todas las registradas.
  const { data: bases } = useQuery({ queryKey: ['bases-tickets'], queryFn: api.basesTickets, staleTime: 5 * 60_000 });
  const baseElegida = bases?.find((b) => b.id === datos.base_datos);
  const avisar = useAvisos();
  const qc = useQueryClient();
  const router = useRouter();
  const set = (k: keyof typeof vacio) => (e: { target: { value: string } }) => setDatos((d) => ({ ...d, [k]: e.target.value }));

  const crear = useMutation({
    mutationFn: () =>
      api.crearTicket({
        ...datos,
        url_sir: datos.url_sir || enlaceExterno(datos.numero),
        pasos: pasos.filter((p) => p.descripcion.trim() || p.script?.trim()),
      }),
    onSuccess: (t) => {
      qc.invalidateQueries({ queryKey: ['tickets'] });
      qc.invalidateQueries({ queryKey: claves.resumen });
      avisar('exito', 'Ticket creado', t.numero);
      setDatos(vacio);
      setPasos([]);
      onCerrar();
      router.push(`/admin/tickets/${t.numero}`);
    },
    onError: (e) => avisar('error', 'No se pudo crear el ticket', (e as Error).message),
  });

  return (
    <Dialog
      abierto={abierto}
      onCerrar={onCerrar}
      titulo="Registrar un caso"
      descripcion="Con el problema basta. Lo demás se puede completar después."
      ancho="max-w-2xl"
      pie={
        <>
          <Button variante="fantasma" onClick={onCerrar}>
            Cancelar
          </Button>
          <Button onClick={() => crear.mutate()} cargando={crear.isPending} disabled={!datos.titulo.trim()}>
            Crear ticket
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        {/*
          Primero el problema, que es lo único que siempre se sabe. El resto se
          clasifica después, y no debería estorbar para registrar un caso: antes el
          formulario abría pidiendo un número del SIR y un sistema, y no todo lo que
          entra viene del SIR ni pertenece a un sistema conocido.
        */}
        <Campo etiqueta="¿Qué está pasando?" htmlFor="nt-titulo">
          <Input
            id="nt-titulo"
            value={datos.titulo}
            onChange={set('titulo')}
            placeholder="El corresponsal no puede ingresar con su PIN"
            autoFocus
          />
        </Campo>

        <Campo etiqueta="Descripción" htmlFor="nt-desc" ayuda="El mensaje tal como llegó, con los datos que traiga.">
          <Textarea
            id="nt-desc"
            value={datos.descripcion}
            onChange={set('descripcion')}
            placeholder="Mensaje original de quien reporta, datos del caso, lo que se ve en pantalla…"
          />
        </Campo>

        {/*
          Los pasos van antes de la clasificación: lo que se hizo importa más que de
          qué sistema era. Empiezan vacíos —un ticket se puede registrar sin ellos, que
          es lo normal cuando el caso acaba de entrar— y se van añadiendo al resolverlo.
        */}
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <p className="text-[14px] font-medium">Pasos de lo que se hizo</p>
            <span className="text-[12.5px] text-muted-foreground">opcional</span>
          </div>

          {pasos.map((paso, i) => (
            <div key={i} className="space-y-2 rounded-lg border p-3">
              <div className="flex items-start gap-2">
                <span className="mt-2 grid size-5 shrink-0 place-items-center rounded-full bg-surface-muted text-[12px] font-medium tabular">
                  {i + 1}
                </span>
                <Textarea
                  value={paso.descripcion}
                  onChange={(e) => cambiarPaso(i, { descripcion: e.target.value })}
                  placeholder="Qué se revisó o se hizo en este paso"
                  rows={2}
                />
                <button
                  type="button"
                  onClick={() => setPasos((previos) => previos.filter((_, j) => j !== i))}
                  className="mt-1.5 shrink-0 text-[12.5px] text-muted-foreground hover:text-danger"
                >
                  Quitar
                </button>
              </div>

              {paso.script === null ? (
                <button
                  type="button"
                  onClick={() => cambiarPaso(i, { script: '' })}
                  className="ml-7 text-[12.5px] font-medium text-primary hover:underline"
                >
                  Añadir la consulta que lo comprueba
                </button>
              ) : (
                <div className="ml-7 space-y-1.5">
                  <Textarea
                    value={paso.script}
                    onChange={(e) => cambiarPaso(i, { script: e.target.value })}
                    placeholder="SELECT …"
                    rows={3}
                    spellCheck={false}
                    className="font-mono text-[13px]"
                  />
                  <div className="flex items-center gap-2">
                    <span className="text-[12.5px] text-muted-foreground">Esta consulta solo lee</span>
                    <button
                      type="button"
                      onClick={() => cambiarPaso(i, { script: null })}
                      className="ml-auto text-[12.5px] text-muted-foreground hover:text-danger"
                    >
                      Quitar consulta
                    </button>
                  </div>
                </div>
              )}
            </div>
          ))}

          <Button
            variante="fantasma"
            onClick={() => setPasos((previos) => [...previos, { descripcion: '', script: null }])}
          >
            <Plus className="size-4" />
            {pasos.length === 0 ? 'Añadir un paso' : 'Añadir otro paso'}
          </Button>
        </div>

        <button
          type="button"
          onClick={() => setMasDetalles((v) => !v)}
          className="flex w-full items-center gap-2 rounded-lg border px-3 py-2.5 text-left text-[14px] font-medium transition-colors hover:bg-surface-muted/50"
          aria-expanded={masDetalles}
        >
          <ChevronDown className={cn('size-4 text-muted-foreground transition-transform', masDetalles && 'rotate-180')} />
          Clasificación y referencia
          <span className="ml-auto text-[12.5px] font-normal text-muted-foreground">opcional</span>
        </button>

        {masDetalles && (
          <div className="animate-entrada grid gap-4 rounded-lg border p-4 sm:grid-cols-2">
            <Campo etiqueta="Sistema o aplicación" htmlFor="nt-sistema" ayuda="Escriba uno nuevo si no está en la lista.">
              <Input
                id="nt-sistema"
                value={datos.sistema}
                onChange={set('sistema')}
                list="nt-sistemas"
                placeholder="Sin clasificar"
              />
              <datalist id="nt-sistemas">
                {sistemas.map((s) => (
                  <option key={s} value={s} />
                ))}
              </datalist>
            </Campo>

            <Campo etiqueta="Módulo" htmlFor="nt-modulo">
              <Input id="nt-modulo" value={datos.modulo} onChange={set('modulo')} placeholder="Usuarios / Reset de clave" />
            </Campo>

            <Campo etiqueta="Prioridad" htmlFor="nt-prioridad">
              <Select id="nt-prioridad" value={datos.prioridad} onChange={set('prioridad')}>
                <option value="">Sin definir</option>
                <option>Alta</option>
                <option>Media</option>
                <option>Baja</option>
              </Select>
            </Campo>

            <Campo etiqueta="Quién registra" htmlFor="nt-autor">
              <Input id="nt-autor" value={datos.autor} onChange={set('autor')} placeholder="Su nombre" />
            </Campo>

            <Campo
              etiqueta="Referencia"
              htmlFor="nt-numero"
              ayuda="Número en otro sistema, si lo tiene. Vacío, se numera solo."
            >
              <Input id="nt-numero" value={datos.numero} onChange={set('numero')} placeholder="2026091974" className="font-mono" />
            </Campo>

            <Campo etiqueta="Enlace al caso original" htmlFor="nt-url">
              <Input id="nt-url" value={datos.url_sir} onChange={set('url_sir')} placeholder="https://…" />
            </Campo>

            {/* Solo aparece si hay alguna base habilitada: sin ellas no significa nada. */}
            {bases && bases.length > 0 && (
              <div className="sm:col-span-2">
                <Campo
                  etiqueta="Base de datos a investigar"
                  htmlFor="nt-base"
                  ayuda="Solo indica de dónde leer los datos al revisar el caso. El ticket se guarda siempre en la base de conocimiento."
                >
                  <Select id="nt-base" value={datos.base_datos} onChange={set('base_datos')}>
                    <option value="">Usar la base por defecto</option>
                    {bases.map((b) => (
                      <option key={b.id} value={b.id}>
                        {b.color === 'rojo' ? '\u26A0 ' : ''}
                        {b.etiqueta}
                        {b.grupo === 'produccion' ? ' · datos reales' : b.grupo === 'pruebas' ? ' · pruebas' : ''}
                      </option>
                    ))}
                  </Select>
                  {baseElegida?.nota && <p className="mt-1.5 text-[12.5px] text-muted-foreground">{baseElegida.nota}</p>}
                </Campo>
              </div>
            )}
          </div>
        )}
      </div>
    </Dialog>
  );
}
