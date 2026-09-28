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

  const cuenta = (e: FiltroEstado) =>
    todos?.filter((t) => (e === 'todos' ? true : e === 'activos' ? t.estado !== 'resuelto' : t.estado === e)).length;
  const lista = data?.filter((t) => (estado === 'todos' ? true : estado === 'activos' ? t.estado !== 'resuelto' : t.estado === estado));

  const actualizarParams = (cambios: Record<string, string>) => {
    const p = new URLSearchParams(params.toString());
    Object.entries(cambios).forEach(([k, v]) => (v ? p.set(k, v) : p.delete(k)));
    router.replace(`${rutaActual}?${p}`, { scroll: false });
  };

  return (
    <>
      <EncabezadoPagina
        titulo="Tickets"
        descripcion="Base de conocimiento de casos: descripción, causa raíz, solución y cada paso de la investigación."
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
          <Vacio icono={<IconoTicket />} titulo="No hay tickets con estos filtros" descripcion="Cambie los filtros o registre un ticket nuevo." />
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
                {lista.map((t) => (
                  <tr key={t.numero} onClick={() => router.push(`/admin/tickets/${t.numero}`)} className="group cursor-pointer transition-colors hover:bg-surface-muted/50">
                    <td className="max-w-xl px-5 py-3">
                      <p className="truncate font-medium group-hover:text-primary">{t.titulo}</p>
                      <p className="mt-0.5 truncate text-xs text-muted-foreground">
                        <span className="font-mono">{t.numero}</span>
                        {t.modulo && <> · {t.modulo}</>}
                      </p>
                    </td>
                    <td className="px-3 py-3 whitespace-nowrap text-muted-foreground">{t.sistema ?? '—'}</td>
                    <td className="px-3 py-3">
                      <Badge tono={ESTADO_TICKET[t.estado].tono} punto>
                        {ESTADO_TICKET[t.estado].etiqueta}
                      </Badge>
                    </td>
                    <td className="px-3 py-3 whitespace-nowrap">
                      {t.prioridad ? <Badge tono={t.prioridad === 'Alta' ? 'danger' : t.prioridad === 'Media' ? 'warning' : 'neutral'}>{t.prioridad}</Badge> : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="tabular px-3 py-3 text-right text-muted-foreground">{t.pasos}</td>
                    <td className="px-5 py-3 text-right whitespace-nowrap text-muted-foreground">{fechaCorta(t.creado_en)}</td>
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
      }),
    onSuccess: (t) => {
      qc.invalidateQueries({ queryKey: ['tickets'] });
      qc.invalidateQueries({ queryKey: claves.resumen });
      avisar('exito', 'Ticket creado', t.numero);
      setDatos(vacio);
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

        <button
          type="button"
          onClick={() => setMasDetalles((v) => !v)}
          className="flex w-full items-center gap-2 rounded-lg border px-3 py-2.5 text-left text-[13px] font-medium transition-colors hover:bg-surface-muted/50"
          aria-expanded={masDetalles}
        >
          <ChevronDown className={cn('size-4 text-muted-foreground transition-transform', masDetalles && 'rotate-180')} />
          Clasificación y referencia
          <span className="ml-auto text-[12px] font-normal text-muted-foreground">opcional</span>
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
                  {baseElegida?.nota && <p className="mt-1.5 text-[12px] text-muted-foreground">{baseElegida.nota}</p>}
                </Campo>
              </div>
            )}
          </div>
        )}
      </div>
    </Dialog>
  );
}
