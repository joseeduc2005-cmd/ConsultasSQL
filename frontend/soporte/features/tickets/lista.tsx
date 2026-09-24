'use client';

import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Plus, Search, Ticket as IconoTicket } from 'lucide-react';
import { useDeferredValue, useState } from 'react';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { EncabezadoPagina } from '@/soporte/components/layout/encabezado';
import { Badge } from '@/soporte/components/ui/badge';
import { Button } from '@/soporte/components/ui/button';
import { Card } from '@/soporte/components/ui/card';
import { Dialog } from '@/soporte/components/ui/dialog';
import { Cargando, ErrorCarga, Vacio } from '@/soporte/components/ui/estados';
import { Campo, Input, Select, Textarea } from '@/soporte/components/ui/form';
import { Segmented } from '@/soporte/components/ui/segmented';
import { useAvisos } from '@/soporte/components/ui/toast';
import { api } from '@/soporte/lib/api';
import { claves } from '@/soporte/lib/consultas';
import { ESTADO_TICKET, fechaCorta } from '@/soporte/lib/formato';
import type { EstadoTicket } from '@/soporte/lib/types';

const SISTEMAS = ['Corresponsales', 'Ja Movil', 'Ja Web', 'Ja Pagos'];
type FiltroEstado = 'activos' | EstadoTicket | 'todos';

export function PaginaTickets() {
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
            {SISTEMAS.map((s) => (
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
  const vacio = { numero: '', titulo: '', descripcion: '', sistema: 'Corresponsales', modulo: '', prioridad: '', autor: '', url_sir: '', base_datos: '' };
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
        url_sir: datos.url_sir || (/^\d+$/.test(datos.numero) ? `https://sir.jardinazuayo.fin.ec/sir/front/ticket.form.php?id=${datos.numero}` : ''),
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
      titulo="Nuevo ticket"
      descripcion="Registre el caso tal como llegó; la investigación se agrega luego como pasos."
      ancho="max-w-2xl"
      pie={
        <>
          <Button variante="fantasma" onClick={onCerrar}>
            Cancelar
          </Button>
          <Button onClick={() => crear.mutate()} cargando={crear.isPending} disabled={!datos.numero.trim() || !datos.titulo.trim()}>
            Crear ticket
          </Button>
        </>
      }
    >
      <div className="grid gap-4 sm:grid-cols-2">
        <Campo etiqueta="Número de ticket (SIR)" htmlFor="nt-numero">
          <Input id="nt-numero" value={datos.numero} onChange={set('numero')} placeholder="2026091974" className="font-mono" autoFocus />
        </Campo>
        <Campo etiqueta="Sistema" htmlFor="nt-sistema">
          <Select id="nt-sistema" value={datos.sistema} onChange={set('sistema')}>
            {SISTEMAS.map((s) => (
              <option key={s}>{s}</option>
            ))}
          </Select>
        </Campo>
        <div className="sm:col-span-2">
          <Campo etiqueta="Título" htmlFor="nt-titulo">
            <Input id="nt-titulo" value={datos.titulo} onChange={set('titulo')} placeholder="Corresponsal no puede ingresar con su PIN" />
          </Campo>
        </div>
        <div className="sm:col-span-2">
          <Campo etiqueta="Descripción" htmlFor="nt-desc">
            <Textarea id="nt-desc" value={datos.descripcion} onChange={set('descripcion')} placeholder="Mensaje original del solicitante, datos del socio, capturas descritas…" />
          </Campo>
        </div>
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
        <div className="sm:col-span-2">
          <Campo
            etiqueta="Base de datos a investigar"
            htmlFor="nt-base"
            ayuda="Contra qué base se consultará este caso. El ticket siempre se guarda en la base de conocimiento; esto solo indica de dónde se leen los datos."
          >
            <Select id="nt-base" value={datos.base_datos} onChange={set('base_datos')}>
              <option value="">Usar la base por defecto</option>
              {bases?.map((b) => (
                <option key={b.id} value={b.id}>
                  {b.color === 'rojo' ? '⚠ ' : ''}
                  {b.etiqueta}
                  {b.grupo === 'produccion' ? ' · datos reales' : b.grupo === 'pruebas' ? ' · pruebas' : ''}
                </option>
              ))}
            </Select>
            {baseElegida?.nota && <p className="mt-1.5 text-[12px] text-muted-foreground">{baseElegida.nota}</p>}
          </Campo>
        </div>
        <Campo etiqueta="Autor" htmlFor="nt-autor">
          <Input id="nt-autor" value={datos.autor} onChange={set('autor')} placeholder="Nombre de quien registra" />
        </Campo>
        <Campo etiqueta="URL en el SIR (opcional)" htmlFor="nt-url" ayuda="Si se deja vacío se arma con el número.">
          <Input id="nt-url" value={datos.url_sir} onChange={set('url_sir')} />
        </Campo>
      </div>
    </Dialog>
  );
}
