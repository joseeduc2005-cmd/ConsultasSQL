'use client';

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { FilePlus2 } from 'lucide-react';
import { useState } from 'react';
import Link from 'next/link';
import { Button } from '@/soporte/components/ui/button';
import { Dialog } from '@/soporte/components/ui/dialog';
import { Campo, Input } from '@/soporte/components/ui/form';
import { useAvisos } from '@/soporte/components/ui/toast';
import { api } from '@/soporte/lib/api';
import { claves } from '@/soporte/lib/consultas';

/** Agrega el diagnóstico como un paso de investigación en un ticket existente. */
export function RegistrarEnTicket({ diagnosticoId, ticketActual }: { diagnosticoId: number; ticketActual?: string | null }) {
  const [abierto, setAbierto] = useState(false);
  const [ticket, setTicket] = useState(ticketActual ?? '');
  const [registrado, setRegistrado] = useState<string | null>(null);
  const avisar = useAvisos();
  const qc = useQueryClient();

  const { data: tickets } = useQuery({
    queryKey: claves.tickets({ abiertos: true }),
    queryFn: () => api.tickets(),
    enabled: abierto,
    select: (lista) => lista.filter((t) => t.estado !== 'resuelto'),
  });

  const registrar = useMutation({
    mutationFn: () => api.registrarDiagnostico(diagnosticoId, ticket.trim()),
    onSuccess: (paso) => {
      setRegistrado(paso.ticket);
      setAbierto(false);
      qc.invalidateQueries({ queryKey: ['ticket', paso.ticket] });
      qc.invalidateQueries({ queryKey: ['tickets'] });
      avisar('exito', `Registrado como paso ${paso.numero}`, `Ticket ${paso.ticket}`);
    },
    onError: (e) => avisar('error', 'No se pudo registrar', (e as Error).message),
  });

  if (registrado) {
    return (
      <Link href={`/soporte/tickets/${registrado}`} className="text-[13px] font-medium text-primary hover:underline">
        Ver en ticket {registrado}
      </Link>
    );
  }

  return (
    <>
      <Button variante="borde" tamano="sm" onClick={() => setAbierto(true)}>
        <FilePlus2 />
        Registrar en ticket
      </Button>
      <Dialog
        abierto={abierto}
        onCerrar={() => setAbierto(false)}
        titulo="Registrar en ticket"
        descripcion="El diagnóstico se agrega como un nuevo paso de investigación, con las consultas y conclusiones."
        pie={
          <>
            <Button variante="fantasma" onClick={() => setAbierto(false)}>
              Cancelar
            </Button>
            <Button onClick={() => registrar.mutate()} cargando={registrar.isPending} disabled={!ticket.trim()}>
              Registrar
            </Button>
          </>
        }
      >
        <Campo etiqueta="Número de ticket" htmlFor="ticket-registro" ayuda="Tickets abiertos o en investigación sugeridos.">
          <Input
            id="ticket-registro"
            list="tickets-abiertos"
            value={ticket}
            onChange={(e) => setTicket(e.target.value)}
            placeholder="2026091974"
            autoFocus
            onKeyDown={(e) => e.key === 'Enter' && ticket.trim() && registrar.mutate()}
          />
        </Campo>
        <datalist id="tickets-abiertos">
          {tickets?.map((t) => (
            <option key={t.numero} value={t.numero}>
              {t.titulo}
            </option>
          ))}
        </datalist>
      </Dialog>
    </>
  );
}
