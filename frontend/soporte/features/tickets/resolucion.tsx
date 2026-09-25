'use client';

import { BookPlus, Pencil, X } from 'lucide-react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import Link from 'next/link';
import { api } from '@/soporte/lib/api';
import { claves, useEstado } from '@/soporte/lib/consultas';
import { Badge } from '@/soporte/components/ui/badge';
import { Button } from '@/soporte/components/ui/button';
import { Card, CardHeader } from '@/soporte/components/ui/card';
import { useAvisos } from '@/soporte/components/ui/toast';
import type { TicketDetalle } from '@/soporte/lib/types';

/**
 * Causa raíz y solución del ticket, escritas por quien lo atendió.
 *
 * Hasta ahora estos dos campos se mostraban pero no se podían escribir desde ninguna
 * pantalla: el conocimiento se quedaba encerrado en el ticket, visible para quien lo
 * abriera e invisible para quien llegara con el mismo problema dos meses después.
 *
 * Al publicarlos pasan a la base de conocimiento, y la búsqueda de precedentes los
 * encuentra la próxima vez. Ese es el ciclo completo: se resuelve una vez, se
 * responde siempre.
 */
export function Resolucion({ ticket }: { ticket: TicketDetalle }) {
  const avisar = useAvisos();
  const qc = useQueryClient();
  const [editando, setEditando] = useState(false);
  const [causa, setCausa] = useState(ticket.causa_raiz ?? '');
  const [solucion, setSolucion] = useState(ticket.solucion ?? '');
  const [sql, setSql] = useState('');
  const [publicado, setPublicado] = useState<{ id: string; titulo: string } | null>(null);

  const { data: estado } = useEstado();
  const hayResolucion = Boolean((ticket.causa_raiz ?? '').trim() || (ticket.solucion ?? '').trim());

  const guardar = useMutation({
    mutationFn: () => api.actualizarTicket(ticket.numero, { causa_raiz: causa, solucion }),
    onSuccess: () => {
      setEditando(false);
      qc.invalidateQueries({ queryKey: claves.ticket(ticket.numero) });
      avisar('exito', 'Resolución guardada', 'Ya se puede publicar en la base de conocimiento.');
    },
    onError: (e) => avisar('error', 'No se pudo guardar', (e as Error).message),
  });

  const publicar = useMutation({
    mutationFn: () => api.publicarTicket(ticket.numero, sql.trim() || undefined),
    onSuccess: (r) => {
      setPublicado(r);
      avisar(
        'exito',
        r.creado ? 'Publicado en la base de conocimiento' : 'Artículo actualizado',
        `${r.titulo}${r.ejecutable ? ' · con script ejecutable' : ''}`,
      );
    },
    onError: (e) => avisar('error', 'No se pudo publicar', (e as Error).message),
  });

  return (
    <Card>
      <CardHeader
        titulo="Causa raíz y solución"
        descripcion="Lo que se encontró y cómo se resolvió. Al publicarlo, sirve para el próximo caso igual."
        icono={<Pencil />}
        acciones={
          editando ? (
            <div className="flex items-center gap-2">
              <Button variante="fantasma" tamano="sm" onClick={() => setEditando(false)}>
                <X />
                Cancelar
              </Button>
              <Button tamano="sm" onClick={() => guardar.mutate()} cargando={guardar.isPending}>
                Guardar
              </Button>
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <Button variante="borde" tamano="sm" onClick={() => setEditando(true)}>
                <Pencil />
                {hayResolucion ? 'Editar' : 'Escribir'}
              </Button>
              {hayResolucion &&
                (publicado ? (
                  <Link
                    href={`/admin?articulo=${encodeURIComponent(publicado.id)}`}
                    className="text-[13px] font-medium text-primary hover:underline"
                  >
                    Ver en categorías
                  </Link>
                ) : (
                  <Button tamano="sm" onClick={() => publicar.mutate()} cargando={publicar.isPending}>
                    <BookPlus />
                    Publicar
                  </Button>
                ))}
            </div>
          )
        }
      />

      <div className="space-y-4 px-5 pb-5">
        {editando ? (
          <>
            <Campo
              etiqueta="Causa raíz"
              valor={causa}
              onChange={setCausa}
              ayuda="Qué estaba mal de verdad, no el síntoma."
            />
            <Campo
              etiqueta="Solución aplicada"
              valor={solucion}
              onChange={setSolucion}
              ayuda="Qué se hizo para arreglarlo, con los pasos si fueron varios."
            />
            <div>
              <p className="mb-1 text-[12px] font-medium text-muted-foreground">SQL, si hubo (opcional)</p>
              <textarea
                value={sql}
                onChange={(e) => setSql(e.target.value)}
                rows={4}
                spellCheck={false}
                placeholder={'UPDATE CORRESPONSALES.USUARIO\nSET USR_ESTADO = 1\nWHERE USR_CODIGO = :usr_codigo;'}
                className="w-full rounded-lg border bg-surface-muted/40 px-3 py-2 font-mono text-[12.5px] outline-none focus:border-primary/50"
              />
              <p className="mt-1 text-[11.5px] text-muted-foreground">
                Los <span className="font-mono">:marcadores</span> se vuelven campos del formulario del artículo, así
                sirve para el próximo caso.
              </p>
            </div>
          </>
        ) : (
          <>
            <Vista titulo="Causa raíz" texto={ticket.causa_raiz} vacio="Aún sin causa raíz confirmada." />
            <Vista titulo="Solución aplicada" texto={ticket.solucion} vacio="Aún sin solución registrada." />
          </>
        )}

        {!editando && hayResolucion && !publicado && (
          <p className="text-[12px] text-muted-foreground">
            Publicar lo deja como artículo en el árbol de categorías. A partir de ahí, cuando entre un ticket
            parecido, aparece entre los casos ya resueltos.
            {estado && !estado.correccionesEjecutables && ' El SQL se guarda, pero sin botón de ejecutar mientras la escritura esté apagada.'}
          </p>
        )}
      </div>
    </Card>
  );
}

function Campo({
  etiqueta,
  valor,
  onChange,
  ayuda,
}: {
  etiqueta: string;
  valor: string;
  onChange: (v: string) => void;
  ayuda: string;
}) {
  return (
    <div>
      <p className="mb-1 text-[12px] font-medium text-muted-foreground">{etiqueta}</p>
      <textarea
        value={valor}
        onChange={(e) => onChange(e.target.value)}
        rows={4}
        className="w-full rounded-lg border bg-surface px-3 py-2 text-[13px] leading-relaxed outline-none focus:border-primary/50"
      />
      <p className="mt-1 text-[11.5px] text-muted-foreground">{ayuda}</p>
    </div>
  );
}

function Vista({ titulo, texto, vacio }: { titulo: string; texto?: string | null; vacio: string }) {
  return (
    <div>
      <div className="mb-1 flex items-center gap-2">
        <p className="text-[11.5px] font-semibold tracking-wide text-muted-foreground uppercase">{titulo}</p>
        {!texto?.trim() && <Badge tono="neutral">Pendiente</Badge>}
      </div>
      <p className={`text-[13.5px] leading-relaxed whitespace-pre-line ${texto?.trim() ? '' : 'text-muted-foreground italic'}`}>
        {texto?.trim() || vacio}
      </p>
    </div>
  );
}
