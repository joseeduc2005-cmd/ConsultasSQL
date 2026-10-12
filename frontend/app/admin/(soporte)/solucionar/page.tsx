'use client';

import { Search, Sparkle } from 'lucide-react';
import { useMutation } from '@tanstack/react-query';
import { useState } from 'react';
import { api } from '@/soporte/lib/api';
import { Button } from '@/soporte/components/ui/button';
import { Card } from '@/soporte/components/ui/card';
import { PrecedentesVista } from '@/soporte/features/diagnostico/precedentes';
import type { Precedente } from '@/soporte/lib/types';

/**
 * Buscar una solución a partir del error, sin crear ticket.
 *
 * Es la puerta de entrada del sistema visto como base de conocimiento: alguien pega
 * lo que le está pasando —con sus palabras, como se lo contaron— y se le responde con
 * lo que ya se resolvió antes.
 *
 * No pregunta de qué sistema es ni pide identificadores. Eso es a propósito: quien
 * consulta muchas veces no lo sabe, y el texto del error suele bastar para encontrar
 * el caso parecido. Para investigar un caso concreto contra la base está el ticket.
 */
export default function SolucionarPage() {
  const [texto, setTexto] = useState('');
  const [resultado, setResultado] = useState<{ consulta: string; precedentes: Precedente[] } | null>(null);

  const buscar = useMutation({
    mutationFn: () => api.solucionar(texto),
    onSuccess: setResultado,
  });

  const puedeBuscar = texto.trim().length >= 8;

  return (
    <div className="mx-auto max-w-3xl space-y-6 py-2">
      <header>
        <h1 className="text-xl font-semibold tracking-tight">Buscar solución</h1>
        <p className="mt-1 text-[14px] text-muted-foreground">
          Escriba el error tal como se lo reportaron. El sistema busca en todo lo que ya se resolvió: tickets
          cerrados con su causa y artículos del árbol de categorías.
        </p>
      </header>

      <Card className="p-4">
        <textarea
          value={texto}
          onChange={(e) => setTexto(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && puedeBuscar) buscar.mutate();
          }}
          rows={5}
          autoFocus
          placeholder={'Por ejemplo:\nLa socia dice que no puede ingresar, le sale Unexpected Error y ya intentó varias veces con la clave y con el PIN.'}
          className="w-full resize-none rounded-lg border bg-surface px-3.5 py-3 text-[14px] leading-relaxed outline-none focus:border-primary/50"
        />
        <div className="mt-3 flex flex-wrap items-center gap-3">
          <Button onClick={() => buscar.mutate()} cargando={buscar.isPending} disabled={!puedeBuscar}>
            {!buscar.isPending && <Search />}
            Buscar solución
          </Button>
          <span className="text-[12.5px] text-muted-foreground">
            {puedeBuscar ? 'Ctrl + Enter para buscar' : 'Escriba un poco más para poder buscar'}
          </span>
        </div>
      </Card>

      {buscar.isError && (
        <p className="rounded-lg border border-danger/25 bg-danger-soft/50 px-4 py-3 text-[14px]">
          No se pudo buscar: {(buscar.error as Error).message}
        </p>
      )}

      {resultado &&
        (resultado.precedentes.length > 0 ? (
          <PrecedentesVista precedentes={resultado.precedentes} />
        ) : (
          <Card className="p-6 text-center">
            <Sparkle className="mx-auto size-5 text-muted-foreground" />
            <p className="mt-2 text-[14px] font-medium">Nada parecido todavía</p>
            <p className="mx-auto mt-1 max-w-md text-[14px] text-muted-foreground">
              Ningún caso resuelto se parece a este. Registre un ticket: cuando lo resuelvan y escriban la causa y la
              solución, el próximo que llegue con lo mismo lo encontrará aquí.
            </p>
          </Card>
        ))}
    </div>
  );
}
