'use client';

import { BookOpen, History, Code } from 'lucide-react';
import Link from 'next/link';
import { Badge } from '@/soporte/components/ui/badge';
import { Card } from '@/soporte/components/ui/card';
import type { Precedente } from '@/soporte/lib/types';
import { cn } from '@/soporte/lib/utils';

/**
 * Casos parecidos que el equipo ya resolvió.
 *
 * Sin base conectada es lo único que puede responder el sistema, y responde bastante:
 * buena parte del trabajo de soporte es volver a investigar algo que alguien ya
 * investigó. Con base conectada acompaña al diagnóstico.
 *
 * El porcentaje es relativo al más parecido, no absoluto: dice «cuánto se parece este
 * comparado con el mejor», que es lo que la persona va a leer de todos modos.
 */
export function PrecedentesVista({ precedentes }: { precedentes: Precedente[] }) {
  if (precedentes.length === 0) return null;

  return (
    <section className="space-y-3">
      <div className="flex items-center gap-2">
        <h3 className="text-[13px] font-semibold tracking-wide text-muted-foreground uppercase">
          Casos parecidos ya resueltos ({precedentes.length})
        </h3>
      </div>

      <div className="space-y-2">
        {precedentes.map((p) => (
          <Card key={`${p.tipo}-${p.id}`} className="p-3.5">
            <div className="flex items-start gap-3">
              <span
                className={cn(
                  'mt-0.5 grid size-8 shrink-0 place-items-center rounded-lg [&_svg]:size-4',
                  p.tipo === 'ticket' ? 'bg-info-soft text-info' : 'bg-primary-soft text-primary',
                )}
              >
                {p.tipo === 'ticket' ? <History /> : <BookOpen />}
              </span>

              <div className="min-w-0 flex-1">
                <div className="flex flex-wrap items-center gap-2">
                  <Link href={p.enlace} className="text-[13.5px] font-medium hover:text-primary hover:underline">
                    {p.titulo}
                  </Link>
                  <Badge tono="neutral">{p.tipo === 'ticket' ? 'Ticket' : 'Artículo'}</Badge>
                  {p.sql && (
                    <Badge tono="warning">
                      <Code className="size-3.5" />
                      Con script
                    </Badge>
                  )}
                  <span className="tabular ml-auto text-[12px] text-muted-foreground">
                    {Math.round(p.relevancia * 100)} %
                  </span>
                </div>

                {p.detalle && (
                  <p className="mt-1 line-clamp-3 text-[12.5px] leading-relaxed text-muted-foreground">{p.detalle}</p>
                )}

                <p className="mt-1 text-[11.5px] text-muted-foreground">
                  {p.tipo === 'ticket' ? `Caso ${p.id}` : 'Artículo del árbol de categorías'}
                  {p.categoria ? ` · ${p.categoria}` : ''}
                </p>
              </div>
            </div>
          </Card>
        ))}
      </div>

      <p className="text-[11.5px] text-muted-foreground">
        Salen de la base de conocimiento: tickets resueltos que dejaron causa raíz o solución escrita, y artículos
        del árbol. Los que traen script pesan más, porque además de explicar dan con qué arreglarlo.
      </p>
    </section>
  );
}
