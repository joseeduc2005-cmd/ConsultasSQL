import { cn } from '@/soporte/lib/utils';

interface Opcion<T extends string> {
  valor: T;
  etiqueta: string;
  conteo?: number;
}

interface Props<T extends string> {
  opciones: Opcion<T>[];
  valor: T;
  onChange: (valor: T) => void;
  className?: string;
}

export function Segmented<T extends string>({ opciones, valor, onChange, className }: Props<T>) {
  return (
    <div role="tablist" className={cn('inline-flex h-9 items-center gap-0.5 rounded-lg bg-surface-muted p-1', className)}>
      {opciones.map((o) => {
        const activo = o.valor === valor;
        return (
          <button
            key={o.valor}
            role="tab"
            type="button"
            aria-selected={activo}
            onClick={() => onChange(o.valor)}
            className={cn(
              'inline-flex h-7 items-center gap-1.5 rounded-md px-3 text-[13px] font-medium transition-all',
              activo ? 'bg-surface text-foreground shadow-card' : 'text-muted-foreground hover:text-foreground',
            )}
          >
            {o.etiqueta}
            {o.conteo !== undefined && (
              <span className={cn('tabular text-xs', activo ? 'text-muted-foreground' : 'text-muted-foreground/70')}>{o.conteo}</span>
            )}
          </button>
        );
      })}
    </div>
  );
}
