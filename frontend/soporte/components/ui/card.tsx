import type { HTMLAttributes, ReactNode } from 'react';
import { cn } from '@/soporte/lib/utils';

export function Card({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('rounded-xl border bg-surface shadow-card', className)} {...props} />;
}

interface EncabezadoProps {
  titulo: ReactNode;
  descripcion?: ReactNode;
  acciones?: ReactNode;
  icono?: ReactNode;
  className?: string;
}

export function CardHeader({ titulo, descripcion, acciones, icono, className }: EncabezadoProps) {
  return (
    <div className={cn('flex items-start justify-between gap-4 border-b px-5 py-4', className)}>
      <div className="flex min-w-0 items-start gap-3">
        {icono && (
          <div className="mt-0.5 grid size-8 shrink-0 place-items-center rounded-lg bg-surface-muted text-muted-foreground [&_svg]:size-4">
            {icono}
          </div>
        )}
        <div className="min-w-0">
          <h2 className="text-[15px] font-semibold tracking-tight">{titulo}</h2>
          {descripcion && <p className="mt-0.5 text-[14px] text-muted-foreground">{descripcion}</p>}
        </div>
      </div>
      {acciones && <div className="flex shrink-0 items-center gap-2">{acciones}</div>}
    </div>
  );
}

export function CardContent({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('p-5', className)} {...props} />;
}
