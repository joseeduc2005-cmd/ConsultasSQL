import type { ReactNode } from 'react';
import { CircleAlert, LoaderCircle } from 'lucide-react';
import { cn } from '@/soporte/lib/utils';

export function Skeleton({ className }: { className?: string }) {
  return <div className={cn('animate-pulse rounded-md bg-surface-muted', className)} />;
}

export function Cargando({ texto = 'Cargando…', className }: { texto?: string; className?: string }) {
  return (
    <div className={cn('flex items-center justify-center gap-2 py-12 text-sm text-muted-foreground', className)}>
      <LoaderCircle className="size-4 animate-spin" />
      {texto}
    </div>
  );
}

export function Vacio({ icono, titulo, descripcion, accion, className }: { icono?: ReactNode; titulo: string; descripcion?: ReactNode; accion?: ReactNode; className?: string }) {
  return (
    <div className={cn('flex flex-col items-center justify-center px-6 py-12 text-center', className)}>
      {icono && <div className="mb-3 grid size-11 place-items-center rounded-xl bg-surface-muted text-muted-foreground [&_svg]:size-5">{icono}</div>}
      <p className="text-sm font-medium">{titulo}</p>
      {descripcion && <p className="mt-1 max-w-sm text-[13px] text-muted-foreground">{descripcion}</p>}
      {accion && <div className="mt-4">{accion}</div>}
    </div>
  );
}

export function ErrorCarga({ error }: { error: unknown }) {
  return (
    <div className="flex items-start gap-3 rounded-lg border border-danger/25 bg-danger-soft px-4 py-3 text-sm text-danger">
      <CircleAlert className="mt-0.5 size-4 shrink-0" />
      <span>{error instanceof Error ? error.message : 'No se pudo cargar la información.'}</span>
    </div>
  );
}
