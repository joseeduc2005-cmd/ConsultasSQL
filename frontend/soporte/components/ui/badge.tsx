import type { HTMLAttributes } from 'react';
import type { Tono } from '@/soporte/lib/formato';
import { cn } from '@/soporte/lib/utils';

const TONOS: Record<Tono, string> = {
  neutral: 'bg-surface-muted text-muted-foreground ring-border',
  primary: 'bg-primary-soft text-primary-soft-foreground ring-primary/20',
  success: 'bg-success-soft text-success ring-success/20',
  warning: 'bg-warning-soft text-warning ring-warning/25',
  orange: 'bg-orange-soft text-orange ring-orange/20',
  danger: 'bg-danger-soft text-danger ring-danger/20',
  info: 'bg-info-soft text-info ring-info/20',
};

const PUNTOS: Record<Tono, string> = {
  neutral: 'bg-muted-foreground',
  primary: 'bg-primary',
  success: 'bg-success',
  warning: 'bg-warning',
  orange: 'bg-orange',
  danger: 'bg-danger',
  info: 'bg-info',
};

interface Props extends HTMLAttributes<HTMLSpanElement> {
  tono?: Tono;
  punto?: boolean;
}

export function Badge({ tono = 'neutral', punto, className, children, ...props }: Props) {
  return (
    <span
      className={cn(
        'inline-flex h-[22px] items-center gap-1.5 rounded-md px-2 text-xs font-medium whitespace-nowrap ring-1 ring-inset',
        TONOS[tono],
        className,
      )}
      {...props}
    >
      {punto && <span className={cn('size-1.5 rounded-full', PUNTOS[tono])} />}
      {children}
    </span>
  );
}

export function Punto({ tono, pulso, className }: { tono: Tono; pulso?: boolean; className?: string }) {
  return <span className={cn('inline-block size-2 rounded-full', PUNTOS[tono], pulso && 'animate-pulso', className)} />;
}
