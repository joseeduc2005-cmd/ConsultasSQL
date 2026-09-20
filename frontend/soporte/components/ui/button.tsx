import { cva, type VariantProps } from 'class-variance-authority';
import { LoaderCircle } from 'lucide-react';
import type { ButtonHTMLAttributes } from 'react';
import { cn } from '@/soporte/lib/utils';

export const estilosBoton = cva(
  'inline-flex shrink-0 items-center justify-center gap-2 whitespace-nowrap rounded-lg text-sm font-medium transition-[background-color,border-color,color,box-shadow,transform] duration-150 active:translate-y-px disabled:pointer-events-none disabled:opacity-50 [&_svg]:size-4 [&_svg]:shrink-0',
  {
    variants: {
      variante: {
        primario: 'bg-primary text-primary-foreground shadow-sm hover:bg-primary-hover',
        secundario: 'bg-surface-muted text-foreground hover:bg-border/70',
        borde: 'border border-border-strong/80 bg-surface text-foreground shadow-card hover:bg-surface-muted',
        fantasma: 'text-muted-foreground hover:bg-surface-muted hover:text-foreground',
        peligro: 'bg-danger text-white shadow-sm hover:opacity-90',
      },
      tamano: {
        sm: 'h-8 px-3 text-[13px]',
        md: 'h-9 px-4',
        lg: 'h-10 px-5',
        icono: 'size-9',
        iconoSm: 'size-8',
      },
    },
    defaultVariants: { variante: 'primario', tamano: 'md' },
  },
);

interface Props extends ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof estilosBoton> {
  cargando?: boolean;
}

export function Button({ className, variante, tamano, cargando, disabled, children, type = 'button', ...props }: Props) {
  return (
    <button type={type} className={cn(estilosBoton({ variante, tamano }), className)} disabled={disabled || cargando} {...props}>
      {cargando && <LoaderCircle className="animate-spin" />}
      {children}
    </button>
  );
}
