import { X } from 'lucide-react';
import { useEffect, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { cn } from '@/soporte/lib/utils';

interface Props {
  abierto: boolean;
  onCerrar: () => void;
  titulo: string;
  descripcion?: string;
  children: ReactNode;
  pie?: ReactNode;
  ancho?: string;
}

export function Dialog({ abierto, onCerrar, titulo, descripcion, children, pie, ancho = 'max-w-lg' }: Props) {
  useEffect(() => {
    if (!abierto) return;
    const alPresionar = (e: KeyboardEvent) => e.key === 'Escape' && onCerrar();
    document.addEventListener('keydown', alPresionar);
    document.body.style.overflow = 'hidden';
    return () => {
      document.removeEventListener('keydown', alPresionar);
      document.body.style.overflow = '';
    };
  }, [abierto, onCerrar]);

  if (!abierto) return null;
  return createPortal(
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto p-4 pt-[10vh]">
      <div className="fixed inset-0 bg-foreground/25 backdrop-blur-[2px]" onClick={onCerrar} />
      <div role="dialog" aria-modal="true" aria-label={titulo} className={cn('relative w-full animate-entrada rounded-xl border bg-surface shadow-pop', ancho)}>
        <div className="flex items-start justify-between gap-4 border-b px-5 py-4">
          <div>
            <h2 className="text-[15px] font-semibold">{titulo}</h2>
            {descripcion && <p className="mt-0.5 text-[13px] text-muted-foreground">{descripcion}</p>}
          </div>
          <button type="button" onClick={onCerrar} className="rounded-md p-1 text-muted-foreground hover:bg-surface-muted hover:text-foreground" aria-label="Cerrar">
            <X className="size-4" />
          </button>
        </div>
        <div className="px-5 py-4">{children}</div>
        {pie && <div className="flex justify-end gap-2 border-t bg-surface-muted/50 px-5 py-3">{pie}</div>}
      </div>
    </div>,
    document.body,
  );
}
