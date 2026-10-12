import { Check, Copy } from 'lucide-react';
import { useState } from 'react';
import { cn } from '@/soporte/lib/utils';

export function useCopiar() {
  const [copiado, setCopiado] = useState(false);
  const copiar = async (texto: string) => {
    try {
      await navigator.clipboard.writeText(texto);
    } catch {
      const area = document.createElement('textarea');
      area.value = texto;
      document.body.appendChild(area);
      area.select();
      document.execCommand('copy');
      area.remove();
    }
    setCopiado(true);
    setTimeout(() => setCopiado(false), 1600);
  };
  return { copiado, copiar };
}

interface Props {
  codigo: string;
  etiqueta?: string;
  className?: string;
  maxAlto?: string;
  tono?: 'normal' | 'peligro';
}

export function CodeBlock({ codigo, etiqueta = 'SQL', className, maxAlto = 'max-h-80', tono = 'normal' }: Props) {
  const { copiado, copiar } = useCopiar();
  return (
    <div
      className={cn(
        'group relative overflow-hidden rounded-lg border',
        tono === 'peligro' ? 'border-danger/30 bg-danger-soft/40' : 'bg-surface-muted',
        className,
      )}
    >
      <div className="flex items-center justify-between border-b border-inherit px-3 py-1.5">
        <span className="text-[12px] font-semibold tracking-wider text-muted-foreground uppercase">{etiqueta}</span>
        <button
          type="button"
          onClick={() => copiar(codigo)}
          className="inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-xs text-muted-foreground transition-colors hover:bg-surface hover:text-foreground"
        >
          {copiado ? <Check className="size-3.5 text-success" /> : <Copy className="size-3.5" />}
          {copiado ? 'Copiado' : 'Copiar'}
        </button>
      </div>
      <pre className={cn('scroll-fino overflow-auto px-3.5 py-3 text-[13px] leading-relaxed', maxAlto)}>
        <code>{codigo}</code>
      </pre>
    </div>
  );
}
