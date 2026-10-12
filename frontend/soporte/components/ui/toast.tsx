import { CircleAlert, CircleCheck, X } from 'lucide-react';
import { createContext, useCallback, useContext, useState, type ReactNode } from 'react';
import { cn } from '@/soporte/lib/utils';

type TipoAviso = 'exito' | 'error';
interface Aviso {
  id: number;
  tipo: TipoAviso;
  titulo: string;
  detalle?: string;
}

const Contexto = createContext<(tipo: TipoAviso, titulo: string, detalle?: string) => void>(() => undefined);

export function AvisosProvider({ children }: { children: ReactNode }) {
  const [avisos, setAvisos] = useState<Aviso[]>([]);

  const quitar = (id: number) => setAvisos((a) => a.filter((x) => x.id !== id));
  const avisar = useCallback((tipo: TipoAviso, titulo: string, detalle?: string) => {
    const id = Date.now() + Math.random();
    setAvisos((a) => [...a.slice(-3), { id, tipo, titulo, detalle }]);
    setTimeout(() => quitar(id), tipo === 'error' ? 6000 : 3500);
  }, []);

  return (
    <Contexto.Provider value={avisar}>
      {children}
      <div className="pointer-events-none fixed right-4 bottom-4 z-[60] flex w-80 flex-col gap-2" aria-live="polite">
        {avisos.map((a) => (
          <div key={a.id} className="pointer-events-auto flex animate-entrada items-start gap-3 rounded-xl border bg-surface p-3.5 shadow-pop">
            {a.tipo === 'exito' ? <CircleCheck className="mt-0.5 size-4 shrink-0 text-success" /> : <CircleAlert className="mt-0.5 size-4 shrink-0 text-danger" />}
            <div className="min-w-0 flex-1">
              <p className={cn('text-sm font-medium')}>{a.titulo}</p>
              {a.detalle && <p className="mt-0.5 text-[14px] text-muted-foreground">{a.detalle}</p>}
            </div>
            <button type="button" onClick={() => quitar(a.id)} className="text-muted-foreground hover:text-foreground" aria-label="Cerrar aviso">
              <X className="size-3.5" />
            </button>
          </div>
        ))}
      </div>
    </Contexto.Provider>
  );
}

export const useAvisos = () => useContext(Contexto);
