'use client';

import { ChevronRight, Menu, Moon, Sun, TriangleAlert, X } from 'lucide-react';
import { usePathname } from 'next/navigation';
import { useEffect } from 'react';
import { useTheme } from '@/app/components/ThemeProvider';
import { useEstado } from '@/soporte/lib/consultas';
import { cn } from '@/soporte/lib/utils';

/**
 * Cabecera del módulo: ubicación, aviso de datos de demostración y tema.
 *
 * Adaptada del Shell de ticket v2.0. Dos cambios respecto al original: el título de
 * la pantalla se deduce de la ruta en vez de los `handle` de react-router, y el tema
 * usa el ThemeProvider de ConsultasSQL para que el interruptor sea uno solo en toda
 * la aplicación.
 */
const TITULOS: { prefijo: string; titulo: string; exacta?: boolean }[] = [
  { prefijo: '/soporte', titulo: 'Panel', exacta: true },
  { prefijo: '/soporte/diagnostico/historial', titulo: 'Resultado de diagnóstico' },
  { prefijo: '/soporte/diagnostico', titulo: 'Diagnósticos' },
  { prefijo: '/soporte/tickets', titulo: 'Tickets' },
  { prefijo: '/soporte/biblioteca', titulo: 'Biblioteca SQL' },
  { prefijo: '/soporte/vigilancia', titulo: 'Vigilancia' },
  { prefijo: '/soporte/interpretar', titulo: 'Interpretar mensaje' },
];

function tituloDeRuta(ruta: string): string {
  // El orden importa: /diagnostico/historial va antes que /diagnostico.
  const encontrado = TITULOS.find((t) => (t.exacta ? ruta === t.prefijo : ruta.startsWith(t.prefijo)));
  return encontrado?.titulo ?? 'Centro de Soporte';
}

export function Cabecera({ menuAbierto, onAlternarMenu }: { menuAbierto: boolean; onAlternarMenu: () => void }) {
  const ruta = usePathname();
  const { theme, toggleTheme } = useTheme();
  const { data: estado } = useEstado();
  const titulo = tituloDeRuta(ruta);
  const oscuro = theme === 'dark';

  useEffect(() => {
    document.title = `${titulo} · Centro de Soporte`;
  }, [titulo]);

  return (
    <header className="flex h-16 shrink-0 items-center gap-3 border-b bg-surface/80 px-4 backdrop-blur sm:px-6">
      <button
        type="button"
        className="rounded-md p-1.5 text-muted-foreground hover:bg-surface-muted lg:hidden"
        onClick={onAlternarMenu}
        aria-label="Menú"
      >
        {menuAbierto ? <X className="size-5" /> : <Menu className="size-5" />}
      </button>

      <nav aria-label="Ubicación" className="flex min-w-0 items-center gap-1.5 text-sm">
        <span className="hidden text-muted-foreground sm:inline">Centro de Soporte</span>
        <ChevronRight className="hidden size-3.5 text-muted-foreground/60 sm:inline" />
        <span className="truncate font-medium">{titulo}</span>
      </nav>

      <div className="ml-auto flex items-center gap-2">
        {estado?.oracle.modo === 'demo' && (
          <span
            className="hidden items-center gap-1.5 rounded-full bg-warning-soft px-2.5 py-1 text-xs font-medium text-warning ring-1 ring-warning/25 ring-inset sm:inline-flex"
            title="Los diagnósticos y la vigilancia usan datos ficticios. Quite SOPORTE_MODO=demo del .env del backend para consultar Oracle."
          >
            <TriangleAlert className="size-3.5" />
            Datos de demostración
          </span>
        )}
        <button
          type="button"
          onClick={toggleTheme}
          className={cn(
            'grid size-9 place-items-center rounded-lg text-muted-foreground transition-colors hover:bg-surface-muted hover:text-foreground',
          )}
          aria-label={oscuro ? 'Usar tema claro' : 'Usar tema oscuro'}
          title={oscuro ? 'Tema claro' : 'Tema oscuro'}
        >
          {oscuro ? <Sun className="size-[18px]" /> : <Moon className="size-[18px]" />}
        </button>
      </div>
    </header>
  );
}
