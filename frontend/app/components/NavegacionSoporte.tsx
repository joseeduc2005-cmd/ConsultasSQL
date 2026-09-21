'use client';

import { BookOpen, LayoutDashboard, Radar, ScanSearch, Ticket } from 'lucide-react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useTheme } from './ThemeProvider';

/**
 * Accesos del Centro de Soporte dentro de la barra lateral del panel.
 *
 * Comparte el lenguaje visual del panel —vidrio, tipografía y tokens `--ink-*`—
 * porque es la misma barra: arriba el soporte, debajo las categorías de artículos.
 */
const SECCIONES = [
  { ruta: '/admin/resumen', etiqueta: 'Resumen', icono: LayoutDashboard, exacta: true },
  { ruta: '/admin/diagnostico', etiqueta: 'Diagnósticos', icono: ScanSearch },
  { ruta: '/admin/tickets', etiqueta: 'Tickets', icono: Ticket },
  { ruta: '/admin/biblioteca', etiqueta: 'Biblioteca SQL', icono: BookOpen },
  { ruta: '/admin/vigilancia', etiqueta: 'Vigilancia', icono: Radar },
];

export default function NavegacionSoporte({
  colapsado = false,
  onNavegar,
}: {
  colapsado?: boolean;
  onNavegar?: () => void;
}) {
  const ruta = usePathname();
  const { theme } = useTheme();
  const claro = theme === 'light';

  return (
    <nav className={colapsado ? 'flex flex-col items-center gap-1' : 'space-y-0.5'}>
      {!colapsado && (
        <p className="px-3 pt-1 pb-2 text-[10.5px] font-semibold uppercase tracking-[0.14em] text-(--ink-500)">
          Centro de Soporte
        </p>
      )}

      {SECCIONES.map(({ ruta: destino, etiqueta, icono: Icono, exacta }) => {
        const activa = exacta ? ruta === destino : ruta.startsWith(destino);
        return (
          <Link
            key={destino}
            href={destino}
            onClick={onNavegar}
            title={colapsado ? etiqueta : undefined}
            aria-current={activa ? 'page' : undefined}
            className={`group relative flex items-center rounded-lg text-sm transition-colors duration-150 ${
              colapsado ? 'size-9 justify-center' : 'h-9 gap-2.5 px-3'
            } ${
              activa
                ? claro
                  ? 'bg-white font-semibold text-(--accent) shadow-xs ring-1 ring-(--line)'
                  : 'bg-[rgba(56,189,248,0.14)] font-semibold text-cyan-100 ring-1 ring-[rgba(56,189,248,0.3)]'
                : claro
                  ? 'text-(--ink-700) hover:bg-white/70 hover:text-(--ink-900)'
                  : 'text-slate-300 hover:bg-[rgba(56,189,248,0.07)] hover:text-slate-100'
            }`}
          >
            {/* Marca de activo: una barra fina en vez de teñir el elemento entero. */}
            {activa && !colapsado && (
              <span className="absolute inset-y-1.5 left-0 w-[3px] rounded-r-full bg-(--accent)" />
            )}
            <Icono
              className={`size-[17px] shrink-0 ${
                activa ? 'text-(--accent)' : claro ? 'text-(--ink-500)' : 'text-slate-400'
              }`}
            />
            {!colapsado && <span className="truncate">{etiqueta}</span>}
          </Link>
        );
      })}
    </nav>
  );
}
