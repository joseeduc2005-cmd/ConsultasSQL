'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useTheme } from './ThemeProvider';

/**
 * Accesos del Centro de Soporte dentro de la barra lateral del panel.
 *
 * Va con el lenguaje visual del panel de administración —no con el del módulo—
 * porque es la misma barra: arriba el soporte, debajo las categorías de artículos.
 */
const SECCIONES = [
  { ruta: '/admin/resumen', etiqueta: 'Resumen', exacta: true },
  { ruta: '/admin/diagnostico', etiqueta: 'Diagnósticos' },
  { ruta: '/admin/tickets', etiqueta: 'Tickets' },
  { ruta: '/admin/biblioteca', etiqueta: 'Biblioteca SQL' },
  { ruta: '/admin/vigilancia', etiqueta: 'Vigilancia' },
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

  if (colapsado) return null;

  return (
    <div className="px-2 pt-3">
      <h2 className="px-1 pb-2 text-sm font-bold uppercase tracking-wide text-(--ink-900)">Centro de Soporte</h2>
      <div className="space-y-1">
        {SECCIONES.map(({ ruta: destino, etiqueta, exacta }) => {
          const activa = exacta ? ruta === destino : ruta.startsWith(destino);
          return (
            <Link
              key={destino}
              href={destino}
              onClick={onNavegar}
              className={`block w-full rounded-lg px-3 py-2 text-sm transition-all duration-150 ${
                activa
                  ? claro
                    ? 'bg-linear-to-r from-[#2363eb] to-[#1aa0c8] font-semibold text-white shadow-md ring-2 ring-[#b9d2ff]'
                    : 'bg-linear-to-r from-[#2363eb] to-[#0ea5e9] font-semibold text-white shadow-md ring-1 ring-cyan-400/50'
                  : claro
                    ? 'text-(--ink-700) hover:bg-white/75 hover:text-(--ink-900)'
                    : 'text-slate-300 hover:bg-[rgba(56,189,248,0.08)] hover:text-slate-100'
              }`}
            >
              {etiqueta}
            </Link>
          );
        })}
      </div>
      <div className="mx-1 mt-3 border-t border-(--line)" />
    </div>
  );
}
