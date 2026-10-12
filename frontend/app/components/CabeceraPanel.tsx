'use client';

import type { ReactNode } from 'react';
import Marca from './Marca';
import ThemeToggle from './ThemeToggle';
import UserIdentityBadge from './UserIdentityBadge';

interface Usuario {
  username?: string;
  role?: string;
}

/**
 * Barra superior del panel de administración.
 *
 * Se extrajo de `admin/page.tsx` para que el Centro de Soporte use exactamente la
 * misma, en vez de tener su propia cabecera: son dos secciones del mismo panel, no
 * dos aplicaciones.
 */
export default function CabeceraPanel({
  subtitulo,
  currentUser,
  isLightTheme,
  onLogout,
  primaryActionLabel,
  onPrimaryAction,
  derecha,
  children,
}: {
  subtitulo: string;
  currentUser: Usuario | null;
  isLightTheme: boolean;
  onLogout: () => void;
  primaryActionLabel: string;
  onPrimaryAction: () => void;
  /** Pastillas extra antes del selector de tema (por ejemplo el conteo de artículos). */
  derecha?: ReactNode;
  /** Segunda fila opcional: buscador, acciones. */
  children?: ReactNode;
}) {
  return (
    <div className="glass-panel-strong sticky top-0 z-40 overflow-visible rounded-2xl border px-5 sm:px-7">
      {/* Una sola fila, y con aire. Aquí entraban logo, título, subtítulo, pastilla, tema
          e identidad en cincuenta y seis píxeles, y debajo una segunda fila de ancho
          completo con el buscador. El buscador se fue a la barra lateral, con el árbol de
          temas, que es con lo único que tiene que ver. */}
      <div className="flex w-full items-center gap-4 py-4">
        <div className="flex min-w-0 items-center gap-3.5">
          <Marca className="size-10" />
          <div className="min-w-0">
            <h1 className="truncate text-[19px] leading-tight font-semibold tracking-[-0.021em] text-(--ink-900)">
              Panel de Administración
              <span className="ml-2 hidden font-normal text-(--ink-600) sm:inline">Soluciones MD</span>
            </h1>
            <p className="mt-0.5 truncate text-[13px] leading-tight text-(--ink-600)">{subtitulo}</p>
          </div>
        </div>
        <div className="ml-auto flex shrink-0 items-center gap-2">
          {derecha}
          <ThemeToggle />
          {/* Separador: agrupa el tema con la identidad sin que se peguen. */}
          <span className="mx-0.5 hidden h-6 w-px bg-(--line) sm:block" />
          <UserIdentityBadge
            username={currentUser?.username || 'usuario'}
            role={currentUser?.role || 'user'}
            isLightTheme={isLightTheme}
            primaryActionLabel={primaryActionLabel}
            onPrimaryAction={onPrimaryAction}
            onLogout={onLogout}
          />
        </div>
      </div>
      {children && <div className="w-full border-t border-(--line) py-3">{children}</div>}
    </div>
  );
}
