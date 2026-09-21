'use client';

import type { ReactNode } from 'react';
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
    <div className="glass-panel-strong sticky top-0 z-40 overflow-visible rounded-2xl border px-4 sm:px-6">
      <div className="w-full py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-linear-to-br from-[#2363eb] to-[#1aa0c8] flex items-center justify-center text-white font-bold shadow-md">
            S
          </div>
          <div>
            <h1 className="text-xl font-bold text-(--ink-900)">Panel de Administracion - Soluciones MD</h1>
            <p className="text-xs text-(--ink-700)">{subtitulo}</p>
          </div>
        </div>
        <div className="ml-auto flex items-center gap-3">
          {derecha}
          <ThemeToggle className="glass-pill" />
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
      {children && <div className="w-full py-3 border-t border-(--line)">{children}</div>}
    </div>
  );
}
