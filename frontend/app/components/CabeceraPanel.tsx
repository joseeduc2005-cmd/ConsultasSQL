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
      <div className="flex w-full items-center gap-4 py-3">
        <div className="flex min-w-0 items-center gap-3">
          <div className="flex size-10 shrink-0 items-center justify-center rounded-xl bg-linear-to-br from-[#2363eb] to-[#1aa0c8] text-[15px] font-bold text-white shadow-sm">
            S
          </div>
          <div className="min-w-0">
            <h1 className="truncate text-[17px] leading-tight font-semibold tracking-tight text-(--ink-900)">
              Panel de Administración
              <span className="ml-2 hidden text-(--ink-700) sm:inline">· Soluciones MD</span>
            </h1>
            <p className="truncate text-[12px] leading-tight text-(--ink-700)">{subtitulo}</p>
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
      {children && <div className="w-full py-3 border-t border-(--line)">{children}</div>}
    </div>
  );
}
