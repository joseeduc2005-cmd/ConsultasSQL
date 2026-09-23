'use client';

import { useEffect, useRef, useState } from 'react';

type UserIdentityBadgeProps = {
  username?: string | null;
  role?: string | null;
  isLightTheme?: boolean;
  onLogout?: (() => void) | null;
  primaryActionLabel?: string | null;
  onPrimaryAction?: (() => void) | null;
};

function toTitleCase(value: string) {
  return value
    .split(' ')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');
}

function formatUsername(username: string) {
  const normalized = String(username || '')
    .trim()
    .replace(/[._-]+/g, ' ')
    .replace(/\s+/g, ' ');

  if (!normalized) return 'Usuario Sistema';

  const titled = toTitleCase(normalized);
  if (titled.toLowerCase() === 'admin') return 'Admin Sistema';
  if (titled.toLowerCase() === 'usuario' || titled.toLowerCase() === 'user') return 'Usuario Sistema';
  return titled;
}

function formatRole(role: string) {
  const normalized = String(role || '').trim().toLowerCase();
  if (normalized === 'admin') return 'Administrador';
  if (normalized === 'superadmin') return 'Super Admin';
  return 'Usuario';
}

function getInitials(label: string) {
  const parts = String(label || '').trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return 'US';
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return `${parts[0][0] || ''}${parts[1][0] || ''}`.toUpperCase();
}

/**
 * Identidad de quien está dentro, con su menú.
 *
 * Antes era una tarjeta con borde, fondo propio y sombra grande: pesaba más que el
 * título del panel y competía con él. Ahora el avatar lleva el color y el resto es
 * texto sobre la barra; el recuadro solo aparece al pasar por encima o al abrir.
 */
export default function UserIdentityBadge({
  username,
  role,
  isLightTheme = true,
  onLogout = null,
  primaryActionLabel = null,
  onPrimaryAction = null,
}: UserIdentityBadgeProps) {
  const [open, setOpen] = useState(false);
  const [shouldRenderMenu, setShouldRenderMenu] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const displayName = formatUsername(String(username || ''));
  const displayRole = formatRole(String(role || 'user'));
  const initials = getInitials(displayName);
  const isAdmin = ['admin', 'superadmin'].includes(String(role || '').trim().toLowerCase());

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (!rootRef.current) return;
      if (!rootRef.current.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  useEffect(() => {
    if (open) {
      setShouldRenderMenu(true);
      return;
    }
    const timer = window.setTimeout(() => setShouldRenderMenu(false), 180);
    return () => window.clearTimeout(timer);
  }, [open]);

  const hover = isLightTheme ? 'hover:bg-slate-900/5' : 'hover:bg-white/10';
  const activo = open ? (isLightTheme ? 'bg-slate-900/5' : 'bg-white/10') : '';

  return (
    <div ref={rootRef} className="relative z-70">
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        aria-haspopup="menu"
        aria-expanded={open}
        className={`flex items-center gap-2.5 rounded-xl py-1.5 pr-2 pl-1.5 text-left transition-colors ${hover} ${activo}`}
      >
        <span
          className={`flex size-9 shrink-0 items-center justify-center rounded-full text-[13px] font-semibold text-white ${
            isAdmin
              ? 'bg-linear-to-br from-[#2363eb] to-[#1aa0c8]'
              : 'bg-linear-to-br from-[#5f84ba] to-[#46618f]'
          }`}
        >
          {initials}
        </span>

        <span className="hidden min-w-0 sm:block">
          <span className={`block truncate text-[13.5px] font-semibold leading-tight ${isLightTheme ? 'text-slate-800' : 'text-slate-100'}`}>
            {displayName}
          </span>
          <span className={`block text-[11.5px] leading-tight ${isLightTheme ? 'text-slate-500' : 'text-slate-400'}`}>
            {displayRole}
          </span>
        </span>

        <svg
          className={`shrink-0 transition-transform duration-200 ${open ? 'rotate-180' : ''} ${isLightTheme ? 'text-slate-400' : 'text-slate-500'}`}
          width="14"
          height="14"
          viewBox="0 0 20 20"
          fill="none"
          aria-hidden="true"
        >
          <path d="M5 7.5L10 12.5L15 7.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </button>

      {shouldRenderMenu && (
        <div
          role="menu"
          className={`absolute right-0 top-[calc(100%+0.5rem)] z-80 min-w-[236px] overflow-hidden rounded-xl border p-1.5 backdrop-blur-md transition-all duration-200 ${
            open ? 'translate-y-0 scale-100 opacity-100' : 'pointer-events-none -translate-y-1 scale-95 opacity-0'
          } ${
            isLightTheme
              ? 'border-slate-200/80 bg-white/95 shadow-[0_16px_38px_rgba(15,23,42,0.13)]'
              : 'border-white/10 bg-[rgba(8,14,36,0.92)] shadow-[0_16px_38px_rgba(0,4,18,0.55)]'
          }`}
        >
          {/* En móvil el nombre no cabe en la barra, así que encabeza el menú. */}
          <div className={`mb-1 border-b px-3 py-2 sm:hidden ${isLightTheme ? 'border-slate-200/70' : 'border-white/10'}`}>
            <p className={`truncate text-[13.5px] font-semibold ${isLightTheme ? 'text-slate-800' : 'text-slate-100'}`}>{displayName}</p>
            <p className={`text-[11.5px] ${isLightTheme ? 'text-slate-500' : 'text-slate-400'}`}>{displayRole}</p>
          </div>

          {primaryActionLabel && onPrimaryAction && (
            <button
              type="button"
              role="menuitem"
              onClick={() => {
                setOpen(false);
                onPrimaryAction();
              }}
              className={`flex w-full items-center gap-2.5 rounded-lg px-3 py-2 text-left text-[13.5px] font-medium transition-colors ${
                isLightTheme ? 'text-slate-700 hover:bg-slate-100' : 'text-slate-200 hover:bg-white/10'
              }`}
            >
              <IconoEnlace className={isLightTheme ? 'text-slate-400' : 'text-slate-500'} />
              {primaryActionLabel}
            </button>
          )}

          {onLogout && (
            <button
              type="button"
              role="menuitem"
              onClick={() => {
                setOpen(false);
                onLogout();
              }}
              className={`flex w-full items-center gap-2.5 rounded-lg px-3 py-2 text-left text-[13.5px] font-medium transition-colors ${
                isLightTheme ? 'text-rose-600 hover:bg-rose-50' : 'text-rose-400 hover:bg-[rgba(244,63,94,0.12)]'
              }`}
            >
              <IconoSalir />
              Cerrar sesión
            </button>
          )}
        </div>
      )}
    </div>
  );
}

function IconoEnlace({ className }: { className?: string }) {
  return (
    <svg className={`size-4 shrink-0 ${className ?? ''}`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M15 3h6v6M10 14 21 3M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
    </svg>
  );
}

function IconoSalir() {
  return (
    <svg className="size-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9" />
    </svg>
  );
}
