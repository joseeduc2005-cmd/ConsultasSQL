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
  const isAdmin = String(role || '').trim().toLowerCase() === 'admin' || String(role || '').trim().toLowerCase() === 'superadmin';

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (!rootRef.current) return;
      if (!rootRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  useEffect(() => {
    if (open) {
      setShouldRenderMenu(true);
      return;
    }

    const timer = window.setTimeout(() => {
      setShouldRenderMenu(false);
    }, 180);

    return () => window.clearTimeout(timer);
  }, [open]);

  return (
    <div ref={rootRef} className="relative z-[70]">
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        className="flex items-center gap-2.5 rounded-2xl border border-white/40 bg-white/55 px-2.5 py-2 text-left shadow-[0_10px_25px_rgba(15,23,42,0.08)] backdrop-blur-md transition hover:bg-white/70"
      >
        <div
          className={`flex h-10 w-10 items-center justify-center rounded-full text-sm font-bold text-white shadow-md ${
            isAdmin
              ? 'bg-gradient-to-br from-[#2363eb] to-[#1aa0c8]'
              : 'bg-gradient-to-br from-[#5f84ba] to-[#46618f]'
          }`}
        >
          {initials}
        </div>

        <div className="min-w-0 pr-1">
          <p className={`truncate text-base font-semibold leading-none ${isLightTheme ? 'text-slate-800' : 'text-slate-100'}`}>
            {displayName}
          </p>
          <p className={`mt-0.5 text-xs ${isLightTheme ? 'text-slate-500' : 'text-slate-400'}`}>{displayRole}</p>
        </div>

        <svg
          className={`transition duration-200 ${open ? 'rotate-180' : ''} ${isLightTheme ? 'text-slate-500' : 'text-slate-400'}`}
          width="16"
          height="16"
          viewBox="0 0 20 20"
          fill="none"
          aria-hidden="true"
        >
          <path d="M5 7.5L10 12.5L15 7.5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </button>

      {shouldRenderMenu && (
        <div
          className={`absolute right-0 top-[calc(100%+0.55rem)] z-[80] min-w-[210px] overflow-hidden rounded-2xl border border-white/50 bg-white/95 p-1.5 shadow-[0_18px_40px_rgba(15,23,42,0.14)] backdrop-blur-md transition-all duration-200 ${
            open ? 'translate-y-0 scale-100 opacity-100' : 'pointer-events-none -translate-y-1 scale-95 opacity-0'
          }`}
        >
          {primaryActionLabel && onPrimaryAction && (
            <button
              type="button"
              onClick={() => {
                setOpen(false);
                onPrimaryAction();
              }}
              className="flex w-full items-center justify-between rounded-xl px-3 py-2 text-sm font-medium text-sky-700 transition hover:bg-sky-50"
            >
              <span>{primaryActionLabel}</span>
              <span aria-hidden="true">↗</span>
            </button>
          )}

          {onLogout && (
            <button
              type="button"
              onClick={() => {
                setOpen(false);
                onLogout();
              }}
              className="flex w-full items-center justify-between rounded-xl px-3 py-2 text-sm font-medium text-rose-600 transition hover:bg-rose-50"
            >
              <span>Cerrar sesión</span>
              <span aria-hidden="true">↗</span>
            </button>
          )}
        </div>
      )}
    </div>
  );
}
