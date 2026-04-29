'use client';

import { useTheme } from './ThemeProvider';

type ThemeToggleProps = {
  className?: string;
};

export default function ThemeToggle({ className = '' }: ThemeToggleProps) {
  const { theme, toggleTheme } = useTheme();
  const isLightTheme = theme === 'light';

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className={`rounded-full border px-3 py-1.5 text-xs font-semibold uppercase tracking-wide transition ${isLightTheme
        ? 'border-sky-300 bg-sky-50 text-sky-700 hover:bg-sky-100'
        : 'border-cyan-500/50 bg-slate-900 text-cyan-200 hover:bg-slate-800'} ${className}`}
      aria-label="Cambiar tema"
      title="Cambiar entre claro y oscuro"
    >
      {isLightTheme ? 'Nocturno Azul' : 'Brillo Azul'}
    </button>
  );
}
