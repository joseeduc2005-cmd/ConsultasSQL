'use client';

import { useTheme } from './ThemeProvider';

type ThemeToggleProps = {
  className?: string;
};

/**
 * Interruptor de tema: un riel con el sol y la luna, y una perilla que se desliza.
 *
 * Antes era un botón con el texto «Nocturno Azul», que obligaba a leer para saber
 * qué hacía y además decía el tema *al que iba a cambiar*, no el que estaba puesto.
 * Los dos iconos se ven siempre y la perilla marca cuál está activo.
 */
export default function ThemeToggle({ className = '' }: ThemeToggleProps) {
  const { theme, toggleTheme } = useTheme();
  const claro = theme === 'light';

  return (
    <button
      type="button"
      role="switch"
      aria-checked={!claro}
      onClick={toggleTheme}
      title={claro ? 'Cambiar a modo oscuro' : 'Cambiar a modo claro'}
      aria-label="Cambiar tema"
      className={`group relative inline-flex h-9 w-[68px] shrink-0 items-center rounded-full border p-1 transition-colors duration-300 ${
        claro
          ? 'border-slate-200 bg-slate-100 hover:border-slate-300'
          : 'border-white/10 bg-slate-800 hover:border-white/20'
      } ${className}`}
    >
      {/*
        El icono del tema activo va DENTRO de la perilla, y el otro queda apagado en
        el riel. Con los dos en el riel la perilla tapaba justo el activo, y se leía
        al revés: parecía indicar el tema al que ibas a cambiar.
      */}
      <span className="pointer-events-none absolute inset-0 flex items-center justify-between px-[10px]">
        <Sol className={`size-[13px] transition-opacity duration-300 ${claro ? 'opacity-0' : 'text-slate-500 opacity-100'}`} />
        <Luna className={`size-[13px] transition-opacity duration-300 ${claro ? 'text-slate-400 opacity-100' : 'opacity-0'}`} />
      </span>

      <span
        className={`pointer-events-none relative grid size-7 place-items-center rounded-full shadow-sm ring-1 transition-transform duration-300 ease-out ${
          claro ? 'translate-x-0 bg-white ring-black/5' : 'translate-x-[32px] bg-slate-900 ring-white/15'
        }`}
      >
        {claro ? <Sol className="size-[15px] text-amber-500" /> : <Luna className="size-[14px] text-sky-300" />}
      </span>
    </button>
  );
}

function Sol({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" aria-hidden="true">
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
    </svg>
  );
}

function Luna({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
      <path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z" />
    </svg>
  );
}
