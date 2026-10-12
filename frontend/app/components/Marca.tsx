'use client';

/**
 * La marca del sistema.
 *
 * Era una «S» dentro de un cuadrado azul con degradado. Una letra suelta no dice nada —
 * podría ser de cualquier cosa— y el degradado era lo más saturado de toda la pantalla,
 * que es mucho peso para algo que nadie necesita mirar dos veces.
 *
 * Ahora dibuja lo que hace el sistema: una lupa buscando dentro de una pila de datos.
 * Es lo mismo que cuenta el producto —se busca algo en una base— y se lee a dieciséis
 * píxeles, que es donde de verdad se ve.
 *
 * Va en trazo y hereda el color, así que sirve igual sobre claro y sobre oscuro sin
 * tener dos versiones que se desincronicen.
 */
export function Marca({ className = 'size-9' }: { className?: string }) {
  return (
    <span
      className={`grid shrink-0 place-items-center rounded-[11px] bg-(--accent) text-white ${className}`}
      aria-hidden="true"
    >
      <svg viewBox="0 0 24 24" fill="none" className="size-[62%]">
        {/* Las capas de la base, de más lejana a más cercana. */}
        <ellipse cx="12" cy="5.4" rx="7.4" ry="2.9" stroke="currentColor" strokeWidth="1.7" />
        <path
          d="M4.6 5.4v5.6c0 1.6 3.3 2.9 7.4 2.9"
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinecap="round"
        />
        <path
          d="M19.4 5.4v3.1"
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinecap="round"
        />
        {/* La lupa: lo que el sistema hace con esa base. */}
        <circle cx="15.6" cy="14.4" r="4" stroke="currentColor" strokeWidth="1.7" />
        <path d="M18.7 17.5 21.3 20.1" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
      </svg>
    </span>
  );
}

export default Marca;
