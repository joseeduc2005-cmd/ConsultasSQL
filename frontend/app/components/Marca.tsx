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
      {/*
        El dibujo va centrado dentro del recuadro, que es lo que fallaba: la base ocupaba
        la mitad izquierda y la lupa se salia por la derecha y por abajo, asi que el
        conjunto quedaba descolgado hacia una esquina. Se ve enseguida cuando el recuadro
        es un circulo o un cuadrado con mucho redondeo.
      */}
      <svg viewBox="0 0 24 24" fill="none" className="size-[64%]">
        <g
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
          transform="translate(0.2 -0.4)"
        >
          {/* La base: su tapa y los dos costados. */}
          <ellipse cx="10.6" cy="6.3" rx="6.3" ry="2.5" />
          <path d="M4.3 6.3v5.8c0 1.4 2.8 2.5 6.3 2.5" />
          <path d="M16.9 6.3v2.6" />
          {/* La lupa, apoyada sobre la base y dentro del recuadro. */}
          <circle cx="14.4" cy="14.6" r="4.1" />
          <path d="m17.5 17.7 2.4 2.4" />
        </g>
      </svg>
    </span>
  );
}

export default Marca;
