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
        Cinco trazos no caben en veinte pixeles.
        Llevaba una base de datos con su tapa y sus dos costados, mas una lupa encima: a
        tamano de cabecera eso se convierte en una mancha y se lee como «algo raro». Se
        queda la lupa sola, con el trazo mas grueso y centrada: es lo que hace el sistema
        —buscar una solucion— y a ese tamano se reconoce de un vistazo, que es lo unico
        que se le pide a una marca.
      */}
      <svg viewBox="0 0 24 24" fill="none" className="size-[58%]">
        <g stroke="currentColor" strokeWidth="2.2" strokeLinecap="round">
          <circle cx="10.5" cy="10.5" r="6.5" />
          <path d="m15.4 15.4 4.1 4.1" />
        </g>
      </svg>
    </span>
  );
}

export default Marca;
