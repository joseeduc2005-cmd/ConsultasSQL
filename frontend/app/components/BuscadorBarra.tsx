'use client';

import { Search, X } from 'lucide-react';

/**
 * El buscador, en la barra lateral y encima de todo lo demás.
 *
 * Vivía en la cabecera, en una segunda fila que ocupaba el ancho entero de la pantalla
 * para un campo que se usa con una mano. Y estaba lejos de lo único con lo que tiene que
 * ver: el árbol de temas, que está en la barra. Buscar y navegar son la misma tarea
 * —encontrar un tema— y tenerlas en dos sitios obliga a cruzar la pantalla para algo que
 * se hace a cada rato.
 *
 * Arriba del todo porque es lo primero que se intenta. Quien ya sabe dónde está baja al
 * árbol; quien no lo sabe, escribe.
 */
export function BuscadorBarra({
  valor,
  onCambio,
  colapsado,
  etiqueta = 'Filtrar la lista',
}: {
  valor: string;
  onCambio: (texto: string) => void;
  colapsado?: boolean;
  etiqueta?: string;
}) {
  // Contraída, la barra tiene sitio para un icono y nada más. Un campo de texto de
  // cuarenta píxeles no es un campo de texto.
  if (colapsado) {
    return (
      <div className="grid place-items-center pb-2">
        <span
          className="grid size-9 place-items-center rounded-lg text-(--ink-600)"
          title={etiqueta}
        >
          <Search className="size-[18px]" />
        </span>
      </div>
    );
  }

  return (
    <div className="relative px-1 pb-3">
      <Search className="pointer-events-none absolute top-1/2 left-3.5 size-4 -translate-y-1/2 text-(--ink-500)" />
      <input
        type="search"
        value={valor}
        onChange={(e) => onCambio(e.target.value)}
        placeholder={etiqueta}
        aria-label={etiqueta}
        className="input-glass w-full py-2 pr-8 pl-9 text-[14px]"
      />
      {valor && (
        <button
          type="button"
          onClick={() => onCambio('')}
          aria-label="Borrar la búsqueda"
          className="absolute top-1/2 right-3 grid size-5 -translate-y-1/2 place-items-center rounded-full text-(--ink-500) transition-colors hover:text-(--ink-900)"
        >
          <X className="size-3.5" />
        </button>
      )}
    </div>
  );
}

export default BuscadorBarra;
