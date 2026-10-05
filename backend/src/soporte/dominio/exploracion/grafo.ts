/**
 * El grafo que la base declara sola.
 *
 * Las claves foráneas son aristas y las tablas nodos. Nadie lo dibuja: viene con la
 * base, puesto por quien la diseñó hace años, y el motor ya lo introspecciona al
 * arrancar — lo que pasa es que durante mucho tiempo nadie lo leía.
 *
 * Dominio puro: recibe un esquema y devuelve relaciones. No consulta nada.
 */

export interface ClaveForanea {
  column: string;
  referencedTable: string;
  referencedColumn: string;
  constraintName?: string;
}

export interface TablaEsquema {
  name: string;
  columns?: { name: string; type?: string }[];
  /** Las aristas del grafo. El motor ya las trae; durante mucho tiempo no se usaron. */
  foreignKeys?: ClaveForanea[];
}

/**
 * Nombres con los que cada dato suele aparecer.
 *
 * La lista es el conocimiento: no hace falta saber el esquema de nadie, basta con que
 * la columna se llame de alguna forma razonable. Cuando aparezca una base que use
 * otro nombre, se añade aquí y sirve para todas.
 */

export interface Arista {
  hacia: string;
  /** Columna de la tabla de partida que sostiene la relación. */
  columnaLocal: string;
  /** Columna de la tabla de destino con la que empareja. */
  columnaRemota: string;
  sentido: 'padre' | 'hijo';
}

function mismaTabla(a: string, b: string): boolean {
  // El motor normaliza los nombres, pero una FK puede venir cualificada con su esquema
  // («corresponsales.usuario») y la tabla no, o al revés.
  const sinEsquema = (n: string) => n.toLowerCase().split('.').pop() ?? n.toLowerCase();
  return sinEsquema(a) === sinEsquema(b);
}

/**
 * Las tablas vecinas de una, en las dos direcciones.
 *
 * Esto es el grafo: nodos las tablas, aristas las claves foráneas. El motor ya las
 * introspecciona al arrancar y hasta ahora nadie las leía — la exploración adivinaba
 * por el nombre de las columnas en vez de seguir las relaciones que la base declara.
 */
export function vecinas(esquema: TablaEsquema[], nombreTabla: string): Arista[] {
  const aristas: Arista[] = [];

  const tabla = esquema.find((t) => mismaTabla(t.name, nombreTabla));
  for (const fk of tabla?.foreignKeys ?? []) {
    if (!esquema.some((t) => mismaTabla(t.name, fk.referencedTable))) continue;
    aristas.push({
      hacia: esquema.find((t) => mismaTabla(t.name, fk.referencedTable))!.name,
      columnaLocal: fk.column,
      columnaRemota: fk.referencedColumn,
      sentido: 'padre',
    });
  }

  for (const otra of esquema) {
    if (mismaTabla(otra.name, nombreTabla)) continue;
    for (const fk of otra.foreignKeys ?? []) {
      if (!mismaTabla(fk.referencedTable, nombreTabla)) continue;
      aristas.push({
        hacia: otra.name,
        columnaLocal: fk.referencedColumn,
        columnaRemota: fk.column,
        sentido: 'hijo',
      });
    }
  }

  return aristas;
}

/** Hasta dónde se camina y cuántas tablas se tocan: esto corre dentro de un diagnóstico. */
