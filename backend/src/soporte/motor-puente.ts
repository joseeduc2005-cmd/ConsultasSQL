/**
 * Puente con el registro de bases del motor.
 *
 * La exploración genérica necesita dos cosas de cada base: su esquema —que el motor
 * ya introspecciona al arrancar— y una forma de ejecutar una consulta contra ella.
 * Las dos las tiene `MultiDbRegistry`, así que aquí solo se adaptan.
 *
 * Lo único que este archivo aporta de verdad son dos cuidados:
 *
 *   - Cada motor escribe los parámetros distinto: Postgres `$1`, MySQL `?`, Oracle
 *     `:p1`. La exploración escribe uno solo y aquí se traduce.
 *   - Todo pasa por el guardián de solo lectura antes de salir. `executeCompiledQuery`
 *     ejecuta lo que le den, incluidas escrituras, y esto nunca debe escribir.
 */
import { asegurarSoloLectura } from './oracle/guardia.ts';
import type { Ejecutor, TablaEsquema } from './exploracion-generica.ts';

/** Lo que se usa del registro del motor, sin arrastrar todo el objeto. */
export interface RegistroMotor {
  getDatabaseById(id: string): { id: string; type?: string; label?: string } | null | undefined;
  getSchemaTables(id: string): TablaEsquema[] | undefined;
  executeCompiledQuery(consulta: { databaseId: string; sql: string; params?: unknown[] | Record<string, unknown> }): Promise<Record<string, unknown>[]>;
  getDatabaseSummaries?(): { id: string; label?: string; type?: string; connected?: boolean }[];
}

/** Lo que se le ofrece a alguien para que diga sobre qué base trabajó. */
export interface BaseVisible {
  id: string;
  etiqueta: string;
  motor: string;
}

let registro: RegistroMotor | null = null;

/** Lo llama `app.js` al arrancar, con el registro ya cargado. */
export function configurarMotor(r: RegistroMotor): void {
  registro = r;
}

export function motorDisponible(): boolean {
  return registro !== null;
}

/** El esquema introspeccionado de una base, o vacío si no hay o no se ha introspeccionado. */
export function esquemaDe(databaseId: string): TablaEsquema[] {
  if (!registro) return [];
  try {
    return registro.getSchemaTables(databaseId) ?? [];
  } catch {
    return [];
  }
}

/**
 * Las bases a las que el backend llegó de verdad.
 *
 * Se filtran por `connected` y no se devuelven todas las configuradas a propósito: el
 * desplegable es para que alguien que no se sabe los nombres elija el suyo, y ofrecerle
 * una base que el sistema no alcanzó es ofrecerle una respuesta que nadie va a poder
 * comprobar. Sin motor configurado la lista va vacía, y la pantalla lo dice.
 */
export function basesConectadas(): BaseVisible[] {
  if (!registro?.getDatabaseSummaries) return [];
  try {
    return registro
      .getDatabaseSummaries()
      .filter((b) => b.connected === true)
      .map((b) => ({ id: b.id, etiqueta: b.label?.trim() || b.id, motor: (b.type ?? '').toLowerCase() }));
  } catch {
    return [];
  }
}

export function tipoDe(databaseId: string): string | null {
  try {
    return registro?.getDatabaseById(databaseId)?.type ?? null;
  } catch {
    return null;
  }
}

/**
 * Traduce los marcadores `$1, $2…` al estilo del motor de destino.
 *
 * La exploración escribe un solo SQL y no tiene por qué saber contra qué base va.
 */
function adaptar(sql: string, parametros: unknown[], tipo: string): { sql: string; params: unknown[] | Record<string, unknown> } {
  if (tipo === 'mysql' || tipo === 'mariadb') {
    return { sql: sql.replace(/\$\d+/g, '?'), params: parametros };
  }
  if (tipo === 'oracle') {
    // Oracle nombra sus binds; se le pasan como objeto p1, p2…
    const params: Record<string, unknown> = {};
    parametros.forEach((v, i) => {
      params[`p${i + 1}`] = v;
    });
    return { sql: sql.replace(/\$(\d+)/g, (_, n) => `:p${n}`), params };
  }
  return { sql, params: parametros };
}

/**
 * Un ejecutor de solo lectura para la base indicada.
 *
 * Devuelve `null` si la base no está registrada: es mejor no explorar que explorar a
 * ciegas contra algo que no se sabe qué es.
 */
export function ejecutorPara(databaseId: string): Ejecutor | null {
  if (!registro) return null;
  const base = (() => {
    try {
      return registro!.getDatabaseById(databaseId);
    } catch {
      return null;
    }
  })();
  if (!base) return null;

  const tipo = (base.type ?? 'postgres').toLowerCase();
  // Mongo no habla SQL; la exploración genérica no le aplica.
  if (tipo === 'mongodb') return null;

  return async (sql: string, parametros: unknown[]) => {
    // El guardián es la última barrera: aquí solo se leen datos, nunca se escriben.
    asegurarSoloLectura(sql);
    const adaptado = adaptar(sql, parametros, tipo);
    const filas = await registro!.executeCompiledQuery({
      databaseId,
      sql: adaptado.sql,
      params: adaptado.params,
    });
    // Oracle devuelve las columnas en mayúsculas; se normalizan para que la
    // exploración encuentre `fila[columna]` con el nombre que le dio el esquema.
    return (filas ?? []).map((f) => {
      const salida: Record<string, unknown> = { ...f };
      for (const [k, v] of Object.entries(f)) salida[k.toLowerCase()] = v;
      return salida;
    });
  };
}

/** Citado de identificadores según el motor: Oracle y Postgres usan comillas dobles, MySQL acentos graves. */
export function citarSegunMotor(nombre: string, tipo: string | null): string {
  const limpio = nombre.replace(/["`]/g, '');
  return tipo === 'mysql' || tipo === 'mariadb' ? `\`${limpio}\`` : `"${limpio}"`;
}
