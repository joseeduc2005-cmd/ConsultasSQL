/**
 * Última barrera antes de enviar SQL a Oracle: solo se permiten sentencias de lectura.
 * Complementa (no reemplaza) a un usuario de base de datos con permisos solo de SELECT
 * y a la transacción READ ONLY que abre la fuente Oracle.
 */
const PROHIBIDAS =
  /\b(insert|update|delete|merge|drop|alter|truncate|grant|revoke|create|rename|begin|declare|execute|exec|call|commit|rollback|savepoint|lock|dbms_\w+|utl_\w+)\b/i;

export function asegurarSoloLectura(sql: string): void {
  const limpio = sql
    .replace(/--[^\n]*/g, ' ')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/'(?:[^']|'')*'/g, "''")
    .trim()
    .replace(/;\s*$/, '');

  if (!/^(select|with)\b/i.test(limpio)) {
    throw new Error('Solo se permiten consultas SELECT.');
  }
  if (limpio.includes(';')) {
    throw new Error('No se permiten múltiples sentencias.');
  }
  const prohibida = limpio.match(PROHIBIDAS);
  if (prohibida) {
    throw new Error(`Palabra no permitida en consulta de lectura: ${prohibida[0].toUpperCase()}.`);
  }
}
