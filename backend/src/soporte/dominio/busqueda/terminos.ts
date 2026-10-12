/**
 * Convertir lo que escribe una persona en términos de búsqueda.
 *
 * Esto es dominio puro: no sabe que existe PostgreSQL, ni HTTP, ni nada. Recibe un
 * texto y devuelve palabras. Por eso se puede probar sin levantar nada.
 */

/**
 * Palabras que no distinguen nada: las de relleno del español y las que salen en todos
 * los tickets. Las cortas están aquí desde que el mínimo de longitud bajó a dos: hacía
 * falta para no perder «qr», «pos» o «pin», pero dejaba entrar «de» y «la».
 *
 * El diccionario español de PostgreSQL también las descarta, así que esto es redundante
 * a propósito: deja la garantía escrita donde se lee, y no dependiendo de qué haga por
 * dentro un motor que puede cambiar de versión.
 */
export const VACIAS = new Set([
  'de', 'la', 'el', 'en', 'un', 'lo', 'al', 'se', 'su', 'mi', 'tu', 'es', 'ya', 'si',
  'no', 'me', 'le', 'te', 'ha', 'he', 'va', 'yo', 'eso', 'ese', 'esa', 'ser', 'son',
  'sus', 'mas', 'muy', 'hay', 'dos', 'tres',
  'para', 'pero', 'como', 'esta', 'este', 'esto', 'sobre', 'desde', 'hasta', 'cuando',
  'porque', 'tambien', 'favor', 'gracias', 'estimados', 'buenos', 'buenas', 'dias',
  'tardes', 'saludos', 'atentos', 'quedamos', 'adjunto', 'reporta', 'reportado',
  'solicita', 'solicitan', 'comenta', 'indica', 'nos', 'les', 'que', 'con', 'del',
  'los', 'las', 'una', 'por', 'sistema', 'usuario', 'error', 'problema', 'caso',
]);

/** Más de esto y la frase es un correo entero pegado; no hace falta todo para buscar. */
const MAX_TERMINOS = 24;

/**
 * Los términos de una frase, listos para buscar.
 *
 * Se unen con OR y no con AND: el título de un tema rara vez coincide palabra por
 * palabra con el de otro, y exigir todas devolvía siempre vacío. El orden por relevancia
 * se encarga de que las que más coinciden suban.
 *
 * Devuelve `null` cuando no queda nada con lo que buscar.
 */
export function aTerminos(texto: string): string[] {
  const palabras = texto
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    // Dos caracteres, no cuatro. El mínimo alto estaba para quitar «de», «la», «si»,
    // pero de eso ya se encarga el diccionario español de PostgreSQL, que las descarta
    // al convertirlas en lexemas. Lo que sí se llevaba por delante era el vocabulario
    // del dominio —`qr`, `pos`, `pin`, `mac`, `sms`, `otp`— y los números cortos: un
    // pago con QR no encontraba el incidente de QR porque el término se tiraba antes
    // de llegar a la consulta, igual que pasaba con «300».
    .filter((p) => p.length >= 2 && !VACIAS.has(p));

  return [...new Set(palabras)].slice(0, MAX_TERMINOS);
}

/** La forma que entiende `to_tsquery`: los términos unidos por OR. */
export function aConsulta(texto: string): string | null {
  const terminos = aTerminos(texto);
  return terminos.length > 0 ? terminos.join(' | ') : null;
}
