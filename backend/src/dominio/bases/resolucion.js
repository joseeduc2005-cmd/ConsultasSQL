/**
 * Decidir contra qué base se ejecuta una consulta.
 *
 * Una de las decisiones de más riesgo del sistema: equivocarse aquí significa correr
 * una consulta contra la base que no era. Estaba dentro de `app.js`, y además atada a
 * `multiDbRegistry` — una variable global del módulo — así que no había forma de
 * probarla sin arrancar el servidor y conectar seis bases.
 *
 * Al sacarla, las bases se reciben como argumento en vez de ir a buscarlas. Es el
 * cambio que convierte una función intestable en una que se prueba con una lista de
 * tres objetos, y de paso deja dicho de qué depende en lugar de esconderlo.
 */

/**
 * Qué bases menciona un texto escrito por una persona.
 *
 * Reconoce dos cosas: el identificador exacto de una base configurada, y el nombre del
 * motor —«oracle», «postgres», «mysql»— que puede señalar a varias a la vez.
 *
 * @param {string} text          lo que escribió la persona
 * @param {Array<{id: string, type: string}>} basesConfiguradas  las registradas
 * @returns {string[]} los identificadores mencionados, sin repetir
 */
export function extractDatabaseHintsFromText(text, basesConfiguradas = []) {
  const input = String(text || '').trim().toLowerCase();
  if (!input) return [];

  const configured = Array.isArray(basesConfiguradas) ? basesConfiguradas : [];
  if (configured.length === 0) return [];

  const ids = new Set();
  const mentionsOracle = /\boracle\b/.test(input);
  const mentionsPostgres = /\bpostgres(?:ql)?\b|\bpg\b/.test(input);
  const mentionsMySql = /\bmysql\b/.test(input);

  for (const db of configured) {
    const dbId = String(db.id || '').trim();
    const dbType = String(db.type || '').trim().toLowerCase();
    if (!dbId) continue;

    if (input.includes(dbId.toLowerCase())) {
      ids.add(dbId);
      continue;
    }

    if (mentionsOracle && dbType === 'oracle') ids.add(dbId);
    if (mentionsPostgres && dbType === 'postgres') ids.add(dbId);
    if (mentionsMySql && dbType === 'mysql') ids.add(dbId);
  }

  return [...ids];
}

/**
 * Las bases contra las que correr una consulta.
 *
 * Lo que se pide explícitamente manda sobre lo que se adivina del texto. Es la decisión
 * correcta: si alguien eligió una base en la pantalla, no hay nada que deducir.
 */
export function resolveRequestedDatabasesForQuery(text, requestedDatabases = [], basesConfiguradas = []) {
  const explicit = Array.isArray(requestedDatabases)
    ? requestedDatabases.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  if (explicit.length > 0) {
    return [...new Set(explicit)];
  }

  return [...new Set(extractDatabaseHintsFromText(text, basesConfiguradas))];
}
