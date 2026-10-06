/**
 * Cuándo lo que alguien escribió es un error pegado y no una pregunta.
 *
 * Si el texto trae «exception», «timeout», «stack trace» o similares, el sistema deja
 * de intentar entenderlo como una consulta y lo trata como un error que analizar. Es
 * una sola regla, pero decide por qué camino entero va la petición.
 *
 * Está aquí y no en infrastructure/query/ErrorAnalysisEngine.js, donde vivía, porque
 * es una regla y no infraestructura: comparar un texto con una lista de palabras no
 * necesita red ni base. Tenerla allí obligaba a dominio/consultas/decidir.js a
 * importar hacia afuera. El motor de análisis la reexporta para que nada de lo que ya
 * la usaba cambie.
 */

/**
 * Las palabras que delatan un error.
 *
 * Es deliberadamente generosa —«null», «connection» y «undefined» aparecen también en
 * preguntas normales— y eso es una decisión: equivocarse mandando una pregunta al
 * analizador de errores solo cuesta una respuesta peor, mientras que no reconocer un
 * error pegado deja a la persona sin lo único que le servía.
 */
export const ERROR_TRIGGER_RE = /\b(error|exception|failed|failure|refused|denied|timeout|timed\s+out|undefined|null|cannot|syntax\s+error|connection|authentication|unauthorized|forbidden|trace|stack|traceback|fatal|panic|crash)\b/i;

export function detectErrorAnalysisInput(text = '') {
  return ERROR_TRIGGER_RE.test(String(text || ''));
}
