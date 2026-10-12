/**
 * El catálogo de conocimiento, servido desde PostgreSQL.
 *
 * Este archivo es el único del lado de la búsqueda que sabe SQL. Todo lo que hay arriba
 * —el dominio y el caso de uso— habla con el puerto y no se entera de que debajo hay
 * `tsvector`, `ts_rank` ni índices GIN.
 *
 * Que todo el SQL esté junto tiene una ventaja concreta: cuando una consulta se pone
 * lenta o hay que cambiar de motor, se sabe exactamente dónde mirar.
 */
import { ejecutar, filas } from '../../knowledge/db.ts';
import type {
  BusquedaRegistrada,
  Catalogo,
  Pregunta,
  TemaEncontrado,
  TicketEncontrado,
} from '../../puertos/catalogo.ts';

/**
 * Lo aprendido empuja, no manda.
 *
 * Tope de 0,25 sobre una cobertura que va de 0 a 1, así que no puede subir un tema que
 * no tenga nada que ver por mucho que lo hayan abierto, y lo que de verdad coincide
 * sigue ganando sin ayuda.
 */
const EMPUJE_POR_VISITA = 0.03;
const EMPUJE_MAXIMO = 0.25;

/** Un tema con script vale más: además de explicar, trae el arreglo. */
const VENTAJA_CON_SCRIPT = 1.4;

export const catalogoPostgres: Catalogo = {
  async contarTemas() {
    const [fila] = await filas<{ total: number }>('SELECT count(*)::int AS total FROM knowledge_base');
    return fila?.total ?? 0;
  },

  /**
   * En cuántos temas aparece cada término.
   *
   * Una búsqueda por índice por término; son veinticuatro como mucho y van por el GIN.
   * Se cuenta por raíz, no por letra exacta: «imprimió», «imprime» e «imprimiendo»
   * caen en el mismo lexema y cuentan como el mismo término.
   */
  frecuenciaDe(terminos) {
    return filas<{ termino: string; apareceEn: number }>(
      `SELECT t.termino,
              (SELECT count(*)::int FROM knowledge_base k
                WHERE k.busqueda @@ to_tsquery('spanish', t.termino)) AS "apareceEn"
         FROM unnest($1::text[]) AS t(termino)`,
      [terminos],
    );
  },

  buscarTemas({ terminos, pesos, limite }: Pregunta, soloPublicos: boolean): Promise<TemaEncontrado[]> {
    const consulta = terminos.join(' | ');
    const pesoTotal = pesos.reduce((a, b) => a + b, 0) || 1;

    return filas<TemaEncontrado>(
      `WITH p(termino, peso) AS (SELECT * FROM unnest($4::text[], $5::float8[]))
       SELECT * FROM (
         SELECT k.id::text, k.titulo, k.descripcion, k.categoria,
                k.script_json->>'sql' AS sql,
                -- Qué parte del peso de la pregunta cubre este tema.
                (SELECT coalesce(sum(p.peso), 0) FROM p
                  WHERE k.busqueda @@ to_tsquery('spanish', p.termino)) / $6 AS cobertura,
                -- Cuántas veces se abrió este tema tras buscar con estas palabras.
                (SELECT coalesce(sum(a.veces), 0) FROM soporte.aprendizaje a
                  WHERE a.articulo = k.id::text AND a.termino = ANY($4::text[])) AS aprendido,
                ts_rank(k.busqueda, q) * (CASE WHEN k.script_json IS NOT NULL THEN ${VENTAJA_CON_SCRIPT} ELSE 1 END) AS relevancia
         FROM knowledge_base k, to_tsquery('spanish', $1) q
         WHERE k.busqueda @@ q
           AND ($3 = false OR coalesce(k.visible_para, 'todos') = 'todos')
       ) s
       ORDER BY cobertura + least(aprendido * ${EMPUJE_POR_VISITA}, ${EMPUJE_MAXIMO}) DESC, relevancia DESC
       LIMIT $2`,
      [consulta, limite, soloPublicos, terminos, pesos, pesoTotal],
    );
  },

  buscarTickets({ terminos, pesos, limite }: Pregunta, excluir: string | null): Promise<TicketEncontrado[]> {
    const consulta = terminos.join(' | ');
    const pesoTotal = pesos.reduce((a, b) => a + b, 0) || 1;

    return filas<TicketEncontrado>(
      `WITH p(termino, peso) AS (SELECT * FROM unnest($4::text[], $5::float8[]))
       SELECT t.numero, t.titulo, t.causa_raiz AS "causaRaiz", t.solucion, t.sistema,
              ts_rank(t.busqueda, q) AS relevancia,
              (SELECT coalesce(sum(p.peso), 0) FROM p
                WHERE t.busqueda @@ to_tsquery('spanish', p.termino)) / $6 AS cobertura
       FROM soporte.tickets t, to_tsquery('spanish', $1) q
       WHERE t.busqueda @@ q
         AND t.numero <> $2
         AND t.estado = 'resuelto'
         AND (t.causa_raiz IS NOT NULL OR t.solucion IS NOT NULL)
       ORDER BY cobertura DESC, relevancia DESC
       LIMIT $3`,
      [consulta, excluir ?? '', limite, terminos, pesos, pesoTotal],
    );
  },

  async anotarBusqueda(texto, autor, resultados, confianza) {
    await ejecutar(
      'INSERT INTO soporte.busquedas (texto, autor, resultados, confianza) VALUES ($1, $2, $3, $4)',
      [texto.trim().slice(0, 500), autor, resultados, confianza],
    );
  },

  async anotarAcierto(terminos, texto, tema) {
    // La misma apertura cierra la búsqueda que la originó: lo que quede sin cerrar es
    // lo que no le sirvió a nadie.
    await ejecutar(
      `UPDATE soporte.busquedas SET abrio = true
        WHERE id = (SELECT id FROM soporte.busquedas
                     WHERE lower(btrim(texto)) = lower(btrim($1)) AND abrio = false
                     ORDER BY creado_en DESC LIMIT 1)`,
      [texto],
    );
    await ejecutar(
      `INSERT INTO soporte.aprendizaje (termino, articulo)
       SELECT t, $2 FROM unnest($1::text[]) AS t
       ON CONFLICT (termino, articulo)
       DO UPDATE SET veces = soporte.aprendizaje.veces + 1, ultima_vez = now()`,
      [terminos, tema],
    );
  },

  busquedasSinRespuesta(dias, limite) {
    return filas<BusquedaRegistrada>(
      `SELECT lower(btrim(texto)) AS texto, count(*)::int AS veces,
              max(resultados) AS resultados
         FROM soporte.busquedas
        WHERE creado_en > now() - ($1 || ' days')::interval
          AND abrio = false
        GROUP BY lower(btrim(texto))
        ORDER BY count(*) DESC, max(creado_en) DESC
        LIMIT $2`,
      [String(dias), limite],
    );
  },
};
