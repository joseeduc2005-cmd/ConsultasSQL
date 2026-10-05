/**
 * Los aportes, guardados en PostgreSQL.
 *
 * Único archivo del lado de aportes que sabe SQL. Publicar un tema escribe en
 * `knowledge_base`; todo lo demás vive en `soporte.aportes`.
 */
import { fila, filas } from '../../knowledge/db.ts';
import type { Aporte, EstadoAporte } from '../../dominio/aportes/aporte.ts';
import type { AporteParaGuardar, RepositorioAportes, TemaParaPublicar } from '../../puertos/repositorio-aportes.ts';

const COLUMNAS = `id::text, autor, titulo, problema, pasos, pasos_json, sql_sugerido, base, caso,
                  estado, nota, revisado_por, revisado_en, articulo, creado_en`;

export const aportesPostgres: RepositorioAportes = {
  async guardar(a: AporteParaGuardar) {
    const creado = await fila<Aporte>(
      `INSERT INTO soporte.aportes (autor, titulo, problema, pasos, pasos_json, sql_sugerido, base, caso)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       RETURNING ${COLUMNAS}`,
      [a.autor, a.titulo, a.problema, a.pasos, JSON.stringify(a.pasosJson), a.sqlSugerido, a.base, a.caso],
    );
    return creado!;
  },

  obtener(id) {
    return fila<Aporte>(`SELECT ${COLUMNAS} FROM soporte.aportes WHERE id::text = $1`, [id]);
  },

  deAutor(autor) {
    return filas<Aporte>(
      `SELECT ${COLUMNAS} FROM soporte.aportes WHERE autor = $1 ORDER BY id DESC LIMIT 50`,
      [autor],
    );
  },

  listar(estado: EstadoAporte | 'todos' | null) {
    const filtro = estado === 'todos' ? null : (estado ?? 'pendiente');
    return filas<Aporte>(
      `SELECT ${COLUMNAS} FROM soporte.aportes
       WHERE ($1::text IS NULL OR estado = $1)
       ORDER BY (estado = 'pendiente') DESC, id DESC
       LIMIT 100`,
      [filtro],
    );
  },

  async contarPendientes() {
    const [f] = await filas<{ total: number }>(
      "SELECT COUNT(*)::int AS total FROM soporte.aportes WHERE estado = 'pendiente'",
    );
    return f?.total ?? 0;
  },

  /**
   * `creado_por` es el revisor, que es quien publica; `creado_por_username` queda con el
   * nombre de quien lo aportó, que es la autoría que importa cuando alguien lo lee.
   */
  async publicarTema(t: TemaParaPublicar) {
    const creado = await fila<{ id: string }>(
      `INSERT INTO knowledge_base
         (titulo, contenido, contenido_md, descripcion, categoria, tipo_solucion,
          visible_para, creado_por, creado_por_username, actualizado_por, tags)
       VALUES ($1, $2, $2, $3, $4, 'lectura', $5, $6, $7, $8, $9)
       RETURNING id::text`,
      [t.titulo, t.contenido, t.descripcion, t.categoria, t.visible, t.revisorId, t.autor, t.revisorNombre, ['aporte']],
    );
    return creado!.id;
  },

  marcarAceptado(id, tema, nota, revisor) {
    return fila<Aporte>(
      `UPDATE soporte.aportes
          SET estado = 'aceptado', articulo = $2, nota = $3,
              revisado_por = $4, revisado_en = now()
        WHERE id::text = $1
        RETURNING ${COLUMNAS}`,
      [id, tema, nota, revisor],
    );
  },

  marcarRechazado(id, motivo, revisor) {
    return fila<Aporte>(
      `UPDATE soporte.aportes
          SET estado = 'rechazado', nota = $2, revisado_por = $3, revisado_en = now()
        WHERE id::text = $1
        RETURNING ${COLUMNAS}`,
      [id, motivo, revisor],
    );
  },
};
