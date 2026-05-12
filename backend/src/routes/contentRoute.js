// ─── Content routes: /api/articles/*, /api/comments/* ────────────────────────
import { Router } from 'express';

/**
 * @param {{
 *   pool: import('pg').Pool,
 *   getUserFromRequest: Function,
 *   ensureDynamicSchemaReady: Function,
 *   parseStructuredScript: Function,
 *   validateBusinessScriptDefinition: Function,
 * }} deps
 */
export function createContentRouter({
  pool,
  getUserFromRequest,
  ensureDynamicSchemaReady,
  parseStructuredScript,
  validateBusinessScriptDefinition,
}) {
  const router = Router();

  const VALID_SOLUTION_TYPES = ['lectura', 'ejecutable', 'database', 'script'];

  // ── Articles ──────────────────────────────────────────────────────────────

  // GET /api/articles
  router.get('/articles', async (req, res) => {
    try {
      // Determine caller role for visibility filtering
      const callerRole = String(req.headers['x-user-role'] || req.query.role || '').trim().toLowerCase();
      const isAdmin = callerRole === 'admin';

      // Build visibility filter:
      //   admin → sees all articles
      //   user  → sees 'todos' and 'usuario'
      //   guest → sees only 'todos'
      let visibilityClause = '';
      let visibilityParams = [];
      if (!isAdmin) {
        if (callerRole === 'user' || callerRole === 'usuario') {
          visibilityClause = `WHERE COALESCE(visible_para,'todos') IN ('todos','usuario')`;
        } else {
          visibilityClause = `WHERE COALESCE(visible_para,'todos') = 'todos'`;
        }
      }

      const result = await pool.query(`
        SELECT
          id, titulo,
          COALESCE(descripcion, '') AS descripcion,
          tags, categoria, subcategoria,
          contenido, pasos, campos_formulario,
          script, script_json, contenido_md, tipo_solucion,
          COALESCE(visible_para,'todos') AS visible_para,
          creado_por_username,
          actualizado_por,
          fecha AS creado_en,
          actualizado AS actualizado_en
        FROM knowledge_base
        ${visibilityClause}
        ORDER BY titulo
      `, visibilityParams);

      const articles = result.rows.map((article) => ({
        id: article.id,
        titulo: article.titulo,
        categoria: article.categoria,
        subcategoria: article.subcategoria,
        tags: article.tags || [],
        descripcion: article.descripcion,
        contenido: article.contenido,
        pasos: article.pasos,
        camposFormulario: article.campos_formulario,
        accionScript: article.script,
        script_json: article.script_json,
        contenido_md: article.contenido_md,
        tipo_solucion: article.tipo_solucion || 'lectura',
        visible_para: article.visible_para || 'todos',
        creado_por_username: article.creado_por_username || null,
        actualizado_por: article.actualizado_por || null,
        creado_en: article.creado_en,
        actualizado_en: article.actualizado_en || null,
      }));

      console.log(`[ARTICLES] ✅ ${articles.length} artículos obtenidos de BD`);
      return res.status(200).json({ success: true, data: articles });
    } catch (error) {
      console.error('[ARTICLES] ❌ Error obteniendo artículos:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor', details: error.message });
    }
  });

  // POST /api/articles
  router.post('/articles', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      if (!user) {
        return res.status(401).json({ success: false, error: 'Usuario no autenticado' });
      }

      const role = req.headers['x-user-role'] || user?.role;
      if (!role || role !== 'admin') {
        return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere rol admin para crear artículos.' });
      }

      const { titulo, categoria, subcategoria, tags, descripcion, contenido_md, tipo_solucion, accion_script, campos_formulario, script_json, visible_para } = req.body;

      if (!titulo || !contenido_md) {
        return res.status(400).json({ success: false, error: 'Título y contenido_md son obligatorios' });
      }

      if (!VALID_SOLUTION_TYPES.includes(tipo_solucion)) {
        return res.status(400).json({ success: false, error: 'tipo_solucion debe ser "lectura", "ejecutable", "database" o "script"' });
      }

      const VALID_VISIBILITIES = ['todos', 'admin', 'usuario'];
      const resolvedVisibility = VALID_VISIBILITIES.includes(visible_para) ? visible_para : 'todos';

      if (tipo_solucion === 'script') {
        await ensureDynamicSchemaReady(false);
        const parsedBusinessScript = parseStructuredScript(script_json);
        try {
          validateBusinessScriptDefinition(parsedBusinessScript);
        } catch (validationError) {
          return res.status(400).json({
            success: false,
            error: validationError instanceof Error ? validationError.message : 'script_json empresarial inválido',
          });
        }
      }

      let userId = user?.id;
      if (!userId && user?.username) {
        const userResult = await pool.query('SELECT id FROM users WHERE username = $1', [user.username]);
        if (userResult.rowCount > 0) userId = userResult.rows[0].id;
      }

      if (!userId) {
        return res.status(400).json({ success: false, error: 'No se pudo determinar el ID de usuario para creado_por' });
      }

      const creadorUsername = user?.username || null;

      const insertQuery = `
        INSERT INTO knowledge_base (
          titulo, descripcion, tags, contenido, categoria, subcategoria,
          contenido_md, tipo_solucion, campos_formulario, script, script_json,
          creado_por, creado_por_username, visible_para, fecha
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
        RETURNING id, titulo, categoria, subcategoria, contenido_md, tipo_solucion,
                  campos_formulario, script, script_json,
                  COALESCE(visible_para,'todos') AS visible_para,
                  creado_por_username, fecha AS creado_en
      `;

      const values = [
        titulo,
        descripcion || '',
        tags ? (Array.isArray(tags) ? tags : tags.split(',').map((t) => t.trim())) : [],
        contenido_md,
        categoria || '',
        subcategoria || '',
        contenido_md,
        tipo_solucion,
        campos_formulario ? (typeof campos_formulario === 'string' ? campos_formulario : JSON.stringify(campos_formulario)) : null,
        accion_script || null,
        script_json ? (typeof script_json === 'string' ? script_json : JSON.stringify(script_json)) : null,
        userId,
        creadorUsername,
        resolvedVisibility,
      ];

      const result = await pool.query(insertQuery, values);
      const row = result.rows[0];
      const newArticle = {
        ...row,
        camposFormulario: row.campos_formulario,
        accionScript: row.script,
        script_json: row.script_json,
        visible_para: row.visible_para || 'todos',
        creado_por_username: row.creado_por_username || null,
      };

      console.log(`[ARTICLES] ✅ Nuevo artículo creado: ${newArticle.titulo} (ID: ${newArticle.id}) por ${creadorUsername}`);
      return res.status(201).json({ success: true, data: newArticle, message: 'Artículo creado exitosamente' });
    } catch (error) {
      console.error('[ARTICLES] ❌ Error creando artículo:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor', details: error.message });
    }
  });

  // DELETE /api/articles/:id
  router.delete('/articles/:id', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = req.headers['x-user-role'] || user?.role;
      if (!role || role !== 'admin') {
        return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere rol admin para eliminar artículos.' });
      }

      const { id } = req.params;
      if (!id) return res.status(400).json({ success: false, error: 'ID de artículo inválido' });

      const result = await pool.query('DELETE FROM knowledge_base WHERE id = $1 RETURNING id, titulo', [id]);

      if (result.rows.length === 0) {
        return res.status(404).json({ success: false, error: 'Artículo no encontrado' });
      }

      console.log(`[ARTICLES] ✅ Artículo eliminado: ${result.rows[0].titulo} (ID: ${result.rows[0].id})`);
      return res.status(200).json({ success: true, message: 'Artículo eliminado exitosamente' });
    } catch (error) {
      console.error('[ARTICLES] ❌ Error eliminando artículo:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor', details: error.message });
    }
  });

  // PUT /api/articles/:id
  router.put('/articles/:id', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = req.headers['x-user-role'] || user?.role;
      if (!role || role !== 'admin') {
        return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere rol admin para editar artículos.' });
      }

      const { id } = req.params;
      if (!id) return res.status(400).json({ success: false, error: 'ID de artículo requerido' });

      const { titulo, categoria, subcategoria, tags, descripcion, contenido_md, tipo_solucion, accion_script, campos_formulario, script_json, visible_para } = req.body;

      if (!titulo || !contenido_md || !tipo_solucion) {
        return res.status(400).json({ success: false, error: 'Los campos titulo, contenido_md y tipo_solucion son requeridos' });
      }

      if (!VALID_SOLUTION_TYPES.includes(tipo_solucion)) {
        return res.status(400).json({ success: false, error: 'tipo_solucion debe ser "lectura", "ejecutable", "database" o "script"' });
      }

      const VALID_VISIBILITIES = ['todos', 'admin', 'usuario'];
      const resolvedVisibility = VALID_VISIBILITIES.includes(visible_para) ? visible_para : 'todos';

      const editorUsername = (getUserFromRequest(req))?.username || role || 'admin';

      if (tipo_solucion === 'script') {
        await ensureDynamicSchemaReady(false);
        const parsedBusinessScript = parseStructuredScript(script_json);
        try {
          validateBusinessScriptDefinition(parsedBusinessScript);
        } catch (validationError) {
          return res.status(400).json({
            success: false,
            error: validationError instanceof Error ? validationError.message : 'script_json empresarial inválido',
          });
        }
      }

      const updateQuery = `
        UPDATE knowledge_base SET
          titulo=$1, descripcion=$2, tags=$3, contenido=$4, categoria=$5,
          subcategoria=$6, contenido_md=$7, tipo_solucion=$8,
          campos_formulario=$9, script=$10, script_json=$11,
          visible_para=$12, actualizado_por=$13, actualizado=NOW()
        WHERE id=$14
        RETURNING id, titulo, categoria, subcategoria, descripcion, tags,
                  contenido_md, tipo_solucion, campos_formulario, script, script_json,
                  COALESCE(visible_para,'todos') AS visible_para,
                  creado_por_username, actualizado_por,
                  fecha AS creado_en, actualizado AS actualizado_en
      `;

      const values = [
        titulo,
        descripcion || '',
        tags ? (Array.isArray(tags) ? tags : tags.split(',').map((t) => t.trim())) : [],
        contenido_md,
        categoria || '',
        subcategoria || '',
        contenido_md,
        tipo_solucion,
        campos_formulario ? (typeof campos_formulario === 'string' ? campos_formulario : JSON.stringify(campos_formulario)) : null,
        accion_script || null,
        script_json ? (typeof script_json === 'string' ? script_json : JSON.stringify(script_json)) : null,
        resolvedVisibility,
        editorUsername,
        id,
      ];

      const result = await pool.query(updateQuery, values);

      if (result.rows.length === 0) {
        return res.status(404).json({ success: false, error: 'Artículo no encontrado' });
      }

      const updatedRow = result.rows[0];
      return res.status(200).json({
        success: true,
        data: {
          ...updatedRow,
          camposFormulario: updatedRow.campos_formulario,
          accionScript: updatedRow.script,
          script_json: updatedRow.script_json,
          visible_para: updatedRow.visible_para || 'todos',
          creado_por_username: updatedRow.creado_por_username || null,
          actualizado_por: updatedRow.actualizado_por || null,
          actualizado_en: updatedRow.actualizado_en || null,
        },
        message: 'Artículo actualizado exitosamente',
      });
    } catch (error) {
      console.error('[ARTICLES] ❌ Error editando artículo:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor', details: error.message });
    }
  });

  // GET /api/article-executions — Auditoría de ejecuciones (solo admin)
  router.get('/article-executions', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = req.headers['x-user-role'] || user?.role;
      if (role !== 'admin') {
        return res.status(403).json({ success: false, error: 'Acceso denegado: solo admins' });
      }

      const articleId = req.query.article_id ? Number(req.query.article_id) : null;
      const limit = Math.min(Number(req.query.limit || 100), 500);

      const result = await pool.query(
        `SELECT id, article_id, article_titulo, ejecutado_por, ejecutado_rol,
                parametros_usados, filas_resultado, duracion_ms, estado, mensaje_error, ejecutado_en
         FROM article_executions
         ${articleId ? 'WHERE article_id = $1' : ''}
         ORDER BY ejecutado_en DESC
         LIMIT ${articleId ? '$2' : '$1'}`,
        articleId ? [articleId, limit] : [limit]
      );

      return res.status(200).json({ success: true, data: result.rows });
    } catch (error) {
      console.error('[AUDIT] ❌ Error obteniendo ejecuciones:', error);
      return res.status(500).json({ success: false, error: 'Error interno', details: error.message });
    }
  });

  // ── Comments ──────────────────────────────────────────────────────────────

  // GET /api/comments
  router.get('/comments', async (req, res) => {
    try {
      const articleId = req.query.article_id;
      if (!articleId) {
        return res.status(400).json({ success: false, error: 'article_id es requerido' });
      }

      const result = await pool.query(
        `SELECT id, article_id, parent_id, author_username, author_role, content, created_at
         FROM comments WHERE article_id = $1 ORDER BY created_at ASC`,
        [String(articleId)]
      );

      return res.status(200).json({ success: true, data: result.rows });
    } catch (error) {
      console.error('[COMMENTS] ❌ Error obteniendo comentarios:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor', details: error.message });
    }
  });

  // POST /api/comments
  router.post('/comments', async (req, res) => {
    try {
      const { article_id, parent_id, author_username, author_role, content } = req.body;

      if (!article_id || !author_username || !content?.trim()) {
        return res.status(400).json({ success: false, error: 'Campos requeridos: article_id, author_username, content' });
      }

      const result = await pool.query(
        `INSERT INTO comments (article_id, parent_id, author_username, author_role, content)
         VALUES ($1,$2,$3,$4,$5)
         RETURNING id, article_id, parent_id, author_username, author_role, content, created_at`,
        [String(article_id), parent_id ? Number(parent_id) : null, String(author_username), String(author_role || 'user'), String(content).trim()]
      );

      return res.status(201).json({ success: true, data: result.rows[0] });
    } catch (error) {
      console.error('[COMMENTS] ❌ Error creando comentario:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor', details: error.message });
    }
  });

  // DELETE /api/comments/:id
  router.delete('/comments/:id', async (req, res) => {
    try {
      const id = Number(req.params.id);
      if (Number.isNaN(id)) {
        return res.status(400).json({ success: false, error: 'ID inválido' });
      }

      await pool.query('DELETE FROM comments WHERE id = $1', [id]);
      return res.status(200).json({ success: true });
    } catch (error) {
      console.error('[COMMENTS] ❌ Error eliminando comentario:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor', details: error.message });
    }
  });

  return router;
}
