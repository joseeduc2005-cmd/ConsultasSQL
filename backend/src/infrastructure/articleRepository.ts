// src/infrastructure/articleRepository.ts

import { KnowledgeArticle } from '../domain/KnowledgeArticle';
import { getDatabase } from './database';
import { randomUUID } from 'crypto';

export interface IArticleRepository {
  findById(id: string): Promise<KnowledgeArticle | null>;
  findAll(): Promise<KnowledgeArticle[]>;
  findByTags(tags: string[]): Promise<KnowledgeArticle[]>;
  findCategories(): Promise<{ categoria: string; subcategorias: string[] }[]>;
  findByCategoria(categoria: string, subcategoria?: string): Promise<KnowledgeArticle[]>;
  searchArticles(query: string): Promise<KnowledgeArticle[]>;
  create(article: KnowledgeArticle): Promise<KnowledgeArticle>;
  update(article: KnowledgeArticle): Promise<KnowledgeArticle>;
  delete(id: string): Promise<boolean>;
  findByCreadoPor(creadoPor: string): Promise<KnowledgeArticle[]>;
}

export class ArticleRepository implements IArticleRepository {
  async findById(id: string): Promise<KnowledgeArticle | null> {
    const db = getDatabase();
    try {
      const result = await db.query(
        'SELECT id, titulo, tags, contenido, descripcion, categoria, subcategoria, pasos, campos_formulario, script, creado_por, fecha, actualizado FROM knowledge_base WHERE id = $1',
        [id]
      );

      if (result.rows.length === 0) {
        return null;
      }

      const row = result.rows[0];
      return new KnowledgeArticle(
        row.id,
        row.titulo,
        row.tags || [],
        row.contenido,
        row.creado_por,
        new Date(row.fecha),
        row.actualizado ? new Date(row.actualizado) : undefined,
        row.descripcion,
        row.categoria,
        row.subcategoria,
        row.pasos,
        row.campos_formulario,
        row.script
      );
    } catch (error) {
      console.error('Error en findById:', error);
      throw error;
    }
  }

  async findAll(): Promise<KnowledgeArticle[]> {
    const db = getDatabase();
    try {
      const result = await db.query(
        'SELECT id, titulo, tags, contenido, descripcion, categoria, subcategoria, pasos, campos_formulario, script, creado_por, fecha, actualizado FROM knowledge_base ORDER BY fecha DESC'
      );

      return result.rows.map(
        (row) =>
          new KnowledgeArticle(
            row.id,
            row.titulo,
            row.tags || [],
            row.contenido,
            row.creado_por,
            new Date(row.fecha),
            row.actualizado ? new Date(row.actualizado) : undefined,
            row.descripcion,
            row.categoria,
            row.subcategoria,
            row.pasos,
            row.campos_formulario,
            row.script
          )
      );
    } catch (error) {
      console.error('Error en findAll:', error);
      throw error;
    }
  }

  async findByTags(tags: string[]): Promise<KnowledgeArticle[]> {
    const db = getDatabase();
    try {
      // Normalizar tags a minúsculas
      const normalizedTags = tags.map((t) => t.toLowerCase());

      const result = await db.query(
        `SELECT id, titulo, tags, contenido, descripcion, categoria, subcategoria, pasos, campos_formulario, script, creado_por, fecha, actualizado
         FROM knowledge_base
         WHERE tags && ARRAY[${normalizedTags.map((_, i) => `$${i + 1}`).join(', ')}]::text[]
         ORDER BY fecha DESC`,
        normalizedTags
      );

      return result.rows.map(
        (row) =>
          new KnowledgeArticle(
            row.id,
            row.titulo,
            row.tags || [],
            row.contenido,
            row.creado_por,
            new Date(row.fecha),
            row.actualizado ? new Date(row.actualizado) : undefined,
            row.descripcion,
            row.categoria,
            row.subcategoria,
            row.pasos,
            row.campos_formulario,
            row.script
          )
      );
    } catch (error) {
      console.error('Error en findByTags:', error);
      throw error;
    }
  }

  async create(article: KnowledgeArticle): Promise<KnowledgeArticle> {
    const db = getDatabase();
    try {
      const result = await db.query(
        `INSERT INTO knowledge_base (id, titulo, tags, contenido, descripcion, categoria, subcategoria, pasos, campos_formulario, script, creado_por, fecha)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         RETURNING id, titulo, tags, contenido, descripcion, categoria, subcategoria, pasos, campos_formulario, script, creado_por, fecha, actualizado`,
        [
          article.id || randomUUID(),
          article.titulo,
          article.tags,
          article.contenido,
          article.descripcion || null,
          article.categoria,
          article.subcategoria,
          article.pasos ? JSON.stringify(article.pasos) : null,
          article.camposFormulario ? JSON.stringify(article.camposFormulario) : null,
          article.script,
          article.creadoPor,
          article.fecha,
        ]
      );

      const row = result.rows[0];
      return new KnowledgeArticle(
        row.id,
        row.titulo,
        row.tags || [],
        row.contenido,
        row.creado_por,
        new Date(row.fecha),
        row.actualizado ? new Date(row.actualizado) : undefined,
        row.descripcion,
        row.categoria,
        row.subcategoria,
        row.pasos,
        row.campos_formulario,
        row.script
      );
    } catch (error) {
      console.error('Error en create:', error);
      throw error;
    }
  }

  async update(article: KnowledgeArticle): Promise<KnowledgeArticle> {
    const db = getDatabase();
    try {
      const result = await db.query(
        `UPDATE knowledge_base
         SET titulo = $1, tags = $2, contenido = $3, descripcion = $4, categoria = $5, subcategoria = $6, pasos = $7, campos_formulario = $8, script = $9, actualizado = NOW()
         WHERE id = $10
         RETURNING id, titulo, tags, contenido, descripcion, categoria, subcategoria, pasos, campos_formulario, script, creado_por, fecha, actualizado`,
        [
          article.titulo,
          article.tags,
          article.contenido,
          article.descripcion || null,
          article.categoria,
          article.subcategoria,
          article.pasos ? JSON.stringify(article.pasos) : null,
          article.camposFormulario ? JSON.stringify(article.camposFormulario) : null,
          article.script,
          article.id,
        ]
      );

      if (result.rows.length === 0) {
        throw new Error('Artículo no encontrado');
      }

      const row = result.rows[0];
      return new KnowledgeArticle(
        row.id,
        row.titulo,
        row.tags || [],
        row.contenido,
        row.creado_por,
        new Date(row.fecha),
        row.actualizado ? new Date(row.actualizado) : undefined,
        row.descripcion,
        row.categoria,
        row.subcategoria,
        row.pasos,
        row.campos_formulario,
        row.script
      );
    } catch (error) {
      console.error('Error en update:', error);
      throw error;
    }
  }

  async findCategories(): Promise<{ categoria: string; subcategorias: string[] }[]> {
    const db = getDatabase();
    try {
      const result = await db.query(
        'SELECT categoria, array_agg(DISTINCT subcategoria) as subcategorias FROM knowledge_base WHERE categoria IS NOT NULL GROUP BY categoria ORDER BY categoria'
      );
      return result.rows.map(row => ({
        categoria: row.categoria,
        subcategorias: row.subcategorias || []
      }));
    } catch (error) {
      console.error('Error en findCategories:', error);
      throw error;
    }
  }

  async findByCategoria(categoria: string, subcategoria?: string): Promise<KnowledgeArticle[]> {
    const db = getDatabase();
    try {
      let query = 'SELECT id, titulo, tags, contenido, descripcion, categoria, subcategoria, pasos, campos_formulario, script, creado_por, fecha, actualizado FROM knowledge_base WHERE categoria = $1';
      const params = [categoria];

      if (subcategoria) {
        query += ' AND subcategoria = $2';
        params.push(subcategoria);
      }

      query += ' ORDER BY fecha DESC';

      const result = await db.query(query, params);
      return result.rows.map(row => new KnowledgeArticle(
        row.id,
        row.titulo,
        row.tags || [],
        row.contenido,
        row.creado_por,
        new Date(row.fecha),
        row.actualizado ? new Date(row.actualizado) : undefined,
        row.descripcion,
        row.categoria,
        row.subcategoria,
        row.pasos,
        row.campos_formulario,
        row.script
      ));
    } catch (error) {
      console.error('Error en findByCategoria:', error);
      throw error;
    }
  }

  async searchArticles(query: string): Promise<KnowledgeArticle[]> {
    const db = getDatabase();
    try {
      const searchTerm = `%${query.toLowerCase()}%`;
      const result = await db.query(
        `SELECT id, titulo, tags, contenido, descripcion, categoria, subcategoria, pasos, campos_formulario, script, creado_por, fecha, actualizado
         FROM knowledge_base
         WHERE LOWER(titulo) LIKE $1 OR LOWER(contenido) LIKE $1 OR $1 = ANY(LOWER(tags::text)::text[])
         ORDER BY fecha DESC`,
        [searchTerm]
      );
      return result.rows.map(row => new KnowledgeArticle(
        row.id,
        row.titulo,
        row.tags || [],
        row.contenido,
        row.creado_por,
        new Date(row.fecha),
        row.actualizado ? new Date(row.actualizado) : undefined,
        row.descripcion,
        row.categoria,
        row.subcategoria,
        row.pasos,
        row.campos_formulario,
        row.script
      ));
    } catch (error) {
      console.error('Error en searchArticles:', error);
      throw error;
    }
  }

  async delete(id: string): Promise<boolean> {
    const db = getDatabase();
    try {
      const result = await db.query('DELETE FROM knowledge_base WHERE id = $1', [id]);
      return (result.rowCount ?? 0) > 0;
    } catch (error) {
      console.error('Error en delete:', error);
      throw error;
    }
  }

  async findByCreadoPor(creadoPor: string): Promise<KnowledgeArticle[]> {
    const db = getDatabase();
    try {
      const result = await db.query(
        `SELECT id, titulo, tags, contenido, creado_por, fecha, actualizado 
         FROM knowledge_base 
         WHERE creado_por = $1 
         ORDER BY fecha DESC`,
        [creadoPor]
      );

      return result.rows.map(
        (row) =>
          new KnowledgeArticle(
            row.id,
            row.titulo,
            row.tags || [],
            row.contenido,
            row.creado_por,
            new Date(row.fecha),
            row.actualizado ? new Date(row.actualizado) : undefined,
            row.descripcion,
            row.categoria,
            row.subcategoria,
            row.pasos,
            row.campos_formulario,
            row.script
          )
      );
    } catch (error) {
      console.error('Error en findByCreadoPor:', error);
      throw error;
    }
  }
}
