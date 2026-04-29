// src/application/usecases/createArticle.ts

import { KnowledgeArticle } from '../../domain/KnowledgeArticle';
import { ArticleRepository } from '../../infrastructure/articleRepository';
import { randomUUID } from 'crypto';

export interface CreateArticleRequest {
  titulo: string;
  tags: string[];
  contenido: string;
  descripcion?: string;
  categoria?: string;
  subcategoria?: string;
  pasos?: any[];
  camposFormulario?: Array<{ name: string; label: string; type: string; required?: boolean }>;
  script?: string;
  creadoPor: string;
}

export class CreateArticleUseCase {
  private articleRepository: ArticleRepository;

  constructor(articleRepository: ArticleRepository) {
    this.articleRepository = articleRepository;
  }

  async execute(request: CreateArticleRequest): Promise<KnowledgeArticle> {
    if (!request.titulo || request.titulo.trim().length === 0) {
      throw new Error('El título es requerido');
    }

    if (!request.contenido || request.contenido.trim().length === 0) {
      throw new Error('El contenido es requerido');
    }

    if (!Array.isArray(request.tags) || request.tags.length === 0) {
      throw new Error('Al menos un tag es requerido');
    }

    const normalizedTags = request.tags.map((tag) => tag.toLowerCase().trim());

    const article = new KnowledgeArticle(
      randomUUID(),
      request.titulo,
      normalizedTags,
      request.contenido,
      request.creadoPor,
      new Date(),
      undefined,
      request.descripcion,
      request.categoria,
      request.subcategoria,
      request.pasos,
      request.camposFormulario,
      request.script
    );

    return this.articleRepository.create(article);
  }
}
