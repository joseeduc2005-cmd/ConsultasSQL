// src/application/usecases/updateArticle.ts

import { KnowledgeArticle } from '../../domain/KnowledgeArticle';
import { ArticleRepository } from '../../infrastructure/articleRepository';

export interface UpdateArticleRequest {
  id: string;
  titulo: string;
  tags: string[];
  contenido: string;
}

export class UpdateArticleUseCase {
  private articleRepository: ArticleRepository;

  constructor(articleRepository: ArticleRepository) {
    this.articleRepository = articleRepository;
  }

  async execute(request: UpdateArticleRequest): Promise<KnowledgeArticle> {
    if (!request.titulo || request.titulo.trim().length === 0) {
      throw new Error('El título es requerido');
    }

    if (!request.contenido || request.contenido.trim().length === 0) {
      throw new Error('El contenido es requerido');
    }

    if (!Array.isArray(request.tags) || request.tags.length === 0) {
      throw new Error('Al menos un tag es requerido');
    }

    const article = await this.articleRepository.findById(request.id);
    if (!article) {
      throw new Error('Artículo no encontrado');
    }

    const normalizedTags = request.tags.map((tag) => tag.toLowerCase().trim());

    article.titulo = request.titulo;
    article.tags = normalizedTags;
    article.contenido = request.contenido;

    return this.articleRepository.update(article);
  }
}
