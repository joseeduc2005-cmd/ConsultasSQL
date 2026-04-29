// src/application/usecases/deleteArticle.ts

import { ArticleRepository } from '../../infrastructure/articleRepository';

export class DeleteArticleUseCase {
  private articleRepository: ArticleRepository;

  constructor(articleRepository: ArticleRepository) {
    this.articleRepository = articleRepository;
  }

  async execute(id: string): Promise<boolean> {
    const article = await this.articleRepository.findById(id);
    if (!article) {
      throw new Error('Artículo no encontrado');
    }

    return this.articleRepository.delete(id);
  }
}
