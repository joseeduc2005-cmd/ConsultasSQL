// src/application/useCases/SearchArticlesUseCase.ts

import { KnowledgeArticle } from '../../domain/KnowledgeArticle';
import { IArticleRepository } from '../../infrastructure/articleRepository';

export class SearchArticlesUseCase {
  constructor(private articleRepository: IArticleRepository) {}

  async execute(query: string): Promise<KnowledgeArticle[]> {
    if (!query.trim()) {
      return await this.articleRepository.findAll();
    }
    return await this.articleRepository.searchArticles(query);
  }
}