// src/application/usecases/searchByTags.ts

import { KnowledgeArticle } from '../../domain/KnowledgeArticle';
import { ArticleRepository } from '../../infrastructure/articleRepository';

export interface SearchByTagsRequest {
  tags: string[];
}

export class SearchByTagsUseCase {
  private articleRepository: ArticleRepository;

  constructor(articleRepository: ArticleRepository) {
    this.articleRepository = articleRepository;
  }

  async execute(request: SearchByTagsRequest): Promise<KnowledgeArticle[]> {
    if (!Array.isArray(request.tags) || request.tags.length === 0) {
      return this.articleRepository.findAll();
    }

    const normalizedTags = request.tags.map((tag) => tag.toLowerCase().trim());

    return this.articleRepository.findByTags(normalizedTags);
  }
}
