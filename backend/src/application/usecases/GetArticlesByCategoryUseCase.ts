// src/application/useCases/GetArticlesByCategoryUseCase.ts

import { KnowledgeArticle } from '../../domain/KnowledgeArticle';
import { IArticleRepository } from '../../infrastructure/articleRepository';

export class GetArticlesByCategoryUseCase {
  constructor(private articleRepository: IArticleRepository) {}

  async execute(categoria: string, subcategoria?: string): Promise<KnowledgeArticle[]> {
    return await this.articleRepository.findByCategoria(categoria, subcategoria);
  }
}