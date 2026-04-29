// src/application/useCases/GetCategoriesUseCase.ts

import { IArticleRepository } from '../../infrastructure/articleRepository';

export class GetCategoriesUseCase {
  constructor(private articleRepository: IArticleRepository) {}

  async execute(): Promise<{ categoria: string; subcategorias: string[] }[]> {
    return await this.articleRepository.findCategories();
  }
}