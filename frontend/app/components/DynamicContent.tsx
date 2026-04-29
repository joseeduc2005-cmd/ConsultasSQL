// app/components/DynamicContent.tsx

'use client';

import { useState, useEffect } from 'react';
import { KnowledgeArticle } from '../types';
import DynamicForm from './DynamicForm';

interface DynamicContentProps {
  articles: KnowledgeArticle[];
  onExecuteSolution: (articleId: string | number, formData: Record<string, any>) => Promise<string>;
}

export default function DynamicContent({ articles, onExecuteSolution }: DynamicContentProps) {
  const [selectedArticle, setSelectedArticle] = useState<KnowledgeArticle | null>(null);

  useEffect(() => {
    if (articles.length > 0) {
      setSelectedArticle(articles[0]);
    } else {
      setSelectedArticle(null);
    }
  }, [articles]);

  if (articles.length === 0) {
    return (
      <div className="flex-1 p-8 text-center">
        <div className="text-gray-500">
          <p className="text-lg mb-2">No hay artículos disponibles</p>
          <p>Selecciona una subcategoría del menú lateral</p>
        </div>
      </div>
    );
  }

  if (!selectedArticle) {
    return (
      <div className="flex-1 p-8">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/2 mb-4"></div>
          <div className="h-4 bg-gray-200 rounded w-full mb-2"></div>
          <div className="h-4 bg-gray-200 rounded w-3/4"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 p-8 overflow-y-auto">
      {/* Lista de artículos */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold mb-4">Artículos disponibles</h3>
        <div className="space-y-2">
          {articles.map((article) => (
            <button
              key={article.id}
              onClick={() => setSelectedArticle(article)}
              className={`w-full text-left p-3 rounded-lg border ${
                selectedArticle.id === article.id
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 hover:border-gray-300'
              }`}
            >
              <h4 className="font-medium text-gray-800">{article.titulo}</h4>
              <p className="text-sm text-gray-600 mt-1">
                {article.tags.join(', ')}
              </p>
            </button>
          ))}
        </div>
      </div>

      {/* Contenido del artículo seleccionado */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-2xl font-bold text-gray-800 mb-4">{selectedArticle.titulo}</h2>

        <div className="mb-4">
          <div className="flex flex-wrap gap-2 mb-2">
            {selectedArticle.tags.map((tag) => (
              <span
                key={tag}
                className="px-2 py-1 bg-blue-100 text-blue-800 text-sm rounded"
              >
                {tag}
              </span>
            ))}
          </div>
        </div>

        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-2">Descripción</h3>
          <div className="prose max-w-none">
            {selectedArticle.contenido}
          </div>
        </div>

        {selectedArticle.pasos && selectedArticle.pasos.length > 0 && (
          <div className="mb-6">
            <h3 className="text-lg font-semibold mb-2">Pasos a seguir</h3>
            <ol className="list-decimal list-inside space-y-2">
              {selectedArticle.pasos.map((paso: any, index: number) => (
                <li key={index} className="text-gray-700">
                  {paso.descripcion}
                </li>
              ))}
            </ol>
          </div>
        )}

        <DynamicForm
          article={selectedArticle}
          onSubmit={(formData) => onExecuteSolution(selectedArticle.id, formData)}
        />
      </div>
    </div>
  );
}