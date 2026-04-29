// app/components/Sidebar.tsx

'use client';

import { useState, useEffect } from 'react';
import { useTheme } from './ThemeProvider';

interface Category {
  categoria: string;
  subcategorias: string[];
}

import { KnowledgeArticle } from '../types';

function normalizeLabel(value: string) {
  return value.trim().toLowerCase();
}

function normalizeForComparison(value: string) {
  return (value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/\b(de|del|la|las|el|los|con|y)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function looksLikeDuplicateLabel(subcategory: string, title: string) {
  const sub = normalizeForComparison(subcategory);
  const art = normalizeForComparison(title);
  if (!sub || !art) return false;
  return sub === art || sub.includes(art) || art.includes(sub);
}

function isGenericSubcategoryLabel(subcategory: string) {
  const normalized = normalizeForComparison(subcategory);
  return normalized === 'otros temas' || normalized === 'otro tema';
}

interface SidebarProps {
  categories: Category[];
  articles?: KnowledgeArticle[];
  onSelectCategory?: (categoria: string, subcategoria?: string) => void;
  onSelectArticle?: (article: KnowledgeArticle) => void;
  selectedCategory?: string;
  selectedSubcategory?: string;
  selectedArticle?: KnowledgeArticle;
  collapsed?: boolean;
  onToggleCollapse?: () => void;
}

export default function Sidebar({ categories, articles, onSelectCategory, onSelectArticle, selectedCategory, selectedSubcategory, selectedArticle, collapsed, onToggleCollapse }: SidebarProps) {
  const { theme } = useTheme();
  const isLightTheme = theme === 'light';
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());
  const [localActiveCategory, setLocalActiveCategory] = useState(selectedCategory || '');
  const [localActiveSubcategory, setLocalActiveSubcategory] = useState(selectedSubcategory || '');
  const [internalCollapsed, setInternalCollapsed] = useState(false);
  const isCollapsed = collapsed ?? internalCollapsed;

  const handleToggleCollapse = () => {
    if (onToggleCollapse) {
      onToggleCollapse();
      return;
    }
    setInternalCollapsed((prev) => !prev);
  };

  useEffect(() => {
    // Mostrar todas las categorías desplegadas por defecto para mayor visibilidad.
    setExpandedCategories(new Set(categories.map((c) => c.categoria)));
  }, [categories]);

  useEffect(() => {
    setLocalActiveCategory(selectedCategory || '');
    setLocalActiveSubcategory(selectedSubcategory || '');
  }, [selectedCategory, selectedSubcategory]);

  const toggleCategory = (categoria: string) => {
    const newExpanded = new Set(expandedCategories);
    if (newExpanded.has(categoria)) {
      newExpanded.delete(categoria);
    } else {
      newExpanded.add(categoria);
    }
    setExpandedCategories(newExpanded);
  };

  const handleSubcategoryClick = (categoria: string, subcategoria: string) => {
    setLocalActiveCategory(categoria);
    setLocalActiveSubcategory(subcategoria);
    onSelectCategory?.(categoria, subcategoria);

    const firstArticle = (articles || []).find(
      (article) =>
        normalizeForComparison(article.categoria || '') === normalizeForComparison(categoria) &&
        normalizeForComparison(article.subcategoria || '') === normalizeForComparison(subcategoria)
    );

    if (firstArticle) {
      onSelectArticle?.(firstArticle);
      return;
    }

    const fallbackArticle = (articles || []).find(
      (article) => normalizeForComparison(article.categoria || '') === normalizeForComparison(categoria)
    );
    if (fallbackArticle) onSelectArticle?.(fallbackArticle);
  };

  if (categories.length === 0) {
    return (
      <div className="w-full h-full overflow-hidden flex flex-col">
        <div className="glass-panel rounded-xl p-4 text-[color:var(--ink-700)] text-sm">No hay categorías disponibles.</div>
      </div>
    );
  }

  const articleMap = categories.map((category) => ({
    ...category,
    subcategorias: category.subcategorias.map((subcategoria) => ({
      subcategoria,
      articles: (articles || []).filter(
        (a) => a.categoria === category.categoria && a.subcategoria === subcategoria
      ),
    })),
  }));

  if (articles && articles.length > 0) {
    console.log('[Sidebar] Total articles received:', articles.length);
    console.log('[Sidebar] Articles:', articles);
    console.log('[Sidebar] Article map:', articleMap);
  }

  return (
    <div className="w-full h-full overflow-hidden flex flex-col transition-all duration-200">
      {/* Header */}
      <div className={`p-4 border-b border-[color:var(--line)] flex items-center ${isCollapsed ? 'justify-center' : 'justify-between'}`}>
        {!isCollapsed && <h2 className="text-sm font-bold text-[color:var(--ink-900)] uppercase tracking-wide">Categorías</h2>}
        <button
          onClick={handleToggleCollapse}
          className={`p-1 rounded-lg text-[color:var(--ink-700)] transition-colors ${isLightTheme ? 'hover:bg-white/70 hover:text-[color:var(--ink-900)]' : 'hover:bg-slate-800/70 hover:text-white'}`}
          title={isCollapsed ? 'Expandir' : 'Contraer'}
        >
          {isCollapsed ? (
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
          ) : (
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
            </svg>
          )}
        </button>
      </div>

      {/* Categorías */}
      {!isCollapsed && (
        <div className="flex-1 min-h-0 px-2 py-3 space-y-1 overflow-y-scroll pr-1">
          {categories.map((category) => (
            <div key={category.categoria}>
              <button
                onClick={() => toggleCategory(category.categoria)}
                className={`w-full text-left px-3 py-2 rounded-lg flex items-center justify-between font-medium transition-all duration-150 text-sm ${
                  localActiveCategory === category.categoria
                    ? (isLightTheme
                      ? 'bg-white text-[color:var(--ink-900)] shadow-sm ring-1 ring-[color:var(--line)]'
                      : 'bg-slate-900 text-white shadow-sm ring-1 ring-slate-600')
                    : (isLightTheme
                      ? 'text-[color:var(--ink-800)] hover:bg-white/75'
                      : 'text-slate-200 hover:bg-slate-800/80')
                }`}
              >
                <span>{category.categoria}</span>
                <svg
                  className={`w-4 h-4 text-[color:var(--ink-600)] transition-transform duration-200 ${
                    expandedCategories.has(category.categoria) ? 'rotate-90' : ''
                  }`}
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
              </button>
              {expandedCategories.has(category.categoria) && (
                <div className="ml-2 mt-1 space-y-1 border-l-2 border-[color:var(--line)] pl-2">
                  {articleMap
                    .find((c) => c.categoria === category.categoria)
                    ?.subcategorias.map((subgroup) => {
                      const hideSubcategoryHeading = isGenericSubcategoryLabel(subgroup.subcategoria);
                      const visibleArticles = subgroup.articles.filter((article) => {
                        if (hideSubcategoryHeading) return true;
                        if (subgroup.articles.length === 1) return false;
                        return !looksLikeDuplicateLabel(subgroup.subcategoria, article.titulo || '');
                      });

                      return (
                        <div key={subgroup.subcategoria}>
                          {!hideSubcategoryHeading && (
                            <button
                              onClick={() => handleSubcategoryClick(category.categoria, subgroup.subcategoria)}
                              className={`w-full text-left py-1 px-2 rounded-md text-xs font-semibold uppercase tracking-wider transition-all duration-150 ${
                                localActiveCategory === category.categoria && localActiveSubcategory === subgroup.subcategoria
                                  ? (isLightTheme
                                    ? 'bg-white text-[color:var(--ink-900)] ring-1 ring-[color:var(--line)]'
                                    : 'bg-slate-900 text-white ring-1 ring-slate-600')
                                  : (isLightTheme
                                    ? 'text-[color:var(--ink-700)] hover:bg-white/70'
                                    : 'text-slate-300 hover:bg-slate-800/70')
                              }`}
                            >
                              {subgroup.subcategoria}
                            </button>
                          )}
                          {visibleArticles.map((article) => (
                            <button
                              key={`${article.id}`}
                              onClick={() => {
                                onSelectArticle?.(article);
                                setLocalActiveCategory(category.categoria);
                                setLocalActiveSubcategory(subgroup.subcategoria);
                              }}
                              className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-all duration-150 ${
                                selectedArticle?.id === article.id
                                  ? 'bg-gradient-to-r from-[#2363eb] to-[#1aa0c8] text-white font-semibold shadow-md ring-2 ring-[#b9d2ff]'
                                  : (isLightTheme
                                    ? 'text-[color:var(--ink-700)] hover:bg-white/75 hover:text-[color:var(--ink-900)]'
                                    : 'text-slate-300 hover:bg-slate-800/80 hover:text-white')
                              }`}
                            >
                              {article.titulo}
                            </button>
                          ))}
                        </div>
                      );
                    })}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

