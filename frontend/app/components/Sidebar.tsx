// app/components/Sidebar.tsx

'use client';

import { useState, useEffect } from 'react';
import { useTheme } from './ThemeProvider';

interface Category {
  categoria: string;
  subcategorias: string[];
}

import { KnowledgeArticle } from '../types';
import { SIN_SUBCATEGORIA, subcategoriaDe } from './BarraLateralPanel';

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
  // `SIN_SUBCATEGORIA` entra aquí porque es un relleno, no algo que alguien escribiera:
  // pintar un encabezado «General» encima de los artículos inventa una división que no
  // existe en los datos.
  return normalized === 'otros temas'
    || normalized === 'otro tema'
    || normalized === normalizeForComparison(SIN_SUBCATEGORIA);
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

export default function Sidebar({ categories, articles, onSelectCategory, onSelectArticle, selectedCategory, selectedSubcategory, selectedArticle, collapsed }: SidebarProps) {
  const { theme } = useTheme();
  const isLightTheme = theme === 'light';
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());
  const [localActiveCategory, setLocalActiveCategory] = useState(selectedCategory || '');
  const [localActiveSubcategory, setLocalActiveSubcategory] = useState(selectedSubcategory || '');
  const isCollapsed = collapsed ?? false;

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
        <div className="glass-panel rounded-xl p-4 text-(--ink-700) text-sm">No hay categorías disponibles.</div>
      </div>
    );
  }

  // El reparto compara con el mismo nombre que usó el agrupado.
  //
  // Aquí se comparaba `a.subcategoria` en crudo contra la etiqueta ya sustituida, y para
  // un artículo sin subcategoría eso era `null === 'General'`: la categoría salía en la
  // lista y al desplegarla no había nada. Como ningún artículo tiene subcategoría, no se
  // veía ni uno.
  const articleMap = categories.map((category) => ({
    ...category,
    subcategorias: category.subcategorias.map((subcategoria) => ({
      subcategoria,
      articles: (articles || []).filter(
        (a) => a.categoria === category.categoria && subcategoriaDe(a) === subcategoria
      ),
    })),
  }));

  return (
    <div className="w-full flex flex-col transition-all duration-200">
      {/* La flecha de contraer vive en la cabecera de la barra (BarraLateralPanel):
          aquí solo va la etiqueta de sección, para que no parezca que contrae
          únicamente las categorías. */}
      {!isCollapsed && (
        <p className="px-3 pt-1 pb-2 text-[10.5px] font-semibold uppercase tracking-[0.14em] text-(--ink-500)">
          Categorías
        </p>
      )}

      {/* Categorías */}
      {!isCollapsed && (
        <div className="space-y-0.5 pb-2">
          {categories.map((category) => (
            <div key={category.categoria}>
              <button
                onClick={() => toggleCategory(category.categoria)}
                className={`w-full text-left px-3 h-9 rounded-lg flex items-center justify-between font-medium transition-colors duration-150 text-sm ${
                  localActiveCategory === category.categoria
                    ? (isLightTheme
                      ? 'bg-white text-(--ink-900) shadow-xs ring-1 ring-(--line)'
                      : 'bg-[rgba(6,182,212,0.14)] text-cyan-100 ring-1 ring-[rgba(6,182,212,0.3)]')
                    : (isLightTheme
                      ? 'text-(--ink-800) hover:bg-white/70'
                      : 'text-slate-300 hover:bg-[rgba(56,189,248,0.07)] hover:text-cyan-100')
                }`}
              >
                <span>{category.categoria}</span>
                <svg
                  className={`w-4 h-4 text-(--ink-600) transition-transform duration-200 ${
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
                <div className="ml-3 mt-0.5 mb-1 space-y-0.5 border-l border-(--line) pl-2">
                  {articleMap
                    .find((c) => c.categoria === category.categoria)
                    ?.subcategorias.map((subgroup) => {
                      const hideSubcategoryHeading = isGenericSubcategoryLabel(subgroup.subcategoria);
                      const visibleArticles = subgroup.articles.filter((article) => {
                        if (hideSubcategoryHeading) return true;
                        if (subgroup.articles.length === 1) return false;
                        return !looksLikeDuplicateLabel(subgroup.subcategoria, article.titulo || '');
                      });

                      // When subcategory has exactly 1 article: show the article button directly
                      // (same look as every other article). No separate heading needed.
                      const singleArticle = !hideSubcategoryHeading && subgroup.articles.length === 1
                        ? subgroup.articles[0]
                        : null;

                      if (singleArticle) {
                        return (
                          <button
                            key={`${singleArticle.id}`}
                            onClick={() => {
                              onSelectArticle?.(singleArticle);
                              setLocalActiveCategory(category.categoria);
                              setLocalActiveSubcategory(subgroup.subcategoria);
                            }}
                            className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-all duration-150 ${
                              selectedArticle?.id === singleArticle.id
                                ? (isLightTheme
                                  ? 'bg-white text-(--accent) font-semibold shadow-xs ring-1 ring-(--line)'
                                  : 'bg-[rgba(56,189,248,0.14)] text-cyan-100 font-semibold ring-1 ring-[rgba(56,189,248,0.3)]')
                                : (isLightTheme
                                  ? 'text-(--ink-700) hover:bg-white/70 hover:text-(--ink-900)'
                                  : 'text-slate-300 hover:bg-[rgba(56,189,248,0.08)] hover:text-slate-100')
                            }`}
                          >
                            {singleArticle.titulo}
                          </button>
                        );
                      }

                      return (
                        <div key={subgroup.subcategoria}>
                          {!hideSubcategoryHeading && visibleArticles.length > 0 && (
                            <p className={`px-3 pt-2 pb-0.5 text-[10px] font-semibold uppercase tracking-widest select-none ${
                              isLightTheme ? 'text-(--ink-500)' : 'text-slate-500'
                            }`}>
                              {subgroup.subcategoria}
                            </p>
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
                                  ? (isLightTheme
                                    ? 'bg-linear-to-r from-[#2363eb] to-[#1aa0c8] text-white font-semibold shadow-md ring-2 ring-[#b9d2ff]'
                                    : 'bg-linear-to-r from-[#2363eb] to-[#0ea5e9] text-white font-semibold shadow-md ring-1 ring-cyan-400/50 shadow-cyan-500/20')
                                  : (isLightTheme
                                    ? 'text-(--ink-700) hover:bg-white/75 hover:text-(--ink-900)'
                                    : 'text-slate-300 hover:bg-[rgba(56,189,248,0.08)] hover:text-slate-100')
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

