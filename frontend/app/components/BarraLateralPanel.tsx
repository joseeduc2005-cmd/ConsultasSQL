'use client';

import { PanelLeftClose, PanelLeftOpen } from 'lucide-react';
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import NavegacionSoporte from './NavegacionSoporte';
import Sidebar from './Sidebar';
import type { KnowledgeArticle } from '../types';

interface Categoria {
  categoria: string;
  subcategorias: string[];
}

export function agruparCategorias(articulos: KnowledgeArticle[]): Categoria[] {
  const mapa = new Map<string, Set<string>>();
  articulos.forEach((a) => {
    const cat = a.categoria || 'Sin categoria';
    const sub = a.subcategoria || 'General';
    if (!mapa.has(cat)) mapa.set(cat, new Set());
    mapa.get(cat)?.add(sub);
  });
  return Array.from(mapa.entries()).map(([categoria, subcats]) => ({
    categoria,
    subcategorias: Array.from(subcats),
  }));
}

/**
 * Barra lateral completa del panel: accesos del Centro de Soporte arriba y el árbol
 * de categorías de artículos debajo.
 *
 * Es la misma en las dos secciones a propósito. Antes, al entrar al Centro de Soporte
 * las categorías desaparecían y daba la sensación de haber salido del panel; ahora la
 * barra no cambia nunca y solo se reemplaza el contenido de la derecha.
 *
 * En `/admin` recibe los artículos que la página ya tiene cargados. En las pantallas
 * de soporte no hay quien se los pase, así que los pide por su cuenta y al elegir uno
 * vuelve a `/admin` con ese artículo abierto.
 */
export default function BarraLateralPanel({
  articles,
  categories,
  onSelectArticle,
  onSelectCategory,
  selectedArticle,
  selectedCategory,
  selectedSubcategory,
  collapsed,
  onToggleCollapse,
  mostrarSoporte = true,
  onNavegar,
}: {
  articles?: KnowledgeArticle[];
  categories?: Categoria[];
  onSelectArticle?: (a: KnowledgeArticle) => void;
  onSelectCategory?: (categoria: string, subcategoria?: string) => void;
  selectedArticle?: KnowledgeArticle;
  selectedCategory?: string;
  selectedSubcategory?: string;
  collapsed?: boolean;
  onToggleCollapse?: () => void;
  mostrarSoporte?: boolean;
  onNavegar?: () => void;
}) {
  const router = useRouter();
  const autonoma = articles === undefined;
  const [propios, setPropios] = useState<KnowledgeArticle[]>([]);

  useEffect(() => {
    if (!autonoma) return;
    let vigente = true;
    (async () => {
      try {
        const token = localStorage.getItem('token');
        const rol = localStorage.getItem('userRole') || 'user';
        const res = await fetch('/api/articles', {
          headers: { 'x-user-role': rol, ...(token ? { Authorization: `Bearer ${token}` } : {}) },
        });
        const data = await res.json();
        const lista: KnowledgeArticle[] = data?.success && Array.isArray(data.data)
          ? data.data
          : Array.isArray(data) ? data : [];
        if (vigente) setPropios(lista);
      } catch {
        /* sin artículos la barra sigue siendo útil por los accesos de soporte */
      }
    })();
    return () => { vigente = false; };
  }, [autonoma]);

  const lista = autonoma ? propios : articles;
  const grupos = categories ?? agruparCategorias(lista ?? []);

  return (
    <div className="flex h-full min-h-0 flex-col">
      {/* La flecha de contraer manda sobre toda la barra, así que va en su propia
          cabecera y no pegada a "Categorías", donde parecía contraer solo ese bloque. */}
      <div className={`flex items-center px-1 pb-2 ${collapsed ? 'justify-center' : 'justify-end'}`}>
        <button
          type="button"
          onClick={onToggleCollapse}
          title={collapsed ? 'Expandir panel' : 'Contraer panel'}
          aria-label={collapsed ? 'Expandir panel' : 'Contraer panel'}
          aria-expanded={!collapsed}
          className="grid size-8 place-items-center rounded-lg text-(--ink-600) transition-colors hover:bg-white/70 hover:text-(--ink-900)"
        >
          {collapsed ? <PanelLeftOpen className="size-[18px]" /> : <PanelLeftClose className="size-[18px]" />}
        </button>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto pr-0.5">
        {mostrarSoporte && <NavegacionSoporte colapsado={collapsed} onNavegar={onNavegar} />}
        {mostrarSoporte && !collapsed && <div className="mx-3 my-3 border-t border-(--line)" />}
        <Sidebar
          categories={grupos}
          articles={lista}
          onSelectArticle={
            onSelectArticle ??
            // Desde una pantalla de soporte, abrir un artículo devuelve al panel con él seleccionado.
            ((a) => router.push(`/admin?articulo=${encodeURIComponent(a.id)}`))
          }
          onSelectCategory={onSelectCategory}
          selectedArticle={selectedArticle}
          selectedCategory={selectedCategory}
          selectedSubcategory={selectedSubcategory}
          collapsed={collapsed}
        />
      </div>
    </div>
  );
}
