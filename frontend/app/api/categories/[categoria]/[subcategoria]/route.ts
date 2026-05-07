import { NextRequest } from 'next/server';
import { B, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET(_req: NextRequest, { params }: { params: Promise<{ categoria: string; subcategoria: string }> }) {
  try {
    const { categoria, subcategoria } = await params;
    const res = await fetch(`${B}/api/categories/${categoria}/${subcategoria}`);
    return proxyJson(res);
  } catch (e) { return err500('categories/sub', e); }
}
