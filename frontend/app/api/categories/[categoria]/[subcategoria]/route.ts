// app/api/categories/[categoria]/[subcategoria]/route.ts

import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ categoria: string; subcategoria: string }> }
) {
  try {
    void request;
    const { categoria, subcategoria } = await params;
    const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';
    const response = await fetch(`${backendUrl}/api/categories/${categoria}/${subcategoria}`);
    const data = await response.json();
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('Error obteniendo artículos por categoría:', error);
    return NextResponse.json({ error: 'Error interno del servidor' }, { status: 500 });
  }
}