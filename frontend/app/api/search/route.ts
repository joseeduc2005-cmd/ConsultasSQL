// app/api/search/route.ts

import { NextRequest, NextResponse } from 'next/server';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const query = searchParams.get('q') || '';

    const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';
    const response = await fetch(`${backendUrl}/api/search?q=${encodeURIComponent(query)}`);
    const data = await response.json();

    return NextResponse.json(data);
  } catch (error) {
    console.error('Error en búsqueda:', error);
    return NextResponse.json({ error: 'Error interno del servidor' }, { status: 500 });
  }
}
