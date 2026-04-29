// app/api/articles/route.ts

import { NextRequest, NextResponse } from 'next/server';

export async function GET(request: NextRequest) {
  try {
    const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';
    const response = await fetch(`${backendUrl}/api/articles`);
    const data = await response.json();

    if (!response.ok) {
      console.error('[ARTICLES] ❌ Error backend:', data);
      return NextResponse.json({ error: 'Error backend al obtener artículos' }, { status: response.status });
    }

    console.log(`[ARTICLES] ✅ ${Array.isArray(data.data) ? data.data.length : 0} artículos obtenidos del backend`);
    return NextResponse.json(data);
  } catch (error) {
    console.error('[ARTICLES] ❌ Error:', error);
    return NextResponse.json({ error: 'Error interno del servidor' }, { status: 500 });
  }
}


export async function POST(request: NextRequest) {
  try {
    const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';
    const body = await request.json();
    
    const response = await fetch(`${backendUrl}/api/articles`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': request.headers.get('Authorization') || '',
        'x-user-role': request.headers.get('x-user-role') || '',
      },
      body: JSON.stringify(body),
    });
    
    const data = await response.json();
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('Error creating article:', error);
    return NextResponse.json({ error: 'Error interno del servidor' }, { status: 500 });
  }
}
