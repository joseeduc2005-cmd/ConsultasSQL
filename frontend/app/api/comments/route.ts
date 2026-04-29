import { NextRequest, NextResponse } from 'next/server';

// GET /api/comments?article_id=xxx
export async function GET(request: NextRequest) {
  const articleId = request.nextUrl.searchParams.get('article_id');
  if (!articleId) {
    return NextResponse.json({ error: 'article_id requerido' }, { status: 400 });
  }

  try {
    const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';
    const response = await fetch(`${backendUrl}/api/comments?article_id=${encodeURIComponent(articleId)}`);
    const data = await response.json();
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('[COMMENTS GET] Error:', error);
    return NextResponse.json({ error: 'Error al obtener comentarios' }, { status: 500 });
  }
}

// POST /api/comments
export async function POST(request: NextRequest) {
  try {
    const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';
    const body = await request.json();
    const response = await fetch(`${backendUrl}/api/comments`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    const data = await response.json();
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('[COMMENTS POST] Error:', error);
    return NextResponse.json({ error: 'Error al guardar comentario' }, { status: 500 });
  }
}
