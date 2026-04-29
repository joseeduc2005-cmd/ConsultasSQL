import { NextRequest, NextResponse } from 'next/server';

const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';

export async function GET(request: NextRequest) {
  try {
    const q = request.nextUrl.searchParams.get('q') || '';
    const query = q ? `?q=${encodeURIComponent(q)}` : '';

    const response = await fetch(`${backendUrl}/api/query/suggestions${query}`, {
      method: 'GET',
      headers: {
        Authorization: request.headers.get('Authorization') || '',
        'x-user-role': request.headers.get('x-user-role') || '',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('[API][query/suggestions] Error:', error);
    return NextResponse.json(
      { success: false, error: 'Error interno del servidor' },
      { status: 500 }
    );
  }
}