import { NextRequest, NextResponse } from 'next/server';

const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';

export async function GET(request: NextRequest) {
  try {
    const text = request.nextUrl.searchParams.get('text') || '';
    const response = await fetch(`${backendUrl}/api/query/analyze?text=${encodeURIComponent(text)}`, {
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
    console.error('[API][query/analyze] Error:', error);
    return NextResponse.json(
      { success: false, error: 'Error interno del servidor' },
      { status: 500 }
    );
  }
}
