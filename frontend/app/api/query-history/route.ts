import { NextRequest, NextResponse } from 'next/server';

const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';

function buildForwardHeaders(request: NextRequest) {
  return {
    Authorization: request.headers.get('Authorization') || '',
    'x-user-role': request.headers.get('x-user-role') || '',
  };
}

export async function GET(request: NextRequest) {
  try {
    const limit = request.nextUrl.searchParams.get('limit') || '50';
    const response = await fetch(`${backendUrl}/api/query-history?limit=${encodeURIComponent(limit)}`, {
      method: 'GET',
      headers: buildForwardHeaders(request),
      cache: 'no-store',
    });

    const data = await response.json();
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('[API][query-history] Error:', error);
    return NextResponse.json(
      { success: false, error: 'Error interno del servidor' },
      { status: 500 }
    );
  }
}

export async function DELETE(request: NextRequest) {
  try {
    const response = await fetch(`${backendUrl}/api/query-history`, {
      method: 'DELETE',
      headers: buildForwardHeaders(request),
      cache: 'no-store',
    });

    const data = await response.json();
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('[API][query-history][DELETE] Error:', error);
    return NextResponse.json(
      { success: false, error: 'Error interno del servidor' },
      { status: 500 }
    );
  }
}
