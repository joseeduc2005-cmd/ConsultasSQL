import { NextRequest, NextResponse } from 'next/server';

const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';

export async function GET(request: NextRequest) {
  try {
    const query = request.nextUrl.searchParams.toString();
    const suffix = query ? `?${query}` : '';

    const response = await fetch(`${backendUrl}/api/db/schema${suffix}`, {
      method: 'GET',
      headers: {
        Authorization: request.headers.get('Authorization') || '',
        'x-user-role': request.headers.get('x-user-role') || '',
      },
      cache: 'no-store',
    });

    console.info('[API][db/schema] passthrough', {
      databaseId: request.nextUrl.searchParams.get('databaseId') || '',
      query,
    });

    const data = await response.json();
    if (!response.ok) {
      return NextResponse.json(data, { status: response.status });
    }

    return NextResponse.json(data, { status: 200 });
  } catch (error) {
    console.error('[API][db/schema] Error:', error);
    return NextResponse.json(
      { success: false, error: 'Error interno del servidor' },
      { status: 500 },
    );
  }
}
