import { NextRequest, NextResponse } from 'next/server';

const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';

export async function GET(request: NextRequest) {
  try {
    const response = await fetch(`${backendUrl}/api/admin/tables`, {
      method: 'GET',
      headers: {
        Authorization: request.headers.get('Authorization') || '',
        'x-user-role': request.headers.get('x-user-role') || '',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    if (!response.ok) {
      return NextResponse.json(data, { status: response.status });
    }

    const tables = Array.isArray(data?.data)
      ? data.data
      : Array.isArray(data?.tables)
        ? data.tables
        : [];

    return NextResponse.json(
      {
        success: true,
        data: tables,
        tables,
      },
      { status: 200 },
    );
  } catch (error) {
    console.error('[API][admin/tables] Error:', error);
    return NextResponse.json(
      { success: false, error: 'Error interno del servidor' },
      { status: 500 },
    );
  }
}
