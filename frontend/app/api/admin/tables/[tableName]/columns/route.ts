import { NextRequest, NextResponse } from 'next/server';

const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ tableName: string }> },
) {
  try {
    const { tableName: rawTableName } = await params;
    const tableName = encodeURIComponent(rawTableName);
    const response = await fetch(`${backendUrl}/api/admin/tables/${tableName}/columns`, {
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

    const columns = Array.isArray(data?.data?.columns)
      ? data.data.columns
      : Array.isArray(data?.columns)
        ? data.columns
        : [];

    return NextResponse.json(
      {
        success: true,
        data: { columns },
        columns,
      },
      { status: 200 },
    );
  } catch (error) {
    console.error('[API][admin/tables/:tableName/columns] Error:', error);
    return NextResponse.json(
      { success: false, error: 'Error interno del servidor' },
      { status: 500 },
    );
  }
}
