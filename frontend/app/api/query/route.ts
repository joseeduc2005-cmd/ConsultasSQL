import { NextRequest, NextResponse } from 'next/server';

const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';

export async function POST(request: NextRequest) {
  try {
    // PASSTHROUGH PURO: Leer el body como texto sin modificar
    const rawBody = await request.text();
    console.log('FRONTEND REQUEST:', rawBody);

    const response = await fetch(`${backendUrl}/api/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: request.headers.get('Authorization') || '',
        'x-user-role': request.headers.get('x-user-role') || '',
      },
      body: rawBody, // Pasar body intacto, sin JSON.stringify()
      cache: 'no-store',
    });

    const responseBody = await response.text();

    return new NextResponse(responseBody, {
      status: response.status,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('❌ [API][query] Error:', error);
    return NextResponse.json(
      { success: false, error: 'Error interno del servidor' },
      { status: 500 }
    );
  }
}
