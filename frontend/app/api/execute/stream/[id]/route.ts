import { NextRequest } from 'next/server';
import { randomUUID } from 'crypto';

// Server-side only — BACKEND_URL is never exposed to the browser
const backendUrl = process.env.BACKEND_URL || 'http://localhost:3002';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const search = request.nextUrl.searchParams.toString();
  const suffix = search ? `?${search}` : '';

  // Propagar o generar correlation ID para poder correlacionar logs SSE con el backend
  const requestId = request.headers.get('x-request-id') || randomUUID();

  try {
    const upstream = await fetch(
      `${backendUrl}/api/execute/stream/${id}${suffix}`,
      {
        headers: {
          Accept: 'text/event-stream',
          // Reenviar autenticación — sin esto, el backend recibe requests sin sesión
          Authorization: request.headers.get('Authorization') || '',
          'x-user-role': request.headers.get('x-user-role') || '',
          'x-request-id': requestId,
          ...(request.headers.get('cookie') ? { cookie: request.headers.get('cookie')! } : {}),
        },
        // @ts-ignore — Node.js fetch supports duplex for streaming
        duplex: 'half',
      }
    );

    if (!upstream.ok || !upstream.body) {
      return new Response(
        `event: done\ndata: ${JSON.stringify({ success: false, message: 'Error al conectar con el backend' })}\n\n`,
        {
          status: 502,
          headers: {
            'Content-Type': 'text/event-stream',
            'X-Request-Id': requestId,
          },
        }
      );
    }

    return new Response(upstream.body, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
        'X-Request-Id': requestId,
      },
    });
  } catch (err) {
    console.error('[SSE-PROXY] Error:', err, { requestId, articleId: id });
    return new Response(
      `event: done\ndata: ${JSON.stringify({ success: false, message: 'Error interno del proxy SSE' })}\n\n`,
      {
        status: 500,
        headers: {
          'Content-Type': 'text/event-stream',
          'X-Request-Id': requestId,
        },
      }
    );
  }
}
