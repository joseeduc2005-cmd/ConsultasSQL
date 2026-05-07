import { NextRequest } from 'next/server';

// Server-side only — BACKEND_URL is never exposed to the browser
const backendUrl = process.env.BACKEND_URL || 'http://localhost:3002';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const search = request.nextUrl.searchParams.toString();
  const suffix = search ? `?${search}` : '';

  try {
    const upstream = await fetch(
      `${backendUrl}/api/execute/stream/${id}${suffix}`,
      {
        headers: { Accept: 'text/event-stream' },
        // @ts-ignore — Node.js fetch supports duplex
        duplex: 'half',
      }
    );

    if (!upstream.ok || !upstream.body) {
      return new Response(
        `event: done\ndata: ${JSON.stringify({ success: false, message: 'Error al conectar con el backend' })}\n\n`,
        {
          status: 502,
          headers: { 'Content-Type': 'text/event-stream' },
        }
      );
    }

    return new Response(upstream.body, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
      },
    });
  } catch (err) {
    console.error('[SSE-PROXY] Error:', err);
    return new Response(
      `event: done\ndata: ${JSON.stringify({ success: false, message: 'Error interno del proxy SSE' })}\n\n`,
      {
        status: 500,
        headers: { 'Content-Type': 'text/event-stream' },
      }
    );
  }
}
