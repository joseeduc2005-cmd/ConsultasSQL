/**
 * Shared backend proxy utilities for Next.js API routes.
 * All calls to the backend stay server-side — never exposed to the browser.
 */
import { NextRequest, NextResponse } from 'next/server';

export const B = process.env.BACKEND_URL || 'http://localhost:3002';

/** Auth headers to forward from the incoming request */
export function fwd(req: NextRequest) {
  return {
    Authorization: req.headers.get('Authorization') || '',
    'x-user-role': req.headers.get('x-user-role') || '',
  };
}

/** Auth headers + Content-Type (for JSON bodies) */
export function fwdJson(req: NextRequest) {
  return { ...fwd(req), 'Content-Type': 'application/json' };
}

/** Standard 500 error response */
export function err500(tag: string, e: unknown) {
  console.error(`[${tag}]`, e);
  return NextResponse.json({ success: false, error: 'Error interno del servidor' }, { status: 500 });
}

/** Proxy a backend JSON response straight through */
export async function proxyJson(res: Response) {
  const data = await res.json();
  return NextResponse.json(data, { status: res.status });
}

/** Proxy a backend response as raw text (preserves exact body) */
export async function proxyRaw(res: Response) {
  const body = await res.text();
  return new NextResponse(body, {
    status: res.status,
    headers: { 'Content-Type': 'application/json' },
  });
}
