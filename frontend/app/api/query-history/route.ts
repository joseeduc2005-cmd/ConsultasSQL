import { NextRequest } from 'next/server';
import { B, fwd, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET(request: NextRequest) {
  try {
    const limit = request.nextUrl.searchParams.get('limit') || '50';
    const res = await fetch(`${B}/api/query-history?limit=${encodeURIComponent(limit)}`, { headers: fwd(request), cache: 'no-store' });
    return proxyJson(res);
  } catch (e) { return err500('query-history/GET', e); }
}

export async function DELETE(request: NextRequest) {
  try {
    const res = await fetch(`${B}/api/query-history`, { method: 'DELETE', headers: fwd(request), cache: 'no-store' });
    return proxyJson(res);
  } catch (e) { return err500('query-history/DELETE', e); }
}
