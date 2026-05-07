import { NextRequest } from 'next/server';
import { B, fwd, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET(request: NextRequest) {
  try {
    const text = request.nextUrl.searchParams.get('text') || '';
    const res = await fetch(`${B}/api/query/analyze?text=${encodeURIComponent(text)}`, { headers: fwd(request), cache: 'no-store' });
    return proxyJson(res);
  } catch (e) { return err500('query/analyze', e); }
}
