import { NextRequest } from 'next/server';
import { B, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET(request: NextRequest) {
  try {
    const q = request.nextUrl.searchParams.get('q') || '';
    const res = await fetch(`${B}/api/search?q=${encodeURIComponent(q)}`);
    return proxyJson(res);
  } catch (e) { return err500('search', e); }
}
