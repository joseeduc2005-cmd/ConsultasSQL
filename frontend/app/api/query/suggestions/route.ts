import { NextRequest } from 'next/server';
import { B, fwd, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET(request: NextRequest) {
  try {
    const q = request.nextUrl.searchParams.get('q') || '';
    const res = await fetch(`${B}/api/query/suggestions${q ? `?q=${encodeURIComponent(q)}` : ''}`, { headers: fwd(request), cache: 'no-store' });
    return proxyJson(res);
  } catch (e) { return err500('query/suggestions', e); }
}
