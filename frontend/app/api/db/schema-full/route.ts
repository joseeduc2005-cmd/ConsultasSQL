import { NextRequest } from 'next/server';
import { B, fwd, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET(request: NextRequest) {
  try {
    const query = request.nextUrl.searchParams.toString();
    const res = await fetch(`${B}/api/db/schema-full${query ? `?${query}` : ''}`, { headers: fwd(request), cache: 'no-store' });
    return proxyJson(res);
  } catch (e) { return err500('db/schema-full', e); }
}
