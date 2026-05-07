import { NextRequest } from 'next/server';
import { B, fwd, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET(request: NextRequest, { params }: { params: Promise<{ name: string }> }) {
  try {
    const { name } = await params;
    const query = request.nextUrl.searchParams.toString();
    const res = await fetch(`${B}/api/process/${encodeURIComponent(name)}${query ? `?${query}` : ''}`, { headers: fwd(request), cache: 'no-store' });
    return proxyJson(res);
  } catch (e) { return err500('process/:name', e); }
}
