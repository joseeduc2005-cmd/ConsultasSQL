import { NextRequest, NextResponse } from 'next/server';
import { B, err500, proxyJson, fwdJson } from '@/app/lib/backend-client';

export async function GET(request: NextRequest) {
  try {
    // Se reenvía la query (showAll) para poder pedir también las bases que ahora
    // mismo no están conectadas.
    const res = await fetch(`${B}/api/distributed/databases${request.nextUrl.search}`, { cache: 'no-store' });
    if (!res.ok) return NextResponse.json({ databases: [] }, { status: res.status });
    return proxyJson(res);
  } catch (e) { return err500('distributed/databases/GET', e); }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const res = await fetch(`${B}/api/distributed/databases`, {
      method: 'POST',
      headers: fwdJson(request),
      body: JSON.stringify(body),
    });
    return proxyJson(res);
  } catch (e) { return err500('distributed/databases/POST', e); }
}
