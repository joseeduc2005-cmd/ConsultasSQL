import { NextRequest, NextResponse } from 'next/server';
import { B, fwdJson, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET() {
  try {
    const res = await fetch(`${B}/api/articles`);
    if (!res.ok) return NextResponse.json({ error: 'Error backend al obtener artículos' }, { status: res.status });
    return proxyJson(res);
  } catch (e) { return err500('articles/GET', e); }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const res = await fetch(`${B}/api/articles`, {
      method: 'POST',
      headers: fwdJson(request),
      body: JSON.stringify(body),
    });
    return proxyJson(res);
  } catch (e) { return err500('articles/POST', e); }
}
