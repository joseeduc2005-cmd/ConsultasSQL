import { NextRequest } from 'next/server';
import { B, fwd, fwdJson, err500, proxyJson } from '@/app/lib/backend-client';

export async function PUT(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    const body = await request.json();
    const res = await fetch(`${B}/api/articles/${id}`, { method: 'PUT', headers: fwdJson(request), body: JSON.stringify(body) });
    return proxyJson(res);
  } catch (e) { return err500('articles/PUT', e); }
}

export async function DELETE(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    const res = await fetch(`${B}/api/articles/${id}`, { method: 'DELETE', headers: fwd(request) });
    return proxyJson(res);
  } catch (e) { return err500('articles/DELETE', e); }
}
