import { NextRequest, NextResponse } from 'next/server';
import { B, err500, proxyJson } from '@/app/lib/backend-client';

export async function DELETE(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id: rawId } = await params;
  const id = parseInt(rawId, 10);
  if (isNaN(id)) return NextResponse.json({ error: 'ID inválido' }, { status: 400 });
  try {
    const res = await fetch(`${B}/api/comments/${id}`, { method: 'DELETE' });
    return proxyJson(res);
  } catch (e) { return err500('comments/DELETE', e); }
}
