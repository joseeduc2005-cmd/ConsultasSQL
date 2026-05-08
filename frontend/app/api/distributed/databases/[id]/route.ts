import { NextRequest, NextResponse } from 'next/server';
import { B, err500, fwdJson } from '@/app/lib/backend-client';

export async function DELETE(request: NextRequest, { params }: { params: { id: string } }) {
  try {
    const res = await fetch(`${B}/api/distributed/databases/${params.id}`, {
      method: 'DELETE',
      headers: fwdJson(request),
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      return NextResponse.json(body, { status: res.status });
    }
    const body = await res.json();
    return NextResponse.json(body);
  } catch (e) { return err500('distributed/databases/DELETE', e); }
}
