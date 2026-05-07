import { NextRequest, NextResponse } from 'next/server';
import { B, err500 } from '@/app/lib/backend-client';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const res = await fetch(`${B}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!res.ok) return NextResponse.json({ error: data.error || 'Error en autenticación' }, { status: res.status });
    return NextResponse.json(data);
  } catch (e) { return err500('auth/login', e); }
}
