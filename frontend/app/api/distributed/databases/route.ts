import { NextResponse } from 'next/server';
import { B, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET() {
  try {
    const res = await fetch(`${B}/api/distributed/databases`);
    if (!res.ok) return NextResponse.json({ databases: [] }, { status: res.status });
    return proxyJson(res);
  } catch (e) { return err500('distributed/databases/GET', e); }
}
