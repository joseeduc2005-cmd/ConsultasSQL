import { NextRequest, NextResponse } from 'next/server';
import { B, fwd, err500 } from '@/app/lib/backend-client';

export async function GET(request: NextRequest) {
  try {
    const res = await fetch(`${B}/api/admin/tables`, { headers: fwd(request), cache: 'no-store' });
    const data = await res.json();
    if (!res.ok) return NextResponse.json(data, { status: res.status });
    const tables = Array.isArray(data?.data) ? data.data : Array.isArray(data?.tables) ? data.tables : [];
    return NextResponse.json({ success: true, data: tables, tables });
  } catch (e) { return err500('admin/tables', e); }
}
