import { NextRequest, NextResponse } from 'next/server';
import { B, fwd, err500 } from '@/app/lib/backend-client';

export async function GET(request: NextRequest, { params }: { params: Promise<{ tableName: string }> }) {
  try {
    const { tableName } = await params;
    const res = await fetch(`${B}/api/admin/tables/${encodeURIComponent(tableName)}/columns`, { headers: fwd(request), cache: 'no-store' });
    const data = await res.json();
    if (!res.ok) return NextResponse.json(data, { status: res.status });
    const columns = Array.isArray(data?.data?.columns) ? data.data.columns : Array.isArray(data?.columns) ? data.columns : [];
    return NextResponse.json({ success: true, data: { columns }, columns });
  } catch (e) { return err500('admin/tables/columns', e); }
}
