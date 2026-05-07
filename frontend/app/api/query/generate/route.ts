import { NextRequest } from 'next/server';
import { B, fwdJson, err500, proxyJson } from '@/app/lib/backend-client';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const res = await fetch(`${B}/api/query/generate`, { method: 'POST', headers: fwdJson(request), body: JSON.stringify(body), cache: 'no-store' });
    return proxyJson(res);
  } catch (e) { return err500('query/generate', e); }
}
