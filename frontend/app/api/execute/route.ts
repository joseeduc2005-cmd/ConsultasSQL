import { NextRequest } from 'next/server';
import { B, fwd, err500, proxyRaw } from '@/app/lib/backend-client';

// Mutación → nunca cachear
export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  try {
    const rawBody = await request.text();
    const res = await fetch(`${B}/api/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...fwd(request) },
      body: rawBody,
      cache: 'no-store',
    });
    return proxyRaw(res);
  } catch (e) { return err500('execute', e); }
}
