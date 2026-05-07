import { NextRequest } from 'next/server';
import { B, fwd, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET(request: NextRequest) {
  try {
    const res = await fetch(`${B}/api/auth/me`, { headers: fwd(request) });
    return proxyJson(res);
  } catch (e) { return err500('auth/me', e); }
}
