import { NextRequest, NextResponse } from 'next/server';
import { B, err500 } from '@/app/lib/backend-client';

export async function POST(request: NextRequest) {
  try {
    const cookieHeader = request.headers.get('cookie') || '';
    const tokenFromCookie = cookieHeader.split(';').map(c => c.trim()).find(c => c.startsWith('token='))?.split('=').slice(1).join('=');
    const decodedToken = tokenFromCookie ? decodeURIComponent(tokenFromCookie) : '';
    const authHeader = request.headers.get('Authorization') || (decodedToken ? `Bearer ${decodedToken}` : '');

    await fetch(`${B}/api/auth/logout`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(authHeader ? { Authorization: authHeader } : {}),
        ...(cookieHeader ? { cookie: cookieHeader } : {}),
      },
    });

    return NextResponse.json({ success: true });
  } catch (e) { return err500('auth/logout', e); }
}
