// app/middleware.ts

import { NextRequest, NextResponse } from 'next/server';

export function middleware(request: NextRequest) {
  const token = request.cookies.get('token')?.value;
  const pathname = request.nextUrl.pathname;

  // Si intenta acceder a rutas protegidas sin token
  if ((pathname.startsWith('/dashboard') || pathname.startsWith('/admin')) && !token) {
    // Permitir que el frontend maneje la redirección basada en localStorage
    // ya que NextJS no tiene acceso directo a localStorage en middleware
    return NextResponse.next();
  }

  return NextResponse.next();
}

export const config = {
  matcher: ['/dashboard/:path*', '/admin/:path*'],
};
