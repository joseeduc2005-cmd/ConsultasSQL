import { NextRequest, NextResponse } from 'next/server';
import { B, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET(request: NextRequest) {
  const articleId = request.nextUrl.searchParams.get('article_id');
  if (!articleId) return NextResponse.json({ error: 'article_id requerido' }, { status: 400 });
  try {
    const res = await fetch(`${B}/api/comments?article_id=${encodeURIComponent(articleId)}`);
    return proxyJson(res);
  } catch (e) { return err500('comments/GET', e); }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const res = await fetch(`${B}/api/comments`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return proxyJson(res);
  } catch (e) { return err500('comments/POST', e); }
}
