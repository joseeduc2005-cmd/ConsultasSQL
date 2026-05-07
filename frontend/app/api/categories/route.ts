import { B, err500, proxyJson } from '@/app/lib/backend-client';

export async function GET() {
  try {
    const res = await fetch(`${B}/api/categories`);
    return proxyJson(res);
  } catch (e) { return err500('categories', e); }
}
