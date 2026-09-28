import { NextRequest } from 'next/server';
import { B, err500, fwd, fwdJson, proxyRaw } from '@/app/lib/backend-client';

/**
 * Proxy de todo /api/soporte-cliente/* hacia el backend.
 *
 * Es una sola ruta comodín en lugar de un archivo por endpoint: el módulo de
 * soporte tiene 18 rutas y ninguna necesita lógica propia aquí, solo reenvío.
 * Como el resto de handlers de A, la llamada al backend nunca sale del servidor.
 */
async function reenviar(peticion: NextRequest, contexto: { params: Promise<{ ruta: string[] }> }) {
  const { ruta } = await contexto.params;
  const conCuerpo = peticion.method !== 'GET' && peticion.method !== 'HEAD';
  try {
    const respuesta = await fetch(`${B}/api/soporte-cliente/${ruta.join('/')}${peticion.nextUrl.search}`, {
      method: peticion.method,
      headers: conCuerpo ? fwdJson(peticion) : fwd(peticion),
      body: conCuerpo ? await peticion.text() : undefined,
      cache: 'no-store',
    });
    return proxyRaw(respuesta);
  } catch (e) {
    return err500(`soporte-cliente/${peticion.method} ${ruta.join('/')}`, e);
  }
}

export const GET = reenviar;
export const POST = reenviar;
export const PATCH = reenviar;
export const PUT = reenviar;
export const DELETE = reenviar;
