import { PLAYBOOKS } from '../playbooks/index.ts';

export interface Clasificacion {
  sistema: string | null;
  categoria: string | null;
  playbook: { id: string; confianza: number; motivos: string[] } | null;
  /** Casos que no se resuelven con un diagnóstico (proveedor externo, trámite administrativo). */
  orientacion: string | null;
}

interface ReglaTema {
  categoria: string;
  sistema?: string;
  patrones: RegExp[];
  orientacion: string;
}

/** Temas frecuentes que no tienen diagnóstico automático pero sí una orientación conocida. */
const TEMAS: ReglaTema[] = [
  {
    categoria: 'Proveedor externo',
    sistema: 'Corresponsales',
    patrones: [/recarga/i, /facilito/i, /\bclaro\b/i, /movistar/i, /\bcnt\b/i, /tuenti/i],
    orientacion:
      'Revisar primero si el proveedor (Facilito) reporta una caída. En 2026091131 el problema fue del proveedor y se restableció sin intervención de Sistemas.',
  },
  {
    categoria: 'Actas y pagos',
    sistema: 'Corresponsales',
    patrones: [/\bacta/i, /pago (al|a) corresponsal/i, /factura/i],
    orientacion:
      'Si el acta tiene un pago en estado Cerrado, la aplicación no permite editarla. La corrección se escala a Administración de Aplicaciones con el script del ticket 2026090874.',
  },
  {
    categoria: 'Recaudaciones',
    sistema: 'Corresponsales',
    patrones: [/recaudaci/i, /municipio/i, /\bgad\b/i, /comprobante/i, /reimpresi/i],
    orientacion:
      'Confirmar primero si el cobro se procesó (la transacción) y separar del problema de impresión del comprobante, como en 2026082703.',
  },
  {
    categoria: 'Pagos QR',
    sistema: 'Ja Movil',
    patrones: [/\bqr\b/i, /coral/i, /etikos/i],
    orientacion: 'Los pagos QR dependen de proveedores externos (Etikos). Solicitar cédula, cuenta y hora de la operación.',
  },
];

const SISTEMAS: [RegExp, string][] = [
  [/corresponsal|tendero|\bpos\b|\bmac\b|\bimei\b/i, 'Corresponsales'],
  [/ja ?m[oó]vil|app m[oó]vil|aplicaci[oó]n del celular/i, 'Ja Movil'],
  [/ja ?web|banca (web|virtual)|javirtual/i, 'Ja Web'],
  [/ja ?pagos|chas|p2p|p2b/i, 'Ja Pagos'],
];

export function clasificarConReglas(texto: string): Clasificacion {
  const puntajes = PLAYBOOKS.map((p) => {
    const motivos = p.senales.map((r) => texto.match(r)?.[0]).filter((m): m is string => Boolean(m));
    return { id: p.id, categoria: p.categoria, sistema: p.sistema, puntaje: motivos.length, motivos };
  }).sort((a, b) => b.puntaje - a.puntaje);

  const tema = TEMAS.map((t) => ({ ...t, puntaje: t.patrones.filter((r) => r.test(texto)).length }))
    .filter((t) => t.puntaje > 0)
    .sort((a, b) => b.puntaje - a.puntaje)[0];

  const mejor = puntajes[0];
  const sistema = SISTEMAS.find(([r]) => r.test(texto))?.[1] ?? null;

  // Un tema conocido sin diagnóstico gana si tiene más señales que el mejor playbook.
  if (tema && tema.puntaje >= mejor.puntaje) {
    return { sistema: tema.sistema ?? sistema, categoria: tema.categoria, playbook: null, orientacion: tema.orientacion };
  }
  if (mejor.puntaje === 0) {
    return { sistema, categoria: null, playbook: null, orientacion: null };
  }
  const segundo = puntajes[1]?.puntaje ?? 0;
  const confianza = Math.min(0.95, 0.35 + mejor.puntaje * 0.15 + (mejor.puntaje - segundo) * 0.1);
  return {
    sistema: sistema ?? mejor.sistema,
    categoria: mejor.categoria,
    playbook: { id: mejor.id, confianza: Number(confianza.toFixed(2)), motivos: mejor.motivos },
    orientacion: null,
  };
}
