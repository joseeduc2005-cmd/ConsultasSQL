/**
 * Extracción determinística de identificadores desde texto libre (tickets, WhatsApp, correos).
 * No usa modelo de lenguaje: expresiones regulares y validaciones propias de Ecuador.
 */

export interface Identificadores {
  macs: string[];
  cedulas: string[];
  usuarios: string[];
  tickets: string[];
  fechas: string[];
  cuentas: string[];
  /** Numeros de acta de pago al corresponsal (ticket 2026090874). */
  actas: string[];
}

/** Valida una cédula ecuatoriana (provincia, tercer dígito y dígito verificador módulo 10). */
export function cedulaValida(cedula: string): boolean {
  if (!/^\d{10}$/.test(cedula)) return false;
  const provincia = Number(cedula.slice(0, 2));
  if (!((provincia >= 1 && provincia <= 24) || provincia === 30)) return false;
  if (Number(cedula[2]) >= 6) return false;
  const suma = [...cedula.slice(0, 9)].reduce((acc, d, i) => {
    const p = Number(d) * (i % 2 === 0 ? 2 : 1);
    return acc + (p > 9 ? p - 9 : p);
  }, 0);
  return (10 - (suma % 10)) % 10 === Number(cedula[9]);
}

const unicos = (valores: string[]) => [...new Set(valores)];

const DOMINIOS = /\.(com|ec|fin|net|org|gob|edu|html?|php|js|ts|jpg|png|pdf|txt|sql)$/i;

/**
 * `esquema.tabla` tiene la misma forma que `nombre.apellido`, así que el ticket
 * 2026083744 —que menciona `fisja.clientes` en el texto— dejaba «fisja.clientes»
 * como nombre de usuario y el diagnóstico salía a buscar a una persona que no
 * existe. Los esquemas son tres y conocidos; descartarlos sale barato.
 */
const ESQUEMAS = /^(corresponsales|fisja|jst_personas|sys|system|dual)\./i;

function aIso(dia: number, mes: number, anio: number): string | null {
  const fecha = new Date(Date.UTC(anio, mes - 1, dia));
  if (fecha.getUTCMonth() !== mes - 1 || anio < 2000 || anio > 2100) return null;
  return fecha.toISOString().slice(0, 10);
}

export function extraerIdentificadores(texto: string): Identificadores {
  const tickets: string[] = [];
  const cedulas: string[] = [];

  // Números de 10 dígitos: ticket del SIR (ej. 2026083744) o cédula.
  for (const m of texto.matchAll(/\b\d{10}\b/g)) {
    const numero = m[0];
    const antes = texto.slice(Math.max(0, m.index - 25), m.index).toLowerCase();
    const pareceTicket = /ticket|caso|sir|incidente|requerimiento|#\s*$/.test(antes) || /^20[2-3]\d(0[1-9]|1[0-2])\d{4}$/.test(numero);
    const mencionaCedula = /c[eé]dula|c\.?\s?i\.?|identificaci/.test(antes);
    if (mencionaCedula && cedulaValida(numero)) cedulas.push(numero);
    else if (pareceTicket) tickets.push(numero);
    else if (cedulaValida(numero)) cedulas.push(numero);
  }

  const macs = [...texto.matchAll(/\b\d{15}\b/g)].map((m) => m[0]);

  // El punto final de una frase no debe impedir la detección: antes «el usuario es
  // demo.duplicado.» no encontraba nada. Se sigue descartando «a.b.c», que no es un
  // nombre de usuario sino una ruta o un dominio.
  const usuarios = [...texto.matchAll(/(?<![@\w.])([a-z][a-z0-9]{1,30}\.[a-z][a-z0-9]{1,30})(?![\w@])(?!\.[a-z0-9])/gi)]
    .map((m) => m[1].toLowerCase())
    .filter((u) => !DOMINIOS.test(u) && !/^(p|a)\.m$/.test(u) && !ESQUEMAS.test(u));

  // «acta 53656» — el ticket 2026090874 lo trae así en el texto.
  const actas = [...texto.matchAll(/\bactas?\s*(?:n[°º.]?|no\.?|numero|número)?\s*:?\s*(\d{3,10})\b/gi)].map((m) => m[1]);

  const fechas: string[] = [];
  for (const m of texto.matchAll(/\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b/g)) {
    const iso = aIso(Number(m[1]), Number(m[2]), Number(m[3]));
    if (iso) fechas.push(iso);
  }
  for (const m of texto.matchAll(/\b(\d{4})-(\d{2})-(\d{2})\b/g)) {
    const iso = aIso(Number(m[3]), Number(m[2]), Number(m[1]));
    if (iso) fechas.push(iso);
  }

  const cuentas = [...texto.matchAll(/cuenta\s*(?:n[°º.]?|no\.?|número|numero)?\s*:?\s*(\d{5,10})\b/gi)].map((m) => m[1]);

  return {
    macs: unicos(macs),
    cedulas: unicos(cedulas),
    usuarios: unicos(usuarios),
    tickets: unicos(tickets),
    fechas: unicos(fechas).sort(),
    cuentas: unicos(cuentas),
    actas: unicos(actas),
  };
}
