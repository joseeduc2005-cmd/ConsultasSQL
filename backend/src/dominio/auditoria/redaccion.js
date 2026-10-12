/**
 * Qué se guarda de una petición y qué no.
 *
 * Auditar todo y guardar todo no son lo mismo. Por las rutas de este sistema pasan
 * contraseñas, tokens y cédulas de socios, y una tabla de auditoría es justo el sitio
 * donde no deben acabar: es la que más se consulta, la que más se exporta y la que
 * nadie limpia. Así que el rastro queda entero —quién, cuándo, qué ruta, con qué
 * resultado— y del contenido queda lo justo para entender qué se hizo.
 *
 * Es código puro a propósito: decidir qué se tacha es una regla del negocio, no un
 * detalle de PostgreSQL, y así se puede probar sin levantar nada.
 */

/** Claves cuyo valor no se guarda nunca, ni enmascarado. */
export const CLAVES_SECRETAS = [
  'password', 'contrasena', 'contraseña', 'clave', 'pass',
  'token', 'api_key', 'apikey', 'secret', 'authorization', 'auth',
  'refresh_token', 'access_token', 'hash', 'cookie', 'credentials',
];

/** Claves que llevan un identificador de persona: se guardan, pero tapadas. */
export const CLAVES_IDENTIFICADORAS = ['cedula', 'identificacion', 'documento', 'dni', 'ruc'];

const TACHADO = '[omitido]';
const MAX_TEXTO = 300;
const MAX_CLAVES = 40;

/**
 * Tapa el medio de un identificador: 0105609176 → 0105****76.
 *
 * Se dejan los extremos porque sirven para reconocer el caso al leer la auditoría sin
 * reconstruir el número. Lo que es corto se tacha entero: tapar el medio de algo de
 * cuatro cifras no tapa nada.
 */
export function enmascararIdentificador(valor) {
  const texto = String(valor ?? '');
  if (texto.length < 7) return TACHADO;
  return texto.slice(0, 4) + '*'.repeat(texto.length - 6) + texto.slice(-2);
}

const esSecreta = (clave) => {
  const k = String(clave).toLowerCase();
  return CLAVES_SECRETAS.some((s) => k === s || k.includes(s));
};

const esIdentificadora = (clave) => {
  const k = String(clave).toLowerCase();
  return CLAVES_IDENTIFICADORAS.some((s) => k === s || k.includes(s));
};

/**
 * Deja un valor en algo que se pueda guardar: corto, sin anidar hasta el infinito y
 * sin secretos. La profundidad está limitada porque un cuerpo puede traer un árbol
 * entero y la auditoría no es un volcado del cuerpo, es un resumen de lo que pasó.
 */
function redactarValor(valor, profundidad) {
  if (valor === null || valor === undefined) return null;

  if (typeof valor === 'string') {
    return valor.length > MAX_TEXTO ? valor.slice(0, MAX_TEXTO) + '…' : valor;
  }
  if (typeof valor === 'number' || typeof valor === 'boolean') return valor;

  if (profundidad <= 0) return '[…]';

  if (Array.isArray(valor)) {
    return valor.slice(0, 10).map((v) => redactarValor(v, profundidad - 1));
  }
  if (typeof valor === 'object') return redactarCuerpo(valor, profundidad - 1);

  return String(valor).slice(0, MAX_TEXTO);
}

/**
 * El cuerpo de una petición, listo para guardarse.
 *
 * Las claves se conservan siempre, incluso las secretas: saber que alguien mandó una
 * contraseña es parte del rastro, y el valor no hace falta para eso.
 */
export function redactarCuerpo(cuerpo, profundidad = 3) {
  if (!cuerpo || typeof cuerpo !== 'object' || Array.isArray(cuerpo)) return {};

  const salida = {};
  let n = 0;
  for (const [clave, valor] of Object.entries(cuerpo)) {
    if (n >= MAX_CLAVES) {
      salida['…'] = `y ${Object.keys(cuerpo).length - MAX_CLAVES} campos más`;
      break;
    }
    n += 1;

    if (esSecreta(clave)) salida[clave] = TACHADO;
    else if (esIdentificadora(clave)) salida[clave] = enmascararIdentificador(valor);
    else salida[clave] = redactarValor(valor, profundidad);
  }
  return salida;
}

/** Los métodos que cambian algo. Un GET no cambia nada y se audita aparte. */
export const METODOS_QUE_CAMBIAN = ['POST', 'PUT', 'PATCH', 'DELETE'];

export const cambiaAlgo = (metodo) => METODOS_QUE_CAMBIAN.includes(String(metodo || '').toUpperCase());

/**
 * Rutas que no se auditan.
 *
 * Solo el sondeo de salud, que no es una acción de nadie: lo llama el navegador y el
 * supervisor del servicio cada pocos segundos y llenaría la tabla de ruido sin decir
 * nada. Todo lo demás entra, incluido lo que falla y lo que se deniega — un intento
 * rechazado es justo lo que hay que poder ver después.
 */
export const RUTAS_SIN_AUDITORIA = ['/health', '/api/health', '/favicon.ico'];

/**
 * Tampoco se audita el sondeo previo de CORS.
 *
 * Un OPTIONS no lo manda nadie: lo manda el navegador por su cuenta antes de la
 * petición de verdad, y esa sí se audita. Registrarlo duplicaría cada fila sin
 * añadir un solo dato sobre quién hizo qué.
 */
export const METODOS_SIN_AUDITORIA = ['OPTIONS', 'HEAD'];

export function seAudita(metodo, ruta) {
  if (METODOS_SIN_AUDITORIA.includes(String(metodo || '').toUpperCase())) return false;
  const camino = String(ruta || '').split('?')[0];
  if (RUTAS_SIN_AUDITORIA.includes(camino)) return false;
  return true;
}
