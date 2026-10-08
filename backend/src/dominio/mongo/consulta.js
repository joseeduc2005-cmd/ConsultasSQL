/**
 * Leer una consulta de MongoDB tal como la gente la escribe, y dejar pasar solo lectura.
 *
 * Dos cosas que faltaban, y las dos se ven con un documento real delante:
 *
 *   { _id: ObjectId('6824b0c4…'), institutionCode: NumberInt('4'),
 *     conciliationDate: ISODate('2025-05-14T10:03:32.007Z'), debitTotal: Double('234.54') }
 *
 * Eso no es JSON: las claves van sin comillas, las cadenas van con comilla simple y los
 * valores vienen envueltos en funciones. Es lo que copia cualquiera de Compass o de
 * mongosh, y el ejecutor hacía `JSON.parse` y reventaba con «Unexpected token 'O'». O
 * sea que para usar Mongo había que traducir la consulta a mano antes de pegarla, que es
 * justo el trabajo que se supone que ahorra la herramienta.
 *
 * Y la otra: un artículo de Mongo no pasaba por ninguna comprobación de escritura. El
 * validador tenía un `return` temprano —«es Mongo, no hace falta validar el SQL»— y con
 * eso un `deleteMany` se guardaba tan tranquilo. La regla es que el sistema no escribe
 * en ninguna base; Mongo es una base.
 *
 * Lo que sale de aquí no son objetos del driver: son marcadores en forma canónica
 * (`{ $oid }`, `{ $date }`), que es lo que permite probar esto sin levantar un Mongo y
 * deja la conversión a tipos donde corresponde, en la capa que habla con el motor.
 */

/** Lo que solo lee. */
export const LECTURAS_MONGO = new Set([
  'find', 'findone', 'aggregate', 'countdocuments', 'count', 'distinct',
  'estimateddocumentcount',
]);

/**
 * Lo que escribe. No se usa para decidir —eso lo decide la lista de lecturas— sino para
 * poder decir qué es lo que se está rechazando, que ayuda más que un «no se reconoce».
 */
export const ESCRITURAS_MONGO = new Set([
  'insertone', 'insertmany', 'updateone', 'updatemany', 'replaceone',
  'deleteone', 'deletemany', 'findoneandupdate', 'findoneanddelete',
  'findoneandreplace', 'bulkwrite', 'drop', 'createindex', 'dropindex',
  'renamecollection', 'mapreduce',
]);

/** Claves de una consulta que no nombran la operación: la acompañan. */
const ACOMPANAN = new Set([
  'collection', 'coll', 'db', 'database', 'projection', 'sort', 'limit', 'skip',
  'options', 'filter', 'pipeline', 'query', 'modo', 'origen', 'databaseid',
  'target_database', 'sql',
]);

/**
 * Las etapas de un pipeline que escriben.
 *
 * `$out` y `$merge` guardan el resultado del agregado en una colección: un `aggregate`
 * con cualquiera de las dos no es una lectura por mucho que se llame aggregate. Es el
 * mismo caso que el CTE que borra en PostgreSQL —se lee como consulta y toca la base— y
 * por eso se comprueba aparte.
 */
const ETAPAS_QUE_ESCRIBEN = new Set(['$out', '$merge']);

// ─── Leer la sintaxis de la consola de Mongo ─────────────────────────────────

/** Lo que se acepta dentro de una consulta, y en qué se convierte. */
const CONVERSIONES = {
  objectid: (a) => ({ $oid: String(a[0] ?? '') }),
  isodate: (a) => ({ $date: a.length ? String(a[0]) : new Date().toISOString() }),
  date: (a) => ({ $date: a.length ? String(a[0]) : new Date().toISOString() }),
  numberint: (a) => Number(a[0] ?? 0),
  numberlong: (a) => Number(a[0] ?? 0),
  double: (a) => Number(a[0] ?? 0),
  number: (a) => Number(a[0] ?? 0),
  numberdecimal: (a) => ({ $numberDecimal: String(a[0] ?? '0') }),
  uuid: (a) => String(a[0] ?? ''),
};

const ESPACIO = /\s/;
const INICIO_NUMERO = /[-+.\d]/;
const LETRA = /[A-Za-z_$]/;
const SIGUE_IDENT = /[A-Za-z0-9_$]/;

class Lector {
  constructor(texto) {
    this.t = texto;
    this.i = 0;
  }

  get actual() { return this.t[this.i]; }

  fin() { return this.i >= this.t.length; }

  falla(que) {
    const cerca = this.t.slice(Math.max(0, this.i - 20), this.i + 20).replace(/\s+/g, ' ');
    throw new Error(`No se entiende la consulta de Mongo: ${que} (cerca de «…${cerca}…»)`);
  }

  /** Espacios y comentarios. Un `/` seguido de `*` o de `/` es comentario, no expresión. */
  saltar() {
    while (!this.fin()) {
      if (ESPACIO.test(this.actual)) { this.i += 1; continue; }
      if (this.actual === '/' && this.t[this.i + 1] === '/') {
        const fin = this.t.indexOf('\n', this.i);
        this.i = fin === -1 ? this.t.length : fin;
        continue;
      }
      if (this.actual === '/' && this.t[this.i + 1] === '*') {
        const fin = this.t.indexOf('*/', this.i + 2);
        this.i = fin === -1 ? this.t.length : fin + 2;
        continue;
      }
      return;
    }
  }

  valor() {
    this.saltar();
    if (this.fin()) this.falla('se acaba antes de tiempo');
    const c = this.actual;
    if (c === '{') return this.objeto();
    if (c === '[') return this.arreglo();
    if (c === '"' || c === "'") return this.cadena();
    if (c === '/') return this.expresionRegular();
    if (INICIO_NUMERO.test(c)) return this.numero();
    if (LETRA.test(c)) return this.palabra();
    return this.falla(`no se esperaba «${c}»`);
  }

  objeto() {
    const salida = {};
    this.i += 1; // {
    this.saltar();
    if (this.actual === '}') { this.i += 1; return salida; }
    for (;;) {
      this.saltar();
      const clave = (this.actual === '"' || this.actual === "'") ? this.cadena() : this.identificador();
      this.saltar();
      if (this.actual !== ':') this.falla(`falta «:» después de «${clave}»`);
      this.i += 1;
      salida[clave] = this.valor();
      this.saltar();
      if (this.actual === ',') { this.i += 1; this.saltar(); if (this.actual === '}') { this.i += 1; return salida; } continue; }
      if (this.actual === '}') { this.i += 1; return salida; }
      return this.falla('falta «,» o «}»');
    }
  }

  arreglo() {
    const salida = [];
    this.i += 1; // [
    this.saltar();
    if (this.actual === ']') { this.i += 1; return salida; }
    for (;;) {
      salida.push(this.valor());
      this.saltar();
      if (this.actual === ',') { this.i += 1; this.saltar(); if (this.actual === ']') { this.i += 1; return salida; } continue; }
      if (this.actual === ']') { this.i += 1; return salida; }
      return this.falla('falta «,» o «]»');
    }
  }

  /** Comilla simple o doble, las dos con `\` como escape. */
  cadena() {
    const comilla = this.actual;
    this.i += 1;
    let salida = '';
    while (!this.fin()) {
      const c = this.actual;
      if (c === '\\') {
        const siguiente = this.t[this.i + 1];
        const escapes = { n: '\n', t: '\t', r: '\r', b: '\b', f: '\f' };
        salida += escapes[siguiente] ?? siguiente ?? '';
        this.i += 2;
        continue;
      }
      if (c === comilla) { this.i += 1; return salida; }
      salida += c;
      this.i += 1;
    }
    return this.falla('una cadena se queda sin cerrar');
  }

  numero() {
    const desde = this.i;
    if (this.actual === '+' || this.actual === '-') this.i += 1;
    while (!this.fin() && /[\d.eE+-]/.test(this.actual)) {
      // Un `-` solo sigue siendo parte del número detrás de un exponente.
      if ((this.actual === '+' || this.actual === '-') && !/[eE]/.test(this.t[this.i - 1])) break;
      this.i += 1;
    }
    const crudo = this.t.slice(desde, this.i);
    const n = Number(crudo);
    if (Number.isNaN(n)) this.falla(`«${crudo}» no es un número`);
    return n;
  }

  identificador() {
    const desde = this.i;
    while (!this.fin() && SIGUE_IDENT.test(this.actual)) this.i += 1;
    if (this.i === desde) this.falla('se esperaba un nombre');
    return this.t.slice(desde, this.i);
  }

  /** `/patron/banderas` → forma canónica, que es la que entiende el driver. */
  expresionRegular() {
    this.i += 1; // /
    let patron = '';
    while (!this.fin() && this.actual !== '/') {
      if (this.actual === '\\') { patron += this.actual + (this.t[this.i + 1] ?? ''); this.i += 2; continue; }
      patron += this.actual;
      this.i += 1;
    }
    if (this.fin()) this.falla('una expresión regular se queda sin cerrar');
    this.i += 1; // /
    let banderas = '';
    while (!this.fin() && /[a-z]/.test(this.actual)) { banderas += this.actual; this.i += 1; }
    return { $regularExpression: { pattern: patron, options: banderas } };
  }

  /** `true`, `null`, `new Date(...)`, `ObjectId('…')` — todo lo que empieza por letra. */
  palabra() {
    const nombre = this.identificador();
    const bajo = nombre.toLowerCase();
    if (bajo === 'true') return true;
    if (bajo === 'false') return false;
    if (bajo === 'null') return null;
    if (bajo === 'undefined') return null;
    if (bajo === 'new') { this.saltar(); return this.palabra(); }

    this.saltar();
    if (this.actual !== '(') {
      return this.falla(`«${nombre}» suelto: ¿falta ponerlo entre comillas?`);
    }
    this.i += 1; // (
    const argumentos = [];
    this.saltar();
    if (this.actual === ')') { this.i += 1; } else {
      for (;;) {
        argumentos.push(this.valor());
        this.saltar();
        if (this.actual === ',') { this.i += 1; continue; }
        if (this.actual === ')') { this.i += 1; break; }
        return this.falla(`falta «)» en ${nombre}(…)`);
      }
    }

    const convertir = CONVERSIONES[bajo];
    // Sin conversión conocida no se adivina: una función que no se entiende podría ser
    // cualquier cosa, y pasarla tal cual al motor es lo contrario de una puerta.
    if (!convertir) this.falla(`no se reconoce «${nombre}(…)»`);
    return convertir(argumentos);
  }
}

/**
 * Convierte lo que se pegó en una estructura, aceptando la sintaxis de la consola.
 *
 * Acepta JSON estricto —que es un subconjunto— y también claves sin comillas, comillas
 * simples, comentarios, comas sobrantes y los envoltorios de tipo que escribe Mongo.
 * Un objeto ya construido pasa tal cual.
 */
export function parsearConsultaMongo(entrada) {
  if (entrada && typeof entrada === 'object') return entrada;
  const texto = String(entrada ?? '').trim();
  if (!texto) throw new Error('La consulta de Mongo está vacía.');
  const lector = new Lector(texto);
  const valor = lector.valor();
  lector.saltar();
  if (!lector.fin()) lector.falla('sobra texto al final');
  return valor;
}

/** Qué operación pide una consulta, en minúsculas, o `null` si no se puede saber. */
export function operacionDeMongo(consulta) {
  if (!consulta || typeof consulta !== 'object') return null;
  const declarada = String(consulta.operation ?? consulta.operacion ?? '').trim().toLowerCase();
  if (declarada) return declarada;
  for (const clave of Object.keys(consulta)) {
    const bajo = clave.toLowerCase();
    if (ACOMPANAN.has(bajo)) continue;
    if (LECTURAS_MONGO.has(bajo) || ESCRITURAS_MONGO.has(bajo)) return bajo;
  }
  // `{collection, filter}` sin más es una búsqueda: es como se guardaron los artículos
  // antiguos y negárselo ahora sería romperlos por una cuestión de forma.
  if (consulta.filter !== undefined || consulta.query !== undefined) return 'find';
  if (consulta.pipeline !== undefined) return 'aggregate';
  return null;
}

/**
 * Se niega si la consulta de Mongo escribe. La misma regla que para SQL, otra sintaxis.
 *
 * Esto no existía: el validador de artículos tenía un `return` temprano para Mongo con
 * el comentario «no hace falta validar el SQL», y era verdad que no hacía falta validar
 * SQL —pero sí hacía falta mirar la operación. Con eso, un artículo con `deleteMany` se
 * guardaba sin que nadie dijera nada.
 */
export function asegurarConsultaDeMongo(entrada) {
  const consulta = parsearConsultaMongo(entrada);
  const operacion = operacionDeMongo(consulta);

  if (!operacion) {
    throw new Error(
      'No se reconoce qué hace esa consulta de Mongo: escriba find, findOne, aggregate, '
      + 'countDocuments o distinct.',
    );
  }
  if (!LECTURAS_MONGO.has(operacion)) {
    const conocida = ESCRITURAS_MONGO.has(operacion);
    throw new Error(
      conocida
        ? `El sistema no ejecuta ${operacion}: un artículo solo consulta, también en Mongo. `
          + 'Use find, findOne, aggregate, countDocuments o distinct.'
        : `No se reconoce «${operacion}» como una consulta de Mongo. `
          + 'Use find, findOne, aggregate, countDocuments o distinct.',
    );
  }

  // Un agregado que termina en $out o en $merge guarda el resultado en una colección.
  // Se llama aggregate y escribe, igual que el CTE que borra se llama SELECT y borra.
  if (operacion === 'aggregate') {
    const etapas = Array.isArray(consulta.aggregate) ? consulta.aggregate
      : Array.isArray(consulta.pipeline) ? consulta.pipeline : [];
    for (const etapa of etapas) {
      for (const clave of Object.keys(etapa ?? {})) {
        if (ETAPAS_QUE_ESCRIBEN.has(clave)) {
          throw new Error(
            `Ese agregado termina en ${clave}, que guarda el resultado en una colección. `
            + 'Quítelo: un artículo solo consulta.',
          );
        }
      }
    }
  }

  return consulta;
}
