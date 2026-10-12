/**
 * Pruebas del armado de consultas relacionales.
 *
 * Lo que se comprueba no es que el SQL salga bonito: es que ningún valor acabe pegado
 * dentro de la sentencia. Este código construye lo que después se ejecuta contra una
 * base de producción, y la diferencia entre una consulta y una inyección es
 * exactamente si el valor viajó como $1 o como texto.
 *
 * Y se comprueba que el puerto del esquema se usa de verdad. Esa es la prueba que
 * faltaba: dominio/scripts/validacion.js estuvo catorce commits reventando con un
 * ReferenceError en cada comprobación de tabla permitida, y no se notó porque las
 * pruebas que había cubrían las funciones puras de ese fichero y justo no las cuatro
 * que tenían la dependencia.
 *
 *   node --test src/dominio/sql/relacional.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  asegurarTipoDeColumna,
  buildRelationalMutationQuery,
  buildRelationalSelectQuery,
  buildWriteDetailExplanation,
  normalizeStrictSqlRawPayload,
} from './relacional.js';

/** Un esquema de mentira que lo permite todo; cada prueba estrecha lo que necesita. */
const esquemaAbierto = (extra = {}) => ({
  validarTipo: () => {},
  permiteTabla: () => true,
  columnasDe: () => ['id', 'nombre', 'cedula', 'socio_id', 'saldo'],
  ...extra,
});

const SCRIPT_SELECT = {
  tabla: 'socios',
  join: [{ tabla: 'cuentas', on: { 'socios.id': 'cuentas.socio_id' } }],
  where: { 'socios.cedula': '0105609176' },
};

describe('la regla de tipos de columna', () => {
  it('deja pasar el valor del tipo que toca', () => {
    assert.doesNotThrow(() => asegurarTipoDeColumna('string', 'nombre', 'Ana'));
    assert.doesNotThrow(() => asegurarTipoDeColumna('number', 'saldo', 10));
    assert.doesNotThrow(() => asegurarTipoDeColumna('boolean', 'activo', false));
  });

  it('rechaza el que no', () => {
    assert.throws(() => asegurarTipoDeColumna('number', 'saldo', 'diez'), /saldo/);
    assert.throws(() => asegurarTipoDeColumna('boolean', 'activo', 'si'), /activo/);
    assert.throws(() => asegurarTipoDeColumna('string', 'nombre', 42), /nombre/);
  });

  it('un número que no es finito no cuela por ser number', () => {
    // typeof NaN es 'number', así que sin la comprobación de finitud pasaría.
    assert.throws(() => asegurarTipoDeColumna('number', 'saldo', NaN));
    assert.throws(() => asegurarTipoDeColumna('number', 'saldo', Infinity));
  });

  it('sin tipo conocido no valida nada', () => {
    // Caracterización, y conviene tenerla escrita: una tabla que no está en el caché
    // del esquema pasa sin comprobar. Es el comportamiento de hoy.
    assert.doesNotThrow(() => asegurarTipoDeColumna(undefined, 'x', { raro: true }));
    assert.doesNotThrow(() => asegurarTipoDeColumna('', 'x', 'lo que sea'));
  });

  it('el nulo pasa siempre: lo ausente no tiene tipo', () => {
    assert.doesNotThrow(() => asegurarTipoDeColumna('number', 'saldo', null));
    assert.doesNotThrow(() => asegurarTipoDeColumna('number', 'saldo', undefined));
  });

  it('el cero se valida; el false no es un número y se rechaza', () => {
    assert.doesNotThrow(() => asegurarTipoDeColumna('number', 'saldo', 0));
    assert.throws(() => asegurarTipoDeColumna('number', 'saldo', false));
  });
});

describe('un SELECT con uniones', () => {
  it('el valor viaja como parámetro y no dentro del SQL', () => {
    // La prueba que de verdad importa de todo este fichero.
    const r = buildRelationalSelectQuery(SCRIPT_SELECT, {}, {}, esquemaAbierto());
    assert.match(r.query, /\$1/);
    assert.deepEqual(r.whereValues, ['0105609176']);
    assert.equal(r.query.includes('0105609176'), false,
      'si el valor aparece en el texto del SQL, es una inyección esperando');
  });

  it('arma la unión por las columnas que se le dieron', () => {
    const r = buildRelationalSelectQuery(SCRIPT_SELECT, {}, {}, esquemaAbierto());
    assert.match(r.query, /FROM "socios"/);
    assert.match(r.query, /JOIN "cuentas" ON "socios"\."id" = "cuentas"\."socio_id"/);
  });

  it('cada nombre va entrecomillado, no interpolado suelto', () => {
    const r = buildRelationalSelectQuery(SCRIPT_SELECT, {}, {}, esquemaAbierto());
    assert.match(r.query, /"socios"\."cedula"/);
  });

  it('pasa cada valor por el validador del esquema, con su tabla y su columna', () => {
    const vistos = [];
    buildRelationalSelectQuery(SCRIPT_SELECT, {}, {},
      esquemaAbierto({ validarTipo: (t, c, v) => vistos.push([t, c, v]) }));
    assert.deepEqual(vistos, [['socios', 'cedula', '0105609176']]);
  });

  it('un validador que rechaza detiene el armado', () => {
    assert.throws(() => buildRelationalSelectQuery(SCRIPT_SELECT, {}, {},
      esquemaAbierto({ validarTipo: () => { throw new Error('Tipo invalido para cedula'); } })), /cedula/);
  });

  it('exige el esquema: olvidarlo revienta, no deja de validar en silencio', () => {
    // Esta es la prueba que justifica que el parámetro no tenga valor por defecto.
    // Con uno, una comprobación que corre antes de escribir en producción se apagaría
    // sin que nadie lo notara, que es el peor fallo posible.
    assert.throws(() => buildRelationalSelectQuery(SCRIPT_SELECT, {}, {}), TypeError);
  });
});

describe('comparar una columna contra otra', () => {
  // La forma real: un objeto con `op` y `field`. Sin `op` se trata como un valor
  // literal, que es el camino de la prueba anterior.
  const script = { tabla: 'socios', where: { 'socios.id': { op: '=', field: 'cuentas.socio_id' } } };

  it('pregunta al esquema si esa otra tabla está permitida', () => {
    const preguntadas = [];
    buildRelationalSelectQuery(script, {}, {},
      esquemaAbierto({ permiteTabla: (n) => { preguntadas.push(n); return true; } }));
    assert.deepEqual(preguntadas, ['cuentas']);
  });

  it('si el esquema la niega, no arma nada', () => {
    // Sin esto se podría unir con cualquier tabla de la base escribiéndola en el
    // artículo, que es exactamente lo que la lista de permitidas existe para evitar.
    assert.throws(() => buildRelationalSelectQuery(script, {}, {},
      esquemaAbierto({ permiteTabla: () => false })), /no permitida/);
  });

  it('y comprueba que la columna exista en esa tabla', () => {
    // Esta es, literalmente, la comprobación que llevaba catorce commits lanzando
    // ReferenceError en vez de validar.
    assert.throws(() => buildRelationalSelectQuery(script, {}, {},
      esquemaAbierto({ columnasDe: () => ['otra_cosa'] })), /Columna no permitida/);
  });
});

describe('una escritura relacional', () => {
  const script = { tabla: 'socios', where: { 'socios.id': 7 } };

  it('los marcadores empiezan donde se le dice', () => {
    // Importa porque una escritura une sus SET con su WHERE: si el WHERE volviera a
    // empezar en $1, pisaría los parámetros del SET y tocaría otra fila.
    const a = buildRelationalMutationQuery(script, {}, {}, 1, esquemaAbierto());
    const b = buildRelationalMutationQuery(script, {}, {}, 5, esquemaAbierto());
    assert.match(a.whereClause, /\$1\b/);
    assert.match(b.whereClause, /\$5\b/);
    assert.equal(b.whereClause.includes('$1'), false, 'con startIndex 5 no debe haber $1');
    assert.deepEqual(b.whereValues, [7]);
  });

  it('ancla la fila por ctid para no tocar más de la que toca', () => {
    // Caracterización de algo que conviene no perder de vista: la condición lleva un
    // ctid añadido. Es lo que ata la escritura a la fila exacta que se seleccionó.
    const r = buildRelationalMutationQuery(script, {}, {}, 1, esquemaAbierto());
    assert.match(r.whereClause, /ctid/);
  });

  it('también exige el esquema', () => {
    assert.throws(() => buildRelationalMutationQuery(script, {}, {}, 1), TypeError);
  });
});

describe('explicar una escritura en castellano', () => {
  it('no revienta con lo vacío', () => {
    assert.doesNotThrow(() => buildWriteDetailExplanation({}, {}, {}));
  });
});

describe('normalizar un SQL crudo estricto', () => {
  it('no revienta con lo ausente', () => {
    assert.doesNotThrow(() => normalizeStrictSqlRawPayload(null));
    assert.doesNotThrow(() => normalizeStrictSqlRawPayload(undefined));
    assert.doesNotThrow(() => normalizeStrictSqlRawPayload({}));
  });
});
