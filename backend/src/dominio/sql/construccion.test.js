/**
 * Pruebas de caracterización del armado de SQL.
 *
 * Aquí es donde una definición se convierte en una sentencia. Lo que más importa no es
 * que el texto salga bonito, sino que los valores viajen como parámetros y nunca
 * pegados dentro del SQL — que es la diferencia entre una consulta y una inyección.
 *
 *   node --test src/dominio/sql/construccion.test.js
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  applyOptionalSqlFilterTransform,
  buildManualSqlQuery,
  countPositionalSqlPlaceholders,
  getValueByPath,
  inferManualPlaceholderCast,
  isMissingParamValue,
  isWriteConfirmed,
  normalizeLimitValue,
  normalizeManualSqlParamsInput,
} from './construccion.js';

describe('contar marcadores de una sentencia', () => {
  it('cuenta los distintos, no las apariciones', () => {
    assert.equal(countPositionalSqlPlaceholders('SELECT $1, $2 FROM t WHERE a = $1'), 2);
  });

  it('una sentencia sin marcadores no tiene ninguno', () => {
    assert.equal(countPositionalSqlPlaceholders('SELECT 1'), 0);
    assert.equal(countPositionalSqlPlaceholders(''), 0);
  });
});

describe('el tipo que se le pone a un marcador', () => {
  // Estas pruebas existen porque esta funcion rompio la demostracion dos veces por el
  // mismo motivo: adivinaba el tipo mirando lo que hubiera por la zona en vez de con que
  // se compara el marcador. El sintoma siempre fue el mismo, «el operador no existe», y
  // siempre se descubrio probando a mano.
  const tipoDe = (sql, variable) => {
    const marcador = `:${variable}`;
    return inferManualPlaceholderCast(sql, variable, sql.indexOf(marcador), marcador);
  };

  it('un rango de fechas no lleva tipo: lo resuelve la base', () => {
    // Bastaba un «>=» delante para que esto dijera 'int', y entonces PostgreSQL
    // respondia «el operador no existe: timestamp >= integer». Cualquier articulo con un
    // rango de fechas estaba roto, que es lo primero que pide un reporte.
    assert.equal(tipoDe('SELECT 1 FROM t WHERE t.fecha >= :desde', 'desde'), null);
    assert.equal(tipoDe('SELECT 1 FROM t WHERE t.fecha <= :hasta', 'hasta'), null);
    assert.equal(tipoDe('SELECT 1 FROM t WHERE t.fecha BETWEEN :desde AND :hasta', 'desde'), null);
    assert.equal(tipoDe('SELECT 1 FROM a WHERE a.periodo = :periodo', 'periodo'), null);
  });

  it('una cedula detras de un JOIN tampoco: no es un identificador', () => {
    // Esto miraba ciento veinte caracteres alrededor y, si veia cualquier «algo_id =»,
    // devolvia 'uuid'. O sea que una consulta con un JOIN normal rompia con «el operador
    // no existe: character varying = uuid» — y la mayoria de los articulos llevan JOIN.
    const sql = 'SELECT 1 FROM cuentas c JOIN socios s ON s.id = c.socio_id WHERE s.cedula = :cedula';
    assert.equal(tipoDe(sql, 'cedula'), null);
  });

  it('un identificador si, por el nombre o por la columna', () => {
    assert.equal(tipoDe('SELECT 1 FROM t WHERE t.algo = :id', 'id'), 'uuid');
    assert.equal(tipoDe('SELECT 1 FROM c WHERE c.socio_id = :socio', 'socio'), 'uuid');
  });

  it('un entero cuando lo numerico esta en el nombre, la columna o un agregado', () => {
    assert.equal(tipoDe('SELECT 1 FROM a WHERE a.algo = :numero_acta', 'numero_acta'), 'int');
    assert.equal(tipoDe('SELECT 1 FROM t WHERE t.monto > :tope', 'tope'), 'int');
    assert.equal(tipoDe('SELECT 1 FROM t GROUP BY a HAVING count(*) > :veces', 'veces'), 'int');
  });

  it('texto cuando se compara con ILIKE', () => {
    assert.equal(tipoDe('SELECT 1 FROM s WHERE s.nombres ILIKE :texto', 'texto'), 'text');
  });
});

describe('se ejecuta lo que se escribio', () => {
  // Toda consulta con un parametro pasaba por una reescritura automatica que convertia
  // «campo = :x» en «(campo = :x OR :x IS NULL)». O sea que la consulta que corria no era
  // la que habia escrito nadie: con el dato vacio el filtro desaparecia y salia la tabla
  // entera. Y para que ese «$1 IS NULL» no se quedara sin tipo habia que adivinarlo por
  // el nombre de la columna, que es lo que fallaba contra cualquier base ajena.
  const armar = (sql, datos) => buildManualSqlQuery(sql, datos).text.replace(/\s+/g, ' ');

  it('un parametro se cambia por su marcador y nada mas', () => {
    assert.equal(
      armar('SELECT 1 FROM socios s WHERE s.cedula = :cedula', { cedula: '011' }),
      'SELECT 1 FROM socios s WHERE s.cedula = $1',
    );
  });

  it('el mismo parametro dos veces comparte marcador', () => {
    const r = buildManualSqlQuery(
      'SELECT 1 FROM s WHERE a = :x OR b = :x', { x: '9' },
    );
    assert.equal(r.text.replace(/\s+/g, ' '), 'SELECT 1 FROM s WHERE a = $1 OR b = $1');
    assert.deepEqual(r.values, ['9']);
  });

  it('no se castea por el nombre de la columna', () => {
    // Cualquier «algo_id» se casteaba a uuid. En la base de conocimiento los
    // identificadores son uuid y colaba; contra una base donde «socio_id» es un entero
    // fallaba con «el operador no existe: integer = uuid». El tipo lo deduce el motor de
    // la columna con la que se compara, que es quien lo sabe.
    assert.equal(
      armar('SELECT 1 FROM cuentas c WHERE c.socio_id = :socio_id', { socio_id: '4' }),
      'SELECT 1 FROM cuentas c WHERE c.socio_id = $1',
    );
  });

  it('un ILIKE conserva el patron que escribio la persona', () => {
    const r = buildManualSqlQuery('SELECT 1 FROM s WHERE nombres ILIKE :texto', { texto: '%ana%' });
    assert.equal(r.text.replace(/\s+/g, ' '), 'SELECT 1 FROM s WHERE nombres ILIKE $1');
    assert.deepEqual(r.values, ['%ana%']);
  });

  it('un parametro dentro de un parentesis llega entero', () => {
    // Este era el peor: el regex de la reescritura retrocedia ante «:cedula)» y casaba
    // «:cedul», asi que el parametro se quedaba sin valor y sobraba una letra fuera del
    // parentesis. Rompia cualquier consulta con el parametro en un CTE, una subconsulta
    // o un IN (...), que es la forma de casi todas.
    const r = buildManualSqlQuery(
      'WITH suyo AS (SELECT id FROM socios WHERE cedula = :cedula) SELECT * FROM suyo',
      { cedula: '0110000411' },
    );
    assert.equal(
      r.text.replace(/\s+/g, ' '),
      'WITH suyo AS (SELECT id FROM socios WHERE cedula = $1) SELECT * FROM suyo',
    );
    assert.deepEqual(r.values, ['0110000411']);
  });

  it('y dentro de un IN tambien', () => {
    const r = buildManualSqlQuery(
      'SELECT 1 FROM t WHERE id IN (SELECT id FROM u WHERE x = :valor)', { valor: '9' },
    );
    assert.equal(r.text.replace(/\s+/g, ' '), 'SELECT 1 FROM t WHERE id IN (SELECT id FROM u WHERE x = $1)');
    assert.deepEqual(r.values, ['9']);
  });

  it('una consulta sin parametros se ejecuta tal cual', () => {
    const sql = 'SELECT mac, count(*) FROM puntos_atencion GROUP BY mac HAVING count(*) > 1';
    const r = buildManualSqlQuery(sql, {});
    assert.equal(r.text, sql);
    assert.deepEqual(r.values, []);
  });
});

describe('un «:algo» dentro de un texto no es un parametro', () => {
  // `TO_CHAR(fecha, 'HH24:MI')` es como se escribe una hora en Oracle, y aparece en
  // cualquier informe. El `:MI` se tomaba por un parametro: el formato salia convertido
  // en `'YYYY-MM-DD HH24$1'` y se mandaba un parametro de mas con valor nulo, asi que
  // Oracle respondia «1 bind placeholders were used but 2 bind values were provided».
  const armar = (sql, datos) => buildManualSqlQuery(sql, datos);

  it('respeta un formato de fecha de Oracle', () => {
    const r = armar(
      "SELECT TO_CHAR(t.abierto_en, 'YYYY-MM-DD HH24:MI') AS abierto FROM t WHERE t.cedula = :cedula",
      { cedula: '012' },
    );
    assert.ok(r.text.includes("'YYYY-MM-DD HH24:MI'"), r.text);
    assert.deepEqual(r.values, ['012']);
    assert.deepEqual(r.placeholderOrder, ['cedula']);
  });

  it('respeta una hora literal', () => {
    const r = armar("SELECT 1 FROM t WHERE hora = '10:30' AND x = :valor", { valor: '9' });
    assert.ok(r.text.includes("'10:30'"), r.text);
    assert.deepEqual(r.values, ['9']);
  });

  it('respeta un comentario', () => {
    const r = armar(['-- ojo con :esto', 'SELECT 1 FROM t WHERE x = :valor'].join(String.fromCharCode(10)), { valor: '9' });
    assert.ok(r.text.includes(':esto'), r.text);
    assert.deepEqual(r.values, ['9']);
  });

  it('un texto con dos puntos no convierte la consulta en parametrizada', () => {
    const r = armar("SELECT 'texto con :palabra' AS a FROM t", {});
    assert.deepEqual(r.values, []);
    assert.ok(r.text.includes(':palabra'), r.text);
  });

  it('la comilla repetida no rompe el conteo', () => {
    const r = armar("SELECT 'O''Brien :x' AS a FROM t WHERE b = :real", { real: '1' });
    assert.deepEqual(r.values, ['1']);
  });
});

describe('el filtro opcional, cuando se pide', () => {
  it('deja el nombre del parametro entero aunque siga un parentesis', () => {
    assert.equal(
      applyOptionalSqlFilterTransform('WHERE (cedula = :cedula)'),
      'WHERE ((cedula = :cedula OR :cedula IS NULL))',
    );
    assert.equal(
      applyOptionalSqlFilterTransform('WHERE id IN (SELECT id FROM t WHERE x = :valor)'),
      'WHERE id IN (SELECT id FROM t WHERE (x = :valor OR :valor IS NULL))',
    );
  });

  it('la comparacion va primero, para que el motor deduzca el tipo', () => {
    // Antes se generaba «($1 IS NULL OR campo = $1)» y PostgreSQL no podia deducir el
    // tipo: lo resuelve por la primera aparicion, y en «$1 IS NULL» no hay nada de donde.
    const salida = applyOptionalSqlFilterTransform('WHERE cedula = :cedula');
    assert.ok(salida.indexOf('cedula = :cedula') < salida.indexOf(':cedula IS NULL'));
  });
});

describe('qué cuenta como valor ausente', () => {
  it('null, undefined y el texto vacío faltan', () => {
    assert.equal(isMissingParamValue(null), true);
    assert.equal(isMissingParamValue(undefined), true);
    assert.equal(isMissingParamValue(''), true);
  });

  it('el cero y el false NO faltan', () => {
    // Esta es la trampa clásica: con una comprobación de verdad/falsedad, un saldo de
    // cero o un estado «false» se tratarían como si no los hubieran mandado.
    assert.equal(isMissingParamValue(0), false);
    assert.equal(isMissingParamValue(false), false);
  });
});

describe('leer un valor por su ruta', () => {
  it('baja por los puntos', () => {
    assert.equal(getValueByPath({ a: { b: { c: 7 } } }, 'a.b.c'), 7);
  });

  it('una ruta que no existe da undefined, no revienta', () => {
    assert.equal(getValueByPath({ a: 1 }, 'a.b.c'), undefined);
    assert.equal(getValueByPath(null, 'a'), undefined);
  });
});

describe('confirmación para escribir', () => {
  it('busca una marca concreta dentro de los parámetros', () => {
    // No recibe un booleano: mira `__write_confirmed` dentro de lo que llegó. Que haya
    // que mandar esa marca, y que un valor cualquiera no valga, es lo que evita que una
    // escritura se cuele por un campo mal rellenado.
    assert.equal(isWriteConfirmed({ __write_confirmed: true }), true);
    assert.equal(isWriteConfirmed({ __write_confirmed: 'true' }), true);
    assert.equal(isWriteConfirmed({ __write_confirmed: 'si' }), false);
    assert.equal(isWriteConfirmed({ __write_confirmed: 1 }), false);
    assert.equal(isWriteConfirmed({}), false);
    assert.equal(isWriteConfirmed(undefined), false);
  });

  it('pasarle un booleano suelto no confirma nada', () => {
    // Conviene tenerlo escrito: quien la llame mal se queda sin escribir, que es el
    // lado seguro del error.
    assert.equal(isWriteConfirmed(true), false);
  });
});

describe('normalizar el límite de filas', () => {
  it('acepta un número, venga como número o como texto', () => {
    assert.equal(normalizeLimitValue(50), 50);
    assert.equal(normalizeLimitValue('50'), 50);
  });

  it('lo ausente cae al valor por defecto', () => {
    assert.equal(normalizeLimitValue(null), 0);
    assert.equal(normalizeLimitValue(undefined, 100), 100);
    assert.equal(normalizeLimitValue(''), 0);
  });

  it('lo que no es un número positivo lanza, no se cuela', () => {
    // Importa que lance en vez de caer al valor por defecto: un límite inválido suele
    // ser un error de quien llama, y taparlo devolvería un resultado que no pidió.
    assert.throws(() => normalizeLimitValue('; DROP TABLE t'), /Límite inválido/);
    assert.throws(() => normalizeLimitValue(0), /Límite inválido/);
    assert.throws(() => normalizeLimitValue(-5), /Límite inválido/);
  });
});

describe('normalizar los parámetros que llegan', () => {
  it('un objeto pasa tal cual', () => {
    const r = normalizeManualSqlParamsInput({ cedula: '0102030405' });
    assert.equal(typeof r, 'object');
  });

  it('lo ausente no revienta', () => {
    assert.doesNotThrow(() => normalizeManualSqlParamsInput(null));
    assert.doesNotThrow(() => normalizeManualSqlParamsInput(undefined));
  });
});
