/**
 * Datos de prueba en Mongo y en MySQL, para comprobar que el sistema los consulta.
 *
 * En Mongo va una colección de conciliaciones con la forma real: documentos con
 * `ObjectId`, fechas, números y —lo importante— un arreglo anidado de detalles. Ese
 * arreglo es el que obliga a `$unwind`, y es donde se ve si el motor aguanta un agregado
 * de verdad o solo un `find` con dos campos.
 *
 * En MySQL va algo distinto a propósito: cortes de caja por agencia, para probar su
 * dialecto —funciones de ventana, `GROUP_CONCAT`, `DATE_FORMAT`— que no es el de
 * PostgreSQL ni el de Oracle.
 *
 *   node herramientas/demo-mongo-mysql.mjs
 *   node herramientas/demo-mongo-mysql.mjs --borrar
 */
import { MongoClient, ObjectId } from 'mongodb';
import mysql from 'mysql2/promise';
import { readFileSync } from 'node:fs';

const BORRAR = process.argv.includes('--borrar');
const cfg = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8'));
const mg = cfg.databases.find((d) => d.id === 'mongo_local');
const my = cfg.databases.find((d) => d.id === 'mysql_test');

// ── Mongo: conciliaciones con detalle anidado ─────────────────────────────────
const cliente = new MongoClient(mg.connectionString);
await cliente.connect();
const dbm = cliente.db(mg.database);

if (BORRAR) {
  await dbm.collection('conciliaciones').drop().catch(() => undefined);
  console.log('Mongo: colección conciliaciones eliminada.');
} else {
  await dbm.collection('conciliaciones').drop().catch(() => undefined);

  const OBSERVACIONES = [
    'OK',
    'No existe la transacción en la institución',
    'Sin coincidencia, no existe registro en el SWITCH',
    'Valor distinto entre SWITCH e institución',
  ];
  const documentos = [];
  for (let d = 0; d < 20; d += 1) {
    const corte = new Date(Date.UTC(2026, 8, 1 + d, 10, 3, 32, 7));
    const detalles = [];
    const cuantos = 2 + (d % 4);
    for (let k = 0; k < cuantos; k += 1) {
      const conciliado = (d + k) % 3 !== 0;
      detalles.push({
        id: k + 1,
        state: conciliado ? 1 : 0,
        observation: conciliado ? OBSERVACIONES[0] : OBSERVACIONES[1 + ((d + k) % 3)],
        reconciled: conciliado ? 1 : 0,
        uuid: `b1dec13b-b321-44e9-80a3-90b2d9ec${String(1000 + d * 7 + k).slice(0, 4)}`,
        transactionDateSwitch: new Date(corte.getTime() - (k + 1) * 36e5),
        transactionValueSwitch: ((d * 13 + k * 7) % 240) + 5,
        movementCodeSwitch: String(15624691 + d * 31 + k),
        transactionTypeSwitch: k % 2 === 0 ? 'C' : 'D',
        institutionCodeSwitch: 4,
        serviceCodeSwitch: 1,
        transactionStatusSwitch: 1,
        transactionDateInstitution: conciliado ? new Date(corte.getTime() - (k + 1) * 36e5) : null,
        transactionValueInstitution: conciliado ? ((d * 13 + k * 7) % 240) + 5 : 0,
        movementCodeInstitution: conciliado ? String(15624691 + d * 31 + k) : '',
        transactionTypeInstitution: conciliado ? (k % 2 === 0 ? 'C' : 'D') : '',
        serviceCodInstitution: 'QR',
        transactionStatusInstitution: conciliado ? 1 : 0,
        editDate: corte,
      });
    }
    const ok = detalles.filter((x) => x.reconciled === 1).length;
    documentos.push({
      _id: new ObjectId(),
      institutionCode: 4,
      cutOffDate: corte.toISOString().slice(0, 10),
      conciliationDate: corte,
      transactionCount: detalles.length,
      transactionOk: ok,
      transactionError: detalles.length - ok,
      pending: detalles.length - ok,
      reconciled: ok,
      debitTotal: Number(detalles.filter((x) => x.transactionTypeSwitch === 'D')
        .reduce((s, x) => s + x.transactionValueSwitch, 0).toFixed(2)),
      creditTotal: detalles.filter((x) => x.transactionTypeSwitch === 'C')
        .reduce((s, x) => s + x.transactionValueSwitch, 0),
      creationDate: corte,
      creationUser: '',
      editDate: corte,
      editUser: '',
      statusConciliation: ok === detalles.length ? 1 : 0,
      details: detalles,
    });
  }
  await dbm.collection('conciliaciones').insertMany(documentos);
  const n = await dbm.collection('conciliaciones').countDocuments();
  const d = await dbm.collection('conciliaciones').aggregate([
    { $unwind: '$details' }, { $count: 'n' },
  ]).toArray();
  console.log(`Mongo: ${n} conciliaciones con ${d[0]?.n ?? 0} detalles anidados.`);
}
await cliente.close();

// ── MySQL: cortes de caja por agencia ─────────────────────────────────────────
const con = await mysql.createConnection({
  host: my.host, port: my.port, user: my.user, password: my.password, database: my.database,
  multipleStatements: true,
});

if (BORRAR) {
  await con.query('DROP TABLE IF EXISTS cortes_caja, cajeros, agencias');
  console.log('MySQL: tablas eliminadas.');
} else {
  await con.query('DROP TABLE IF EXISTS cortes_caja, cajeros, agencias');
  await con.query(`
    CREATE TABLE agencias (
      id INT PRIMARY KEY, nombre VARCHAR(60) NOT NULL, zona VARCHAR(40) NOT NULL
    ) ENGINE=InnoDB;
    CREATE TABLE cajeros (
      id INT PRIMARY KEY, agencia_id INT NOT NULL, nombre VARCHAR(80) NOT NULL,
      activo TINYINT(1) NOT NULL DEFAULT 1,
      FOREIGN KEY (agencia_id) REFERENCES agencias(id)
    ) ENGINE=InnoDB;
    CREATE TABLE cortes_caja (
      id INT PRIMARY KEY AUTO_INCREMENT, cajero_id INT NOT NULL, fecha DATE NOT NULL,
      apertura DECIMAL(12,2) NOT NULL, ingresos DECIMAL(12,2) NOT NULL,
      egresos DECIMAL(12,2) NOT NULL, cierre DECIMAL(12,2) NOT NULL,
      diferencia DECIMAL(12,2) NOT NULL, observacion VARCHAR(160),
      FOREIGN KEY (cajero_id) REFERENCES cajeros(id),
      INDEX idx_fecha (fecha)
    ) ENGINE=InnoDB;
  `);

  const AGENCIAS = [
    [1, 'Matriz Cuenca', 'Zonal Cuenca'], [2, 'Agencia Gualaceo', 'Zonal Cuenca'],
    [3, 'Agencia Azogues', 'Zonal Azogues'], [4, 'Agencia Paute', 'Zonal Cuenca'],
    [5, 'Agencia Sígsig', 'Zonal Azogues'],
  ];
  await con.query('INSERT INTO agencias (id, nombre, zona) VALUES ?', [AGENCIAS]);

  const NOMBRES = ['Lucía Quizhpe', 'Marco Tenesaca', 'Daniela Pauta', 'Fabián Chuchuca',
    'Verónica Morocho', 'Esteban Peñaloza', 'Paola Yupangui', 'Andrés Lema',
    'Gabriela Narváez', 'Nicolás Cabrera'];
  const cajeros = NOMBRES.map((n, i) => [i + 1, (i % 5) + 1, n, i === 7 ? 0 : 1]);
  await con.query('INSERT INTO cajeros (id, agencia_id, nombre, activo) VALUES ?', [cajeros]);

  const cortes = [];
  for (let d = 0; d < 60; d += 1) {
    const fecha = new Date(Date.now() - d * 864e5).toISOString().slice(0, 10);
    for (const [id] of cajeros.map((c) => [c[0]])) {
      const apertura = 200 + ((d * 17 + id * 29) % 800);
      const ingresos = 500 + ((d * 211 + id * 97) % 9000);
      const egresos = 300 + ((d * 173 + id * 61) % 7000);
      // Un descuadre de vez en cuando: sin eso no hay nada que buscar.
      const descuadre = (d * 7 + id) % 23 === 0 ? (((d + id) % 2 ? 1 : -1) * ((d * 3) % 95 + 5)) / 10 : 0;
      const cierre = apertura + ingresos - egresos + descuadre;
      cortes.push([id, fecha, apertura.toFixed(2), ingresos.toFixed(2), egresos.toFixed(2),
        cierre.toFixed(2), descuadre.toFixed(2),
        descuadre === 0 ? null : (descuadre > 0 ? 'Sobrante en caja' : 'Faltante en caja')]);
    }
  }
  await con.query(
    'INSERT INTO cortes_caja (cajero_id, fecha, apertura, ingresos, egresos, cierre, diferencia, observacion) VALUES ?',
    [cortes],
  );
  const [[{ n }]] = await con.query('SELECT count(*) AS n FROM cortes_caja');
  const [[{ d }]] = await con.query('SELECT count(*) AS d FROM cortes_caja WHERE diferencia <> 0');
  console.log(`MySQL: ${n} cortes de caja, ${d} con descuadre.`);
}
await con.end();
