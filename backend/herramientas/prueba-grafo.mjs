/**
 * Comprueba que la exploración siga el grafo del esquema contra motores de verdad.
 *
 * Lo que se quiere saber no es si la función funciona —eso lo cubren las pruebas de
 * `soporte.test.ts` con un ejecutor falso— sino si la cadena entera se sostiene: que el
 * registro del motor introspeccione las claves foráneas de una base que acaba de ver,
 * que el puente se las pase al módulo, y que caminando por ellas se llegue a la causa
 * en una tabla que no menciona el identificador por ningún lado.
 *
 * Monta su propio escenario —tablas con prefijo `zz_grafo_`— y lo borra al terminar.
 * Solo toca lo que crea; nada con ese prefijo existe en el sistema real.
 *
 * Uso, desde `backend/`:  node herramientas/prueba-grafo.mjs
 */
import 'dotenv/config';
import pg from 'pg';
import mysql from 'mysql2/promise';
import { readFileSync } from 'node:fs';

const PREFIJO = 'zz_grafo_';
const bases = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8')).databases;

let fallos = 0;
const comprobar = (ok, texto, extra = '') => {
  console.log(`${ok ? '  ok  ' : ' FALLA'}  ${texto}${ok ? '' : ` — ${extra}`}`);
  if (!ok) fallos++;
};

/**
 * El escenario: 200 usuarios repartidos en 20 agencias, todas activas menos una.
 *
 * El usuario que se investiga no tiene nada raro en su propia fila — ahí está la
 * gracia: la única forma de encontrar el problema es saltar a su agencia.
 */
const TABLAS = {
  agencias: `${PREFIJO}agencias`,
  perfiles: `${PREFIJO}perfiles`,
  usuarios: `${PREFIJO}usuarios`,
};

const CEDULA_INVESTIGADA = '0102030405';

async function montarPostgres(cliente) {
  await cliente.query(`DROP TABLE IF EXISTS ${TABLAS.usuarios}, ${TABLAS.agencias}, ${TABLAS.perfiles} CASCADE`);
  await cliente.query(`CREATE TABLE ${TABLAS.agencias} (id int PRIMARY KEY, activa int NOT NULL, zona text NOT NULL)`);
  await cliente.query(`CREATE TABLE ${TABLAS.perfiles} (id int PRIMARY KEY, vigente int NOT NULL)`);
  await cliente.query(`CREATE TABLE ${TABLAS.usuarios} (
      id int PRIMARY KEY,
      cedula text NOT NULL,
      estado int NOT NULL,
      agencia_id int NOT NULL REFERENCES ${TABLAS.agencias}(id),
      perfil_id int NOT NULL REFERENCES ${TABLAS.perfiles}(id))`);

  for (let i = 1; i <= 20; i++) {
    await cliente.query(`INSERT INTO ${TABLAS.agencias} VALUES ($1, $2, 'norte')`, [i, i === 9 ? 0 : 1]);
  }
  await cliente.query(`INSERT INTO ${TABLAS.perfiles} VALUES (1, 1), (2, 1)`);
  for (let i = 1; i <= 200; i++) {
    // El investigado va a la agencia 9, la única inactiva. Su propia fila está sana.
    const agencia = i === 7 ? 9 : (i % 19) + 1;
    const cedula = i === 7 ? CEDULA_INVESTIGADA : String(1000000000 + i);
    await cliente.query(`INSERT INTO ${TABLAS.usuarios} VALUES ($1, $2, 1, $3, 1)`, [i, cedula, agencia]);
  }
}

async function montarMysql(conexion) {
  for (const t of [TABLAS.usuarios, TABLAS.agencias, TABLAS.perfiles]) {
    await conexion.query(`DROP TABLE IF EXISTS \`${t}\``);
  }
  await conexion.query(`CREATE TABLE \`${TABLAS.agencias}\` (id int PRIMARY KEY, activa int NOT NULL, zona varchar(20) NOT NULL)`);
  await conexion.query(`CREATE TABLE \`${TABLAS.perfiles}\` (id int PRIMARY KEY, vigente int NOT NULL)`);
  await conexion.query(`CREATE TABLE \`${TABLAS.usuarios}\` (
      id int PRIMARY KEY,
      cedula varchar(20) NOT NULL,
      estado int NOT NULL,
      agencia_id int NOT NULL,
      perfil_id int NOT NULL,
      FOREIGN KEY (agencia_id) REFERENCES \`${TABLAS.agencias}\`(id),
      FOREIGN KEY (perfil_id) REFERENCES \`${TABLAS.perfiles}\`(id))`);

  for (let i = 1; i <= 20; i++) {
    await conexion.query(`INSERT INTO \`${TABLAS.agencias}\` VALUES (?, ?, 'norte')`, [i, i === 9 ? 0 : 1]);
  }
  await conexion.query(`INSERT INTO \`${TABLAS.perfiles}\` VALUES (1, 1), (2, 1)`);
  for (let i = 1; i <= 200; i++) {
    const agencia = i === 7 ? 9 : (i % 19) + 1;
    const cedula = i === 7 ? CEDULA_INVESTIGADA : String(1000000000 + i);
    await conexion.query(`INSERT INTO \`${TABLAS.usuarios}\` VALUES (?, ?, 1, ?, 1)`, [i, cedula, agencia]);
  }
}

/** Una base montada y lista, o null si no se pudo llegar a ella. */
async function prepararEscenario(id) {
  const b = bases.find((x) => x.id === id);
  if (!b) return null;
  try {
    if (b.type === 'postgres') {
      const pool = new pg.Pool({
        host: b.host, port: b.port, database: b.database, user: b.user, password: b.password,
        connectionTimeoutMillis: 4000,
      });
      await montarPostgres(pool);
      return { tipo: 'postgres', limpiar: async () => {
        await pool.query(`DROP TABLE IF EXISTS ${TABLAS.usuarios}, ${TABLAS.agencias}, ${TABLAS.perfiles} CASCADE`);
        await pool.end();
      } };
    }
    if (b.type === 'mysql') {
      const con = await mysql.createConnection({
        host: b.host, port: b.port, database: b.database, user: b.user, password: b.password,
        connectTimeout: 4000,
      });
      await montarMysql(con);
      return { tipo: 'mysql', limpiar: async () => {
        for (const t of [TABLAS.usuarios, TABLAS.agencias, TABLAS.perfiles]) {
          await con.query(`DROP TABLE IF EXISTS \`${t}\``);
        }
        await con.end();
      } };
    }
  } catch (error) {
    console.log(`  --    ${id}: no se pudo preparar (${error.message.slice(0, 70)})`);
  }
  return null;
}

// ─── El recorrido ────────────────────────────────────────────────────────────

const { default: MultiDbRegistry } = await import('../src/infrastructure/distributed/MultiDbRegistry.js');
const { configurarMotor, esquemaDe, ejecutorPara } = await import('../src/soporte/motor-puente.ts');
const { tablasCandidatas, vecinas, explorarTabla, explorarVecindario } = await import(
  '../src/soporte/exploracion-generica.ts'
);

const montados = [];
try {
  for (const id of ['pg_main', 'mysql_test']) {
    const escenario = await prepararEscenario(id);
    if (escenario) montados.push({ id, ...escenario });
  }

  if (montados.length === 0) {
    console.log('\nNinguna base alcanzable. Nada que probar.');
    process.exitCode = 1;
  } else {
    // El registro se carga igual que en el backend: lee el mismo JSON e introspecciona.
    const registro = new MultiDbRegistry();
    await registro.introspectEmptySchemas();
    configurarMotor(registro);

    for (const base of montados) {
      console.log(`\n── ${base.id} (${base.tipo}) ─────────────────────────────`);

      const esquema = esquemaDe(base.id);
      comprobar(esquema.length > 0, 'el motor introspecciona la base', `${esquema.length} tablas`);

      const usuarios = esquema.find((t) => t.name.toLowerCase().includes(`${PREFIJO}usuarios`));
      comprobar(Boolean(usuarios), 'encuentra la tabla recién creada sin saber nada de ella');
      if (!usuarios) continue;

      comprobar(
        (usuarios.foreignKeys ?? []).length === 2,
        'lee sus claves foráneas',
        `${(usuarios.foreignKeys ?? []).length} de 2`,
      );

      const aristas = vecinas(esquema, usuarios.name);
      const padres = aristas.filter((a) => a.sentido === 'padre').map((a) => a.hacia.toLowerCase());
      comprobar(
        padres.some((p) => p.includes('agencias')) && padres.some((p) => p.includes('perfiles')),
        'arma el grafo: agencias y perfiles son sus padres',
        padres.join(', '),
      );

      const ejecutar = ejecutorPara(base.id);
      comprobar(Boolean(ejecutar), 'el puente da un ejecutor de solo lectura');
      if (!ejecutar) continue;

      const candidatas = tablasCandidatas(esquema, 'cedula', 5);
      const nuestra = candidatas.find((c) => c.tabla.toLowerCase().includes(`${PREFIJO}usuarios`));
      comprobar(Boolean(nuestra), 'deduce dónde vive una cédula sin catálogo');
      if (!nuestra) continue;

      // La fila del investigado está sana: mirándola sola no se ve nada.
      const sola = await explorarTabla(ejecutar, nuestra, CEDULA_INVESTIGADA, esquema);
      comprobar(sola === null, 'la fila por sí sola no tiene nada raro — ahí está la gracia');

      const filas = await ejecutar(
        `SELECT * FROM ${nuestra.tabla} WHERE ${nuestra.columna} = $1`,
        [CEDULA_INVESTIGADA],
      );
      comprobar(filas.length === 1, 'trae al investigado', `${filas.length} filas`);
      if (filas.length !== 1) continue;

      const raiz = {
        tabla: nuestra.tabla,
        columna: nuestra.columna,
        identificador: CEDULA_INVESTIGADA,
        diferencias: [],
        poblacion: 200,
        fila: filas[0],
        camino: [],
      };

      const vecindario = await explorarVecindario(ejecutar, raiz, esquema);
      const enAgencias = vecindario.find((h) => h.tabla.toLowerCase().includes(`${PREFIJO}agencias`));

      comprobar(Boolean(enAgencias), 'CAMINANDO EL GRAFO encuentra la causa en la agencia');
      if (enAgencias) {
        comprobar(
          enAgencias.diferencias.some((d) => d.columna.toLowerCase() === 'activa'),
          'y señala la columna exacta',
          enAgencias.diferencias.map((d) => d.columna).join(', '),
        );
        console.log(`        camino: ${enAgencias.camino.join(' / ')}`);
        const d = enAgencias.diferencias.find((x) => x.columna.toLowerCase() === 'activa');
        if (d) console.log(`        activa = ${d.valorActual}, el ${Math.round(d.cuota * 100)}% tiene ${d.valorHabitual}`);
      }

      comprobar(
        !vecindario.some((h) => h.tabla.toLowerCase().includes(`${PREFIJO}perfiles`)),
        'no inventa nada sobre el perfil, que está normal',
      );
      comprobar(!JSON.stringify(vecindario).includes('UPDATE'), 'no redacta ninguna escritura');
    }
  }
} finally {
  for (const base of montados) {
    await base.limpiar().catch(() => undefined);
  }
  await new Promise((listo) => setTimeout(listo, 150));
}

console.log(fallos === 0 ? '\nGRAFO CORRECTO' : `\n${fallos} comprobacion(es) fallaron`);
process.exitCode = fallos === 0 ? 0 : 1;
