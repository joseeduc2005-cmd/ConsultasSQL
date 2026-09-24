/**
 * Reproduce el fallo de login tras recargar la config multibase en caliente.
 *
 * `reloadActiveDatabaseConnection` reemplaza el pool y cierra el anterior. El router
 * de autenticación recibía el pool por valor, así que se quedaba con el cerrado y
 * todo login posterior devolvía un 500 genérico hasta reiniciar el backend: editar
 * `multidb.databases.json` con el sistema arriba dejaba a todos fuera.
 *
 * Aquí se compara el patrón viejo (pool por valor) con el nuevo (proxy al vigente),
 * sin levantar el backend entero.
 *
 * Uso, desde `backend/`:  node herramientas/prueba-recarga-login.mjs
 */
import pg from 'pg';
import { readFileSync } from 'node:fs';

const b = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8')).databases.find((x) => x.id === 'pg_main');
const credenciales = { host: b.host, port: b.port, database: b.database, user: b.user, password: b.password };

let pool = new pg.Pool(credenciales);

// Así lo recibía el router antes: una referencia al pool de ese momento.
const porValor = pool;
// Así lo recibe ahora: siempre el vigente.
const porProxy = { query: (...args) => pool.query(...args) };

const login = async (quien, cliente) => {
  try {
    const r = await cliente.query('SELECT id FROM users WHERE username = $1', ['admin']);
    console.log(`${quien.padEnd(26)} OK    ${r.rowCount} fila`);
    return true;
  } catch (e) {
    console.log(`${quien.padEnd(26)} FALLA ${e.message.slice(0, 54)}`);
    return false;
  }
};

console.log('Antes de recargar');
await login('  pool por valor', porValor);
await login('  proxy al vigente', porProxy);

// Lo que hace reloadActiveDatabaseConnection: pool nuevo, y se cierra el anterior.
console.log('\n   recargando la config multibase...');
const anterior = pool;
pool = new pg.Pool(credenciales);
await anterior.end();

console.log('\nDespues de recargar');
const viejo = await login('  pool por valor', porValor);
const nuevo = await login('  proxy al vigente', porProxy);

await pool.end();
console.log(
  !viejo && nuevo
    ? '\nEl fallo se reproduce con el patron viejo, y el proxy lo evita.'
    : '\nRevisar: el resultado no es el esperado.',
);
process.exit(!viejo && nuevo ? 0 : 1);
