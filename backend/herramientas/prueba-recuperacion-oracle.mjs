// La fuente tiene que recuperarse SIN reiniciar el proceso: es el caso real.
import 'dotenv/config';
import { execSync } from 'node:child_process';

const { fuente } = await import('./src/soporte/oracle/index.ts');
const sonda = async (etiqueta) => {
  const t = Date.now();
  try {
    const r = await fuente.consultar('usuarioPorUsuario', { usuario: 'demo.duplicado' });
    console.log(etiqueta.padEnd(30), 'OK   ', r.length, 'fila ·', Date.now() - t, 'ms');
    return true;
  } catch (e) {
    console.log(etiqueta.padEnd(30), 'FALLA', Date.now() - t, 'ms ·', e.message.split('\n')[0].slice(0, 60));
    return false;
  }
};

await sonda('1. antes del reinicio');

console.log('\n   reiniciando el contenedor (mismo proceso node vivo)...');
execSync('docker restart oracle-pruebas', { stdio: 'ignore' });
await sonda('2. justo despues');

console.log('   esperando a que Oracle abra...');
for (let i = 0; i < 40; i += 1) {
  await new Promise((r) => setTimeout(r, 8000));
  const logs = execSync('docker logs --tail 12 oracle-pruebas 2>&1', { encoding: 'utf8' });
  if (logs.includes('DATABASE IS READY TO USE')) {
    console.log('   Oracle abierto\n');
    break;
  }
}

const ok = await sonda('3. despues, sin reiniciar node');
console.log(ok ? '\nSE RECUPERA SOLA' : '\nNO SE RECUPERA');
await fuente.cerrar();
process.exit(ok ? 0 : 1);
