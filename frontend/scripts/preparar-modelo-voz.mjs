/**
 * Deja el modelo de voz listo para los navegadores que no saben dictar.
 *
 * Chrome y Edge traen reconocimiento propio y no necesitan nada de esto. Firefox y Brave
 * no, y ninguna librería puede añadirlo sin un modelo — así que se les sirve uno que
 * corre dentro del navegador con WebAssembly. La ventaja sobre lo de Chrome es que el
 * audio no sale del equipo: Chrome transcribe mandándolo a Google.
 *
 * El modelo no va al repositorio: son 40 MB que no tienen por qué viajar en cada clon.
 * Este script lo descarga y lo deja en `public/vosk/`, que está ignorado por git. Hay
 * que correrlo una vez por entorno donde se despliegue.
 *
 *   cd frontend && node scripts/preparar-modelo-voz.mjs
 *
 * Si no se corre, el sistema no se rompe: el botón de dictar avisa de que el modelo no
 * está instalado en el servidor.
 */
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, rmSync, writeFileSync, statSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

// El modelo pequeño de español de Vosk. El grande (1,4 GB) no tiene sentido en un
// navegador; este reconoce bien dictado de oficina y pesa cuarenta veces menos.
const ORIGEN = 'https://alphacephei.com/vosk/models/vosk-model-small-es-0.42.zip';
const CARPETA_INTERNA = 'vosk-model-small-es-0.42';

const raiz = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const destino = join(raiz, 'public', 'vosk');
const salida = join(destino, 'modelo-es.tar.gz');
const temporal = join(destino, '.tmp');

if (existsSync(salida)) {
  console.log(`Ya está: ${salida} (${(statSync(salida).size / 1024 / 1024).toFixed(1)} MB)`);
  console.log('Para rehacerlo, bórrelo y vuelva a correr esto.');
  process.exit(0);
}

mkdirSync(temporal, { recursive: true });
const zip = join(temporal, 'modelo.zip');

console.log(`Descargando ${ORIGEN}`);
console.log('Son unos 40 MB; puede tardar.');
const r = await fetch(ORIGEN);
if (!r.ok) {
  console.error(`No se pudo descargar: HTTP ${r.status}`);
  process.exit(1);
}
writeFileSync(zip, Buffer.from(await r.arrayBuffer()));
console.log(`Descargado: ${(statSync(zip).size / 1024 / 1024).toFixed(1)} MB`);

/**
 * El `tar` que hay que usar.
 *
 * En Windows se apunta al de System32 a propósito: es bsdtar, que descomprime zip. Si
 * se deja el nombre suelto y la terminal es Git Bash, se coge el GNU tar de MSYS, que no
 * abre zip y además toma «C:» por un host remoto al que conectarse.
 */
const TAR = process.platform === 'win32'
  ? join(process.env.SystemRoot || String.raw`C:\Windows`, 'System32', 'tar.exe')
  : 'tar';

console.log('Descomprimiendo…');
execFileSync(TAR, ['-xf', zip, '-C', temporal], { stdio: 'inherit' });

const extraido = join(temporal, CARPETA_INTERNA);
if (!existsSync(extraido)) {
  const hay = readdirSync(temporal).join(', ');
  console.error(`El zip no trae «${CARPETA_INTERNA}». Contiene: ${hay}`);
  process.exit(1);
}

// vosk-browser pide el modelo en tar.gz; los oficiales vienen en zip.
console.log('Reempaquetando como tar.gz…');
execFileSync(TAR, ['-czf', salida, '-C', temporal, CARPETA_INTERNA], { stdio: 'inherit' });
rmSync(temporal, { recursive: true, force: true });

console.log(`\nListo: ${salida} (${(statSync(salida).size / 1024 / 1024).toFixed(1)} MB)`);
console.log('Firefox y Brave ya pueden dictar.');
