/**
 * Evalúa el reconocimiento de tickets contra los casos REALES del equipo.
 *
 * Sin acceso a la Oracle de producción no se puede medir si los diagnósticos aciertan
 * la causa, pero sí las dos etapas que van antes y que fallan en silencio:
 *
 *   1. ¿El texto del ticket cae en el playbook que le toca? (`clasificarConReglas`)
 *   2. ¿Se le saca el usuario, la cédula o la MAC con que investigar? (`extraerIdentificadores`)
 *
 * Si cualquiera de las dos falla, el ticket muere en «sin hallazgos» por más fino
 * que esté el diagnóstico. Y son las dos que se calibraron leyendo tres tickets a
 * mano, así que son las que más sospecha merecen.
 *
 * Uso, desde `backend/`:
 *   node herramientas/evaluar-tickets-reales.mjs <ruta/tickets.txt>
 *
 * El archivo es el que genera el dashboard del Centro de Conocimiento: bloques
 * separados por "====" con líneas CLAVE: valor.
 */
import { readFileSync } from 'node:fs';

const { clasificarConReglas } = await import('../src/soporte/interpreter/reglas.ts');
const { extraerIdentificadores } = await import('../src/soporte/interpreter/extraer.ts');

const ruta = process.argv[2];
if (!ruta) {
  console.error('Falta la ruta a tickets.txt');
  process.exit(1);
}

function parsear(texto) {
  return texto
    .split(/^={5,}$/m)
    .map((bloque) => {
      const campos = {};
      const pasos = [];
      for (const linea of bloque.split(/\r?\n/)) {
        const m = linea.match(/^([A-Z_]+):\s*(.*)$/);
        if (!m) continue;
        if (m[1] === 'PASO') pasos.push(m[2]);
        else campos[m[1]] = m[2];
      }
      campos.PASOS = pasos;
      return campos;
    })
    .filter((t) => t.TICKET);
}

const tickets = parsear(readFileSync(ruta, 'utf8'));
console.log(`Tickets reales: ${tickets.length}\n`);

let clasificados = 0;
let conDatos = 0;
let completos = 0;

for (const t of tickets) {
  // Lo que ve el sistema al llegar el ticket: solo título y descripción. La causa
  // raíz y los pasos son el resultado de la investigación, no la entrada.
  const texto = `${t.TITULO ?? ''} ${t.DESCRIPCION ?? ''}`;
  const c = clasificarConReglas(texto);
  const ids = extraerIdentificadores(texto);

  const playbook = c.playbook?.id ?? null;
  const datos = [
    ids.usuarios[0] ? `usuario=${ids.usuarios[0]}` : '',
    ids.cedulas[0] ? `cedula=${ids.cedulas[0]}` : '',
    ids.macs[0] ? `mac=${ids.macs[0]}` : '',
  ].filter(Boolean);

  if (playbook) clasificados += 1;
  if (datos.length > 0) conDatos += 1;
  if (playbook && datos.length > 0) completos += 1;

  const marca = playbook && datos.length ? 'OK  ' : playbook ? 'SIN DATOS' : 'SIN PLAYBOOK';
  console.log(`${marca.padEnd(13)} ${String(t.TICKET).padEnd(24)} ${playbook ?? '-'}`);
  console.log(`              ${(t.TITULO ?? '').slice(0, 78)}`);
  console.log(`              ${datos.join('  ') || '(no se extrajo ningún identificador)'}\n`);
}

const pct = (n) => `${((n / tickets.length) * 100).toFixed(0)} %`;
console.log('-'.repeat(72));
console.log(`Clasificados        : ${clasificados}/${tickets.length}  ${pct(clasificados)}`);
console.log(`Con datos extraídos : ${conDatos}/${tickets.length}  ${pct(conDatos)}`);
console.log(`Investigables       : ${completos}/${tickets.length}  ${pct(completos)}   <- los que llegarían a consultar Oracle`);
