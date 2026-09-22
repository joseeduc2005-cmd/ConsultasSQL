/**
 * Del ticket a la solución publicada.
 *
 * Dos piezas que trabajan juntas:
 *
 *   1. `diagnosticarTicket` — al crear un ticket, saca MAC, usuario o cédula de su
 *      texto (con reglas, sin modelo de lenguaje), elige el diagnóstico que
 *      corresponde y lo ejecuta, dejando el resultado como primer paso.
 *
 *   2. `publicarSolucion` — convierte un diagnóstico con hallazgos en un artículo del
 *      árbol de categorías: qué se revisó, qué se encontró y el SQL ya resuelto.
 *
 * El backend no inventa SQL. Reconoce el problema entre los que los diagnósticos
 * saben comprobar y produce la corrección de su catálogo con los valores del caso.
 * Ante un caso nuevo devuelve «sin hallazgos» y sigue haciendo falta una persona.
 */
import { CORRECCIONES } from './correcciones.ts';
import { extraerIdentificadores } from './interpreter/extraer.ts';
import { clasificarConReglas } from './interpreter/reglas.ts';
import { ejecutar, fila } from './knowledge/db.ts';
import { guardarDiagnostico } from './knowledge/diagnosticos.ts';
import { agregarPaso } from './knowledge/tickets.ts';
import { fuente } from './oracle/index.ts';
import { obtenerPlaybook } from './playbooks/index.ts';
import { ejecutarPlaybook, validarEntrada } from './playbooks/motor.ts';
import type { ResultadoDiagnostico } from './playbooks/tipos.ts';

const SALTO = String.fromCharCode(10);
const ETIQUETA_ESTADO = { ok: 'OK', alerta: 'REVISAR', falla: 'PROBLEMA', omitido: 'OMITIDO', error: 'ERROR' } as const;

/** Elige los datos de entrada que el diagnóstico necesita, si el texto los trae. */
function entradaDesdeTexto(playbookId: string, texto: string): Record<string, string> | null {
  const ids = extraerIdentificadores(texto);
  const disponibles: Record<string, string> = {};
  if (ids.macs[0]) disponibles.mac = ids.macs[0];
  if (ids.usuarios[0]) disponibles.usuario = ids.usuarios[0];
  if (ids.cedulas[0]) disponibles.cedula = ids.cedulas[0];

  const playbook = obtenerPlaybook(playbookId);
  if (!playbook) return null;

  // Solo se conservan las claves que el diagnóstico declara como campos suyos.
  const admitidos = new Set(playbook.campos.map((c) => c.clave));
  const entrada = Object.fromEntries(Object.entries(disponibles).filter(([k]) => admitidos.has(k)));
  return Object.keys(entrada).length > 0 ? entrada : null;
}

export interface ResultadoAutodiagnostico {
  ejecutado: boolean;
  motivo?: string;
  playbook?: string;
  diagnosticoId?: number;
  veredicto?: string;
  /** El diagnóstico completo, para mostrarlo en pantalla sin pedirlo otra vez. */
  resultado?: ResultadoDiagnostico;
}

/**
 * Diagnostica un ticket recién creado a partir de su título y descripción.
 *
 * Nunca lanza: un ticket se crea igual aunque el diagnóstico falle o no aplique.
 */
export async function diagnosticarTicket(ticket: {
  numero: string;
  titulo: string;
  descripcion?: string | null;
}): Promise<ResultadoAutodiagnostico> {
  try {
    const texto = `${ticket.titulo} ${ticket.descripcion ?? ''}`;
    const clasificacion = clasificarConReglas(texto);
    if (!clasificacion.playbook) return { ejecutado: false, motivo: 'El texto no apunta a ningún diagnóstico conocido.' };

    const playbook = obtenerPlaybook(clasificacion.playbook.id);
    if (!playbook) return { ejecutado: false, motivo: 'Diagnóstico no encontrado.' };

    const entrada = entradaDesdeTexto(playbook.id, texto);
    if (!entrada) {
      return { ejecutado: false, motivo: 'El ticket no trae MAC, usuario ni cédula con los que investigar.' };
    }

    const validada = validarEntrada(playbook, entrada);
    const resultado = await ejecutarPlaybook(playbook, validada, fuente);
    const id = await guardarDiagnostico(resultado, ticket.numero);

    await agregarPaso(ticket.numero, {
      titulo: `Diagnóstico automático: ${resultado.nombrePlaybook}`,
      resultado: resultado.pasos.map((p) => `[${ETIQUETA_ESTADO[p.estado]}] ${p.titulo}: ${p.detalle}`).join(SALTO),
      conclusion: resultado.resumen,
      evidencia: `Ejecutado al registrar el ticket · datos detectados: ${Object.entries(validada).map(([k, v]) => `${k}=${v}`).join(', ')}`,
      origen: 'diagnostico',
      diagnostico_id: id,
    });

    return {
      ejecutado: true,
      playbook: playbook.id,
      diagnosticoId: id,
      veredicto: resultado.veredicto,
      resultado: { ...resultado, id, ticket: ticket.numero },
    };
  } catch (error) {
    return { ejecutado: false, motivo: (error as Error).message };
  }
}

// ─── Publicar la solución como artículo ──────────────────────────────────────

function contenidoDeSolucion(r: ResultadoDiagnostico, numeroTicket: string | null): string {
  const lineas: string[] = [`# ${r.nombrePlaybook}`, ''];

  if (numeroTicket) lineas.push(`Caso del ticket **${numeroTicket}**.`, '');
  lineas.push(`**${r.resumen}**`, '');
  lineas.push(`Datos del caso: ${Object.entries(r.entrada).map(([k, v]) => `\`${k} = ${v}\``).join(', ')}.`, '');

  lineas.push('## Qué se revisó', '');
  for (const paso of r.pasos) lineas.push(`- **${ETIQUETA_ESTADO[paso.estado]}** · ${paso.titulo}: ${paso.detalle}`);
  lineas.push('');

  if (r.hallazgos.length > 0) {
    lineas.push('## Qué se encontró', '');
    for (const h of r.hallazgos) {
      lineas.push(`### ${h.titulo}`, '', `Severidad: **${h.severidad}**. ${h.detalle}`, '');
      for (const a of h.acciones ?? []) {
        lineas.push(`**Corrección propuesta:** ${a.descripcion}`, '');
        if (a.sql) lineas.push('```sql', a.sql, '```', '');
        const notas = [
          a.riesgo === 'escritura' ? 'Modifica datos' : 'Solo lectura',
          `responsable: ${a.responsable}`,
          a.biblioteca ? `Biblioteca SQL: ${a.biblioteca}` : '',
          a.correccion && CORRECCIONES[Object.keys(CORRECCIONES).find((k) => CORRECCIONES[k].id === a.correccion) ?? '']
            ? `plantilla reutilizable: «${CORRECCIONES[Object.keys(CORRECCIONES).find((k) => CORRECCIONES[k].id === a.correccion)!].titulo}»`
            : '',
        ].filter(Boolean);
        lineas.push(`_${notas.join(' · ')}._`, '');
      }
    }
  }

  lineas.push(
    '---',
    '',
    `Fuente de los datos: ${r.origenDatos === 'demo' ? 'demostración' : 'Oracle'}. ` +
      'Las sentencias de escritura las ejecuta una persona; el sistema no las aplica.',
  );
  return lineas.join(SALTO);
}

/**
 * Publica un diagnóstico como artículo del árbol de categorías.
 *
 * Es a petición, no automático: un diagnóstico sin hallazgos no merece artículo, y
 * publicar todos llenaría el árbol. Reconoce el artículo por su título, así que
 * volver a publicar el mismo caso lo actualiza en vez de duplicarlo.
 */
export async function publicarSolucion(
  r: ResultadoDiagnostico,
  opciones: { sistema?: string | null; autorId: string; autorNombre: string },
): Promise<{ id: string; titulo: string; creado: boolean }> {
  const numeroTicket = r.ticket ?? null;
  const titulo = numeroTicket
    ? `Solución ${numeroTicket} — ${r.resumen}`
    : `Solución — ${r.nombrePlaybook}: ${r.resumen}`;

  const contenido = contenidoDeSolucion(r, numeroTicket);
  const categoria = opciones.sistema || 'Corresponsales';
  const existente = await fila<{ id: string }>('SELECT id FROM knowledge_base WHERE titulo = $1 LIMIT 1', [titulo]);

  if (existente) {
    await ejecutar(
      `UPDATE knowledge_base
       SET contenido = $2, contenido_md = $2, descripcion = $3, categoria = $4, subcategoria = 'Soluciones',
           tipo_solucion = 'lectura', visible_para = 'admin', actualizado = now(), actualizado_por = $5
       WHERE id = $1`,
      [existente.id, contenido, r.resumen, categoria, opciones.autorNombre],
    );
    return { id: existente.id, titulo, creado: false };
  }

  const creado = await fila<{ id: string }>(
    `INSERT INTO knowledge_base
       (titulo, contenido, contenido_md, descripcion, categoria, subcategoria, tipo_solucion,
        visible_para, creado_por, creado_por_username, tags)
     VALUES ($1, $2, $2, $3, $4, 'Soluciones', 'lectura', 'admin', $5, $6, $7)
     RETURNING id`,
    [
      titulo,
      contenido,
      r.resumen,
      categoria,
      opciones.autorId,
      opciones.autorNombre,
      ['solucion', 'soporte', r.playbook, numeroTicket ?? 'sin-ticket'].filter(Boolean),
    ],
  );
  return { id: creado!.id, titulo, creado: true };
}
