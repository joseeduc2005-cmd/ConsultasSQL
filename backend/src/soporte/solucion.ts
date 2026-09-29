/**
 * Del ticket a la solución publicada.
 *
 * Dos piezas que trabajan juntas:
 *
 *   1. `diagnosticarTicket` — saca MAC, usuario o cédula del texto del ticket (con
 *      reglas, sin modelo de lenguaje), elige el diagnóstico que corresponde y lo
 *      ejecuta, dejando el resultado como paso del ticket.
 *
 *   2. `publicarSolucion` — convierte un diagnóstico con hallazgos en artículos del
 *      árbol de categorías: uno que cuenta el caso y uno ejecutable por cada
 *      corrección que la persona haya elegido.
 *
 * El backend no inventa SQL. Reconoce el problema entre los que los diagnósticos
 * saben comprobar y produce la corrección de su catálogo con los valores del caso.
 * Ante un caso nuevo devuelve «sin hallazgos» y sigue haciendo falta una persona.
 */
import { config } from './config.ts';
import { CORRECCIONES, type Correccion } from './correcciones.ts';
import { explorarDesdeEntrada, type PropuestaExploracion, type ResultadoExploracion } from './exploracion.ts';
import { extraerIdentificadores } from './interpreter/extraer.ts';
import { clasificarConReglas } from './interpreter/reglas.ts';
import { ejecutar, fila } from './knowledge/db.ts';
import { guardarDiagnostico } from './knowledge/diagnosticos.ts';
import { agregarPaso } from './knowledge/tickets.ts';
import { fuente, fuentePara } from './oracle/index.ts';
import { buscarPrecedentes, type Precedente } from './precedentes.ts';
import { obtenerPlaybook } from './playbooks/index.ts';
import { ejecutarPlaybook, validarEntrada } from './playbooks/motor.ts';
import type { ResultadoDiagnostico, Severidad } from './playbooks/tipos.ts';

const SALTO = String.fromCharCode(10);

/**
 * Tipo del artículo de una corrección.
 *
 * `script` enciende el botón de ejecutar del panel; `lectura` lo deja como
 * documentación con su SQL a la vista. Por defecto lo segundo: publicar no debería
 * poder escribir en Oracle hasta que alguien lo habilite a propósito.
 */
function tipoEjecutable(): 'script' | 'lectura' {
  return config.correccionesEjecutables ? 'script' : 'lectura';
}
const ETIQUETA_ESTADO = { ok: 'OK', alerta: 'REVISAR', falla: 'PROBLEMA', omitido: 'OMITIDO', error: 'ERROR' } as const;

/** Elige los datos de entrada que el diagnóstico necesita, si el texto los trae. */
function entradaDesdeTexto(playbookId: string, texto: string): Record<string, string> | null {
  const ids = extraerIdentificadores(texto);
  const disponibles: Record<string, string> = {};
  if (ids.macs[0]) disponibles.mac = ids.macs[0];
  if (ids.usuarios[0]) disponibles.usuario = ids.usuarios[0];
  if (ids.cedulas[0]) disponibles.cedula = ids.cedulas[0];
  if (ids.actas[0]) disponibles.acta = ids.actas[0];

  const playbook = obtenerPlaybook(playbookId);
  if (!playbook) return null;

  // Solo se conservan las claves que el diagnóstico declara como campos suyos.
  const admitidos = new Set(playbook.campos.map((c) => c.clave));
  const entrada = Object.fromEntries(Object.entries(disponibles).filter(([k]) => admitidos.has(k)));
  if (Object.keys(entrada).length > 0) return entrada;
  // Los que sirven sin datos corren igual: saben algo del caso aunque no haya a quién buscar.
  return playbook.sinDatos ? {} : null;
}

export interface ResultadoAutodiagnostico {
  ejecutado: boolean;
  motivo?: string;
  playbook?: string;
  diagnosticoId?: number;
  veredicto?: string;
  /** El diagnóstico completo, para mostrarlo en pantalla sin pedirlo otra vez. */
  resultado?: ResultadoDiagnostico;
  /**
   * Lo que sale de comparar al usuario con los que sí están operando. Corre siempre,
   * también —sobre todo— cuando ningún playbook reconoce el caso.
   */
  exploracion?: ResultadoExploracion;
  /**
   * Casos parecidos ya resueltos, de la base de conocimiento. Salen siempre: con
   * Oracle acompañan al diagnóstico, y sin ella son lo único que puede responder
   * — que no es poco, porque el sistema también vale para no volver a investigar
   * lo que alguien ya investigó.
   */
  precedentes?: Precedente[];
}

/** Identificadores del texto, sin filtrar por los campos de ningún playbook. */
function identificadoresDelTexto(texto: string): Record<string, string> {
  const ids = extraerIdentificadores(texto);
  return {
    ...(ids.usuarios[0] ? { usuario: ids.usuarios[0] } : {}),
    ...(ids.cedulas[0] ? { cedula: ids.cedulas[0] } : {}),
    ...(ids.macs[0] ? { mac: ids.macs[0] } : {}),
  };
}

/**
 * Diagnostica un ticket a partir de su título y descripción.
 *
 * Dos capas: primero los playbooks, que reconocen los problemas que alguien escribió
 * como reglas; después la exploración, que compara contra la población sana y sirve
 * justo para lo que las reglas no cubren. La segunda corre aunque la primera no
 * aplique, que es el caso de un problema nuevo.
 *
 * Nunca lanza: el ticket sigue existiendo igual aunque el diagnóstico falle.
 */
export async function diagnosticarTicket(ticket: {
  numero: string;
  titulo: string;
  descripcion?: string | null;
  base_datos?: string | null;
}, ejecutadoPor?: string | null): Promise<ResultadoAutodiagnostico> {
  // El ticket manda contra qué base se investiga. Si eligió una que no está marcada
  // para tickets, no se cae de otra forma más adelante: se dice aquí.
  const texto = `${ticket.titulo} ${ticket.descripcion ?? ''}`;
  const precedentes = await buscarPrecedentes(texto, { excluirTicket: ticket.numero });

  const suFuente = fuentePara(ticket.base_datos);
  if (!suFuente) {
    // Se distinguen los dos motivos: no es lo mismo que falte configurar la conexión
    // que haber elegido una base que no sirve para esto.
    const motivo = ticket.base_datos
      ? `La base «${ticket.base_datos}» no está habilitada para investigar tickets. Revise la marca «soporte.tickets» en multidb.databases.json.`
      : precedentes.length > 0
        ? 'Sin base conectada no se pudo comprobar el caso en Oracle, pero hay casos parecidos ya resueltos.'
        : 'Todavía no hay ninguna base configurada para investigar, y no se encontró ningún caso parecido en la base de conocimiento.';
    return { ejecutado: false, motivo, precedentes };
  }

  const identificadores = identificadoresDelTexto(texto);
  const tieneDatos = Object.keys(identificadores).length > 0;
  const exploracion = tieneDatos
    ? await explorarDesdeEntrada(suFuente, identificadores)
    : { explorado: false, motivo: 'El ticket no trae MAC, usuario ni cédula con los que investigar.' };

  try {
    // Cuando la vía de los playbooks no llega a ninguna parte pero la exploración sí
    // encontró algo, eso se guarda como diagnóstico: es el escenario del problema
    // nuevo, y sin número no habría a qué colgar la publicación.
    const sinPlaybook = async (motivo: string): Promise<ResultadoAutodiagnostico> =>
      exploracion.propuesta
        ? await registrarExploracion(ticket, identificadores, exploracion, suFuente, ejecutadoPor)
        : { ejecutado: false, motivo, exploracion, precedentes };

    const clasificacion = clasificarConReglas(texto);
    if (!clasificacion.playbook) return await sinPlaybook('El texto no apunta a ningún diagnóstico conocido.');

    const playbook = obtenerPlaybook(clasificacion.playbook.id);
    if (!playbook) return await sinPlaybook('Diagnóstico no encontrado.');

    const entrada = entradaDesdeTexto(playbook.id, texto);
    if (!entrada) {
      return await sinPlaybook('El ticket no trae MAC, usuario ni cédula con los que investigar.');
    }

    const validada = validarEntrada(playbook, entrada);
    const resultado = await ejecutarPlaybook(playbook, validada, suFuente);
    const id = await guardarDiagnostico(resultado, ticket.numero, ejecutadoPor);

    await agregarPaso(ticket.numero, {
      titulo: `Diagnóstico: ${resultado.nombrePlaybook}`,
      resultado: resultado.pasos.map((p) => `[${ETIQUETA_ESTADO[p.estado]}] ${p.titulo}: ${p.detalle}`).join(SALTO),
      conclusion: resultado.resumen,
      evidencia: `Datos detectados: ${Object.entries(validada).map(([k, v]) => `${k}=${v}`).join(', ')}`,
      origen: 'diagnostico',
      diagnostico_id: id,
    });

    return {
      ejecutado: true,
      playbook: playbook.id,
      diagnosticoId: id,
      veredicto: resultado.veredicto,
      resultado: { ...resultado, id, ticket: ticket.numero },
      exploracion,
      precedentes,
    };
  } catch (error) {
    return { ejecutado: false, motivo: (error as Error).message, exploracion, precedentes };
  }
}

/**
 * Guarda como diagnóstico lo que encontró la exploración cuando ningún playbook
 * reconoció el caso.
 *
 * Sin esto el problema nuevo se quedaba en pantalla y sin número: no había
 * diagnóstico al que colgar la publicación, que es justo lo que hace falta para que
 * la corrección termine siendo un artículo de categorías.
 */
async function registrarExploracion(
  ticket: { numero: string },
  entrada: Record<string, string>,
  exploracion: ResultadoExploracion,
  fuenteUsada: { modo: 'demo' | 'oracle' },
  ejecutadoPor?: string | null,
): Promise<ResultadoAutodiagnostico> {
  const p = exploracion.propuesta!;
  const inicio = new Date().toISOString();
  const detalle = p.anomalias
    .map((a) => `${a.columna}: tiene ${a.valorActual}, y el ${Math.round(a.cuota * 100)} % de los que operan tiene ${a.valorSano}.`)
    .join(SALTO);

  const resultado: ResultadoDiagnostico = {
    playbook: 'exploracion',
    nombrePlaybook: 'Exploración de datos',
    entrada,
    veredicto: 'problema',
    resumen: p.resumen,
    pasos: [],
    hallazgos: [
      {
        severidad: 'media',
        titulo: 'Diferencias con los usuarios que están operando',
        detalle: `${detalle}${SALTO}${SALTO}Comparado contra ${p.poblacion} usuarios con transacciones en los últimos ${p.dias} días. Ningún diagnóstico del catálogo reconoció este caso.`,
      },
    ],
    origenDatos: fuenteUsada.modo,
    inicio,
    duracionMs: 0,
  };

  const id = await guardarDiagnostico(resultado, ticket.numero, ejecutadoPor);
  await agregarPaso(ticket.numero, {
    titulo: 'Exploración: diferencias con los usuarios que operan',
    resultado: detalle,
    conclusion: p.resumen,
    evidencia: `Sin diagnóstico conocido para este caso. Comparado contra ${p.poblacion} usuarios activos.`,
    origen: 'diagnostico',
    diagnostico_id: id,
  });

  return {
    ejecutado: true,
    playbook: 'exploracion',
    diagnosticoId: id,
    veredicto: 'problema',
    resultado: { ...resultado, id, ticket: ticket.numero },
    exploracion,
  };
}

/**
 * Publica la solución que escribió una persona en el ticket, como artículo.
 *
 * Es la otra mitad del ciclo. Los diagnósticos publican lo que encontró el sistema;
 * esto publica lo que resolvió alguien: el caso llega, se investiga a mano, se escribe
 * la causa y la solución, y desde ahí pasa a la base de conocimiento. La próxima vez
 * que entre un ticket parecido, la búsqueda de precedentes lo encuentra.
 *
 * Sin esto el conocimiento se quedaba encerrado en el ticket: visible si alguien lo
 * abría, invisible para el que llega con el mismo problema dos meses después.
 */
export async function publicarSolucionDeTicket(
  ticket: {
    numero: string;
    titulo: string;
    descripcion?: string | null;
    causa_raiz?: string | null;
    solucion?: string | null;
    sistema?: string | null;
    modulo?: string | null;
    base_datos?: string | null;
  },
  opciones: { autorId: string; autorNombre: string; sql?: string | null; dinamico?: boolean },
): Promise<{ id: string; titulo: string; creado: boolean; ejecutable: boolean }> {
  const causa = ticket.causa_raiz?.trim();
  const solucion = ticket.solucion?.trim();
  if (!causa && !solucion) {
    throw new Error('El ticket no tiene causa raíz ni solución escritas todavía.');
  }

  const sql = opciones.sql?.trim() || null;
  const titulo = `${ticket.sistema || 'Soporte'} — ${ticket.titulo}`;
  const lineas = [
    `# ${ticket.titulo}`,
    '',
    `Caso del ticket **${ticket.numero}**.`,
    '',
  ];

  if (ticket.descripcion?.trim()) {
    lineas.push('## Qué reportaron', '', ticket.descripcion.trim(), '');
  }
  if (causa) lineas.push('## Causa raíz', '', causa, '');
  if (solucion) lineas.push('## Solución aplicada', '', solucion, '');
  if (sql) lineas.push('## Sentencia', '', '```sql', sql, '```', '');

  lineas.push('---', '', `Documentado por **${opciones.autorNombre}** desde el ticket ${ticket.numero}.`);

  const dinamico = opciones.dinamico !== false;
  const guardado = await guardarArticulo({
    autorId: opciones.autorId,
    autorNombre: opciones.autorNombre,
    titulo,
    contenido: lineas.filter((l) => l !== '').join(SALTO),
    descripcion: (causa ?? solucion ?? '').slice(0, 300),
    categoria: ticket.sistema || 'Soporte',
    subcategoria: ticket.modulo || 'Casos resueltos',
    tipo: sql ? tipoEjecutable() : 'lectura',
    script: sql ? { sql, databaseId: ticket.base_datos ?? undefined } : null,
    campos: sql && dinamico ? camposDeMarcadores(sql) : null,
    etiquetas: ['solucion', 'ticket', 'soporte', ticket.numero].filter(Boolean),
  });

  return { ...guardado, titulo, ejecutable: Boolean(sql) && tipoEjecutable() === 'script' };
}

// ─── Correcciones que propone un diagnóstico ─────────────────────────────────

export interface CorreccionDisponible {
  id: string;
  titulo: string;
  /** Texto del hallazgo, con los valores del caso. */
  descripcion: string;
  hallazgo: string;
  severidad: Severidad;
  parametros: Record<string, string | number | null>;
}

/**
 * Correcciones de escritura que el diagnóstico propone, sin repetir.
 *
 * El orden es el de los hallazgos, que el motor ya deja de mayor a menor severidad,
 * así que la primera de la lista es la del problema más grave.
 */
export function correccionesDelDiagnostico(r: ResultadoDiagnostico): CorreccionDisponible[] {
  const vistas = new Set<string>();
  const salida: CorreccionDisponible[] = [];

  for (const h of r.hallazgos) {
    for (const a of h.acciones ?? []) {
      if (a.riesgo !== 'escritura' || !a.correccion || vistas.has(a.correccion)) continue;
      const correccion = catalogo(a.correccion);
      if (!correccion) continue;
      vistas.add(correccion.id);
      salida.push({
        id: correccion.id,
        titulo: correccion.titulo,
        descripcion: a.descripcion,
        hallazgo: h.titulo,
        severidad: h.severidad,
        parametros: a.parametros ?? {},
      });
    }
  }
  return salida;
}

function catalogo(id: string): Correccion | undefined {
  return Object.values(CORRECCIONES).find((c) => c.id === id);
}

/** «Corresponsales — Resetear el PIN» → «Resetear el PIN». */
function sinPrefijo(titulo: string): string {
  const partes = titulo.split(' — ');
  return partes.length > 1 ? partes.slice(1).join(' — ') : titulo;
}

// ─── Publicar la solución como artículos ─────────────────────────────────────

interface ArticuloNuevo {
  titulo: string;
  contenido: string;
  descripcion: string;
  categoria: string;
  subcategoria: string;
  tipo: 'script' | 'lectura';
  script?: { sql: string; databaseId?: string } | null;
  campos?: unknown[] | null;
  etiquetas: string[];
  autorId: string;
  autorNombre: string;
}

/**
 * Guarda un artículo reconociéndolo por su título: volver a publicar el mismo caso
 * lo actualiza en vez de duplicarlo.
 */
/**
 * Deja constancia en la bitácora de artículos del panel.
 *
 * El módulo escribe en `knowledge_base` por su cuenta, sin pasar por los endpoints de
 * artículos, así que sus publicaciones no aparecían en `article_changes` y quedaban
 * fuera de la auditoría. En un piloto eso es justo lo que hay que poder rastrear:
 * qué propuso el sistema y quién lo aceptó.
 *
 * No lanza: una solución publicada no debe perderse porque falle su bitácora.
 */
async function auditarArticulo(
  a: ArticuloNuevo,
  id: string,
  creado: boolean,
): Promise<void> {
  try {
    await ejecutar(
      `INSERT INTO article_changes (article_id, article_titulo, accion, realizado_por, realizado_rol, detalle)
       VALUES ($1, $2, $3, $4, 'admin', $5)`,
      [
        id,
        a.titulo,
        creado ? 'crear' : 'actualizar',
        a.autorNombre,
        JSON.stringify({
          origen: 'centro-de-soporte',
          tipo: a.tipo,
          categoria: a.categoria,
          subcategoria: a.subcategoria,
          ejecutable: a.tipo === 'script',
          etiquetas: a.etiquetas,
        }),
      ],
    );
  } catch {
    // La bitácora es un extra; el artículo ya está guardado.
  }
}

/**
 * Da al script la forma que espera el editor de artículos del panel.
 *
 * El módulo guardaba `{ sql, databaseId }`, que basta para ejecutarlo pero no pasa la
 * validación del editor: exige `modo` y `origen`. El resultado era un artículo que se
 * ejecuta pero no se puede volver a guardar desde la pantalla. Se escribe en el
 * formato del sistema y deja de ser un caso aparte.
 */
function comoLoEsperaElPanel(script: { sql: string; databaseId?: string }) {
  return { modo: 'script', origen: 'manual-sql', ...script };
}

async function guardarArticulo(a: ArticuloNuevo): Promise<{ id: string; creado: boolean }> {
  const script = a.script ? JSON.stringify(comoLoEsperaElPanel(a.script)) : null;
  const campos = a.campos ? JSON.stringify(a.campos) : null;
  const existente = await fila<{ id: string }>('SELECT id FROM knowledge_base WHERE titulo = $1 LIMIT 1', [a.titulo]);

  if (existente) {
    await ejecutar(
      `UPDATE knowledge_base
       SET contenido = $2, contenido_md = $2, descripcion = $3, categoria = $4, subcategoria = $5,
           tipo_solucion = $6, script_json = $7, campos_formulario = $8,
           visible_para = 'admin', actualizado = now(), actualizado_por = $9
       WHERE id = $1`,
      [existente.id, a.contenido, a.descripcion, a.categoria, a.subcategoria, a.tipo, script, campos, a.autorNombre],
    );
    await auditarArticulo(a, existente.id, false);
    return { id: existente.id, creado: false };
  }

  const creado = await fila<{ id: string }>(
    `INSERT INTO knowledge_base
       (titulo, contenido, contenido_md, descripcion, categoria, subcategoria, tipo_solucion,
        script_json, campos_formulario, visible_para, creado_por, creado_por_username, tags)
     VALUES ($1, $2, $2, $3, $4, $5, $6, $7, $8, 'admin', $9, $10, $11)
     RETURNING id`,
    [a.titulo, a.contenido, a.descripcion, a.categoria, a.subcategoria, a.tipo, script, campos,
     a.autorId, a.autorNombre, a.etiquetas],
  );
  await auditarArticulo(a, creado!.id, true);
  return { id: creado!.id, creado: true };
}

/** Artículo ejecutable de una corrección, con los valores del caso como sugerencia. */
function contenidoDeCorreccion(c: Correccion, d: CorreccionDisponible, numeroTicket: string | null): string {
  const valores = c.campos
    .map((campo) => `- \`${campo.nombre}\` = **${d.parametros[campo.nombre] ?? '(sin valor)'}** — ${campo.etiqueta}`)
    .join(SALTO);

  return [
    `# ${sinPrefijo(c.titulo)}`,
    '',
    numeroTicket
      ? `Corrección del ticket **${numeroTicket}**, hallazgo _${d.hallazgo}_.`
      : `Hallazgo: _${d.hallazgo}_.`,
    '',
    d.descripcion,
    '',
    c.advertencia ? `> **Atención.** ${c.advertencia}` : '',
    '',
    '## Valores del caso',
    '',
    valores,
    '',
    'El formulario viene con estos valores como sugerencia. Cámbielos si va a aplicar la corrección a otro caso.',
    '',
    c.biblioteca ? `Proviene de la consulta **${c.biblioteca}** de la Biblioteca SQL.` : '',
    `Responsable de la ejecución: **${c.responsable}**.`,
  ]
    .filter((l) => l !== '')
    .join(SALTO);
}

/**
 * Artículo de una corrección que salió de la exploración, no del catálogo aprobado.
 *
 * Se marca como tal a propósito: nadie revisó esta sentencia antes de que el sistema
 * la redactara, y quien la vaya a ejecutar tiene que saberlo.
 */
function contenidoDeExploracion(p: PropuestaExploracion, numeroTicket: string | null): string {
  const filas = p.anomalias.map(
    (a) => `| ${a.columna} | ${a.valorActual} | ${a.valorSano} | ${Math.round(a.cuota * 100)} % |`,
  );

  return [
    '# Corrección propuesta por exploración',
    '',
    numeroTicket ? `Caso del ticket **${numeroTicket}**.` : '',
    '',
    `> **Esta sentencia no viene del catálogo de correcciones aprobadas.** La redactó el sistema comparando ` +
      `este usuario contra los ${p.poblacion} que sí operaron en los últimos ${p.dias} días. Confírmela antes de aplicarla.`,
    '',
    p.resumen,
    '',
    '## En qué se diferencia de los que operan',
    '',
    '| Columna | Tiene | Lo normal | Cuota de la población |',
    '| --- | --- | --- | --- |',
    ...filas,
    '',
    `Confianza: **${Math.round(p.confianza * 100)} %** — es la cuota más floja de las columnas incluidas.`,
    '',
    '## Sentencia',
    '',
    'Una sola, porque todas las columnas viven en la misma fila. Afecta a un único registro, por clave primaria.',
    '',
    '```sql',
    p.plantilla,
    '```',
  ]
    .filter((l) => l !== '')
    .join(SALTO);
}

/**
 * Campos del formulario deducidos de los `:marcadores` del SQL.
 *
 * Si quien sube el script escribe `WHERE USR_CODIGO = :usr_codigo`, el artículo sale
 * con ese campo y sirve para el próximo caso. Sin marcadores queda sin formulario,
 * que es lo correcto para un script de un solo uso.
 */
function camposDeMarcadores(sql: string) {
  const vistos = new Set<string>();
  const campos: { tipo: string; nombre: string; etiqueta: string; requerido: boolean; placeholder: string }[] = [];
  for (const m of sql.matchAll(/:([a-z_][a-z0-9_]*)/gi)) {
    const nombre = m[1].toLowerCase();
    if (vistos.has(nombre)) continue;
    vistos.add(nombre);
    campos.push({ tipo: 'text', nombre, etiqueta: nombre.replace(/_/g, ' '), requerido: true, placeholder: '' });
  }
  return campos.length > 0 ? campos : null;
}

/** Artículo de una solución que trajo una persona, no el sistema. */
function contenidoPropio(
  nombre: string,
  sql: string | null,
  pasos: string | null,
  numeroTicket: string | null,
  autor: string,
): string {
  const lineas = [
    `# ${nombre}`,
    '',
    numeroTicket ? `Caso del ticket **${numeroTicket}**.` : '',
    '',
    `Aportado por **${autor}**: ninguna de las correcciones que propuso el sistema resolvía este caso.`,
    '',
  ];

  if (pasos) {
    lineas.push('## Pasos', '');
    // Cada línea de la caja es un paso; se numeran para que se lean como receta.
    const enumerados = pasos
      .split(SALTO)
      .map((l) => l.trim())
      .filter(Boolean);
    enumerados.forEach((paso, i) => lineas.push(`${i + 1}. ${paso.replace(/^\d+[.)]\s*/, '')}`));
    lineas.push('');
  }

  if (sql) lineas.push('## Sentencia', '', '```sql', sql, '```');

  return lineas.filter((l) => l !== '').join(SALTO);
}

/** Artículo que cuenta el caso: qué se revisó, qué se encontró y qué correcciones salieron. */
function contenidoDeSolucion(
  r: ResultadoDiagnostico,
  numeroTicket: string | null,
  publicadas: { titulo: string; id: string }[],
): string {
  const lineas: string[] = [`# ${r.nombrePlaybook}`, ''];

  if (numeroTicket) lineas.push(`Caso del ticket **${numeroTicket}**.`, '');
  lineas.push(`**${r.resumen}**`, '');
  lineas.push(`Datos del caso: ${Object.entries(r.entrada).map(([k, v]) => `\`${k} = ${v}\``).join(', ')}.`, '');

  if (publicadas.length > 0) {
    lineas.push('## Correcciones listas para ejecutar', '');
    for (const p of publicadas) lineas.push(`- [${p.titulo}](/admin?articulo=${p.id})`);
    lineas.push('');
  }

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
        const correccion = a.correccion ? catalogo(a.correccion) : undefined;
        const notas = [
          a.riesgo === 'escritura' ? 'Modifica datos' : 'Solo lectura',
          `responsable: ${a.responsable}`,
          a.biblioteca ? `Biblioteca SQL: ${a.biblioteca}` : '',
          correccion ? `plantilla reutilizable: «${correccion.titulo}»` : '',
        ].filter(Boolean);
        lineas.push(`_${notas.join(' · ')}._`, '');
      }
    }
  }

  lineas.push(
    '---',
    '',
    `Fuente de los datos: ${r.origenDatos === 'demo' ? 'demostración' : 'Oracle'}. ` +
      'Las sentencias de escritura se revisan antes de aplicarlas.',
  );
  return lineas.join(SALTO);
}

export interface SolucionPublicada {
  /** El artículo que cuenta el caso. */
  articulo: { id: string; titulo: string; creado: boolean };
  /** Un artículo ejecutable por cada corrección elegida. */
  scripts: { id: string; titulo: string; correccion: string; creado: boolean }[];
}

/**
 * Publica un diagnóstico en el árbol de categorías.
 *
 * Sale un artículo con el caso y, por cada corrección que se haya elegido, otro con
 * su script y su formulario. Uno por corrección y no todos en uno porque
 * `knowledge_base` guarda un solo script por artículo: metiéndolos juntos solo el
 * primero quedaba ejecutable y el resto se veían pero no se podían correr.
 *
 * Se guarda la **plantilla** con marcadores, no el SQL ya resuelto: así el artículo
 * sirve para el próximo caso y el valor de este queda como sugerencia del campo.
 *
 * Publicar no ejecuta nada.
 */
export async function publicarSolucion(
  r: ResultadoDiagnostico,
  opciones: {
    sistema?: string | null;
    baseDatos?: string | null;
    autorId: string;
    autorNombre: string;
    /** Ids de las correcciones elegidas. Sin indicar, se publican todas. */
    correcciones?: string[] | null;
    /** Propuesta de la exploración, si quien revisó decidió publicarla también. */
    exploracion?: PropuestaExploracion | null;
    /**
     * Solución propia: cuando ninguna de las propuestas sirve y la trae la persona.
     *
     * Puede ser SQL, una serie de pasos, o las dos cosas — hay arreglos que no son
     * una sentencia sino un procedimiento. `dinamico` decide si los `:marcadores` se
     * vuelven campos del formulario o si el script es de un solo uso.
     */
    propio?: { titulo?: string | null; sql?: string | null; pasos?: string | null; dinamico?: boolean } | null;
  },
): Promise<SolucionPublicada> {
  const numeroTicket = r.ticket ?? null;
  // El sistema sirve para cualquier aplicación: sin uno indicado, no se inventa.
  const categoria = opciones.sistema || 'Sin clasificar';
  const baseDatos = opciones.baseDatos ?? undefined;
  const autor = { autorId: opciones.autorId, autorNombre: opciones.autorNombre };

  const disponibles = correccionesDelDiagnostico(r);
  const elegidas = opciones.correcciones
    ? disponibles.filter((d) => opciones.correcciones!.includes(d.id))
    : disponibles;

  const scripts: SolucionPublicada['scripts'] = [];
  for (const d of elegidas) {
    const c = catalogo(d.id);
    if (!c) continue;
    const titulo = numeroTicket ? `Solución ${numeroTicket} · ${sinPrefijo(c.titulo)}` : c.titulo;
    const guardado = await guardarArticulo({
      ...autor,
      titulo,
      contenido: contenidoDeCorreccion(c, d, numeroTicket),
      descripcion: d.descripcion,
      categoria,
      subcategoria: 'Soluciones',
      tipo: tipoEjecutable(),
      script: { sql: c.plantilla, databaseId: baseDatos },
      campos: c.campos.map((campo) => ({
        tipo: campo.tipo,
        nombre: campo.nombre,
        etiqueta: campo.etiqueta,
        requerido: true,
        placeholder: String(d.parametros[campo.nombre] ?? campo.placeholder ?? ''),
      })),
      etiquetas: ['solucion', 'correccion', 'soporte', r.playbook, numeroTicket ?? 'sin-ticket'].filter(Boolean),
    });
    scripts.push({ id: guardado.id, titulo, correccion: c.id, creado: guardado.creado });
  }

  if (opciones.exploracion) {
    const p = opciones.exploracion;
    const titulo = numeroTicket
      ? `Solución ${numeroTicket} · Corrección propuesta por exploración`
      : 'Corrección propuesta por exploración';
    const guardado = await guardarArticulo({
      ...autor,
      titulo,
      contenido: contenidoDeExploracion(p, numeroTicket),
      descripcion: p.resumen,
      categoria,
      subcategoria: 'Soluciones',
      tipo: tipoEjecutable(),
      script: { sql: p.plantilla, databaseId: baseDatos },
      campos: [
        {
          tipo: 'number',
          nombre: p.clave.campo,
          etiqueta: p.clave.etiqueta,
          requerido: true,
          placeholder: String(p.clave.valor),
        },
      ],
      etiquetas: ['solucion', 'exploracion', 'soporte', r.playbook, numeroTicket ?? 'sin-ticket'].filter(Boolean),
    });
    scripts.push({ id: guardado.id, titulo, correccion: 'exploracion', creado: guardado.creado });
  }

  const sqlPropio = opciones.propio?.sql?.trim();
  const pasosPropios = opciones.propio?.pasos?.trim();
  if (sqlPropio || pasosPropios) {
    const nombre = opciones.propio!.titulo?.trim() || (sqlPropio ? 'Corrección propia' : 'Procedimiento manual');
    const titulo = numeroTicket ? `Solución ${numeroTicket} · ${nombre}` : nombre;
    // Sin SQL el artículo es documentación: hay soluciones que son un procedimiento
    // y no una sentencia, y forzarlas a «script» dejaría un botón que no hace nada.
    const dinamico = opciones.propio!.dinamico !== false;
    const guardado = await guardarArticulo({
      ...autor,
      titulo,
      contenido: contenidoPropio(nombre, sqlPropio ?? null, pasosPropios ?? null, numeroTicket, opciones.autorNombre),
      descripcion: nombre,
      categoria,
      subcategoria: 'Soluciones',
      tipo: sqlPropio ? tipoEjecutable() : 'lectura',
      script: sqlPropio ? { sql: sqlPropio, databaseId: baseDatos } : null,
      campos: sqlPropio && dinamico ? camposDeMarcadores(sqlPropio) : null,
      etiquetas: ['solucion', 'propia', 'soporte', numeroTicket ?? 'sin-ticket'].filter(Boolean),
    });
    scripts.push({ id: guardado.id, titulo, correccion: 'propio', creado: guardado.creado });
  }

  const tituloCaso = numeroTicket
    ? `Solución ${numeroTicket} — ${r.resumen}`
    : `Solución — ${r.nombrePlaybook}: ${r.resumen}`;
  const caso = await guardarArticulo({
    ...autor,
    titulo: tituloCaso,
    contenido: contenidoDeSolucion(r, numeroTicket, scripts),
    descripcion: r.resumen,
    categoria,
    subcategoria: 'Soluciones',
    tipo: 'lectura',
    etiquetas: ['solucion', 'soporte', r.playbook, numeroTicket ?? 'sin-ticket'].filter(Boolean),
  });

  return { articulo: { id: caso.id, titulo: tituloCaso, creado: caso.creado }, scripts };
}
