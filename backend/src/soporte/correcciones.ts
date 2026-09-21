/**
 * Catálogo de correcciones: el puente entre un diagnóstico y un artículo ejecutable.
 *
 * Cada corrección se declara **una sola vez** aquí, con su plantilla SQL y los campos
 * que necesita. De esa única declaración salen dos cosas:
 *
 *   1. La acción sugerida que muestra el diagnóstico, con los valores ya resueltos.
 *   2. El artículo de `knowledge_base`, con `script_json` y `campos_formulario`, que
 *      aparece en el árbol de categorías del panel y se puede ejecutar desde ahí.
 *
 * Antes las plantillas vivían sueltas dentro de cada playbook. Sacarlas aquí evita que
 * el SQL del diagnóstico y el del artículo se separen con el tiempo.
 *
 * Todas son de escritura y ninguna se ejecuta desde el módulo de soporte: el
 * diagnóstico las redacta, y quien las aplica es una persona. Ver SOPORTE.md.
 */
import { ejecutar, fila } from './knowledge/db.ts';
import type { AccionSugerida } from './playbooks/tipos.ts';

export interface CampoCorreccion {
  nombre: string;
  etiqueta: string;
  tipo: 'text' | 'number';
  placeholder?: string;
}

export interface Correccion {
  id: string;
  /** Título del artículo en el árbol de categorías. */
  titulo: string;
  /** Texto que ve el técnico en el hallazgo del diagnóstico. */
  descripcion: string;
  categoria: string;
  subcategoria: string;
  /** Código de la Biblioteca SQL del que proviene, si lo hay. */
  biblioteca?: string;
  responsable: string;
  /** SQL con marcadores `:nombre`, sin resolver. */
  plantilla: string;
  campos: CampoCorreccion[];
  /** Advertencia que acompaña al artículo. */
  advertencia?: string;
}

const RESPONSABLE_DB = 'Administración de Aplicaciones';
const SALTO = String.fromCharCode(10);

const CAMPO_USR_CODIGO: CampoCorreccion = {
  nombre: 'usr_codigo',
  etiqueta: 'Código de usuario (USR_CODIGO)',
  tipo: 'number',
  placeholder: '3003',
};

export const CORRECCIONES: Record<string, Correccion> = {
  reactivarUsuario: {
    id: 'reactivar-usuario',
    titulo: 'Corresponsales — Reactivar un usuario desactivado',
    descripcion: 'Reactivar el usuario (confirmar antes que la desactivación no fue intencional).',
    categoria: 'Corresponsales',
    subcategoria: 'Acceso y usuarios',
    biblioteca: 'U11',
    responsable: RESPONSABLE_DB,
    plantilla: 'UPDATE CORRESPONSALES.USUARIO SET USR_ESTADO = 1 WHERE USR_CODIGO = :usr_codigo;\nCOMMIT;',
    campos: [CAMPO_USR_CODIGO],
    advertencia: 'Confirmar primero que la desactivación no fue intencional.',
  },

  resetearPin: {
    id: 'resetear-pin',
    titulo: 'Corresponsales — Resetear los intentos de PIN',
    descripcion: 'Resetear los intentos de PIN.',
    categoria: 'Corresponsales',
    subcategoria: 'Acceso y usuarios',
    biblioteca: 'U10',
    responsable: RESPONSABLE_DB,
    plantilla: 'UPDATE CORRESPONSALES.USUARIO SET USR_INTENTOS_PIN = 0 WHERE USR_CODIGO = :usr_codigo;\nCOMMIT;',
    campos: [CAMPO_USR_CODIGO],
  },

  resetearYReactivar: {
    id: 'resetear-y-reactivar',
    titulo: 'Corresponsales — Resetear intentos y reactivar por nombre de usuario',
    descripcion: 'Resetear intentos fallidos y reactivar por usuario.',
    categoria: 'Corresponsales',
    subcategoria: 'Acceso y usuarios',
    biblioteca: 'U8',
    responsable: RESPONSABLE_DB,
    plantilla:
      'UPDATE CORRESPONSALES.USUARIO\nSET USR_INTENTOS = 0, USR_ESTADO = 1, USR_INTENTOS_PIN = 0\nWHERE USR_USUARIO = :usuario;\nCOMMIT;',
    campos: [{ nombre: 'usuario', etiqueta: 'Nombre de usuario', tipo: 'text', placeholder: 'nombre.apellido' }],
  },

  alinearSucursalUsuario: {
    id: 'alinear-sucursal-usuario',
    titulo: 'Corresponsales — Alinear la sucursal del usuario con la del corresponsal',
    descripcion: 'Alinear la sucursal del usuario con la del corresponsal.',
    categoria: 'Corresponsales',
    subcategoria: 'Sucursales',
    biblioteca: 'U14',
    responsable: RESPONSABLE_DB,
    plantilla:
      'UPDATE CORRESPONSALES.USUARIO\nSET CODIGO_SUCURSAL = :codigo_sucursal\nWHERE USR_CEDULA = :cedula;\nCOMMIT;',
    campos: [
      { nombre: 'codigo_sucursal', etiqueta: 'Código de sucursal', tipo: 'number', placeholder: '12' },
      { nombre: 'cedula', etiqueta: 'Cédula del usuario', tipo: 'text', placeholder: '0102030405' },
    ],
  },

  sucursalFisja: {
    id: 'sucursal-fisja',
    titulo: 'Corresponsales — Corregir la sucursal en fisja.clientes',
    descripcion:
      'Corrección puntual (la aplicó el técnico que atendió el caso Mall Alto). No corrige la causa estructural.',
    categoria: 'Corresponsales',
    subcategoria: 'Sucursales',
    responsable: RESPONSABLE_DB,
    plantilla:
      'UPDATE fisja.clientes\nSET codigo_sucursal = :codigo_sucursal\nWHERE codigo_socio = :codigo_socio;\nCOMMIT;',
    campos: [
      { nombre: 'codigo_sucursal', etiqueta: 'Código de sucursal correcto', tipo: 'number', placeholder: '12' },
      { nombre: 'codigo_socio', etiqueta: 'Código de socio', tipo: 'number', placeholder: '100033' },
    ],
    advertencia:
      'Corrección puntual: no arregla la causa estructural (ticket 2026083744). El proceso volverá a desincronizar la sucursal hasta desplegar el fix.',
  },
};

/** Sustituye los marcadores `:nombre` por valores literales, escapando las comillas. */
export function resolverPlantilla(
  plantilla: string,
  valores: Record<string, string | number | null | undefined>,
): string {
  return plantilla.replace(/:([a-z_][a-z0-9_]*)/gi, (original, nombre: string) => {
    const valor = valores[nombre.toLowerCase()];
    if (valor === undefined) return original;
    if (valor === null) return 'NULL';
    return typeof valor === 'number' ? String(valor) : `'${String(valor).replace(/'/g, "''")}'`;
  });
}

/**
 * Convierte una corrección del catálogo en la acción que muestra el diagnóstico.
 *
 * Además del SQL resuelto conserva la plantilla y los valores: es lo que permite
 * enlazar con el artículo ejecutable y llevar el formulario ya rellenado.
 */
export function accionDe(
  correccion: Correccion,
  valores: Record<string, string | number | null | undefined>,
): AccionSugerida {
  return {
    descripcion: correccion.descripcion,
    riesgo: 'escritura',
    responsable: correccion.responsable,
    biblioteca: correccion.biblioteca,
    sql: resolverPlantilla(correccion.plantilla, valores),
    correccion: correccion.id,
    parametros: Object.fromEntries(
      correccion.campos.map((c) => [c.nombre, valores[c.nombre] ?? null]),
    ) as Record<string, string | number | null>,
  };
}

// ─── Artículos ejecutables ───────────────────────────────────────────────────

/**
 * Publica cada corrección como artículo de `knowledge_base`, con su `script_json` y
 * su `campos_formulario`, de modo que aparezca en el árbol de categorías del panel
 * y pueda ejecutarse desde ahí con los parámetros que pida el caso.
 *
 * Es idempotente: se reconoce por el título, y se actualiza en vez de duplicar.
 *
 * El artículo queda marcado como escritura y con la advertencia visible. Publicarlo
 * no lo ejecuta: sigue haciendo falta que una persona lo corra.
 */
export async function publicarCorrecciones(opciones: {
  /** Base registrada contra la que se ejecutarían (id de multidb.databases.json). */
  databaseId: string | null;
  /** Usuario al que se atribuye el artículo. */
  autorId: string;
  autorNombre: string;
  /**
   * Con `false` (el valor por defecto) el artículo se publica como documentación:
   * el script y los campos quedan guardados, pero el panel no muestra el botón de
   * ejecutar. Encenderlo es una decisión aparte — hay que confirmar que el usuario
   * de Oracle del motor tenga permiso de escritura y que el equipo acepte que la
   * corrección se aplique desde aquí en vez de desde su cliente.
   */
  ejecutable?: boolean;
}): Promise<{ publicadas: number; actualizadas: number; creadas: number; ejecutables: boolean }> {
  let creadas = 0;
  let actualizadas = 0;
  const tipo = opciones.ejecutable ? 'script' : 'lectura';

  for (const correccion of Object.values(CORRECCIONES)) {
    const contenido = [
      `# ${correccion.titulo}`,
      '',
      correccion.descripcion,
      '',
      correccion.advertencia ? `> **Atención.** ${correccion.advertencia}` : '',
      '',
      '## Sentencia',
      '',
      '```sql',
      correccion.plantilla,
      '```',
      '',
      correccion.biblioteca ? `Proviene de la consulta **${correccion.biblioteca}** de la Biblioteca SQL.` : '',
      `Responsable de la ejecución: **${correccion.responsable}**.`,
      opciones.ejecutable
        ? ''
        : '> Publicado como documentación: el script queda guardado pero no se ejecuta desde el panel.',
      '',
      '_Generado desde el catálogo de correcciones del Centro de Soporte._',
    ]
      .filter((l) => l !== '')
      .join(SALTO);

    const existente = await fila<{ id: string }>(
      'SELECT id FROM knowledge_base WHERE titulo = $1 LIMIT 1',
      [correccion.titulo],
    );

    const campos = correccion.campos.map((c) => ({
      tipo: c.tipo,
      nombre: c.nombre,
      etiqueta: c.etiqueta,
      requerido: true,
      placeholder: c.placeholder ?? '',
    }));
    const script = { sql: correccion.plantilla, databaseId: databaseIdDe(opciones.databaseId) };

    if (existente) {
      await ejecutar(
        `UPDATE knowledge_base
         SET contenido = $2, contenido_md = $2, descripcion = $3, categoria = $4, subcategoria = $5,
             tipo_solucion = $9, campos_formulario = $6, script_json = $7,
             visible_para = 'admin', actualizado = now(), actualizado_por = $8
         WHERE id = $1`,
        [existente.id, contenido, correccion.descripcion, correccion.categoria, correccion.subcategoria,
         JSON.stringify(campos), JSON.stringify(script), opciones.autorNombre, tipo],
      );
      actualizadas += 1;
    } else {
      await ejecutar(
        `INSERT INTO knowledge_base
           (titulo, contenido, contenido_md, descripcion, categoria, subcategoria, tipo_solucion,
            campos_formulario, script_json, visible_para, creado_por, creado_por_username, tags)
         VALUES ($1, $2, $2, $3, $4, $5, $11, $6, $7, 'admin', $8, $9, $10)`,
        [correccion.titulo, contenido, correccion.descripcion, correccion.categoria, correccion.subcategoria,
         JSON.stringify(campos), JSON.stringify(script), opciones.autorId, opciones.autorNombre,
         ['correccion', 'soporte', correccion.biblioteca ?? 'sin-biblioteca'], tipo],
      );
      creadas += 1;
    }
  }

  return { publicadas: creadas + actualizadas, creadas, actualizadas, ejecutables: opciones.ejecutable === true };
}

/** Sin base indicada el artículo queda sin destino y el panel pide elegirla al ejecutar. */
function databaseIdDe(id: string | null): string | undefined {
  return id ?? undefined;
}
