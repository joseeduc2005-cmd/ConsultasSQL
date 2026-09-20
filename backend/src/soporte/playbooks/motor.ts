import { CATALOGO, type ConsultaCatalogo } from '../oracle/catalogo.ts';
import type { FuenteDatos, Parametros } from '../oracle/fuente.ts';
import { solicitudInvalida } from '../http/errores.ts';
import type {
  ConsultaEjecutada,
  Contexto,
  ControlPaso,
  EstadoPaso,
  Hallazgo,
  PasoDiagnostico,
  Playbook,
  ResultadoDiagnostico,
  Veredicto,
} from './tipos.ts';

const PESO_SEVERIDAD = { critica: 0, alta: 1, media: 2, info: 3 } as const;

export function validarEntrada(playbook: Playbook, entradaCruda: Record<string, unknown>): Record<string, string> {
  const entrada: Record<string, string> = {};
  for (const campo of playbook.campos) {
    const valor = typeof entradaCruda[campo.clave] === 'string' ? (entradaCruda[campo.clave] as string).trim() : '';
    if (!valor) {
      if (campo.requerido) throw solicitudInvalida(`${campo.etiqueta} es obligatorio.`);
      continue;
    }
    if (campo.patron && !new RegExp(campo.patron).test(valor)) {
      throw solicitudInvalida(`${campo.etiqueta} no tiene un formato válido${campo.ejemplo ? ` (ej. ${campo.ejemplo})` : ''}.`);
    }
    entrada[campo.clave] = valor;
  }
  for (const grupo of playbook.requiereAlguno ?? []) {
    if (!grupo.some((clave) => entrada[clave])) {
      const etiquetas = grupo.map((c) => playbook.campos.find((x) => x.clave === c)?.etiqueta ?? c);
      throw solicitudInvalida(`Ingrese al menos uno de estos datos: ${etiquetas.join(', ')}.`);
    }
  }
  return entrada;
}

/** Ejecuta un playbook paso a paso, registrando cada consulta, su resultado y los hallazgos. */
export async function ejecutarPlaybook(
  playbook: Playbook,
  entrada: Record<string, string>,
  fuente: FuenteDatos,
): Promise<ResultadoDiagnostico> {
  const inicio = new Date();
  const pasos: PasoDiagnostico[] = [];
  const hallazgos: Hallazgo[] = [];

  const ctx: Contexto = {
    entrada,
    hallazgo: (h) => hallazgos.push(h),
    async paso(id, titulo, fn) {
      const paso: PasoDiagnostico = { id, titulo, estado: 'ok', detalle: '', consultas: [], duracionMs: 0 };
      pasos.push(paso);
      const marcar = (estado: EstadoPaso) => (detalle: string) => {
        paso.estado = estado;
        paso.detalle = detalle;
      };
      const control: ControlPaso = {
        ok: marcar('ok'),
        alerta: marcar('alerta'),
        falla: marcar('falla'),
        omitir: marcar('omitido'),
        async consultar(nombre, parametros: Parametros = {}) {
          const t0 = performance.now();
          const filas = await fuente.consultar(nombre, parametros);
          const definicion: ConsultaCatalogo = CATALOGO[nombre];
          const consulta: ConsultaEjecutada = {
            nombre,
            descripcion: definicion.descripcion,
            biblioteca: definicion.biblioteca,
            sql: definicion.sql,
            parametros,
            filas,
            duracionMs: Math.round(performance.now() - t0),
          };
          paso.consultas.push(consulta);
          return filas;
        },
      };
      const t0 = performance.now();
      try {
        return await fn(control);
      } catch (error) {
        paso.estado = 'error';
        paso.detalle = `No se pudo completar la verificación: ${(error as Error).message}`;
        return undefined;
      } finally {
        paso.duracionMs = Math.round(performance.now() - t0);
      }
    },
  };

  await playbook.ejecutar(ctx);

  hallazgos.sort((a, b) => PESO_SEVERIDAD[a.severidad] - PESO_SEVERIDAD[b.severidad]);
  const problemas = hallazgos.filter((h) => h.severidad !== 'info');
  const conErrores = pasos.some((p) => p.estado === 'error');
  const veredicto: Veredicto = problemas.length ? 'problema' : conErrores ? 'incompleto' : 'sin_hallazgos';
  const resumen = problemas.length
    ? problemas.length === 1
      ? problemas[0].titulo
      : `${problemas[0].titulo} (y ${problemas.length - 1} hallazgo${problemas.length > 2 ? 's' : ''} más)`
    : conErrores
      ? 'Algunas verificaciones no pudieron completarse.'
      : 'No se encontraron problemas en las verificaciones realizadas.';

  return {
    playbook: playbook.id,
    nombrePlaybook: playbook.nombre,
    entrada,
    veredicto,
    resumen,
    pasos,
    hallazgos,
    origenDatos: fuente.modo,
    inicio: inicio.toISOString(),
    duracionMs: Date.now() - inicio.getTime(),
  };
}

/** Sustituye los parámetros :nombre de una sentencia por valores literales, solo para mostrarla. */
export function sqlConValores(sql: string, valores: Record<string, string | number | null | undefined>): string {
  return sql.replace(/:([a-z_][a-z0-9_]*)/gi, (original, nombre: string) => {
    const valor = valores[nombre.toLowerCase()];
    if (valor === undefined) return original;
    if (valor === null) return 'NULL';
    return typeof valor === 'number' ? String(valor) : `'${String(valor).replace(/'/g, "''")}'`;
  });
}

export const texto = (v: unknown): string => (v === null || v === undefined ? '' : String(v));
export const numero = (v: unknown): number => Number(v ?? 0);
