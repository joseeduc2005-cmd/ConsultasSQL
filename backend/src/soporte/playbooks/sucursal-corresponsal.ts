import { CAMPO_CEDULA } from './campos.ts';
import { numero, sqlConValores, texto } from './motor.ts';
import type { Playbook } from './tipos.ts';

const FUENTE_CR = 'corresponsales.corresponsalrepresentante';
const FUENTE_PERSONA = 'jst_personas.vwt_cliente';
const FUENTE_FISJA = 'fisja.clientes';

/**
 * Compara la sucursal del corresponsal en cada fuente.
 * Contexto (ticket 2026083744): el cambio de oficina oficial actualiza
 * corresponsalrepresentante y jst_personas.solicitud_ingreso, pero NO fisja.clientes,
 * que es de donde Corresponsales toma la sucursal para contabilizar las transacciones.
 */
export const sucursalCorresponsal: Playbook = {
  id: 'sucursal-corresponsal',
  nombre: 'Sucursal del corresponsal',
  descripcion:
    'Compara la sucursal del corresponsal en Corresponsales, JST_PERSONAS y FISJA para detectar descuadres contables tras un cambio de oficina.',
  sistema: 'Corresponsales',
  categoria: 'Contabilidad',
  senales: [/sucursal/i, /oficina/i, /agencia/i, /descuadre/i, /contabiliza/i, /jasit/i, /cambio de (oficina|agencia)/i, /comisi/i, /cuentas por cobrar/i],
  ticketsReferencia: ['2026083744'],
  campos: [
    { ...CAMPO_CEDULA, etiqueta: 'Cédula del corresponsal' },
    { clave: 'codigo_socio', etiqueta: 'Código de socio', tipo: 'texto', patron: '^\\d{1,10}$', ejemplo: '382019' },
  ],
  requiereAlguno: [['cedula', 'codigo_socio']],

  async ejecutar(ctx) {
    const { cedula } = ctx.entrada;
    let codigoSocio = ctx.entrada.codigo_socio;

    if (!codigoSocio && cedula) {
      codigoSocio =
        (await ctx.paso('socio', 'Ubicar el código de socio', async (p) => {
          const filas = await p.consultar('representantePorCedula', { cedula });
          if (filas.length === 0) {
            p.falla('La cédula no corresponde a ningún corresponsal registrado.');
            ctx.hallazgo({
              severidad: 'alta',
              titulo: 'No es corresponsal',
              detalle: 'La cédula no existe en corresponsalrepresentante. Si es un socio cliente, la sucursal se toma de JST_PERSONAS.',
            });
            return undefined;
          }
          if (filas.length > 1) p.alerta(`La cédula tiene ${filas.length} registros de corresponsal; se usa el activo más reciente.`);
          else p.ok(`Código de socio ${texto(filas[0].cr_cod_socio)}.`);
          return texto(filas[0].cr_cod_socio);
        })) ?? '';
    }
    if (!codigoSocio) return;

    const fuentes =
      (await ctx.paso('fuentes', 'Sucursal según cada fuente', async (p) => {
        const filas = await p.consultar('sucursalesDeSocio', { codigo_socio: Number(codigoSocio) });
        p.ok(filas.map((f) => `${texto(f.fuente)}: ${texto(f.codigo_sucursal)} ${texto(f.nombre_sucursal)}`.trim()).join(' · ') || 'Sin datos.');
        return filas;
      })) ?? [];

    const de = (fuente: string) => fuentes.filter((f) => f.fuente === fuente);
    const cr = de(FUENTE_CR);
    const persona = de(FUENTE_PERSONA)[0];
    const fisja = de(FUENTE_FISJA)[0];
    const nombre = (f?: Record<string, unknown>) => (f ? `${texto(f.codigo_sucursal)} (${texto(f.nombre_sucursal) || 'sin nombre'})` : 'sin registro');

    await ctx.paso('comparar', 'Comparar fuentes', async (p) => {
      const sucursalesCr = [...new Set(cr.map((f) => numero(f.codigo_sucursal)))];
      if (sucursalesCr.length > 1) {
        ctx.hallazgo({
          severidad: 'alta',
          titulo: 'El corresponsal tiene registros con sucursales distintas',
          detalle: `corresponsalrepresentante devuelve las sucursales ${sucursalesCr.join(', ')}. La consulta del backend toma solo una (ROWNUM = 1).`,
          ticketsRelacionados: ['2026083744'],
        });
      }

      const actual = cr[0];
      if (actual && persona && numero(actual.codigo_sucursal) !== numero(persona.codigo_sucursal)) {
        ctx.hallazgo({
          severidad: 'critica',
          titulo: 'Sucursal distinta entre Corresponsales y JST_PERSONAS',
          detalle: `Corresponsales: ${nombre(actual)} · JST_PERSONAS: ${nombre(persona)}. El proceso oficial de cambio de oficina actualiza ambas; uno de los dos caminos no se ejecutó.`,
          ticketsRelacionados: ['2026083744'],
        });
      }

      if (actual && fisja && numero(actual.codigo_sucursal) !== numero(fisja.codigo_sucursal)) {
        ctx.hallazgo({
          severidad: 'alta',
          titulo: 'fisja.clientes conserva la sucursal anterior',
          detalle: `Corresponsales: ${nombre(actual)} · fisja.clientes: ${nombre(fisja)}. Mientras no se despliegue el fix del ticket 2026083744, las transacciones de este corresponsal se contabilizan en la oficina anterior y generan descuadre en JASIT.`,
          ticketsRelacionados: ['2026083744'],
          acciones: [
            {
              descripcion: 'Corrección puntual (la aplicó el técnico que atendió el caso Mall Alto). No corrige la causa estructural.',
              riesgo: 'escritura',
              responsable: 'Administración de Aplicaciones',
              sql: sqlConValores('UPDATE fisja.clientes\nSET codigo_sucursal = :codigo_sucursal\nWHERE codigo_socio = :codigo_socio;\nCOMMIT;', {
                codigo_sucursal: numero(actual.codigo_sucursal),
                codigo_socio: Number(codigoSocio),
              }),
            },
          ],
        });
      }

      if (!actual) p.alerta('El socio no tiene registro de corresponsal; solo se muestran las demás fuentes.');
      else if (!persona || !fisja) p.alerta('Alguna fuente no devolvió datos para este socio.');
      else p.ok('Comparación completa.');
    });

    if (cedula) {
      await ctx.paso('usuario', 'Sucursal del usuario del sistema', async (p) => {
        const usuarios = await p.consultar('usuarioPorCedula', { cedula });
        const actual = cr[0];
        const distintos = usuarios.filter((u) => actual && numero(u.codigo_sucursal) !== numero(actual.codigo_sucursal));
        if (usuarios.length === 0) return p.omitir('La cédula no tiene usuario en el sistema de Corresponsales.');
        if (distintos.length) {
          p.alerta(`${distintos.length} usuario(s) con sucursal distinta a la del corresponsal.`);
          ctx.hallazgo({
            severidad: 'media',
            titulo: 'Usuario con sucursal desactualizada',
            detalle: distintos.map((u) => `${texto(u.usr_usuario)}: sucursal ${texto(u.codigo_sucursal)}`).join('\n'),
            acciones: [
              {
                descripcion: 'Alinear la sucursal del usuario con la del corresponsal.',
                riesgo: 'escritura',
                responsable: 'Administración de Aplicaciones',
                biblioteca: 'U14',
                sql: sqlConValores('UPDATE CORRESPONSALES.USUARIO\nSET CODIGO_SUCURSAL = :codigo_sucursal\nWHERE USR_CEDULA = :cedula;\nCOMMIT;', {
                  codigo_sucursal: numero(actual!.codigo_sucursal),
                  cedula,
                }),
              },
            ],
          });
        } else {
          p.ok('La sucursal del usuario coincide con la del corresponsal.');
        }
      });
    }
  },
};
