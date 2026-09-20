import { config } from '../config.ts';
import type { Fila } from '../oracle/fuente.ts';
import { CAMPO_CEDULA, CAMPO_MAC, CAMPO_USUARIO } from './campos.ts';
import { numero, sqlConValores, texto } from './motor.ts';
import type { Contexto, Playbook } from './tipos.ts';

const RESPONSABLE_DB = 'Administración de Aplicaciones';

/**
 * "El corresponsal no puede ingresar / resetear clave".
 * Reglas tomadas de los tickets 2026083700, SIN-NUMERO-1105681702 y 2026085232.
 */
export const accesoCorresponsal: Playbook = {
  id: 'acceso-corresponsal',
  nombre: 'Acceso del corresponsal',
  descripcion:
    'Revisa por qué un corresponsal no puede ingresar o resetear su clave: estado del usuario, intentos fallidos, claves temporales, MAC duplicada, vínculo usuario-POS y sucursal.',
  sistema: 'Corresponsales',
  categoria: 'Autenticación',
  senales: [
    /no (le )?(permite|deja|puede) (ingresar|entrar|acceder|loguear)/i,
    /(ingres|acce|login|inicio de sesi)/i,
    /\bpin\b/i,
    /(contrase|clave)/i,
    /bloquead/i,
    /usuario no encontrado/i,
    /unexpected error/i,
    /intentos/i,
  ],
  ticketsReferencia: ['2026083700', 'SIN-NUMERO-1105681702', '2026085232'],
  campos: [CAMPO_MAC, CAMPO_USUARIO, CAMPO_CEDULA],
  requiereAlguno: [['mac', 'usuario', 'cedula']],

  async ejecutar(ctx: Contexto) {
    const { mac, usuario, cedula } = ctx.entrada;

    // 1. Identificar al usuario.
    const usuarios =
      (await ctx.paso('identificar', 'Identificar al usuario', async (p) => {
        const filas = usuario
          ? await p.consultar('usuarioPorUsuario', { usuario })
          : cedula
            ? await p.consultar('usuarioPorCedula', { cedula })
            : await p.consultar('usuariosPorMac', { mac });

        if (filas.length === 0) {
          p.falla(
            usuario || cedula
              ? 'No existe un usuario con ese dato en CORRESPONSALES.USUARIO.'
              : 'Con esa MAC el backend no encuentra ningún usuario con POS activo (findByMac vacío).',
          );
          ctx.hallazgo({
            severidad: 'critica',
            titulo: usuario || cedula ? 'Usuario no registrado' : 'La MAC no está vinculada a un usuario con POS activo',
            detalle:
              usuario || cedula
                ? 'Confirme el usuario o la cédula con el corresponsal. Si es un corresponsal nuevo, puede que aún no se haya creado su usuario.'
                : 'Es el patrón del caso SIN-NUMERO-1105681702: la MAC registrada pertenece a otro equipo o a un POS inactivo, y el backend responde "usuario no encontrado".',
            ticketsRelacionados: ['SIN-NUMERO-1105681702'],
          });
        } else if (filas.length > 1) {
          p.alerta(`Se encontraron ${filas.length} usuarios: ${filas.map((f) => texto(f.usr_usuario)).join(', ')}.`);
          if (mac && !usuario && !cedula) {
            ctx.hallazgo({
              severidad: 'alta',
              titulo: 'Varios usuarios comparten la misma MAC activa',
              detalle: `El login por PIN busca por MAC y encuentra ${filas.length} usuarios. El backend puede autenticar al usuario equivocado o fallar.`,
              ticketsRelacionados: ['2026083700'],
            });
          }
        } else {
          p.ok(`Usuario ${texto(filas[0].usr_usuario)} (código ${texto(filas[0].usr_codigo)}).`);
        }
        return filas;
      })) ?? [];

    const u: Fila | undefined = usuarios[0];

    // 2. Estado, intentos y claves temporales.
    if (u) {
      await ctx.paso('estado', 'Estado e intentos fallidos', async (p) => {
        const problemas: string[] = [];
        if (numero(u.usr_estado) !== 1) {
          problemas.push('usuario inactivo');
          ctx.hallazgo({
            severidad: 'alta',
            titulo: 'Usuario inactivo',
            detalle: `El usuario ${texto(u.usr_usuario)} tiene usr_estado = ${texto(u.usr_estado)}. No podrá ingresar hasta reactivarlo.`,
            acciones: [
              {
                descripcion: 'Reactivar el usuario (confirmar antes que la desactivación no fue intencional).',
                riesgo: 'escritura',
                responsable: RESPONSABLE_DB,
                biblioteca: 'U11',
                sql: sqlConValores('UPDATE CORRESPONSALES.USUARIO SET USR_ESTADO = 1 WHERE USR_CODIGO = :usr_codigo;\nCOMMIT;', {
                  usr_codigo: numero(u.usr_codigo),
                }),
              },
            ],
          });
        }
        if (numero(u.usr_intentos_pin) >= config.reglas.maxIntentosPin) {
          problemas.push(`${texto(u.usr_intentos_pin)} intentos fallidos de PIN`);
          ctx.hallazgo({
            severidad: 'alta',
            titulo: 'Bloqueado por intentos fallidos de PIN',
            detalle: `usr_intentos_pin = ${texto(u.usr_intentos_pin)} (máximo configurado: ${config.reglas.maxIntentosPin}).`,
            acciones: [
              {
                descripcion: 'Resetear los intentos de PIN.',
                riesgo: 'escritura',
                responsable: RESPONSABLE_DB,
                biblioteca: 'U10',
                sql: sqlConValores('UPDATE CORRESPONSALES.USUARIO SET USR_INTENTOS_PIN = 0 WHERE USR_CODIGO = :usr_codigo;\nCOMMIT;', {
                  usr_codigo: numero(u.usr_codigo),
                }),
              },
            ],
          });
        }
        if (numero(u.usr_intentos) >= config.reglas.maxIntentosClave) {
          problemas.push(`${texto(u.usr_intentos)} intentos fallidos de clave`);
          ctx.hallazgo({
            severidad: 'alta',
            titulo: 'Bloqueado por intentos fallidos de contraseña',
            detalle: `usr_intentos = ${texto(u.usr_intentos)} (máximo configurado: ${config.reglas.maxIntentosClave}).`,
            acciones: [
              {
                descripcion: 'Resetear intentos fallidos y reactivar por usuario.',
                riesgo: 'escritura',
                responsable: RESPONSABLE_DB,
                biblioteca: 'U8',
                sql: sqlConValores(
                  'UPDATE CORRESPONSALES.USUARIO\nSET USR_INTENTOS = 0, USR_ESTADO = 1, USR_INTENTOS_PIN = 0\nWHERE USR_USUARIO = :usuario;\nCOMMIT;',
                  { usuario: texto(u.usr_usuario) },
                ),
              },
            ],
          });
        }
        const temporales = [
          numero(u.usr_is_clave_temporal) === 1 ? 'contraseña' : null,
          numero(u.usr_is_pin_temporal) === 1 ? 'PIN' : null,
        ].filter(Boolean);
        if (temporales.length) {
          ctx.hallazgo({
            severidad: 'info',
            titulo: `Tiene ${temporales.join(' y ')} temporal pendiente de cambio`,
            detalle:
              'Al ingresar, el sistema le pedirá definir uno nuevo. Si el cambio "se guarda" pero no persiste, revisar que cumpla la política (ticket 2026085232): contraseña de 8 a 15 caracteres con mayúscula, minúscula, número y carácter especial; PIN de 4 dígitos no consecutivos ni repetidos.',
            ticketsRelacionados: ['2026085232'],
          });
        }
        if (problemas.length) p.falla(`Se detectó: ${problemas.join(', ')}.`);
        else p.ok('Usuario activo, sin bloqueos por intentos fallidos.');
      });
    }

    // 3. MAC: se revisa la ingresada o, si no hay, las de los POS del usuario.
    const macs = mac
      ? [mac]
      : u
        ? ((await ctx.paso('pos-usuario', 'POS asignados al usuario', async (p) => {
            const filas = await p.consultar('posDeUsuario', { usr_codigo: numero(u.usr_codigo) });
            if (filas.length === 0) {
              p.falla('El usuario no tiene ningún POS asignado en usuario_pos.');
              ctx.hallazgo({
                severidad: 'alta',
                titulo: 'Usuario sin POS asignado',
                detalle: 'Sin un POS vinculado el corresponsal no puede autenticarse desde el equipo. Coordinar la asignación con Soporte operativo.',
              });
            } else {
              p.ok(`${filas.length} POS asignado(s): ${filas.map((f) => `${texto(f.pos_mac)} (estado ${texto(f.pos_estado_id)})`).join(', ')}.`);
            }
            return [...new Set(filas.map((f) => texto(f.pos_mac)).filter(Boolean))].slice(0, 5);
          })) ?? [])
        : [];

    for (const m of macs) {
      await ctx.paso(`mac-${m}`, `Registro de la MAC ${m}`, async (p) => {
        const pos = await p.consultar('posPorMac', { mac: m });
        const activos = pos.filter((x) => numero(x.pos_estado_id) === 1);
        if (pos.length === 0) {
          p.falla('La MAC no está registrada en ningún POS.');
          ctx.hallazgo({
            severidad: 'critica',
            titulo: `La MAC ${m} no está registrada`,
            detalle: 'Verificar la MAC/IMEI real del equipo (etiqueta o configuración del POS). Un dígito distinto basta para que el backend no encuentre el usuario.',
          });
          return;
        }
        if (pos.length > 1) {
          p.falla(`La MAC aparece en ${pos.length} POS (${activos.length} activo/s).`);
          ctx.hallazgo({
            severidad: 'critica',
            titulo: `MAC ${m} duplicada en ${pos.length} POS`,
            detalle: pos
              .map(
                (x) =>
                  `POS ${texto(x.pos_id)}: estado ${texto(x.pos_estado_id)}, ${texto(x.ce_razon_social) || 'sin establecimiento'}, corresponsal ${texto(x.cr_identificacion) || '-'}`,
              )
              .join('\n'),
            ticketsRelacionados: ['2026083700', 'SIN-NUMERO-1105681702'],
            acciones: [
              {
                descripcion:
                  'Dejar la MAC solo en el POS activo del corresponsal que usa el equipo. Coordinar la corrección de la asignación con Soporte operativo.',
                riesgo: 'lectura',
                responsable: 'Soporte operativo',
                biblioteca: 'U19',
              },
            ],
          });
        } else if (activos.length === 0) {
          p.falla('El único POS con esta MAC está inactivo.');
          ctx.hallazgo({
            severidad: 'alta',
            titulo: 'POS inactivo',
            detalle: `El POS ${texto(pos[0].pos_id)} tiene pos_estado_id = ${texto(pos[0].pos_estado_id)}. El login filtra por POS activos.`,
          });
        } else {
          p.ok(`MAC única, POS ${texto(activos[0].pos_id)} activo (${texto(activos[0].ce_razon_social)}).`);
        }

        // Sucursal del usuario frente a la del corresponsal dueño del POS activo.
        const sucursalCorresponsal = activos[0]?.cr_sucursal;
        if (u && sucursalCorresponsal != null && numero(u.codigo_sucursal) !== numero(sucursalCorresponsal)) {
          ctx.hallazgo({
            severidad: 'media',
            titulo: 'Sucursal del usuario distinta a la del corresponsal',
            detalle: `usuario.codigo_sucursal = ${texto(u.codigo_sucursal)} y corresponsalrepresentante.codigo_sucursal = ${texto(sucursalCorresponsal)}. En 2026083700 corregir este campo fue parte de la solución.`,
            ticketsRelacionados: ['2026083700'],
            acciones: [
              {
                descripcion: 'Alinear la sucursal del usuario con la del corresponsal.',
                riesgo: 'escritura',
                responsable: RESPONSABLE_DB,
                biblioteca: 'U14',
                sql: sqlConValores('UPDATE CORRESPONSALES.USUARIO\nSET CODIGO_SUCURSAL = :codigo_sucursal\nWHERE USR_CEDULA = :cedula;\nCOMMIT;', {
                  codigo_sucursal: numero(sucursalCorresponsal),
                  cedula: texto(u.usr_cedula),
                }),
              },
            ],
          });
        }
      });

      // Vínculo usuario_pos: el usuario debe estar ligado al POS activo de esa MAC.
      await ctx.paso(`vinculo-${m}`, `Usuarios vinculados a la MAC ${m}`, async (p) => {
        const vinculos = await p.consultar('usuariosDePosPorMac', { mac: m });
        const enActivos = vinculos.filter((v) => numero(v.pos_estado_id) === 1);
        const usuariosActivos = [...new Set(enActivos.map((v) => texto(v.usr_usuario)))];
        if (u && !enActivos.some((v) => numero(v.up_usuario) === numero(u.usr_codigo))) {
          p.falla(`${texto(u.usr_usuario)} no está vinculado a un POS activo con esta MAC.`);
          ctx.hallazgo({
            severidad: 'critica',
            titulo: 'El usuario no está vinculado al POS de esa MAC',
            detalle: `La MAC ${m} está asignada a: ${usuariosActivos.join(', ') || 'ningún usuario activo'}. El equipo que usa el corresponsal no corresponde al registro esperado.`,
            ticketsRelacionados: ['SIN-NUMERO-1105681702'],
          });
        } else if (usuariosActivos.length > 1) {
          p.alerta(`POS activos de esta MAC vinculados a ${usuariosActivos.length} usuarios: ${usuariosActivos.join(', ')}.`);
        } else {
          p.ok(`Vinculada a ${usuariosActivos[0] ?? 'ningún usuario activo'}.`);
        }
      });
    }

    if (macs.length === 0 && !u) {
      await ctx.paso('mac', 'Registro de la MAC', async (p) => p.omitir('Sin MAC ni usuario identificado para revisar.'));
    }
  },
};
