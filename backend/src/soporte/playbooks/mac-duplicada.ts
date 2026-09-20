import { CAMPO_MAC } from './campos.ts';
import { numero, texto } from './motor.ts';
import type { Playbook } from './tipos.ts';

/** Diagnóstico enfocado en una MAC: dónde está registrada y a quién pertenece. */
export const macDuplicada: Playbook = {
  id: 'mac-duplicada',
  nombre: 'Revisión de MAC / POS',
  descripcion:
    'Muestra todos los POS registrados con una MAC, su estado, establecimiento, corresponsal y usuarios vinculados. Detecta duplicados y equipos reasignados.',
  sistema: 'Corresponsales',
  categoria: 'POS',
  senales: [/\bmac\b/i, /\bimei\b/i, /\bpos\b/i, /duplicad/i, /equipo/i, /dispositivo/i],
  ticketsReferencia: ['2026083700', 'SIN-NUMERO-1105681702'],
  campos: [{ ...CAMPO_MAC, requerido: true }],

  async ejecutar(ctx) {
    const { mac } = ctx.entrada;

    const pos =
      (await ctx.paso('pos', 'POS registrados con la MAC', async (p) => {
        const filas = await p.consultar('posPorMac', { mac });
        const activos = filas.filter((f) => numero(f.pos_estado_id) === 1).length;
        if (filas.length === 0) {
          p.falla('No hay ningún POS con esta MAC.');
          ctx.hallazgo({
            severidad: 'alta',
            titulo: 'MAC no registrada',
            detalle: 'Verificar la MAC/IMEI real del equipo. Si es un equipo nuevo, aún no fue registrado.',
          });
        } else if (filas.length > 1) {
          p.falla(`${filas.length} POS con la misma MAC (${activos} activo/s).`);
        } else {
          p.ok(`Un solo POS (${texto(filas[0].pos_id)}), estado ${texto(filas[0].pos_estado_id)}.`);
        }
        return filas;
      })) ?? [];

    const vinculos =
      (await ctx.paso('usuarios', 'Usuarios vinculados', async (p) => {
        const filas = await p.consultar('usuariosDePosPorMac', { mac });
        if (filas.length === 0) p.alerta('Ningún usuario tiene asignado un POS con esta MAC.');
        else p.ok(`${filas.length} vínculo(s): ${filas.map((f) => `${texto(f.usr_usuario)} (POS ${texto(f.up_pos)})`).join(', ')}.`);
        return filas;
      })) ?? [];

    if (pos.length > 1) {
      const representantes = new Set(pos.map((f) => texto(f.cr_identificacion)).filter(Boolean));
      ctx.hallazgo({
        severidad: 'critica',
        titulo: `MAC duplicada en ${pos.length} POS${representantes.size > 1 ? ` de ${representantes.size} corresponsales distintos` : ''}`,
        detalle: pos
          .map((f) => {
            const usuarios = vinculos.filter((v) => numero(v.up_pos) === numero(f.pos_id)).map((v) => texto(v.usr_usuario));
            return `POS ${texto(f.pos_id)} · estado ${texto(f.pos_estado_id)} · ${texto(f.ce_razon_social) || 'sin establecimiento'} · usuarios: ${usuarios.join(', ') || 'ninguno'}`;
          })
          .join('\n'),
        ticketsRelacionados: ['2026083700', 'SIN-NUMERO-1105681702'],
        acciones: [
          {
            descripcion:
              'Confirmar físicamente qué corresponsal usa el equipo y dejar la MAC solo en ese POS activo. Los POS inactivos con la misma MAC interfieren en la búsqueda del login.',
            riesgo: 'lectura',
            responsable: 'Soporte operativo',
            biblioteca: 'U19',
          },
        ],
      });
    }

    const inactivosConUsuarios = vinculos.filter((v) => numero(v.pos_estado_id) !== 1 && numero(v.usr_estado) === 1);
    if (inactivosConUsuarios.length) {
      ctx.hallazgo({
        severidad: 'media',
        titulo: 'Usuarios activos vinculados a POS inactivos',
        detalle: inactivosConUsuarios.map((v) => `${texto(v.usr_usuario)} en POS ${texto(v.up_pos)}`).join('\n'),
      });
    }
  },
};
