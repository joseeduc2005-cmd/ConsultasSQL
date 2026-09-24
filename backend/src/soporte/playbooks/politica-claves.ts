import { numero, texto } from './motor.ts';
import type { Playbook } from './tipos.ts';

/**
 * "El cambio de clave o PIN se guarda pero no persiste / no acepta el formato".
 *
 * Del ticket 2026085232. No es una avería de datos: el sistema sí valida, con reglas
 * distintas para la contraseña y para el PIN, y el corresponsal no las conoce. La
 * pantalla además no muestra el error cuando el cambio se rechaza, así que parece
 * que guardó.
 *
 * El diagnóstico lee la política vigente de CORRESPONSALES.PARAMETRO en vez de
 * repetir de memoria lo que decía el ticket: si alguien cambia un parámetro, la
 * respuesta sigue siendo la correcta.
 */
export const politicaClaves: Playbook = {
  id: 'politica-claves',
  nombre: 'Política de contraseña y PIN',
  descripcion:
    'Responde por qué un cambio de contraseña o PIN de corresponsal se rechaza: lee la política vigente de parámetros y recuerda el fallo de pantalla que oculta el error.',
  sistema: 'Corresponsales',
  categoria: 'Autenticación',
  senales: [
    /(formato|politica|política).{0,25}(clave|contrase|pin)/i,
    /(clave|contrase|pin).{0,60}(no (se )?(guarda|persiste|acepta|cambia)|se guarda pero)/i,
    /no (acepta|admite).{0,20}(clave|contrase|pin)/i,
    /(requisitos|reglas).{0,20}(clave|contrase|pin)/i,
    // «no persiste» es la queja textual del 2026085232 y lo que lo distingue de un
    // bloqueo de acceso, que comparte las palabras «clave» y «PIN».
    /persist/i,
    /(mayuscula|mayúscula|caracter especial|carácter especial)/i,
  ],
  ticketsReferencia: ['2026085232'],
  campos: [],
  sinDatos: true,

  async ejecutar(ctx) {
    const parametros = await ctx.paso('politica', 'Política vigente de clave y PIN', async (p) => {
      const filas = await p.consultar('politicaClaves');
      if (filas.length === 0) {
        p.omitir('No se encontraron parámetros de clave o PIN en CORRESPONSALES.PARAMETRO.');
        return [];
      }
      p.ok(`${filas.length} parámetro(s) de política encontrados.`);
      return filas;
    });

    const detalle = (parametros ?? [])
      .map((f) => `- ${texto(f.par_descripcion)}: ${texto(f.par_valor)}`)
      .join('\n');

    ctx.hallazgo({
      severidad: 'info',
      titulo: 'El cambio se rechaza por política, y la pantalla no lo avisa',
      detalle:
        'Confirmado en el ticket 2026085232: el sistema sí valida la contraseña y el PIN, con reglas distintas ' +
        'para cada uno. Cuando el cambio no cumple, `cambiarClaveCorresponsal()` no marca el estado de error en la ' +
        'rama correspondiente, así que la pantalla responde como si hubiera guardado. De ahí el «se guarda pero no ' +
        'persiste» que reporta el corresponsal.\n\n' +
        (detalle
          ? `Política vigente en CORRESPONSALES.PARAMETRO:\n${detalle}`
          : 'No se pudo leer la política vigente; la del ticket era: contraseña de 8 a 15 caracteres con mayúscula, ' +
            'minúscula, dígito y carácter especial, sin repetir las 3 últimas; PIN de 4 dígitos no consecutivos ni repetidos.'),
      ticketsRelacionados: ['2026085232'],
      acciones: [
        {
          descripcion: 'Indicar al corresponsal los requisitos vigentes y pedirle que reintente con una clave que los cumpla.',
          riesgo: 'lectura',
          responsable: 'Soporte',
        },
        {
          descripcion:
            'Recordar a desarrollo el fallo ya reportado: falta marcar el estado de error en cambiarClaveCorresponsal(), por eso la pantalla no avisa.',
          riesgo: 'lectura',
          responsable: 'Desarrollo',
        },
      ],
    });

    // Un PIN o clave temporal pendiente confunde todavía más el síntoma.
    await ctx.paso('temporales', 'Corresponsales con clave o PIN temporal pendiente', async (p) => {
      const [fila] = await p.consultar('pendientesTemporales');
      const total = numero(fila?.total);
      if (total > 0) {
        p.alerta(`${total} usuario(s) con clave o PIN temporal sin cambiar.`);
      } else {
        p.ok('Ningún usuario arrastra clave o PIN temporal.');
      }
    });
  },
};
