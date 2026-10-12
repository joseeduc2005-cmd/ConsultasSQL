'use client';

import { LayoutDashboard, Search, Ticket } from 'lucide-react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useEffect, useState } from 'react';
import { useTheme } from './ThemeProvider';

/**
 * Accesos del Centro de Soporte dentro de la barra lateral del panel.
 *
 * Comparte el lenguaje visual del panel —vidrio, tipografía y tokens `--ink-*`—
 * porque es la misma barra: arriba el soporte, debajo las categorías de artículos.
 */
/** Las mismas cabeceras que usa el resto del panel para hablar con el backend. */
function cabeceras(): Record<string, string> {
  try {
    const token = localStorage.getItem('token');
    return token ? { Authorization: `Bearer ${token}` } : {};
  } catch {
    return {};
  }
}

/**
 * Las secciones, en dos grupos.
 *
 * Tickets y Aportes van juntos bajo un encabezado común porque las dos son bandejas: a
 * las dos llega algo de otra persona y en las dos hay que decidir. Estaban sueltas en la
 * lista, sin nada que las relacionara.
 *
 * Juntos, pero no mezclados en una sola lista: un ticket es un problema sin resolver y un
 * aporte es una solución ya encontrada. En la misma tabla, «lo que está roto» y «lo que
 * alguien arregló» se leerían igual, y lo que se hace con cada uno no se parece — un
 * ticket se investiga, un aporte se revisa y se publica.
 */
const SECCIONES = [
  { ruta: '/admin/resumen', etiqueta: 'Resumen', icono: LayoutDashboard, exacta: true },
  // La búsqueda por conocimiento va arriba: es lo que responde siempre, con o sin
  // base conectada, y para la mayoría de los casos es la única puerta que hace falta.
  { ruta: '/admin/solucionar', etiqueta: 'Buscar solución', icono: Search },

  // Una sola bandeja. Eran dos entradas —Tickets y Aportes— y lo que llega de otra
  // persona llega por un sitio: los aportes salen ahora dentro de esta lista, marcados
  // como tales y con su propio estado. La pantalla de revisarlos sigue existiendo, se
  // llega pulsando el aporte; lo que desaparece es tener que acordarse de mirarla.
  { ruta: '/admin/tickets', etiqueta: 'Casos y aportes', icono: Ticket, contadorBandeja: true },
  // Se quitaron tres opciones del menú:
  //
  //   Diagnósticos   ejecutaba playbooks contra una base
  //   Biblioteca SQL las consultas catalogadas; el SQL vive ahora en las categorías
  //   Vigilancia     sondeaba la base en segundo plano
  //
  // La Biblioteca sigue existiendo por debajo a propósito: sus consultas son el corpus
  // que le enseña al motor qué palabra del negocio apunta a qué tabla. Quitar la
  // pantalla no toca eso; borrar los datos sí habría empeorado las búsquedas.
];

export default function NavegacionSoporte({
  colapsado = false,
  onNavegar,
}: {
  colapsado?: boolean;
  onNavegar?: () => void;
}) {
  const ruta = usePathname();
  const { theme } = useTheme();
  const claro = theme === 'light';
  // Se consulta a mano y no con useQuery: esta barra también se monta en /admin, que
  // no tiene QueryClientProvider, y un hook de react-query ahí revienta la página
  // entera.
  //
  // Antes también se preguntaba si había base configurada, para apagar Diagnósticos y
  // Vigilancia cuando no la hubiera. Esas dos opciones ya no existen y ninguna de las
  // que quedan necesita una base, así que esa consulta se fue con ellas.
  const [pendientes, setPendientes] = useState(0);
  // Los tickets sin cerrar tambien cuentan: las dos son bandejas, y una de las dos
  // llevaba numero y la otra no, lo cual hacia parecer que solo una esperaba algo.
  const [ticketsActivos, setTicketsActivos] = useState(0);
  useEffect(() => {
    let vigente = true;
    fetch('/api/soporte/aportes?estado=pendiente', { headers: cabeceras() })
      .then((r) => (r.ok ? r.json() : null))
      .then((lista) => {
        if (vigente && Array.isArray(lista)) setPendientes(lista.length);
      })
      .catch(() => undefined);
    fetch('/api/soporte/resumen', { headers: cabeceras() })
      .then((r) => (r.ok ? r.json() : null))
      .then((r) => {
        if (vigente && r?.tickets) setTicketsActivos((r.tickets.abiertos ?? 0) + (r.tickets.investigacion ?? 0));
      })
      .catch(() => undefined);
    return () => {
      vigente = false;
    };
  }, []);

  return (
    <nav className={colapsado ? 'flex flex-col items-center gap-1' : 'space-y-0.5'}>
      {!colapsado && (
        <p className="px-3 pt-1 pb-2 text-[12px] font-semibold uppercase tracking-[0.14em] text-(--ink-500)">
          Centro de Soporte
        </p>
      )}

      {SECCIONES.map((seccion) => {
        // El encabezado «Lo que llega» vivía aquí, separando Tickets de Aportes. Con
        // las dos bandejas unidas en una sola entrada ya no separa nada.
        const { ruta: destino, etiqueta, icono: Icono, exacta, contadorBandeja } = seccion;
        const activa = exacta ? ruta === destino : ruta.startsWith(destino);
        const apagada = false;

        if (apagada) {
          return (
            <span
              key={destino}
              aria-disabled="true"
              title={`${etiqueta} · sin base de datos configurada para investigar`}
              className={`flex cursor-not-allowed items-center rounded-lg text-sm opacity-45 ${
                colapsado ? 'size-9 justify-center' : 'h-9 gap-2.5 px-3'
              } ${claro ? 'text-(--ink-500)' : 'text-slate-500'}`}
            >
              <Icono className="size-[17px] shrink-0" />
              {!colapsado && <span className="truncate">{etiqueta}</span>}
            </span>
          );
        }

        return (
          <Link
            key={destino}
            href={destino}
            onClick={onNavegar}
            title={colapsado ? etiqueta : undefined}
            aria-current={activa ? 'page' : undefined}
            className={`group relative flex items-center rounded-lg text-sm transition-colors duration-150 ${
              colapsado ? 'size-9 justify-center' : 'h-9 gap-2.5 px-3'
            } ${
              activa
                ? claro
                  ? 'bg-white font-semibold text-(--accent) shadow-xs ring-1 ring-(--line)'
                  : 'bg-[rgba(56,189,248,0.14)] font-semibold text-cyan-100 ring-1 ring-[rgba(56,189,248,0.3)]'
                : claro
                  ? 'text-(--ink-700) hover:bg-white/70 hover:text-(--ink-900)'
                  : 'text-slate-300 hover:bg-[rgba(56,189,248,0.07)] hover:text-slate-100'
            }`}
          >
            {/* Marca de activo: una barra fina en vez de teñir el elemento entero. */}
            {activa && !colapsado && (
              <span className="absolute inset-y-1.5 left-0 w-[3px] rounded-r-full bg-(--accent)" />
            )}
            <Icono
              className={`size-[17px] shrink-0 ${
                activa ? 'text-(--accent)' : claro ? 'text-(--ink-500)' : 'text-slate-400'
              }`}
            />
            {!colapsado && <span className="truncate">{etiqueta}</span>}
            {/* Las dos bandejas llevan su numero. El de aportes en ambar porque espera una
                decision de alguien; el de tickets en gris porque es trabajo en curso, no
                algo detenido esperando. */}
            {/* Un solo numero: todo lo que espera, venga de donde venga. En ambar si hay
                algo sin revisar, porque eso es lo que esta detenido esperando a alguien;
                en gris si solo hay casos en curso, que es trabajo andando. */}
            {contadorBandeja && pendientes + ticketsActivos > 0 && (
              <span
                title={
                  pendientes > 0
                    ? `${pendientes} por revisar · ${ticketsActivos} casos sin cerrar`
                    : `${ticketsActivos} casos sin cerrar`
                }
                className={`ml-auto rounded-full px-1.5 text-[12px] font-semibold ${
                  pendientes > 0
                    ? 'bg-(--aviso)/15 text-(--aviso)'
                    : 'bg-(--surface-2) font-medium text-(--ink-600)'
                } ${colapsado ? 'absolute top-0.5 right-0.5 ml-0 px-1' : ''}`}
              >
                {pendientes + ticketsActivos}
              </span>
            )}
          </Link>
        );
      })}
    </nav>
  );
}
