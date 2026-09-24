'use client';

import {
  ChevronDown,
  CircleAlert,
  CircleCheck,
  Clock,
  Copy,
  Database,
  ShieldAlert,
  ShieldCheck,
  TriangleAlert,
  BookPlus,
  Sparkle,
} from 'lucide-react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { useMemo, useState, type ReactNode } from 'react';
import Link from 'next/link';
import { api } from '@/soporte/lib/api';
import { Badge } from '@/soporte/components/ui/badge';
import { Button } from '@/soporte/components/ui/button';
import { Card } from '@/soporte/components/ui/card';
import { Dialog } from '@/soporte/components/ui/dialog';
import { CodeBlock, useCopiar } from '@/soporte/components/ui/code-block';
import { useAvisos } from '@/soporte/components/ui/toast';
import { ETIQUETA_CAMPO, SEVERIDAD, VEREDICTO, fechaHora } from '@/soporte/lib/formato';
import type {
  Hallazgo,
  ResultadoDiagnostico,
  PropuestaExploracion,
  ResultadoExploracion,
  Severidad,
  SolucionPublicada,
} from '@/soporte/lib/types';
import { cn } from '@/soporte/lib/utils';
import { RegistrarEnTicket } from './registrar-en-ticket';

const BORDE_SEVERIDAD: Record<Severidad, string> = {
  critica: 'before:bg-danger',
  alta: 'before:bg-orange',
  media: 'before:bg-warning',
  info: 'before:bg-info',
};


function textoResumen(r: ResultadoDiagnostico): string {
  const lineas = [
    `${r.nombrePlaybook} — ${VEREDICTO[r.veredicto].etiqueta}`,
    `Datos: ${Object.entries(r.entrada).map(([k, v]) => `${ETIQUETA_CAMPO[k] ?? k} ${v}`).join(', ')}`,
    '',
    r.resumen,
    ...r.hallazgos.map((h) => `- [${SEVERIDAD[h.severidad].etiqueta}] ${h.titulo}`),
    '',
    'Verificaciones:',
    ...r.pasos.map((p) => `- ${p.titulo}: ${p.detalle}`),
  ];
  return lineas.join('\n');
}

export function ResultadoDiagnosticoVista({
  resultado,
  exploracion,
}: {
  resultado: ResultadoDiagnostico;
  exploracion?: ResultadoExploracion;
}) {
  const { copiado, copiar } = useCopiar();
  const v = VEREDICTO[resultado.veredicto];
  const Icono = resultado.veredicto === 'problema' ? CircleAlert : resultado.veredicto === 'incompleto' ? TriangleAlert : CircleCheck;
  const consultas = resultado.pasos.reduce((n, p) => n + p.consultas.length, 0);

  return (
    <div className="space-y-5">
      <Card className="overflow-hidden">
        <div
          className={cn(
            'flex flex-wrap items-start gap-4 px-5 py-4',
            resultado.veredicto === 'problema' ? 'bg-danger-soft/60' : resultado.veredicto === 'incompleto' ? 'bg-warning-soft/60' : 'bg-success-soft/60',
          )}
        >
          <div
            className={cn(
              'grid size-10 shrink-0 place-items-center rounded-xl [&_svg]:size-5',
              resultado.veredicto === 'problema' ? 'bg-danger text-white' : resultado.veredicto === 'incompleto' ? 'bg-warning text-white' : 'bg-success text-white',
            )}
          >
            <Icono />
          </div>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <Badge tono={v.tono}>{v.etiqueta}</Badge>
              <span className="text-[13px] text-muted-foreground">{resultado.nombrePlaybook}</span>
            </div>
            <p className="mt-1.5 text-[15px] font-semibold">{resultado.resumen}</p>
          </div>
          <div className="flex items-center gap-2">
            <Button variante="fantasma" tamano="sm" onClick={() => copiar(textoResumen(resultado))}>
              <Copy />
              {copiado ? 'Copiado' : 'Copiar resumen'}
            </Button>
            {resultado.id !== undefined && resultado.hallazgos.length > 0 && (
              <PublicarSolucion resultado={resultado} exploracion={exploracion?.propuesta} />
            )}
            {resultado.id !== undefined && <RegistrarEnTicket diagnosticoId={resultado.id} ticketActual={resultado.ticket} />}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 border-t px-5 py-2.5 text-[12.5px] text-muted-foreground">
          {Object.entries(resultado.entrada).map(([k, valor]) => (
            <span key={k}>
              {ETIQUETA_CAMPO[k] ?? k} <span className="font-mono text-foreground">{valor}</span>
            </span>
          ))}
          <span className="ml-auto flex items-center gap-4">
            <span className="flex items-center gap-1.5">
              <Database className="size-3.5" />
              {resultado.origenDatos === 'demo' ? 'Demostración' : 'Oracle'} · {consultas} consultas
            </span>
            <span className="flex items-center gap-1.5">
              <Clock className="size-3.5" />
              {fechaHora(resultado.inicio)} · {resultado.duracionMs} ms
            </span>
          </span>
        </div>
      </Card>

      {exploracion?.propuesta && <PropuestaExploracionVista propuesta={exploracion.propuesta} />}

      {resultado.hallazgos.length > 0 && <Hallazgos hallazgos={resultado.hallazgos} />}
    </div>
  );
}

/**
 * Los hallazgos ocupan mucha pantalla cuando son varios, y con la propuesta arriba
 * casi nunca hace falta leerlos todos. Vienen plegados salvo que haya uno solo.
 */
function Hallazgos({ hallazgos }: { hallazgos: Hallazgo[] }) {
  const [abierto, setAbierto] = useState(hallazgos.length === 1);
  const criticos = hallazgos.filter((h) => h.severidad === 'critica' || h.severidad === 'alta').length;

  return (
    <section>
      <button
        type="button"
        onClick={() => setAbierto((v) => !v)}
        className="flex w-full items-center gap-2 rounded-lg px-1 py-1.5 text-left transition-colors hover:bg-surface-muted/50"
        aria-expanded={abierto}
      >
        <ChevronDown className={cn('size-4 text-muted-foreground transition-transform', abierto && 'rotate-180')} />
        <h3 className="text-[13px] font-semibold tracking-wide text-muted-foreground uppercase">
          Hallazgos ({hallazgos.length})
        </h3>
        {criticos > 0 && (
          <Badge tono="danger" punto>
            {criticos} grave{criticos === 1 ? '' : 's'}
          </Badge>
        )}
      </button>
      {abierto && (
        <div className="animate-entrada mt-2 space-y-3">
          {hallazgos.map((h, i) => (
            <HallazgoCard key={i} hallazgo={h} />
          ))}
        </div>
      )}
    </section>
  );
}

function HallazgoCard({ hallazgo: h }: { hallazgo: Hallazgo }) {
  const s = SEVERIDAD[h.severidad];
  return (
    <Card className={cn('relative overflow-hidden p-4 pl-5 before:absolute before:inset-y-0 before:left-0 before:w-1', BORDE_SEVERIDAD[h.severidad])}>
      <div className="flex flex-wrap items-start justify-between gap-2">
        <p className="font-medium">{h.titulo}</p>
        <Badge tono={s.tono} punto>
          {s.etiqueta}
        </Badge>
      </div>
      <p className="mt-1.5 text-[13.5px] leading-relaxed whitespace-pre-line text-muted-foreground">{h.detalle}</p>

      {h.ticketsRelacionados && h.ticketsRelacionados.length > 0 && (
        <div className="mt-3 flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
          Casos relacionados:
          {h.ticketsRelacionados.map((t) => (
            <Link key={t} href={`/admin/tickets/${t}`} className="rounded-md bg-surface-muted px-1.5 py-0.5 font-mono text-foreground hover:bg-primary-soft hover:text-primary-soft-foreground">
              {t}
            </Link>
          ))}
        </div>
      )}

      {h.acciones?.map((a, i) => (
        <div key={i} className="mt-3.5 rounded-lg border bg-surface-muted/40 p-3">
          <div className="flex flex-wrap items-center gap-2">
            {a.riesgo === 'escritura' ? (
              <Badge tono="danger">
                <ShieldAlert className="size-3.5" />
                Escritura · requiere aprobación
              </Badge>
            ) : (
              <Badge tono="success">
                <ShieldCheck className="size-3.5" />
                Solo lectura
              </Badge>
            )}
            <span className="text-xs text-muted-foreground">Responsable: {a.responsable}</span>
            {a.biblioteca && (
              <Link href={`/admin/biblioteca?codigo=${a.biblioteca}`} className="ml-auto font-mono text-xs font-medium text-primary hover:underline">
                {a.biblioteca} en Biblioteca
              </Link>
            )}
          </div>
          <p className="mt-2 text-[13px]">{a.descripcion}</p>
          {a.sql && (
            <>
              <CodeBlock codigo={a.sql} className="mt-2.5" etiqueta={a.riesgo === 'escritura' ? 'SQL de corrección' : 'SQL'} tono={a.riesgo === 'escritura' ? 'peligro' : 'normal'} />
              {a.riesgo === 'escritura' && (
                <p className="mt-2 text-[11.5px] text-muted-foreground">
                  Este sistema no ejecuta sentencias de escritura. Revise los valores y envíe la corrección al responsable.
                </p>
              )}
            </>
          )}
        </div>
      ))}
    </Card>
  );
}


/**
 * La propuesta que sale de comparar al usuario contra los que si estan operando.
 *
 * Va arriba de los hallazgos a proposito: cuando existe, es lo primero que hay que
 * mirar, y cuando ningun playbook reconocio el caso es lo unico que hay.
 *
 * Es una sola sentencia aunque las diferencias sean varias, porque todas las columnas
 * viven en la misma fila. Eso es lo que evita entregar cinco SQL para probar de uno
 * en uno.
 */
export function PropuestaExploracionVista({ propuesta: p }: { propuesta: PropuestaExploracion }) {
  const { copiado, copiar } = useCopiar();
  const porcentaje = Math.round(p.confianza * 100);
  const tono = porcentaje >= 90 ? 'success' : porcentaje >= 75 ? 'warning' : 'danger';

  return (
    <Card className="overflow-hidden border-primary/30">
      <div className="flex flex-wrap items-start gap-4 bg-primary-soft/40 px-5 py-4">
        <div className="grid size-10 shrink-0 place-items-center rounded-xl bg-primary text-white [&_svg]:size-5">
          <Sparkle />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <Badge tono="primary">Propuesta del sistema</Badge>
            <Badge tono={tono} punto>
              {porcentaje} % de confianza
            </Badge>
          </div>
          <p className="mt-1.5 text-[15px] font-semibold">{p.resumen}</p>
          <p className="mt-0.5 text-[12.5px] text-muted-foreground">
            Comparado contra {p.poblacion} usuarios que operaron en los ultimos {p.dias} dias.
          </p>
        </div>
        <Button variante="fantasma" tamano="sm" onClick={() => copiar(p.sql)}>
          <Copy />
          {copiado ? 'Copiado' : 'Copiar SQL'}
        </Button>
      </div>

      <div className="space-y-4 px-5 py-4">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-[13px]">
            <thead>
              <tr className="text-[11.5px] tracking-wide text-muted-foreground uppercase">
                <th className="pb-1.5 font-medium">Columna</th>
                <th className="pb-1.5 font-medium">Tiene</th>
                <th className="pb-1.5 font-medium">Lo normal</th>
                <th className="pb-1.5 font-medium">Cuota</th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {p.anomalias.map((a) => (
                <tr key={a.columna}>
                  <td className="py-1.5 font-mono text-[12.5px]">{a.columna}</td>
                  <td className="py-1.5 font-mono text-danger">{a.valorActual}</td>
                  <td className="py-1.5 font-mono text-success">{a.valorSano}</td>
                  <td className="tabular py-1.5 text-muted-foreground">{Math.round(a.cuota * 100)} %</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <CodeBlock codigo={p.sql} etiqueta="Correccion propuesta" tono="peligro" />

        <p className="text-[11.5px] leading-relaxed text-muted-foreground">
          Esta sentencia no viene del catalogo de correcciones aprobadas: la redacto el sistema comparando este caso
          con la poblacion. Afecta a una sola fila, por clave primaria. Confirmela antes de aplicarla; publicarla la
          deja lista en categorias, no la ejecuta.
        </p>
      </div>
    </Card>
  );
}

interface CorreccionElegible {
  id: string;
  descripcion: string;
  hallazgo: string;
  severidad: Severidad;
  parametros: Record<string, string | number | null>;
}

/**
 * Correcciones de escritura que propone el diagnóstico, sin repetir.
 *
 * Es la misma lista que arma el backend al publicar; aquí se deriva de los hallazgos
 * que ya están en pantalla para no pedirla otra vez. Vienen en orden de severidad,
 * porque el motor ordena los hallazgos antes de devolverlos.
 */
function correccionesElegibles(r: ResultadoDiagnostico): CorreccionElegible[] {
  const vistas = new Set<string>();
  const salida: CorreccionElegible[] = [];
  for (const h of r.hallazgos) {
    for (const a of h.acciones ?? []) {
      if (a.riesgo !== 'escritura' || !a.correccion || vistas.has(a.correccion)) continue;
      vistas.add(a.correccion);
      salida.push({
        id: a.correccion,
        descripcion: a.descripcion,
        hallazgo: h.titulo,
        severidad: h.severidad,
        parametros: a.parametros ?? {},
      });
    }
  }
  return salida;
}

/** Una opcion del selector, venga del catalogo o de la exploracion. */
interface OpcionSolucion {
  clave: string;
  titulo: string;
  detalle: string;
  insignia: ReactNode;
}

function FilaOpcion({
  opcion,
  marcada,
  alternar,
}: {
  opcion: OpcionSolucion;
  marcada: boolean;
  alternar: () => void;
}) {
  return (
    <label
      className={cn(
        'flex cursor-pointer gap-3 rounded-lg border p-3 transition-colors',
        marcada ? 'border-primary/40 bg-primary-soft/30' : 'hover:bg-surface-muted/50',
      )}
    >
      <input
        type="checkbox"
        checked={marcada}
        onChange={alternar}
        className="mt-0.5 size-4 shrink-0 accent-[var(--color-primary)]"
      />
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-[13.5px] font-medium">{opcion.titulo}</span>
          {opcion.insignia}
        </div>
        <p className="mt-1 text-[12.5px] text-muted-foreground">{opcion.detalle}</p>
      </div>
    </label>
  );
}

/**
 * Publica el diagnostico en el arbol de categorias: un articulo con el caso y uno
 * ejecutable por cada correccion que se marque.
 *
 * Arriba va una sola, la principal — la explorada si existe, si no la del hallazgo
 * mas grave. El resto queda plegado, porque solo hacen falta si la principal no
 * funciono. Y al final se puede subir un script propio, para cuando ninguna sirve.
 *
 * Marcar no ejecuta nada: solo deja los articulos listos con su formulario.
 */
function PublicarSolucion({
  resultado,
  exploracion,
}: {
  resultado: ResultadoDiagnostico;
  exploracion?: PropuestaExploracion;
}) {
  const avisar = useAvisos();
  const diagnosticoId = resultado.id!;
  // Para avisar en el dialogo cuando lo publicado no va a ser ejecutable.
  const { data: estado } = useQuery({ queryKey: ['estado'], queryFn: api.estado, staleTime: 60_000 });
  const elegibles = useMemo(() => correccionesElegibles(resultado), [resultado]);

  // La explorada manda cuando existe: se compara contra la poblacion y trae su
  // nivel de confianza. Si no, la principal es la del hallazgo mas grave.
  const opciones = useMemo<OpcionSolucion[]>(() => {
    const lista: OpcionSolucion[] = [];
    if (exploracion) {
      lista.push({
        clave: 'exploracion',
        titulo: exploracion.resumen,
        detalle: `${Math.round(exploracion.confianza * 100)} % de confianza · corrige ${
          exploracion.anomalias.length
        } columna${exploracion.anomalias.length === 1 ? '' : 's'} en una sola sentencia. No viene del catalogo aprobado.`,
        insignia: (
          <Badge tono="primary">
            <Sparkle className="size-3.5" />
            Propuesta del sistema
          </Badge>
        ),
      });
    }
    for (const c of elegibles) {
      const sev = SEVERIDAD[c.severidad];
      const params = Object.entries(c.parametros)
        .map(([k, v]) => `${k} = ${v ?? '?'}`)
        .join('  ·  ');
      lista.push({
        clave: c.id,
        titulo: c.descripcion,
        detalle: `Hallazgo: ${c.hallazgo}${params ? `  —  ${params}` : ''}`,
        insignia: (
          <Badge tono={sev.tono} punto>
            {sev.etiqueta}
          </Badge>
        ),
      });
    }
    return lista;
  }, [elegibles, exploracion]);

  const principal = opciones[0];
  const secundarias = opciones.slice(1);

  const [abierto, setAbierto] = useState(false);
  const [verMas, setVerMas] = useState(false);
  const [marcadas, setMarcadas] = useState<string[]>(() => (principal ? [principal.clave] : []));
  const [conPropio, setConPropio] = useState(false);
  const [sqlPropio, setSqlPropio] = useState('');
  const [pasosPropios, setPasosPropios] = useState('');
  const [tituloPropio, setTituloPropio] = useState('');
  const [dinamico, setDinamico] = useState(true);
  const [publicado, setPublicado] = useState<SolucionPublicada | null>(null);

  const publicar = useMutation({
    mutationFn: () =>
      api.publicarSolucion(diagnosticoId, {
        correcciones: marcadas.filter((c) => c !== 'exploracion'),
        exploracion: marcadas.includes('exploracion'),
        propio:
          conPropio && (sqlPropio.trim() || pasosPropios.trim())
            ? { titulo: tituloPropio, sql: sqlPropio, pasos: pasosPropios, dinamico }
            : undefined,
      }),
    onSuccess: (r) => {
      setPublicado(r);
      setAbierto(false);
      const n = r.scripts.length;
      avisar(
        'exito',
        r.articulo.creado ? 'Solucion publicada' : 'Solucion actualizada',
        n === 0
          ? r.articulo.titulo
          : `${r.articulo.titulo} · ${n} correccion${n === 1 ? '' : 'es'} lista${n === 1 ? '' : 's'} para ejecutar`,
      );
    },
    onError: (e) => avisar('error', 'No se pudo publicar', (e as Error).message),
  });

  if (publicado) {
    return (
      <Link
        href={`/admin?articulo=${encodeURIComponent(publicado.articulo.id)}`}
        className="text-[13px] font-medium text-primary hover:underline"
      >
        Ver en categorias
      </Link>
    );
  }

  const alternar = (clave: string) =>
    setMarcadas((previas) => (previas.includes(clave) ? previas.filter((x) => x !== clave) : [...previas, clave]));
  const total = marcadas.length + (conPropio && (sqlPropio.trim() || pasosPropios.trim()) ? 1 : 0);

  return (
    <>
      <Button variante="fantasma" tamano="sm" onClick={() => setAbierto(true)}>
        <BookPlus />
        Publicar como solucion
      </Button>

      <Dialog
        abierto={abierto}
        onCerrar={() => setAbierto(false)}
        titulo="Publicar la solucion"
        descripcion="Cada correccion marcada queda como articulo ejecutable en categorias."
        ancho="max-w-2xl"
        pie={
          <>
            <Button variante="fantasma" tamano="sm" onClick={() => setAbierto(false)}>
              Cancelar
            </Button>
            <Button tamano="sm" onClick={() => publicar.mutate()} cargando={publicar.isPending} disabled={total === 0}>
              <BookPlus />
              Publicar {total > 0 && `(${total})`}
            </Button>
          </>
        }
      >
        <div className="space-y-3">
          {principal && (
            <div>
              <p className="mb-1.5 text-[11.5px] font-semibold tracking-wide text-muted-foreground uppercase">
                Solucion principal
              </p>
              <FilaOpcion
                opcion={principal}
                marcada={marcadas.includes(principal.clave)}
                alternar={() => alternar(principal.clave)}
              />
            </div>
          )}

          {secundarias.length > 0 && (
            <div className="rounded-lg border">
              <button
                type="button"
                onClick={() => setVerMas((v) => !v)}
                className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-[13px] font-medium transition-colors hover:bg-surface-muted/50"
                aria-expanded={verMas}
              >
                <ChevronDown className={cn('size-4 text-muted-foreground transition-transform', verMas && 'rotate-180')} />
                Mas soluciones ({secundarias.length})
                <span className="ml-auto text-[12px] font-normal text-muted-foreground">
                  si la principal no funciono
                </span>
              </button>
              {verMas && (
                <div className="animate-entrada space-y-2 border-t p-3">
                  {secundarias.map((o) => (
                    <FilaOpcion
                      key={o.clave}
                      opcion={o}
                      marcada={marcadas.includes(o.clave)}
                      alternar={() => alternar(o.clave)}
                    />
                  ))}
                </div>
              )}
            </div>
          )}

          <div className="rounded-lg border">
            <button
              type="button"
              onClick={() => setConPropio((v) => !v)}
              className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-[13px] font-medium transition-colors hover:bg-surface-muted/50"
              aria-expanded={conPropio}
            >
              <ChevronDown className={cn('size-4 text-muted-foreground transition-transform', conPropio && 'rotate-180')} />
              Subir mi propia solucion
              <span className="ml-auto text-[12px] font-normal text-muted-foreground">si ninguna sirve</span>
            </button>
            {conPropio && (
              <div className="animate-entrada space-y-2.5 border-t p-3">
                <input
                  value={tituloPropio}
                  onChange={(e) => setTituloPropio(e.target.value)}
                  placeholder="Nombre de la correccion (opcional)"
                  className="w-full rounded-lg border bg-surface px-3 py-2 text-[13px] outline-none focus:border-primary/50"
                />
                <div>
                  <p className="mb-1 text-[12px] font-medium text-muted-foreground">Pasos que seguiste (opcional)</p>
                  <textarea
                    value={pasosPropios}
                    onChange={(e) => setPasosPropios(e.target.value)}
                    rows={4}
                    placeholder={'Un paso por linea. Por ejemplo:\nEntrar al modulo de Corresponsales\nAbrir Opciones > Mantenimiento de usuarios\nBuscar por cedula y quitar la marca de clave temporal'}
                    className="w-full rounded-lg border bg-surface px-3 py-2 text-[12.5px] leading-relaxed outline-none focus:border-primary/50"
                  />
                  <p className="mt-1 text-[11.5px] text-muted-foreground">
                    Para arreglos que no son una sentencia sino un procedimiento. Cada linea se numera como paso.
                  </p>
                </div>

                <div>
                  <p className="mb-1 text-[12px] font-medium text-muted-foreground">SQL (opcional)</p>
                  <textarea
                    value={sqlPropio}
                    onChange={(e) => setSqlPropio(e.target.value)}
                    rows={6}
                    spellCheck={false}
                    placeholder={'UPDATE CORRESPONSALES.USUARIO\nSET USR_ESTADO = 1\nWHERE USR_CODIGO = :usr_codigo;\nCOMMIT;'}
                    className="w-full rounded-lg border bg-surface-muted/40 px-3 py-2 font-mono text-[12.5px] outline-none focus:border-primary/50"
                  />
                </div>

                {sqlPropio.trim() && (
                  <div className="space-y-1.5 rounded-lg border bg-surface-muted/30 p-2.5">
                    <label className="flex cursor-pointer items-start gap-2.5">
                      <input
                        type="radio"
                        checked={dinamico}
                        onChange={() => setDinamico(true)}
                        className="mt-0.5 size-3.5 shrink-0 accent-[var(--color-primary)]"
                      />
                      <span className="text-[12.5px]">
                        <span className="font-medium">Script dinamico.</span> Los{' '}
                        <span className="font-mono">:marcadores</span> se vuelven campos del formulario y el articulo
                        sirve para el proximo caso.
                      </span>
                    </label>
                    <label className="flex cursor-pointer items-start gap-2.5">
                      <input
                        type="radio"
                        checked={!dinamico}
                        onChange={() => setDinamico(false)}
                        className="mt-0.5 size-3.5 shrink-0 accent-[var(--color-primary)]"
                      />
                      <span className="text-[12.5px]">
                        <span className="font-medium">De un solo uso.</span> Se ejecuta tal cual, sin formulario.
                      </span>
                    </label>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        <p className="mt-3 text-[12px] text-muted-foreground">
          Publicar no ejecuta nada: deja los articulos listos en categorias con su formulario.
          {estado && !estado.correccionesEjecutables && (
            <>
              {' '}
              Ahora mismo se publican como documentacion: el SQL y el formulario quedan guardados, pero el panel no
              muestra el boton de ejecutar hasta que se habilite la escritura en Oracle.
            </>
          )}
        </p>
      </Dialog>
    </>
  );
}
