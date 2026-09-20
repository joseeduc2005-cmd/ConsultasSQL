// app/components/ContentPanel.tsx
'use client';

import { useEffect, useRef, useState } from 'react';
import { KnowledgeArticle } from '../types';
import ReactMarkdown from 'react-markdown';
import { useTheme } from './ThemeProvider';

// ─── Lib imports ──────────────────────────────────────────────────────────────
import { ProgressStep, ExecutionResultPayload } from '../lib/contentPanel/types';
import {
  parseStructuredScript, getScriptExecutionFields, getScriptPreviewText,
  getBusinessScriptDetails, scriptRequiresWriteConfirmation, inferArticleExecutionTarget,
} from '../lib/contentPanel/scriptInspector';
import { validateExecutionFieldValue, buildNormalizedParamsPayload, buildPreExecutionSummary } from '../lib/contentPanel/fieldUtils';
import {
  sanitizeExecutionData, getFinalExecutionPayload, buildExecutionFindings,
  buildExecutionSummary, buildHumanExecutionExplanation, getResultRows,
} from '../lib/contentPanel/executionResultHelpers';
import {
  getTimelineToneClasses, isTechnicalProgressMessage,
  buildHumanExecutionSteps, buildAnnotatedSqlScript,
} from '../lib/contentPanel/timelineUtils';

// SSE calls go through the Next.js proxy (/api/execute/stream/[id])
// so the browser never needs to know the backend URL.

interface ContentPanelProps {
  selectedArticle: KnowledgeArticle | null;
  loading: boolean;
  error: string;
  onExecuteSolution: (articleId: number | string, formData: Record<string, any>) => Promise<string>;
  /** Usuario autenticado — si no se pasa, se lee de localStorage */
  currentUser?: { username: string; role: string } | null;
}

// ─────────────────────────────────────────────────────────────────────────────

export default function ContentPanel({ selectedArticle, loading, currentUser: currentUserProp }: ContentPanelProps) {
  const { theme } = useTheme();
  const isLightTheme = theme === 'light';
  const [dynamicInputs, setDynamicInputs] = useState<{ [key: string]: string }>({});
  /** Lee el usuario actual en el momento de llamar — siempre fresco desde localStorage */
  const resolveCurrentUser = (): { username: string; role: string } | null => {
    // 1. Prop (pasada desde admin/page.tsx, la más confiable)
    if (currentUserProp?.username) return currentUserProp;
    // 2. localStorage (siempre re-leído para capturar cambios de cuenta)
    if (typeof window !== 'undefined') {
      try {
        const raw = localStorage.getItem('user');
        if (raw) {
          const parsed = JSON.parse(raw);
          if (parsed?.username) return { username: parsed.username, role: parsed.role || 'user' };
        }
      } catch { /* ignorar */ }
    }
    return null;
  };
  const [executing, setExecuting] = useState(false);
  const [progressSteps, setProgressSteps] = useState<ProgressStep[]>([]);
  const [execDone, setExecDone] = useState(false);
  const [execSuccess, setExecSuccess] = useState(false);
  const [executionResult, setExecutionResult] = useState<ExecutionResultPayload | null>(null);
  const [writeConfirmed, setWriteConfirmed] = useState(false);
  const progressEndRef = useRef<HTMLDivElement | null>(null);
  const hasStructuredScript = Boolean(parseStructuredScript(selectedArticle?.script_json));
  const businessScriptDetails = getBusinessScriptDetails(selectedArticle);
  const solutionType = (selectedArticle?.tipo_solucion || 'lectura').toLowerCase();
  const isExecutableArticle = solutionType === 'ejecutable' || solutionType === 'database' || solutionType === 'script';
  const scriptExecutionFields = getScriptExecutionFields(selectedArticle);
  const legacyExecutionFields = ((selectedArticle?.camposFormulario || []) as any[]).filter((field: any) => field.mode !== 'output');
  const executionFields = hasStructuredScript ? scriptExecutionFields : legacyExecutionFields;
  const preExecutionSummary = buildPreExecutionSummary(executionFields, dynamicInputs);
  const requiresWriteConfirmation = scriptRequiresWriteConfirmation(selectedArticle);
  const executionSummary = buildExecutionSummary(progressSteps, execSuccess, executionResult);
  const humanExecutionExplanation = buildHumanExecutionExplanation(executionResult, executionSummary);
  const sanitizedExecutionResult = sanitizeExecutionData(getFinalExecutionPayload(executionResult));
  const executionFindings = buildExecutionFindings(executionResult);
  const humanWorkflowSteps = buildHumanExecutionSteps(progressSteps, executionResult);
  const executedQueriesList = Array.isArray(executionResult?.executedQueries) ? executionResult.executedQueries : [];
  const annotatedSqlScript = buildAnnotatedSqlScript(executedQueriesList);
  const scriptPreviewText = getScriptPreviewText(selectedArticle);
  const [execView, setExecView] = useState<'script' | 'execute'>('script');
  const latestVisibleError = [...progressSteps].reverse().find((step) => step.type === 'error' && !isTechnicalProgressMessage(step.message));
  const executionTarget = inferArticleExecutionTarget(selectedArticle);
  // Ref para cerrar el EventSource si el componente se desmonta mid-stream
  const esRef = useRef<EventSource | null>(null);

  // Cerrar EventSource activo si el componente se desmonta o cambia de artículo
  useEffect(() => {
    return () => {
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    // Cancelar stream en curso al cambiar de artículo
    if (esRef.current) {
      esRef.current.close();
      esRef.current = null;
    }
    setDynamicInputs({});
    setProgressSteps([]);
    setExecDone(false);
    setExecSuccess(false);
    setExecuting(false);
    setExecutionResult(null);
    setWriteConfirmed(false);
    setExecView('script');
  }, [selectedArticle?.id]);

  useEffect(() => {
    progressEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [progressSteps.length]);

  const handleInputChange = (name: string, value: string) => {
    setDynamicInputs((prev) => ({ ...prev, [name]: value }));
  };

  const resetExec = () => {
    setDynamicInputs({});
    setProgressSteps([]);
    setExecDone(false);
    setExecSuccess(false);
    setExecuting(false);
    setExecutionResult(null);
    setWriteConfirmed(false);
  };

  const handleExecute = () => {
    for (const field of executionFields) {
      const validationError = validateExecutionFieldValue(field, dynamicInputs[field.name]);
      if (validationError) { alert(validationError); return; }
    }

    if (requiresWriteConfirmation && !writeConfirmed) {
      alert('Esta acción modificará datos. Debes confirmar para continuar.');
      return;
    }

    setExecuting(true);
    setProgressSteps([]);
    setExecDone(false);
    setExecSuccess(false);
    setExecutionResult(null);

    const paramsPayload = {
      ...buildNormalizedParamsPayload(executionFields, dynamicInputs),
      __write_confirmed: requiresWriteConfirmation ? (writeConfirmed ? 'true' : 'false') : 'false',
    };
    const paramsStr = encodeURIComponent(JSON.stringify(paramsPayload));
    const extraQuery = new URLSearchParams();
    if (executionTarget.databaseId) extraQuery.set('databaseId', executionTarget.databaseId);
    // Leer usuario en el momento de ejecutar (no en mount) para capturar cambios de cuenta
    const execUser = resolveCurrentUser();
    if (execUser?.username) extraQuery.set('username', execUser.username);
    if (execUser?.role) extraQuery.set('userRole', execUser.role);

    const url = `/api/execute/stream/${selectedArticle!.id}?params=${paramsStr}${extraQuery.toString() ? `&${extraQuery.toString()}` : ''}`;

    const parsedScript = parseStructuredScript(selectedArticle?.script_json);
    const sqlPreview = String((parsedScript as Record<string, unknown> | null)?.sql || '').trim();
    console.info('[ContentPanel][execute] dispatch', { articleId: selectedArticle?.id, databaseId: executionTarget.databaseId || '', sql: sqlPreview.slice(0, 500), url });

    const es = new EventSource(url);
    esRef.current = es;
    let closed = false;

    const closeStream = () => {
      if (!closed) {
        closed = true;
        es.close();
        esRef.current = null;
      }
    };

    es.addEventListener('progress', (e: MessageEvent) => {
      try {
        const data = JSON.parse(e.data);
        setProgressSteps((prev) => [...prev, { message: data.message, type: 'progress' }]);
      } catch {
        // Ignorar eventos malformados
      }
    });

    es.addEventListener('execution-error', (e: MessageEvent) => {
      if (e.data) {
        try {
          const data = JSON.parse(e.data);
          setProgressSteps((prev) => [...prev, { message: data.message, type: 'error' }]);
        } catch {}
      }
    });

    es.addEventListener('done', (e: MessageEvent) => {
      // CRÍTICO: sin try/catch aquí → JSON.parse puede lanzar y dejar executing=true para siempre
      try {
        const data = JSON.parse(e.data);
        setExecuting(false);
        setExecDone(true);
        setExecSuccess(data.success);
        setExecutionResult(data.result || null);
      } catch {
        setExecuting(false);
        setExecDone(true);
        setExecSuccess(false);
        setProgressSteps((prev) => [...prev, { message: '⚠️ Respuesta del servidor malformada', type: 'error' }]);
      } finally {
        closeStream();
      }
    });

    es.onerror = () => {
      if (closed) return;
      setExecuting(false);
      setExecDone(true);
      setExecSuccess(false);
      setExecutionResult(null);
      setProgressSteps((prev) => [...prev, { message: '❌ Error de conexión con el servidor', type: 'error' }]);
      closeStream();
    };
  };

  if (loading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-[#25295c] mb-3"></div>
          <p className="text-(--ink-700)">Cargando...</p>
        </div>
      </div>
    );
  }

  if (!selectedArticle) {
    return (
      <div className="flex-1 flex items-center justify-center p-6">
        <div className="text-center">
          <svg className="w-16 h-16 text-(--ink-500) mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
          <h2 className="text-lg font-semibold text-(--ink-900)">Selecciona una categoría</h2>
          <p className="text-sm text-(--ink-700) mt-1">Elige un problema del menú para ver los detalles</p>
        </div>
      </div>
    );
  }

  const mdPanelClass = isLightTheme
    ? 'dashboard-panel dashboard-elevate rounded-2xl border border-sky-200 bg-white/90 p-6 mb-6 overflow-hidden'
    : 'dashboard-panel dashboard-elevate rounded-2xl border border-slate-700/70 bg-slate-950/55 p-6 mb-6 overflow-hidden';
  const mdPanelSubtitleClass = isLightTheme ? 'text-[11px] text-slate-500' : 'text-[11px] text-slate-400';
  const mdInlineCodeClass = isLightTheme
    ? 'inline-flex items-center rounded-md border border-sky-200 bg-sky-50 px-2 py-1 text-sm font-mono text-sky-800'
    : 'inline-flex items-center rounded-md border border-cyan-500/30 bg-cyan-500/10 px-2 py-1 text-sm font-mono text-cyan-200';
  const mdBlockquoteClass = isLightTheme
    ? 'my-4 border-l-4 border-sky-500 bg-sky-50/70 pl-4 italic text-slate-700'
    : 'my-4 border-l-4 border-cyan-400 bg-slate-900/70 pl-4 italic text-slate-300';

  return (
    <div className="flex-1 overflow-y-auto">
      <div className="p-4 w-full">
        {/* Article header */}
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-(--ink-900) mb-2">{selectedArticle.titulo}</h1>
          {selectedArticle.tags && selectedArticle.tags.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-2">
              {selectedArticle.tags.map((tag, idx) => (
                <span key={idx} className="glass-pill inline-flex items-center px-3 py-1 rounded-full text-xs font-medium text-(--ink-800)">
                  #{tag}
                </span>
              ))}
            </div>
          )}
        </div>

        {/* Markdown solution */}
        {(selectedArticle.contenido_md || selectedArticle.contenido) && (
          <div className={mdPanelClass}>
            <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
              <div>
                <p className={`text-xs font-semibold uppercase tracking-[0.18em] ${isLightTheme ? 'text-sky-700' : 'text-cyan-300'}`}>
                  Soluciones MD
                </p>
                <h2 className="dashboard-gradient-text mt-1 text-lg font-semibold">Solución</h2>
                <p className={`mt-1 ${mdPanelSubtitleClass}`}>Mismo estilo visual del panel SQL automático.</p>
              </div>
              <span className={`rounded-full border px-2.5 py-1 text-[10px] font-semibold ${isLightTheme ? 'border-sky-200 bg-sky-50 text-sky-700' : 'border-cyan-500/30 bg-cyan-500/10 text-cyan-200'}`}>
                Markdown
              </span>
            </div>
            <div className="prose prose-sm max-w-none text-(--ink-800)">
              <ReactMarkdown
                components={{
                  h1: ({ children }) => <h1 className="text-2xl font-bold text-(--ink-900) mb-4 mt-6">{children}</h1>,
                  h2: ({ children }) => <h2 className="text-xl font-semibold text-(--ink-900) mb-3 mt-5">{children}</h2>,
                  h3: ({ children }) => <h3 className="text-lg font-medium text-(--ink-900) mb-2 mt-4">{children}</h3>,
                  p: ({ children }) => <p className="text-(--ink-800) mb-3 leading-relaxed">{children}</p>,
                  ul: ({ children }) => <ul className="list-disc list-inside space-y-2 mb-4 text-(--ink-800)">{children}</ul>,
                  ol: ({ children }) => <ol className="list-decimal list-inside space-y-2 mb-4 text-(--ink-800)">{children}</ol>,
                  li: ({ children }) => <li className="ml-2">{children}</li>,
                  code: ({ className, children }) => {
                    const text = String(children);
                    const isBlockCode = Boolean(className) || text.includes('\n');
                    if (!isBlockCode) return <code className={mdInlineCodeClass}>{children}</code>;
                    return <code className={className}>{children}</code>;
                  },
                  pre: ({ children }) => (
                    <pre className="bg-[#16294e] p-4 rounded-lg overflow-x-auto text-sm font-mono text-[#e9f0ff] mb-4">{children}</pre>
                  ),
                  blockquote: ({ children }) => (
                    <blockquote className={mdBlockquoteClass}>{children}</blockquote>
                  ),
                }}
              >
                {selectedArticle.contenido_md || selectedArticle.contenido}
              </ReactMarkdown>
            </div>
          </div>
        )}

        {/* Execute section */}
        {isExecutableArticle && (
          <div className="glass-panel glass-panel-hover rounded-xl p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-sm font-bold text-(--ink-900) uppercase tracking-wide">🔧 Ejecutar Solución</h2>
              {execDone && (
                <button onClick={resetExec} className="text-xs text-(--accent-strong) hover:text-(--ink-900) font-medium glass-pill px-3 py-1 rounded-lg transition-colors">
                  🔄 Reiniciar
                </button>
              )}
            </div>

            {/* Tab toggle: Script / Ejecución */}
            {scriptPreviewText && !execDone && (
              <div className="flex gap-2 mb-5 glass-pill rounded-xl p-1.5 w-fit">
                <button
                  onClick={() => setExecView('script')}
                  className={`px-5 py-2 rounded-lg text-sm font-semibold transition-colors ${execView === 'script' ? 'bg-(--accent) text-white shadow-xs' : 'text-(--ink-700) hover:text-(--ink-900)'}`}
                >
                  📋 Ver Script
                </button>
                <button
                  onClick={() => setExecView('execute')}
                  className={`px-5 py-2 rounded-lg text-sm font-semibold transition-colors ${execView === 'execute' ? 'bg-(--accent) text-white shadow-xs' : 'text-(--ink-700) hover:text-(--ink-900)'}`}
                >
                  ▶ Ejecutar
                </button>
              </div>
            )}

            {/* Script preview panel */}
            {execView === 'script' && scriptPreviewText && !execDone && (
              <div className="mb-5 rounded-lg border border-slate-700 bg-[#1e1e2e] overflow-hidden">
                <div className="px-3 py-2 bg-[#2a2a3d] border-b border-slate-700 text-xs font-bold uppercase tracking-wide text-slate-300 flex items-center justify-between">
                  <span>📋 Script configurado</span>
                  <button onClick={() => setExecView('execute')} className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-blue-600 hover:bg-blue-500 text-white transition-colors shadow-xs">
                    ▶ Ir a ejecutar
                  </button>
                </div>
                <pre className="p-4 text-xs leading-relaxed font-mono text-[#cdd6f4] overflow-x-auto whitespace-pre">
                  {scriptPreviewText}
                </pre>
              </div>
            )}

            {/* Execution panel */}
            {(execView === 'execute' || execDone) && (
            <div>

            {businessScriptDetails && (
              <div className="mb-5 rounded-lg glass-panel p-3">
                <p className="text-xs font-bold uppercase tracking-wide text-(--ink-900) mb-1">Script</p>
                {businessScriptDetails.script && (
                  <p className="text-sm text-(--ink-800) mb-1"><span className="font-semibold">Proceso:</span> {businessScriptDetails.script}</p>
                )}
                {businessScriptDetails.descripcion && (
                  <p className="text-sm text-(--ink-700) mb-2">{businessScriptDetails.descripcion}</p>
                )}
                {businessScriptDetails.pasos.length > 0 && (
                  <ul className="space-y-2">
                    {businessScriptDetails.pasos.map((step) => (
                      <li key={`business-step-${step.orden}`} className="rounded-sm glass-panel p-2">
                        <p className="text-sm text-(--ink-900) font-medium">Paso {step.orden}: {step.descripcion}</p>
                        <p className="text-xs text-(--ink-700)">Acción: {step.accion}</p>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}

            {/* Dynamic form fields */}
            {!execDone && executionFields.length > 0 && (
              <div className="space-y-3 mb-5">
                {hasStructuredScript && (
                  <div className="rounded-lg glass-panel px-3 py-2 text-xs text-(--ink-700)">
                    Los campos se generaron automáticamente desde el script_json del artículo.
                  </div>
                )}
                {executionFields.map((field: any) => (
                  <div key={field.name}>
                    <label className="block text-sm font-medium text-(--ink-900) mb-1">
                      {field.label}
                      {field.required && <span className="text-red-500 ml-1">*</span>}
                    </label>
                    {field.type === 'textarea' ? (
                      <textarea value={dynamicInputs[field.name] || ''} onChange={(e) => handleInputChange(field.name, e.target.value)} placeholder={`Ingresa ${field.label.toLowerCase()}`} disabled={executing} rows={3} className="input-glass w-full px-3 py-2 rounded-lg text-sm disabled:opacity-70 resize-none" />
                    ) : field.type === 'checkbox' ? (
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input type="checkbox" checked={dynamicInputs[field.name] === 'true' || false} onChange={(e) => handleInputChange(field.name, e.target.checked ? 'true' : 'false')} disabled={executing} className="w-4 h-4 text-[#2363eb] rounded-sm border-[#cfe0ff] focus:ring-2 focus:ring-[#2363eb] disabled:opacity-50" />
                        <span className="text-sm text-(--ink-800)">{field.label}</span>
                      </label>
                    ) : (
                      <input type={field.type || 'text'} value={dynamicInputs[field.name] || ''} onChange={(e) => handleInputChange(field.name, e.target.value)} placeholder={`Ingresa ${field.label.toLowerCase()}`} disabled={executing} className="input-glass w-full px-3 py-2 rounded-lg text-sm disabled:opacity-70" />
                    )}
                  </div>
                ))}
              </div>
            )}

            {!execDone && executionFields.length === 0 && (
              <div className="mb-5 text-sm text-(--ink-700) glass-panel rounded-lg p-3">
                Esta acción no requiere parámetros de entrada.
              </div>
            )}

            {!execDone && requiresWriteConfirmation && (
              <div className="mb-5 rounded-lg border border-amber-200 bg-amber-50 px-3 py-3 text-sm text-amber-900">
                <p className="font-semibold">Esta acción modificará datos en la base de datos.</p>
                <label className="mt-2 flex items-center gap-2 cursor-pointer">
                  <input type="checkbox" checked={writeConfirmed} onChange={(e) => setWriteConfirmed(e.target.checked)} disabled={executing} className="h-4 w-4 rounded-sm border-amber-300" />
                  <span>Confirmo que deseo continuar con UPDATE/DELETE/INSERT</span>
                </label>
              </div>
            )}

            {!execDone && (
              <div className="mb-5 rounded-lg glass-panel px-3 py-2 text-xs text-(--ink-700)">
                {preExecutionSummary}
              </div>
            )}

            {/* Real-time progress log */}
            {(humanWorkflowSteps.length > 0 || Boolean(latestVisibleError)) && (
              <div className="mb-4 glass-panel rounded-lg p-4">
                <p className="text-xs font-bold text-(--ink-900) uppercase tracking-wide mb-2">Progreso del proceso</p>
                <div className="max-h-64 overflow-y-auto pr-1">
                  <div className="space-y-3">
                    {humanWorkflowSteps.map((step, index) => {
                      const tone = getTimelineToneClasses(step.tone);
                      const isDone = execDone && execSuccess;
                      const isCurrent = !execDone && index === humanWorkflowSteps.length - 1;
                      const isCompleted = isDone || (!execDone && index < humanWorkflowSteps.length - 1);
                      return (
                        <div key={`${step.key}-${index}`} className={`rounded-lg border ${tone.row} px-4 py-3`}>
                          <p className={`text-sm font-semibold ${tone.title}`}>{step.icon} Paso {index + 1}: {step.startLabel}</p>
                          <p className={`mt-1 text-sm ${isCompleted || isDone ? 'text-emerald-700' : 'text-(--ink-700)'}`}>
                            {isCompleted || isDone ? `✅ ${step.completeLabel}` : isCurrent ? '⏳ En proceso' : '⏳ Pendiente'}
                          </p>
                        </div>
                      );
                    })}

                    {latestVisibleError && (
                      <div className="rounded-lg border border-red-200 bg-red-50/80 px-4 py-3">
                        <p className="text-sm font-semibold text-red-800">❌ No se pudo completar el proceso</p>
                        <p className="mt-1 text-sm text-red-700">{latestVisibleError.message}</p>
                      </div>
                    )}
                  </div>
                  <div ref={progressEndRef} />
                </div>

                {executing && (
                  <div className="mt-2 text-sm text-(--ink-700) flex items-center gap-2">
                    <span className="inline-block animate-spin">⏳</span>
                    <span>Procesando...</span>
                  </div>
                )}
              </div>
            )}

            {/* Final result */}
            {execDone && (
              <div className={`mb-2 p-3 rounded-lg text-sm font-semibold ${execSuccess ? 'border border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-200' : 'bg-red-50 text-red-800 border border-red-200'}`}>
                {execSuccess ? '✅ Ejecución completada exitosamente' : '❌ La ejecución encontró errores'}
              </div>
            )}

            {execDone && (() => {
              const lines = humanExecutionExplanation.split('\n').map((l: string) => l.trim()).filter(Boolean);
              const isNoticeLog = lines.length > 1;
              if (isNoticeLog) {
                return (
                  <div className="mb-4 rounded-lg border border-slate-700 overflow-hidden" style={{ background: 'rgba(15,23,42,0.85)' }}>
                    <div className="px-3 py-2 border-b border-slate-700 text-[10px] font-bold uppercase tracking-wide text-slate-400" style={{ background: 'rgba(255,255,255,0.03)' }}>
                      📋 Resumen del resultado
                    </div>
                    <div style={{ padding: '10px 14px', fontFamily: 'monospace', fontSize: '11px', lineHeight: '1.8', overflowX: 'auto' }}>
                      {lines.map((line: string, i: number) => {
                        const isSep = /^={3,}/.test(line);
                        const isWarn = /^[⚠✖]/.test(line);
                        const isOk = /^[✔✅]/.test(line);
                        const color = isSep ? 'rgba(148,163,184,0.4)' : isWarn ? '#fbbf24' : isOk ? '#4ade80' : '#7dd3fc';
                        return (
                          <div key={i} style={{ display: 'flex', gap: '8px', color }}>
                            {!isSep && <span style={{ color: '#94a3b8', flexShrink: 0 }}>›</span>}
                            <span>{line}</span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              }
              return (
                <div className="mb-4 p-3 rounded-lg text-sm bg-white border border-slate-200 text-slate-700 dark:bg-slate-900/80 dark:border-slate-700 dark:text-slate-200">
                  <span className="font-semibold text-slate-900 dark:text-slate-100">Resumen del resultado:</span>{' '}
                  {humanExecutionExplanation}
                </div>
              );
            })()}

            {execDone && execSuccess && executionFindings.length > 0 && (
              <div className="mb-4 rounded-lg border border-slate-200 bg-white overflow-hidden dark:bg-slate-900/80 dark:border-slate-700">
                <div className="px-3 py-2 bg-slate-100 border-b border-slate-200 text-xs font-bold uppercase tracking-wide text-slate-800 dark:bg-slate-800 dark:border-slate-700 dark:text-slate-100">
                  Información encontrada
                </div>
                <ul className="p-3 space-y-2 text-sm text-slate-700 dark:text-slate-200">
                  {executionFindings.map((finding, index) => (
                    <li key={index} className="flex items-start gap-2">
                      <span className="text-emerald-600 mt-0.5">•</span>
                      <span>{finding}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {execDone && annotatedSqlScript && (
              <details className="mb-4 rounded-lg border border-slate-700 bg-[#1e1e2e] overflow-hidden">
                <summary className="px-3 py-2 bg-[#2a2a3d] border-b border-slate-700 text-xs font-bold uppercase tracking-wide text-slate-300 cursor-pointer select-none">
                  📋 Ver SQL ejecutado
                </summary>
                <pre className="p-4 text-xs leading-relaxed font-mono text-[#cdd6f4] overflow-x-auto whitespace-pre">
                  {annotatedSqlScript}
                </pre>
              </details>
            )}

            {execDone && execSuccess && (() => {
              const res = executionResult;
              const rows = Array.isArray(res?.resultado) ? res.resultado as Record<string, unknown>[] : [];
              const hasRows = rows.length > 0;
              const resumen = String(res?.resumenHumano || '').trim();
              const isNoticeOutput = !hasRows && resumen.includes('\n');
              if (isNoticeOutput) return null;
              if (!hasRows) return (
                <div className="mb-4 rounded-lg border border-slate-200 bg-slate-50/80 dark:bg-slate-900/50 dark:border-slate-700 px-4 py-3 flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
                  <span>ℹ️</span>
                  <span>Esta operación no devolvió filas tabulares. La descarga CSV y los totales aparecen cuando el resultado es un <span className="font-semibold">SELECT con filas</span>.</span>
                </div>
              );

              const cols = Object.keys(rows[0] || {});
              const isDbmsOutput = cols.length === 1 && cols[0] === 'output';
              if (isDbmsOutput) {
                const lines = rows.map((r) => String(r.output ?? '')).filter(Boolean);
                return (
                  <div className="mb-4 rounded-lg border border-orange-900/40 overflow-hidden" style={{ background: 'rgba(15,10,5,0.9)' }}>
                    <div className="px-3 py-2 border-b border-orange-900/30 text-[10px] font-bold uppercase tracking-wide text-orange-400/80" style={{ background: 'rgba(255,255,255,0.02)' }}>
                      🔶 DBMS_OUTPUT
                    </div>
                    <div style={{ padding: '10px 14px', fontFamily: 'monospace', fontSize: '11px', lineHeight: '1.8', overflowX: 'auto' }}>
                      {lines.map((line, i) => {
                        const isSep = /^={3,}|-{3,}/.test(line);
                        const isWarn = /error|warning|fail|✖|⚠/i.test(line);
                        const isOk = /success|ok|✔|✅|completed/i.test(line);
                        const color = isSep ? 'rgba(180,140,100,0.4)' : isWarn ? '#fbbf24' : isOk ? '#4ade80' : '#fcd9a0';
                        return (
                          <div key={i} style={{ display: 'flex', gap: '8px', color }}>
                            {!isSep && <span style={{ color: 'rgba(180,100,30,0.7)', flexShrink: 0 }}>DBMS_OUTPUT:</span>}
                            <span>{line}</span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              }

              // ── Sumar columnas numéricas ──────────────────────────────
              const numericCols = cols.filter((col) =>
                rows.every((r) => {
                  const v = r[col];
                  return v === null || v === undefined || v === '' || !isNaN(Number(v));
                }) && rows.some((r) => r[col] !== null && r[col] !== undefined && r[col] !== '' && !isNaN(Number(r[col])))
              );
              const totals: Record<string, number> = {};
              numericCols.forEach((col) => {
                totals[col] = rows.reduce((acc, r) => acc + (Number(r[col]) || 0), 0);
              });
              const hasTotals = numericCols.length > 0;

              // ── Descargar Excel ───────────────────────────────────────
              const downloadExcel = async () => {
                const XLSX = await import('xlsx');
                const dataRows: Record<string, string | number>[] = rows.map((r) =>
                  Object.fromEntries(cols.map((c) => [c, r[c] ?? ''])) as Record<string, string | number>
                );
                // Auditoría de descarga Excel — fire-and-forget
                const auditUser = resolveCurrentUser();
                fetch('/api/audit/event', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({
                    evento: 'excel_descarga',
                    username: auditUser?.username || 'anonymous',
                    rol: auditUser?.role || 'user',
                    detalle: {
                      articulo_id: selectedArticle?.id,
                      articulo_titulo: selectedArticle?.titulo,
                      filas: rows.length,
                      columnas: cols.length,
                      archivo: `resultado_${new Date().toISOString().slice(0, 10)}.xlsx`,
                    },
                  }),
                }).catch(() => {});
                if (hasTotals) {
                  const totalsRow: Record<string, string | number> = {};
                  cols.forEach((c, idx) => {
                    if (idx === 0 && !numericCols.includes(c)) totalsRow[c] = 'TOTAL';
                    else if (numericCols.includes(c)) totalsRow[c] = totals[c];
                    else totalsRow[c] = '';
                  });
                  dataRows.push(totalsRow);
                }
                const ws = XLSX.utils.json_to_sheet(dataRows, { header: cols });
                // Ancho automático de columnas
                const colWidths = cols.map((c) => ({
                  wch: Math.max(c.length, ...rows.slice(0, 50).map((r) => String(r[c] ?? '').length), 10),
                }));
                ws['!cols'] = colWidths;
                const wb = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(wb, ws, 'Resultado');
                XLSX.writeFile(wb, `resultado_${new Date().toISOString().slice(0, 10)}.xlsx`);
              };

              return (
                <div className="mb-4 rounded-lg border border-slate-200 bg-white overflow-hidden dark:bg-slate-900/80 dark:border-slate-700">
                  {/* Header con conteo y botón descargar */}
                  <div className="px-3 py-2 bg-slate-100 border-b border-slate-200 dark:bg-slate-800 dark:border-slate-700 flex items-center justify-between gap-2 flex-wrap">
                    <span className="text-xs font-bold uppercase tracking-wide text-slate-800 dark:text-slate-100">
                      Resultado final
                      <span className="ml-2 font-normal text-slate-500 dark:text-slate-400 normal-case tracking-normal">
                        ({rows.length} {rows.length === 1 ? 'fila' : 'filas'}{rows.length > 50 ? ', mostrando 50' : ''})
                      </span>
                    </span>
                    <button
                      onClick={downloadExcel}
                      title="Descargar Excel"
                      className="flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-semibold transition-colors bg-emerald-600 hover:bg-emerald-700 text-white shadow-xs"
                    >
                      <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                      </svg>
                      Descargar Excel
                    </button>
                  </div>

                  {/* Tabla responsive con scroll horizontal Y vertical */}
                  <div className="overflow-x-auto overflow-y-auto max-h-[420px]">
                    <table className="min-w-full text-xs border-collapse">
                      <thead className="sticky top-0 z-10">
                        <tr className="bg-slate-50 dark:bg-slate-800">
                          {cols.map((col) => (
                            <th key={col} className="px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400 whitespace-nowrap border-b border-slate-200 dark:border-slate-700">
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {rows.slice(0, 50).map((row, i) => (
                          <tr key={i} className={`border-t border-slate-100 dark:border-slate-700 ${i % 2 === 0 ? 'bg-white dark:bg-slate-900/60' : 'bg-slate-50/60 dark:bg-slate-800/40'}`}>
                            {cols.map((col) => (
                              <td key={col} className="px-3 py-2 text-slate-700 dark:text-slate-200 whitespace-nowrap max-w-[240px] truncate" title={String(row[col] ?? '')}>
                                {String(row[col] ?? '')}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                      {/* Fila de totales */}
                      {hasTotals && (
                        <tfoot className="sticky bottom-0">
                          <tr className="bg-slate-100 dark:bg-slate-800 border-t-2 border-slate-300 dark:border-slate-600">
                            {cols.map((col, idx) => (
                              <td key={col} className="px-3 py-2 text-xs font-bold whitespace-nowrap text-slate-800 dark:text-slate-100">
                                {idx === 0 && !numericCols.includes(col)
                                  ? <span className="text-[10px] uppercase tracking-wide text-slate-500 dark:text-slate-400">TOTAL</span>
                                  : numericCols.includes(col)
                                    ? new Intl.NumberFormat('es-EC', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(totals[col])
                                    : ''}
                              </td>
                            ))}
                          </tr>
                        </tfoot>
                      )}
                    </table>
                  </div>
                </div>
              );
            })()}

            {/* Execute button */}
            {!execDone && (
              <button onClick={handleExecute} disabled={executing} className="btn-accent w-full disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-4 px-4 rounded-xl text-base transition-colors duration-200 flex items-center justify-center gap-2 shadow-lg">
                {executing ? (<><span className="animate-spin text-lg">⏳</span>Ejecutando...</>) : (<><span className="text-lg">🔧</span>Ejecutar Solución</>)}
              </button>
            )}

            </div>
            )}
          </div>
        )}

        {/* Footer — Auditoría */}
        <div className="mt-6 pt-4 border-t border-(--line) flex flex-wrap gap-x-6 gap-y-1 text-xs text-(--ink-600)">
          {selectedArticle.creado_por_username && (
            <span>✍️ Creado por <span className="font-semibold text-(--ink-800)">{selectedArticle.creado_por_username}</span></span>
          )}
          {selectedArticle.creado_en && (() => {
            const d = new Date(selectedArticle.creado_en);
            return isNaN(d.getTime()) ? null : (
              <span>📅 {d.toLocaleDateString('es-ES', { day: '2-digit', month: 'short', year: 'numeric' })}</span>
            );
          })()}
          {selectedArticle.actualizado_por && (
            <span>🔄 Editado por <span className="font-semibold text-(--ink-800)">{selectedArticle.actualizado_por}</span></span>
          )}
          {selectedArticle.actualizado_en && (() => {
            const d = new Date(selectedArticle.actualizado_en as string);
            return isNaN(d.getTime()) ? null : (
              <span>🕐 {d.toLocaleDateString('es-ES', { day: '2-digit', month: 'short', year: 'numeric' })}{' '}{d.toLocaleTimeString('es-ES', { hour: '2-digit', minute: '2-digit' })}</span>
            );
          })()}
          {selectedArticle.visible_para && selectedArticle.visible_para !== 'todos' && (
            <span>{selectedArticle.visible_para === 'admin' ? '🔒 Solo admins' : '👤 Solo usuarios'}</span>
          )}
        </div>
      </div>
    </div>
  );
}
