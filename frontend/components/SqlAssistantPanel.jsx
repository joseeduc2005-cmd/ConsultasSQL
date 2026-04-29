'use client';

import React, { useEffect, useRef, useState } from 'react';

const QUICK_SUGGESTIONS = [
  'logs activos',
  'usuarios con sesiones',
  'usuarios con logs',
  'usuarios activos',
];

function MiniBarChart({ data, labelKey, valueKey }) {
  const items = data.slice(0, 10);
  const maxVal = Math.max(...items.map((r) => Number(r[valueKey] ?? 0)), 1);
  const barW = Math.max(18, Math.floor(320 / Math.max(items.length, 1)) - 6);
  const chartH = 72;
  const COLORS = ['#22d3ee', '#38bdf8', '#0ea5e9', '#6366f1', '#8b5cf6', '#14b8a6', '#f59e0b', '#ef4444'];

  return (
    <div className="mt-3">
      <p className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-cyan-400">Visualizacion automatica</p>
      <div className="overflow-x-auto">
        <svg width={Math.max(320, items.length * (barW + 6))} height={chartH + 32} className="overflow-visible">
          {items.map((row, i) => {
            const val = Number(row[valueKey] ?? 0);
            const barH = Math.max(3, Math.round((val / maxVal) * chartH));
            const x = i * (barW + 6);
            return (
              <g key={i}>
                <rect x={x} y={chartH - barH} width={barW} height={barH} rx={4} fill={COLORS[i % COLORS.length]} opacity={0.85} />
                <text x={x + barW / 2} y={chartH + 13} textAnchor="middle" fontSize={9} fill="#64748b">
                  {String(row[labelKey] ?? '').slice(0, 7)}
                </text>
                <text x={x + barW / 2} y={chartH - barH - 4} textAnchor="middle" fontSize={9} fill="#e2e8f0">
                  {val}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}

function detectChart(data) {
  if (!data || data.length < 1) return null;
  const keys = Object.keys(data[0]);
  const sample = data.slice(0, Math.min(12, data.length));

  const pickPreferred = (candidates, preferredPatterns) => {
    for (const pattern of preferredPatterns) {
      const found = candidates.find((k) => pattern.test(String(k || '').toLowerCase()));
      if (found) return found;
    }
    return candidates[0] || null;
  };

  const numericKeys = keys.filter((k) => {
    let numericCount = 0;
    for (const row of sample) {
      const val = row?.[k];
      if (val !== null && val !== undefined && val !== '' && !isNaN(Number(val))) numericCount += 1;
    }
    return numericCount >= Math.ceil(sample.length * 0.6);
  });

  const labelKeys = keys.filter((k) => !numericKeys.includes(k));

  if (numericKeys.length >= 1 && labelKeys.length >= 1) {
    const preferredLabel = pickPreferred(labelKeys, [
      /(username|user_name|nombre|name|email|correo|title|titulo|descripcion|detalle)/,
      /(id)$/,
    ]);
    const preferredValue = pickPreferred(numericKeys, [
      /(total|count|cantidad|score|monto|importe|price|amount|activo|enabled|status)/,
      /(id)$/,
    ]);
    if (preferredLabel && preferredValue) {
      return { labelKey: preferredLabel, valueKey: preferredValue };
    }
  }

  // Single-row fallback: if there is no numeric field, show no chart.
  // If there is one numeric field and one label field, chart is still useful.
  if (sample.length === 1 && numericKeys.length >= 1 && labelKeys.length >= 1) {
    return { labelKey: labelKeys[0], valueKey: numericKeys[0] };
  }

  return null;
}

function hasDebugResult(payload) {
  return Boolean(payload && typeof payload === 'object' && payload.resultado !== undefined);
}

function extractResultadoData(payload) {
  if (!payload || typeof payload !== 'object') return { rows: [], total: 0, entidad: '', isNewFormat: false };

  // Nuevo formato: resultado es array directamente
  if (Array.isArray(payload?.resultado)) {
    const meta = payload?.metadata || {};
    return {
      rows: payload.resultado,
      total: Number(meta.total ?? payload.resultado.length ?? 0),
      entidad: String(meta.entidad || '').trim(),
      isNewFormat: true,
    };
  }

  // Formato legacy: resultado es objeto
  const resultado = payload?.resultado && typeof payload.resultado === 'object' && !Array.isArray(payload.resultado)
    ? payload.resultado
    : null;

  const looksLikeDebugObject = Boolean(resultado && (
    Object.prototype.hasOwnProperty.call(resultado, 'tipo_error')
    || Object.prototype.hasOwnProperty.call(resultado, 'flujo_detectado')
    || Object.prototype.hasOwnProperty.call(resultado, 'traza')
    || Object.prototype.hasOwnProperty.call(resultado, 'solucion')
    || Object.prototype.hasOwnProperty.call(resultado, 'causa')
  ));

  const rows = Array.isArray(resultado?.data)
    ? resultado.data
    : (resultado && !looksLikeDebugObject ? [resultado] : []);
  const total = Number(resultado?.total || rows.length || 0);
  const entidad = String(resultado?.entidad || '').trim();
  return { rows, total, entidad, isNewFormat: false, looksLikeDebugObject };
}

function extractClassicRows(payload) {
  if (!payload || typeof payload !== 'object') return [];
  if (Array.isArray(payload.data)) return payload.data;
  if (Array.isArray(payload.rows)) return payload.rows;
  return [];
}

function normalizeExecutionPayload(payload = {}, generatedSql = '') {
  if (hasDebugResult(payload)) {
    const resultadoData = extractResultadoData(payload);

    // Nuevo formato (resultado array): nunca tratar como debug
    if (resultadoData.isNewFormat) {
      return {
        success: payload?.success !== false,
        mode: 'sql',
        resumenHumano: String(payload?.resumenHumano || '').trim(),
        resultado: payload?.resultado,
        metadata: payload?.metadata || {},
        data: resultadoData.rows,
        rowCount: resultadoData.total,
        entidad: resultadoData.entidad,
        executionType: String(payload?.metadata?.executionType || ''),
        sources: Array.isArray(payload?.metadata?.sources) ? payload.metadata.sources : [],
        confidence: Number(payload?.metadata?.confidence ?? 0),
        sql: generatedSql || '',
      };
    }

    // Formato legacy: resultado objeto
    if (resultadoData.rows.length > 0) {
      return {
        success: true,
        mode: 'sql',
        resumenHumano: String(payload?.resumenHumano || '').trim(),
        resultado: payload?.resultado,
        data: resultadoData.rows,
        rowCount: resultadoData.total,
        sql: generatedSql || '',
      };
    }

    // Objeto resultado vacío que puede ser debug
    const isDebugObj = resultadoData.looksLikeDebugObject;
    return {
      success: true,
      mode: isDebugObj ? 'debug' : 'sql',
      resumenHumano: String(payload?.resumenHumano || '').trim(),
      resultado: payload?.resultado,
      data: [],
      rowCount: 0,
      sql: generatedSql || '',
    };
  }

  const rows = extractClassicRows(payload);
  return {
    ...payload,
    success: payload?.success !== false,
    mode: 'sql',
    data: rows,
    rowCount: Number(payload?.rowCount || rows.length || 0),
    sql: generatedSql || payload?.sql || '',
  };
}

export default function SqlAssistantPanel({
  isOpen = false,
  onClose = () => {},
  onQuerySelected = (_query) => {},
}) {
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [generatedQuery, setGeneratedQuery] = useState(null);
  const [executionResult, setExecutionResult] = useState(null);
  const [understanding, setUnderstanding] = useState(null);
  const [fallbackSuggestions, setFallbackSuggestions] = useState([]);
  const [showUnderstanding, setShowUnderstanding] = useState(false);
  const [copied, setCopied] = useState(false);
  const [warning, setWarning] = useState('');
  const [lastGeneratedSQL, setLastGeneratedSQL] = useState('');
  const [lastTimestamp, setLastTimestamp] = useState(0);
  const [isMobile, setIsMobile] = useState(false);
  const [lastSearch, setLastSearch] = useState('');
  const [databaseMode, setDatabaseMode] = useState('auto');
  const textareaRef = useRef(null);

  useEffect(() => {
    const checkMobile = () => setIsMobile(window.innerWidth < 768);
    checkMobile();
    window.addEventListener('resize', checkMobile);
    return () => window.removeEventListener('resize', checkMobile);
  }, []);

  useEffect(() => {
    if (isOpen && textareaRef.current) {
      setTimeout(() => textareaRef.current?.focus(), 120);
    }
  }, [isOpen]);

  const applySuggestion = (suggestion) => {
    setInput(suggestion);
    setError('');
    textareaRef.current?.focus();
  };

  const runQuery = async () => {
    const text = String(input || '').trim();
    if (!text) {
      setError('Escribe una consulta.');
      return;
    }

    setLastSearch(text); // Guardar la última búsqueda
    setLoading(true);
    setError('');
    setWarning('');
    setGeneratedQuery(null);
    setExecutionResult(null);
    setUnderstanding(null);
    setFallbackSuggestions([]);

    try {
      const token = localStorage.getItem('token');
      const role = localStorage.getItem('userRole') || 'user';
      const headers = {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        'x-user-role': role,
      };

      console.info('[sql-assistant][query] passthrough dispatch', { query: text });

      const genRes = await fetch('/api/query/generate', {
        method: 'POST',
        headers,
        body: JSON.stringify({ text, query: text, limit: 50, offset: 0, databaseMode }),
      });
      const genPayload = await genRes.json();

      if (genRes.ok && hasDebugResult(genPayload)) {
        setExecutionResult(normalizeExecutionPayload(genPayload));
        setGeneratedQuery(null);
        setUnderstanding(null);
        setFallbackSuggestions([]);
        return;
      }

      if (!genRes.ok || !genPayload?.success) {
        // UUID sin entidad u otro error con sugerencias: mostrar guía amigable en vez de error rojo
        if (genPayload?.error && Array.isArray(genPayload?.sugerencias)) {
          setFallbackSuggestions(genPayload.sugerencias);
          setWarning(genPayload.error);
          setUnderstanding(genPayload.debug || null);
          setLoading(false);
          return;
        }
        throw new Error(genPayload?.error || 'No se pudo generar SQL.');
      }

      const responseTimestamp = Number(genPayload?.timestamp || 0);
      if (responseTimestamp > 0 && responseTimestamp < lastTimestamp) {
        setWarning('Se recibio una respuesta atrasada. Vuelve a ejecutar la consulta.');
        return;
      }

      const query = genPayload.query || null;
      const nextSQL = String(query?.sql || '').trim();
      if (nextSQL && lastGeneratedSQL && nextSQL === lastGeneratedSQL) {
        setWarning('La consulta genero el mismo SQL que la anterior, pero se ejecutara de nuevo para refrescar resultados.');
      }

      setGeneratedQuery(query);
      setUnderstanding(genPayload.debug || null);
      setFallbackSuggestions(Array.isArray(genPayload.sugerencias) ? genPayload.sugerencias : []);
      setLastGeneratedSQL(nextSQL);
      setLastTimestamp(responseTimestamp || Date.now());

      if (!query?.sql) {
        return;
      }

      const execRes = await fetch('/api/query/execute-generated', {
        method: 'POST',
        headers,
        body: JSON.stringify({
          sql: query.sql,
          query: text,
          ...(query.databaseId ? { databaseId: query.databaseId } : {}),
          ...(databaseMode !== 'auto' ? { databaseHint: databaseMode } : {}),
        }),
      });
      const execPayload = await execRes.json();

      if (execRes.ok && hasDebugResult(execPayload)) {
        setExecutionResult(normalizeExecutionPayload(execPayload, query.sql));
        return;
      }

      if (!execRes.ok || execPayload?.success === false) {
        throw new Error(execPayload?.error || 'No se pudo ejecutar la consulta.');
      }
      setExecutionResult(normalizeExecutionPayload(execPayload, query.sql));
    } catch (err) {
      setError(err?.message || 'Error ejecutando consulta.');
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) runQuery();
  };

  const copySQL = () => {
    if (!generatedQuery?.sql) return;
    navigator.clipboard.writeText(generatedQuery.sql).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  const useInBuilder = () => {
    if (!generatedQuery?.sql) return;
    onQuerySelected(generatedQuery);
    onClose();
  };

  const reset = () => {
    setInput('');
    setError('');
    setWarning('');
    setGeneratedQuery(null);
    setExecutionResult(null);
    setUnderstanding(null);
    setFallbackSuggestions([]);
    setShowUnderstanding(false);
  };

  const isDebugResult = executionResult?.mode === 'debug';
  const debugResult = isDebugResult && executionResult ? executionResult.resultado || {} : null;
  const debugFlow = Array.isArray(debugResult?.flujo_detectado)
    ? debugResult.flujo_detectado
    : debugResult?.flujo_detectado
      ? [String(debugResult.flujo_detectado)]
      : [];
  const debugTrace = Array.isArray(debugResult?.traza) ? debugResult.traza : [];

  const chartConfig = executionResult?.data ? detectChart(executionResult.data) : null;
  const resultCols = executionResult?.data?.length > 0 ? Object.keys(executionResult.data[0]) : [];

  const wrapperClass = isMobile
    ? 'fixed inset-0 z-50 flex flex-col'
    : 'fixed bottom-28 right-6 z-50 w-[500px] max-w-[calc(100vw-24px)]';

  return (
    <>
      {isMobile && isOpen && (
        <div className="fixed inset-0 z-40 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      )}

      {isOpen && (
        <div className={wrapperClass}>
          <div
            className="flex flex-col overflow-hidden rounded-2xl"
            style={{
              background: 'linear-gradient(160deg, rgba(10,14,39,0.98) 0%, rgba(5,10,25,0.99) 100%)',
              border: '1px solid rgba(56,189,248,0.12)',
              boxShadow: '0 32px 80px rgba(0,0,0,0.6), 0 0 0 1px rgba(56,189,248,0.08), 0 0 60px rgba(56,189,248,0.04)',
            }}
          >
            <div
              className="flex flex-shrink-0 items-center justify-between px-5 py-4"
              style={{
                background: 'linear-gradient(90deg, rgba(56,189,248,0.08) 0%, rgba(99,102,241,0.06) 100%)',
                borderBottom: '1px solid rgba(255,255,255,0.05)',
              }}
            >
              <div className="flex items-center gap-3">
                <div
                  className="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-xl text-xs font-semibold"
                  style={{
                    background: 'linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)',
                    boxShadow: '0 4px 14px rgba(14,165,233,0.35)',
                  }}
                >
                  SQL
                </div>
                <div>
                  <p className="text-sm font-semibold text-white">Panel SQL Automatico</p>
                  <p className="text-[11px] text-slate-400">Motor deterministico | Multi-DB</p>
                </div>
              </div>
              <button
                type="button"
                onClick={onClose}
                className="flex h-7 w-7 items-center justify-center rounded-lg text-slate-400 transition-colors hover:bg-white/10 hover:text-white"
              >
                X
              </button>
            </div>

            <div className="flex-1 overflow-y-auto">
              <div className="space-y-4 p-5">
                <div className="flex flex-wrap gap-2">
                  {QUICK_SUGGESTIONS.map((s) => (
                    <button
                      key={s}
                      type="button"
                      onClick={() => applySuggestion(s)}
                      className="rounded-full px-3 py-1 text-xs font-medium text-cyan-300 transition-all duration-200 hover:text-cyan-100"
                      style={{ border: '1px solid rgba(34,211,238,0.25)', background: 'rgba(34,211,238,0.07)' }}
                    >
                      {s}
                    </button>
                  ))}
                </div>

                <div className="flex items-center gap-2">
                  <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Base de datos</span>
                  <div className="flex gap-1">
                    {['auto', 'oracle', 'postgres', 'mysql'].map((mode) => (
                      <button
                        key={mode}
                        type="button"
                        onClick={() => setDatabaseMode(mode)}
                        className="rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase transition-all duration-150"
                        style={{
                          border: databaseMode === mode
                            ? '1px solid rgba(56,189,248,0.6)'
                            : '1px solid rgba(255,255,255,0.08)',
                          background: databaseMode === mode
                            ? 'rgba(56,189,248,0.15)'
                            : 'rgba(255,255,255,0.03)',
                          color: databaseMode === mode ? '#38bdf8' : '#64748b',
                        }}
                      >
                        {mode}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="relative">
                  <textarea
                    ref={textareaRef}
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Ej: usuarios activos con sesiones"
                    rows={3}
                    className="w-full resize-none rounded-2xl px-4 py-3.5 text-sm text-white placeholder:text-slate-500 transition-all duration-200 focus:outline-none"
                    style={{
                      background: 'rgba(255,255,255,0.04)',
                      border: '1px solid rgba(255,255,255,0.08)',
                      boxShadow: 'inset 0 1px 4px rgba(0,0,0,0.3)',
                    }}
                  />
                  <div className="absolute bottom-3 right-3 select-none text-[10px] text-slate-600">Ctrl+Enter</div>
                </div>

                <button
                  type="button"
                  onClick={runQuery}
                  disabled={loading || !input.trim()}
                  className="relative w-full overflow-hidden rounded-2xl py-3 text-sm font-semibold text-white transition-all duration-200 active:scale-[0.98] disabled:cursor-not-allowed disabled:opacity-50"
                  style={{
                    background: loading || !input.trim()
                      ? 'rgba(56,189,248,0.2)'
                      : 'linear-gradient(135deg, #0ea5e9 0%, #3b82f6 50%, #6366f1 100%)',
                    boxShadow: loading || !input.trim()
                      ? 'none'
                      : '0 0 28px rgba(56,189,248,0.28), 0 4px 20px rgba(59,130,246,0.35)',
                  }}
                >
                  {loading ? 'Ejecutando consulta...' : 'Ejecutar consulta'}
                </button>

                {error && (
                  <div className="rounded-xl px-4 py-3 text-xs text-red-300" style={{ border: '1px solid rgba(239,68,68,0.3)', background: 'rgba(239,68,68,0.08)' }}>
                    Error: {error}
                  </div>
                )}

                {warning && (
                  <div className="rounded-xl px-4 py-3 text-xs text-amber-200" style={{ border: '1px solid rgba(245,158,11,0.3)', background: 'rgba(245,158,11,0.08)' }}>
                    {warning}
                  </div>
                )}

                {fallbackSuggestions.length > 0 && (warning || understanding?.confianza < 0.5) && (
                  <div className="rounded-xl px-4 py-3 text-xs text-amber-200" style={{ border: '1px solid rgba(245,158,11,0.3)', background: 'rgba(245,158,11,0.08)' }}>
                    <p className="font-semibold">Quisiste decir:</p>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {fallbackSuggestions.slice(0, 3).map((item) => (
                        <button
                          key={`fb-${item}`}
                          type="button"
                          onClick={() => applySuggestion(item)}
                          className="rounded-full border border-amber-300/40 bg-amber-300/10 px-2.5 py-1 text-[11px] text-amber-100"
                        >
                          {item}
                        </button>
                      ))}
                    </div>
                  </div>
                )}

                {generatedQuery?.sql && (
                  <div key={generatedQuery?.sql} className="overflow-hidden rounded-2xl" style={{ background: '#020617', border: '1px solid rgba(255,255,255,0.05)' }}>
                    <div className="flex items-center justify-between px-4 py-2.5" style={{ borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                      <div className="flex items-center gap-2">
                        <span className="h-2 w-2 rounded-full bg-cyan-400" style={{ boxShadow: '0 0 6px #22d3ee' }} />
                        <span className="text-[11px] font-semibold uppercase tracking-wider text-cyan-300">SQL generado</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="rounded-full px-2 py-0.5 text-[10px] font-medium text-cyan-300" style={{ border: '1px solid rgba(34,211,238,0.25)', background: 'rgba(34,211,238,0.08)' }}>
                          Generado automaticamente
                        </span>
                        <button type="button" onClick={copySQL} className="rounded-lg px-2.5 py-1 text-[10px] font-medium text-slate-300 transition-colors hover:text-white" style={{ border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(255,255,255,0.04)' }}>
                          {copied ? 'Copiado' : 'Copiar'}
                        </button>
                      </div>
                    </div>
                    <pre className="max-h-36 overflow-auto px-4 py-3.5 font-mono text-xs leading-relaxed text-green-300">{generatedQuery.sql}</pre>
                    <div className="flex flex-wrap items-center gap-2 px-4 py-2.5" style={{ borderTop: '1px solid rgba(255,255,255,0.04)' }}>
                      {(generatedQuery.tablaBase || generatedQuery.tabla) && (
                        <span className="rounded-full px-2.5 py-1 text-[10px] text-blue-300" style={{ border: '1px solid rgba(59,130,246,0.35)', background: 'rgba(59,130,246,0.1)' }}>
                          tabla: {generatedQuery.tablaBase || generatedQuery.tabla}
                        </span>
                      )}
                      {executionResult?.executionMs !== undefined && (
                        <span className="rounded-full px-2.5 py-1 text-[10px] text-emerald-300" style={{ border: '1px solid rgba(52,211,153,0.35)', background: 'rgba(52,211,153,0.08)' }}>
                          {executionResult.executionMs}ms
                        </span>
                      )}
                    </div>
                  </div>
                )}

                {understanding && (
                  <div className="overflow-hidden rounded-2xl" style={{ border: '1px solid rgba(255,255,255,0.05)', background: 'rgba(255,255,255,0.02)' }}>
                    <button
                      type="button"
                      onClick={() => setShowUnderstanding((v) => !v)}
                      className="flex w-full items-center justify-between px-4 py-3 text-left text-xs font-semibold text-cyan-200 transition-colors hover:bg-white/5"
                    >
                      <span>Ver como entendi tu consulta</span>
                      <span>{showUnderstanding ? 'Ocultar' : 'Ver'}</span>
                    </button>
                    {showUnderstanding && (
                      <div className="space-y-2 border-t border-white/5 px-4 py-3 text-[11px] text-slate-300">
                        <p><span className="font-semibold text-slate-100">Tabla base:</span> {understanding.tablaSeleccionada || 'N/A'}</p>
                        <p><span className="font-semibold text-slate-100">Tipo:</span> {understanding.tipoConsulta || 'N/A'}</p>
                        <p><span className="font-semibold text-slate-100">Score:</span> {Number(understanding.score || 0).toFixed(2)}</p>
                        <p><span className="font-semibold text-slate-100">Confianza:</span> {(Number(understanding.confianza || 0) * 100).toFixed(0)}%</p>
                        <p><span className="font-semibold text-slate-100">Joins:</span> {Array.isArray(understanding.joins) && understanding.joins.length > 0 ? understanding.joins.join(', ') : 'Sin joins'}</p>
                        <p><span className="font-semibold text-slate-100">Columnas usadas:</span> {Array.isArray(understanding.columnasUsadas) && understanding.columnasUsadas.length > 0 ? understanding.columnasUsadas.join(', ') : 'N/A'}</p>
                        <p><span className="font-semibold text-slate-100">Filtros aplicados:</span> {understanding.filtrosAplicados || 'Sin filtros'}</p>
                        <div>
                          <p className="font-semibold text-slate-100">Top tablas evaluadas:</p>
                          <div className="mt-1 flex flex-wrap gap-2">
                            {(understanding.tablasEvaluadas || []).slice(0, 5).map((entry) => (
                              <span key={`rank-${entry.table}`} className="rounded-full border border-cyan-300/30 bg-cyan-300/10 px-2 py-0.5 text-[10px] text-cyan-100">
                                {entry.table}: {entry.score}
                              </span>
                            ))}
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {isDebugResult && debugResult && (
                  <div className="overflow-hidden rounded-2xl" style={{ border: '1px solid rgba(255,255,255,0.05)' }}>
                    <div className="flex items-center justify-between px-4 py-2.5" style={{ background: 'rgba(255,255,255,0.03)', borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                      <span className="text-[11px] font-semibold uppercase tracking-wider text-amber-200">Modo debugging</span>
                    </div>
                    <div className="space-y-2 px-4 py-3 text-xs text-slate-200">
                      <p className="text-slate-100">{executionResult?.resumenHumano || 'Análisis de error completado.'}</p>
                      <p><span className="font-semibold text-slate-100">Tipo:</span> {String(debugResult?.tipo_error || 'N/A')}</p>
                      <p><span className="font-semibold text-slate-100">Archivo origen:</span> {String(debugResult?.archivo_origen || debugResult?.archivo || 'N/A')}</p>
                      <p><span className="font-semibold text-slate-100">Línea:</span> {String(debugResult?.linea ?? 'N/A')}</p>
                      <p><span className="font-semibold text-slate-100">Capa:</span> {String(debugResult?.capa || 'N/A')}</p>
                      <p><span className="font-semibold text-slate-100">Flujo:</span> {debugFlow.length > 0 ? debugFlow.join(' → ') : 'N/A'}</p>
                      {debugTrace.length > 0 && (
                        <div>
                          <p className="font-semibold text-slate-100">Traza</p>
                          <ul className="mt-1 space-y-1 text-[11px] text-cyan-200">
                            {debugTrace.map((frame, index) => {
                              const line = Number.isFinite(Number(frame?.linea)) ? Number(frame.linea) : null;
                              const fileName = String(frame?.archivo || frame?.raw || '').trim();
                              const label = fileName
                                ? `${fileName}${line !== null ? `:${line}` : ''}`
                                : String(frame?.raw || `frame-${index}`);
                              return <li key={`trace-${index}`}>- {label}</li>;
                            })}
                          </ul>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {executionResult?.success && !isDebugResult && Array.isArray(executionResult.data) && (
                  <div className="overflow-hidden rounded-2xl" style={{ border: '1px solid rgba(255,255,255,0.05)' }}>
                    <div className="flex items-center justify-between px-4 py-2.5" style={{ background: 'rgba(255,255,255,0.03)', borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                      <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-300">Resultado</span>
                      <div className="flex items-center gap-2">
                        {executionResult.executionType && (
                          <span className="rounded-full px-2 py-0.5 text-[10px] text-slate-400" style={{ border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(255,255,255,0.04)' }}>
                            {executionResult.executionType}
                          </span>
                        )}
                        <span className="rounded-full px-2.5 py-1 text-[10px] font-semibold text-emerald-300" style={{ border: '1px solid rgba(52,211,153,0.3)', background: 'rgba(52,211,153,0.08)' }}>
                          {executionResult.rowCount ?? executionResult.data.length} filas
                        </span>
                      </div>
                    </div>
                    {executionResult.resumenHumano && (() => {
                      const lines = executionResult.resumenHumano.split('\n').map(l => l.trim()).filter(Boolean);
                      const isNoticeLog = lines.length > 1;
                      if (isNoticeLog) {
                        return (
                          <div style={{ borderBottom: '1px solid rgba(255,255,255,0.04)', background: 'rgba(15,23,42,0.7)', padding: '10px 14px', fontFamily: 'monospace', fontSize: '11px', lineHeight: '1.7', overflowX: 'auto' }}>
                            {lines.map((line, i) => {
                              const isSep = /^={3,}/.test(line);
                              const isWarn = /^[⚠✖]/.test(line);
                              const isOk = /^[✔✅]/.test(line);
                              const color = isSep ? 'rgba(148,163,184,0.5)' : isWarn ? '#fbbf24' : isOk ? '#4ade80' : '#7dd3fc';
                              return (
                                <div key={i} style={{ display: 'flex', gap: '8px', color }}>
                                  {!isSep && <span style={{ color: 'rgba(100,116,139,0.8)', flexShrink: 0 }}>NOTICE:</span>}
                                  <span>{line}</span>
                                </div>
                              );
                            })}
                          </div>
                        );
                      }
                      return (
                        <div className="px-4 py-2.5 text-xs text-cyan-200" style={{ borderBottom: '1px solid rgba(255,255,255,0.04)', background: 'rgba(34,211,238,0.04)' }}>
                          {executionResult.resumenHumano}
                        </div>
                      );
                    })()}

                    {executionResult.data.length > 0 ? (
                      resultCols.length === 1 && resultCols[0] === 'output' ? (
                        // Oracle DBMS_OUTPUT: render as terminal log
                        <div style={{ background: 'rgba(15,10,5,0.85)', borderTop: '1px solid rgba(255,255,255,0.04)', padding: '10px 14px', fontFamily: 'monospace', fontSize: '11px', lineHeight: '1.8', maxHeight: '280px', overflowY: 'auto', overflowX: 'auto' }}>
                          <div style={{ color: 'rgba(180,100,30,0.6)', fontSize: '10px', marginBottom: '6px', textTransform: 'uppercase', letterSpacing: '0.08em' }}>🔶 DBMS_OUTPUT</div>
                          {executionResult.data.map((row, i) => {
                            const line = String(row?.output ?? '');
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
                      ) : (
                      <div className="max-h-52 overflow-auto">
                        <table className="min-w-full text-xs">
                          <thead>
                            <tr style={{ background: 'rgba(255,255,255,0.04)' }}>
                              {resultCols.map((col) => (
                                <th key={col} className="whitespace-nowrap px-3 py-2.5 text-left text-[10px] font-semibold uppercase tracking-wider text-slate-400">{col}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {executionResult.data.slice(0, 20).map((row, i) => (
                              <tr key={i} className="transition-colors" style={{ borderTop: '1px solid rgba(255,255,255,0.04)', background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)' }}>
                                {resultCols.map((col) => (
                                  <td key={col} className="px-3 py-2 text-slate-200">{String(row?.[col] ?? '')}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      )
                    ) : (
                      <div className="py-8 text-center text-xs text-slate-500">
                        {lastSearch.trim()
                          ? `No se encontraron resultados para "${lastSearch.trim()}"`
                          : 'Sin resultados para esta consulta'}
                      </div>
                    )}

                    {chartConfig && executionResult.data.length > 0 && (
                      <div className="px-4 pb-4" style={{ borderTop: '1px solid rgba(255,255,255,0.05)' }}>
                        <MiniBarChart data={executionResult.data} labelKey={chartConfig.labelKey} valueKey={chartConfig.valueKey} />
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>

            <div className="flex flex-shrink-0 gap-2 px-5 py-3.5" style={{ borderTop: '1px solid rgba(255,255,255,0.05)', background: 'rgba(255,255,255,0.02)' }}>
              {generatedQuery?.sql && (
                <button type="button" onClick={useInBuilder} className="flex-1 rounded-xl py-2 text-xs font-semibold text-blue-300 transition-colors hover:text-blue-100" style={{ border: '1px solid rgba(59,130,246,0.3)', background: 'rgba(59,130,246,0.08)' }}>
                  Usar en Builder
                </button>
              )}
              <button type="button" onClick={reset} className="flex-1 rounded-xl py-2 text-xs font-medium text-slate-400 transition-colors hover:text-slate-200" style={{ border: '1px solid rgba(255,255,255,0.08)', background: 'rgba(255,255,255,0.04)' }}>
                Limpiar
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
