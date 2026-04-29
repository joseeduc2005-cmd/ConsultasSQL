'use client';

import React, { useEffect, useState } from 'react';

export default function QueryHistoryPanel({ userRole = 'user' }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [rows, setRows] = useState([]);
  const [search, setSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');

  useEffect(() => {
    if (!['admin', 'superadmin'].includes(String(userRole || '').toLowerCase())) return;

    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const token = localStorage.getItem('token');
        const response = await fetch('/api/query-history?limit=30', {
          headers: {
            Authorization: `Bearer ${token}`,
            'x-user-role': localStorage.getItem('userRole') || 'user',
          },
        });

        const result = await response.json();
        if (!response.ok || !result?.success) {
          setError(result?.error || 'No se pudo cargar historial.');
          return;
        }

        setRows(Array.isArray(result?.data) ? result.data : []);
      } catch {
        setError('Error de red cargando historial.');
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [userRole]);

  if (!['admin', 'superadmin'].includes(String(userRole || '').toLowerCase())) {
    return null;
  }

  const normalizedSearch = String(search || '').trim().toLowerCase();
  const filteredRows = rows.filter((row) => {
    const statusMatches = statusFilter === 'all' ? true : String(row.status || '').toLowerCase() === statusFilter;
    if (!statusMatches) return false;

    if (!normalizedSearch) return true;
    const haystack = [
      row.username,
      row.user_role,
      row.status,
      row.query_text,
      row.generated_sql,
    ]
      .map((part) => String(part || '').toLowerCase())
      .join(' ');
    return haystack.includes(normalizedSearch);
  });

  const statusBadgeClass = (status) => {
    const normalized = String(status || '').toLowerCase();
    if (normalized === 'ok') return 'border-emerald-500/40 bg-emerald-500/15 text-emerald-300';
    return 'border-red-500/40 bg-red-500/15 text-red-300';
  };

  return (
    <div className="rounded-2xl border border-slate-700/80 bg-slate-900/60 p-3 backdrop-blur-md">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-sm font-semibold text-cyan-200">Historial SQL automatico</p>
        <div className="flex flex-wrap items-center gap-2">
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Buscar usuario o SQL..."
            className="w-52 rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-xs text-slate-100 placeholder:text-slate-500"
          />
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-xs text-slate-200"
          >
            <option value="all">Todos</option>
            <option value="ok">OK</option>
            <option value="error">Error</option>
          </select>
        </div>
      </div>

      {loading && (
        <div className="mt-2 space-y-2">
          {[1, 2, 3].map((id) => (
            <div key={`skeleton-${id}`} className="h-10 animate-pulse rounded-lg bg-slate-800" />
          ))}
        </div>
      )}

      {error && !loading && (
        <div className="mt-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
          {error}
        </div>
      )}

      {!loading && !error && rows.length === 0 && (
        <p className="mt-2 text-xs text-slate-400">Sin consultas registradas aún.</p>
      )}

      {!loading && !error && filteredRows.length > 0 && (
        <div className="mt-2 max-h-56 overflow-auto rounded-xl border border-slate-700">
          <table className="min-w-full text-xs">
            <thead className="bg-slate-950 text-slate-300">
              <tr>
                <th className="px-2 py-2 text-left">Usuario</th>
                <th className="px-2 py-2 text-left">Rol</th>
                <th className="px-2 py-2 text-left">Tiempo</th>
                <th className="px-2 py-2 text-left">Filas</th>
                <th className="px-2 py-2 text-left">Estado</th>
              </tr>
            </thead>
            <tbody>
              {filteredRows.map((row) => (
                <tr key={`hist-${row.id}`} className="border-t border-slate-800 transition-colors hover:bg-slate-800/60">
                  <td className="px-2 py-2 text-slate-200">{row.username}</td>
                  <td className="px-2 py-2 text-slate-300">{row.user_role}</td>
                  <td className="px-2 py-2 text-slate-300">{row.execution_ms} ms</td>
                  <td className="px-2 py-2 text-slate-300">{row.row_count}</td>
                  <td className="px-2 py-2 text-slate-300">
                    <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${statusBadgeClass(row.status)}`}>
                      {row.status}
                    </span>
                    {row.was_cached ? (
                      <span className="ml-1 rounded-full border border-cyan-500/40 bg-cyan-500/15 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-cyan-300">
                        cache
                      </span>
                    ) : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && !error && rows.length > 0 && filteredRows.length === 0 && (
        <p className="mt-2 text-xs text-slate-400">No hay resultados para esos filtros.</p>
      )}
    </div>
  );
}
