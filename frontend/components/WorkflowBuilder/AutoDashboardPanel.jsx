'use client';

import React, { useMemo } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

function toNumeric(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function buildLinePoints(values, width, height, pad = 16) {
  if (!values.length) return '';
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const innerW = width - pad * 2;
  const innerH = height - pad * 2;

  return values
    .map((v, i) => {
      const x = pad + (i * innerW) / Math.max(values.length - 1, 1);
      const y = pad + innerH - ((v - min) / range) * innerH;
      return `${x},${y}`;
    })
    .join(' ');
}

export default function AutoDashboardPanel({ dashboard, data = [] }) {
  const chartType = String(dashboard?.chartType || 'none');

  const prepared = useMemo(() => {
    if (!Array.isArray(data) || data.length === 0) return { labels: [], values: [] };

    if (chartType === 'bar') {
      const xKey = dashboard?.xKey;
      const yKey = dashboard?.yKey;
      return {
        labels: data.slice(0, 12).map((r) => String(r?.[xKey] ?? 'N/A')),
        values: data.slice(0, 12).map((r) => toNumeric(r?.[yKey])),
      };
    }

    if (chartType === 'line') {
      const xKey = dashboard?.xKey;
      const yKey = dashboard?.yKey;
      return {
        labels: data.slice(0, 16).map((r) => String(r?.[xKey] ?? 'N/A')),
        values: data.slice(0, 16).map((r) => toNumeric(r?.[yKey])),
      };
    }

    if (chartType === 'pie') {
      const categoryKey = dashboard?.categoryKey;
      const valueKey = dashboard?.valueKey;
      const grouped = new Map();

      data.slice(0, 24).forEach((row) => {
        const label = String(row?.[categoryKey] ?? 'N/A');
        const value = toNumeric(row?.[valueKey]);
        grouped.set(label, (grouped.get(label) || 0) + value);
      });

      const labels = Array.from(grouped.keys()).slice(0, 8);
      const values = labels.map((label) => grouped.get(label) || 0);
      return { labels, values };
    }

    return { labels: [], values: [] };
  }, [chartType, dashboard, data]);

  if (!dashboard || chartType === 'none' || chartType === 'table') {
    return (
      <div className="rounded-2xl border border-slate-700/80 bg-slate-900/60 p-3 text-xs text-slate-400 backdrop-blur-md">
        Sin gráfico automático para este resultado.
      </div>
    );
  }

  const chartRows = prepared.labels.map((label, idx) => ({
    name: label,
    value: prepared.values[idx],
  }));

  const pieColors = ['#22d3ee', '#38bdf8', '#0ea5e9', '#6366f1', '#8b5cf6', '#14b8a6', '#84cc16', '#f59e0b'];

  if (chartType === 'bar') {
    return (
      <div className="rounded-2xl border border-slate-700/80 bg-slate-900/60 p-3 backdrop-blur-md">
        <p className="text-xs font-semibold uppercase tracking-wide text-cyan-300">Barras automáticas</p>
        <div className="mt-3 h-56 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartRows} margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={{
                  background: '#020617',
                  border: '1px solid #334155',
                  borderRadius: 12,
                  color: '#e2e8f0',
                }}
              />
              <Legend wrapperStyle={{ color: '#cbd5e1' }} />
              <Bar dataKey="value" name="Valor" radius={[8, 8, 2, 2]}>
                {chartRows.map((entry, idx) => (
                  <Cell key={`cell-${entry.name}-${idx}`} fill={pieColors[idx % pieColors.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    );
  }

  if (chartType === 'line') {
    return (
      <div className="rounded-2xl border border-slate-700/80 bg-slate-900/60 p-3 backdrop-blur-md">
        <p className="text-xs font-semibold uppercase tracking-wide text-cyan-300">Serie temporal automática</p>
        <div className="mt-3 h-56 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartRows} margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={{
                  background: '#020617',
                  border: '1px solid #334155',
                  borderRadius: 12,
                  color: '#e2e8f0',
                }}
              />
              <Line type="monotone" dataKey="value" stroke="#22d3ee" strokeWidth={3} dot={{ r: 3, fill: '#38bdf8' }} activeDot={{ r: 5 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    );
  }

  if (chartType === 'pie') {
    return (
      <div className="rounded-2xl border border-slate-700/80 bg-slate-900/60 p-3 backdrop-blur-md">
        <p className="text-xs font-semibold uppercase tracking-wide text-cyan-300">Distribución automática</p>
        <div className="mt-3 h-60 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Tooltip
                contentStyle={{
                  background: '#020617',
                  border: '1px solid #334155',
                  borderRadius: 12,
                  color: '#e2e8f0',
                }}
              />
              <Legend wrapperStyle={{ color: '#cbd5e1' }} />
              <Pie data={chartRows} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={90} innerRadius={36}>
                {chartRows.map((entry, idx) => (
                  <Cell key={`pie-${entry.name}-${idx}`} fill={pieColors[idx % pieColors.length]} />
                ))}
              </Pie>
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
    );
  }

  return null;
}
