// ─── Dashboard shared types ───────────────────────────────────────────────────

export type DashboardUser = {
  username: string;
  role: string;
};

export type QueryHistoryRow = {
  id: number;
  username: string;
  user_role: string;
  query_text: string;
  generated_sql: string;
  execution_ms: number;
  row_count: number;
  was_cached: boolean;
  status: string;
  created_at: string;
};

export type AutoDashboard = {
  chartType: 'bar' | 'line' | 'pie' | 'table' | 'none' | 'kpi' | 'histogram';
  xKey?: string;
  yKey?: string;
  categoryKey?: string;
  valueKey?: string;
  reason?: string;
};

export type PresentationPlan = {
  mode: 'kpi' | 'chart' | 'table' | 'text' | 'chart-table';
  dashboard: AutoDashboard;
  headline: string;
  reason: string;
};

export type AutoQueryResponse = {
  success: boolean;
  mode?: 'sql' | 'debug';
  sql?: string;
  resumenHumano?: string;
  resultado?: Record<string, unknown> | unknown[] | null;
  data?: Record<string, unknown>[];
  count?: number;
  cached?: boolean;
  dashboard?: AutoDashboard;
  error?: string;
};

export type UiNotice = {
  open: boolean;
  title: string;
  message: string;
};
