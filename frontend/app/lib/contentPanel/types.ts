// ─── ContentPanel shared types ────────────────────────────────────────────────
export type TimelineTone = 'select' | 'validation' | 'update' | 'error' | 'info';

export interface ProgressStep {
  message: string;
  type: 'progress' | 'error';
}

export interface ExecutionResultPayload {
  success?: boolean;
  message?: string;
  affectedRows?: number;
  data?: unknown;
  resultado_final?: unknown;
  resultado?: unknown[];
  resumenHumano?: string;
  steps?: string[];
  explicacion?: string;
  workflow?: {
    steps?: Array<{ step?: string; title?: string; executedQuery?: string; executionMs?: number }>;
    context?: Record<string, unknown>;
    final?: unknown;
  };
  workflowContext?: Record<string, unknown>;
  executedQueries?: Array<{ step?: string; title?: string; executedQuery?: string; executionMs?: number }>;
}

export interface ParsedTimelineStep {
  title: string;
  detail: string;
  tone: TimelineTone;
  icon: string;
}

export interface ExecutionField {
  name: string;
  label: string;
  type: 'text' | 'number' | 'checkbox' | 'password' | 'email' | 'date';
  required: boolean;
}

export interface HumanExecutionStep {
  key: string;
  startLabel: string;
  completeLabel: string;
  tone: TimelineTone;
  icon: string;
}
