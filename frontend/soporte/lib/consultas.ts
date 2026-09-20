import { useQuery } from '@tanstack/react-query';
import { api } from './api';

/** Claves de caché centralizadas para invalidar de forma consistente. */
export const claves = {
  estado: ['estado'] as const,
  resumen: ['resumen'] as const,
  tickets: (f?: object) => ['tickets', f ?? {}] as const,
  ticket: (numero: string) => ['ticket', numero] as const,
  consultas: (f?: object) => ['consultas', f ?? {}] as const,
  playbooks: ['playbooks'] as const,
  diagnosticos: ['diagnosticos'] as const,
  diagnostico: (id: number) => ['diagnostico', id] as const,
  monitores: ['monitores'] as const,
  alertas: (estado?: string) => ['alertas', estado ?? 'todas'] as const,
};

export const useEstado = () => useQuery({ queryKey: claves.estado, queryFn: api.estado, refetchInterval: 60_000 });
export const usePlaybooks = () => useQuery({ queryKey: claves.playbooks, queryFn: api.playbooks, staleTime: Infinity });
