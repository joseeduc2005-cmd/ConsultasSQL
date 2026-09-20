import { KeyRound, MonitorSmartphone, Receipt, Store } from 'lucide-react';
import type { ReactNode } from 'react';

/**
 * Icono de cada diagnóstico.
 *
 * En ticket v2.0 vivía dentro de la pantalla de diagnósticos. Se extrajo aquí para
 * que el Panel pueda usarlo sin arrastrar esa pantalla entera, que todavía no se
 * ha traído.
 */
export const ICONO_PLAYBOOK: Record<string, ReactNode> = {
  'acceso-corresponsal': <KeyRound />,
  'mac-duplicada': <MonitorSmartphone />,
  'sucursal-corresponsal': <Store />,
  'reporte-transacciones': <Receipt />,
};
