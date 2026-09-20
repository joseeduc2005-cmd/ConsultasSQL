'use client';

import { Play } from 'lucide-react';
import { useState, type FormEvent } from 'react';
import { Button } from '@/soporte/components/ui/button';
import { Campo, Input } from '@/soporte/components/ui/form';
import type { PlaybookInfo } from '@/soporte/lib/types';

interface Props {
  playbook: PlaybookInfo;
  inicial?: Record<string, string>;
  ejecutando?: boolean;
  onEjecutar: (entrada: Record<string, string>) => void;
  textoBoton?: string;
}

function validar(playbook: PlaybookInfo, valores: Record<string, string>): Record<string, string> {
  const errores: Record<string, string> = {};
  for (const c of playbook.campos) {
    const v = valores[c.clave]?.trim() ?? '';
    if (!v && c.requerido) errores[c.clave] = 'Obligatorio.';
    else if (v && c.patron && !new RegExp(c.patron).test(v)) errores[c.clave] = `Formato no válido${c.ejemplo ? ` (ej. ${c.ejemplo})` : ''}.`;
  }
  for (const grupo of playbook.requiereAlguno) {
    if (!grupo.some((k) => valores[k]?.trim())) errores[grupo[0]] = 'Ingrese al menos uno de los datos de identificación.';
  }
  return errores;
}

export function FormularioPlaybook({ playbook, inicial = {}, ejecutando, onEjecutar, textoBoton = 'Ejecutar diagnóstico' }: Props) {
  const [valores, setValores] = useState<Record<string, string>>(inicial);
  const [errores, setErrores] = useState<Record<string, string>>({});
  const alguno = new Set(playbook.requiereAlguno.flat());

  const enviar = (e: FormEvent) => {
    e.preventDefault();
    const encontrados = validar(playbook, valores);
    setErrores(encontrados);
    if (Object.keys(encontrados).length === 0) {
      onEjecutar(Object.fromEntries(Object.entries(valores).map(([k, v]) => [k, v.trim()]).filter(([, v]) => v)));
    }
  };

  return (
    <form onSubmit={enviar} className="space-y-4" noValidate>
      {alguno.size > 0 && <p className="text-xs text-muted-foreground">Complete al menos uno de los datos de identificación.</p>}
      {playbook.campos.map((c) => (
        <Campo key={c.clave} etiqueta={c.etiqueta + (c.requerido ? '' : alguno.has(c.clave) ? '' : ' (opcional)')} htmlFor={`campo-${c.clave}`} ayuda={c.ayuda} error={errores[c.clave]}>
          <Input
            id={`campo-${c.clave}`}
            type={c.tipo === 'fecha' ? 'date' : 'text'}
            inputMode={c.patron?.includes('\\d') && c.tipo !== 'fecha' ? 'numeric' : undefined}
            placeholder={c.ejemplo}
            value={valores[c.clave] ?? ''}
            onChange={(e) => setValores((v) => ({ ...v, [c.clave]: e.target.value }))}
            className={errores[c.clave] ? 'border-danger focus:border-danger focus:ring-danger/15' : c.tipo === 'fecha' ? '' : 'font-mono'}
            autoComplete="off"
          />
        </Campo>
      ))}
      <Button type="submit" className="w-full" cargando={ejecutando}>
        {!ejecutando && <Play />}
        {ejecutando ? 'Ejecutando verificaciones…' : textoBoton}
      </Button>
    </form>
  );
}
