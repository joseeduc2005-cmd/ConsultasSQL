'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Construction } from 'lucide-react';
import { Card } from '@/soporte/components/ui/card';
import { Vacio } from '@/soporte/components/ui/estados';

/**
 * Marcador para las pantallas que aún no se han traído de ticket v2.0.
 *
 * El Panel enlaza a Tickets, Diagnósticos e Interpretar, así que sin esto la
 * pantalla principal estaría llena de enlaces rotos. Este archivo desaparece
 * cuando la última pantalla esté migrada.
 */
const PENDIENTES: Record<string, string> = {
  tickets: 'Tickets',
  diagnostico: 'Diagnósticos',
  interpretar: 'Interpretar mensaje',
};

export default function Pendiente() {
  const ruta = usePathname();
  const seccion = ruta.split('/')[2] ?? '';
  const nombre = PENDIENTES[seccion];

  return (
    <Card className="mt-10">
      <Vacio
        icono={<Construction />}
        titulo={nombre ? `${nombre}: pantalla en migración` : 'Esta página no existe'}
        descripcion={
          nombre
            ? seccion === 'interpretar'
              ? 'Depende del modelo de lenguaje, que quedó fuera de esta fase a propósito.'
              : 'El backend ya responde; falta traer la interfaz desde ticket v2.0.'
            : undefined
        }
        accion={
          <Link href="/soporte" className="text-sm font-medium text-primary hover:underline">
            Volver al panel
          </Link>
        }
      />
    </Card>
  );
}
