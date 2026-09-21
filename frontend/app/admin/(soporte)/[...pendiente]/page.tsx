'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Construction } from 'lucide-react';
import { Card } from '@/soporte/components/ui/card';
import { Vacio } from '@/soporte/components/ui/estados';

/**
 * Marcador para lo que aún no se ha traído de ticket v2.0, y 404 del módulo.
 *
 * Ya solo queda Interpretar mensaje, que depende del modelo de lenguaje. Cuando
 * esa pantalla exista, este archivo se borra y el módulo queda completo.
 */
const PENDIENTES: Record<string, string> = {
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
            ? 'Depende del modelo de lenguaje, que quedó fuera de esta fase a propósito.'
            : undefined
        }
        accion={
          <Link href="/admin/resumen" className="text-sm font-medium text-primary hover:underline">
            Volver al panel
          </Link>
        }
      />
    </Card>
  );
}
