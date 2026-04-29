/**
 * Parser de instrucciones para contenido MD
 * Extrae comandos de bloques de código YAML
 */

export interface ParsedInstructions {
  instrucciones: string[];
  metadata: Record<string, any>;
}

/**
 * Parsea el contenido MD y extrae las instrucciones ejecutables
 */
export function parseMarkdownInstructions(contentMd: string): ParsedInstructions {
  const result: ParsedInstructions = {
    instrucciones: [],
    metadata: {}
  };

  if (!contentMd) return result;

  // Buscar bloques de código YAML con instrucciones
  const yamlBlockRegex = /```yaml\s*\n([\s\S]*?)\n```/g;
  const matches = contentMd.matchAll(yamlBlockRegex);

  for (const match of matches) {
    const yamlContent = match[1];

    try {
      // Parsear YAML simple (solo líneas con guiones)
      const lines = yamlContent.split('\n').map(line => line.trim()).filter(line => line);

      for (const line of lines) {
        if (line.startsWith('instrucciones:')) {
          // Encontró la sección de instrucciones
          continue;
        }

        if (line.startsWith('- ')) {
          // Es una instrucción
          const instruction = line.substring(2).trim();
          if (instruction) {
            result.instrucciones.push(instruction);
          }
        }
      }
    } catch (error) {
      console.warn('[MD-PARSER] Error parseando YAML:', error);
    }
  }

  console.log(`[MD-PARSER] ✅ ${result.instrucciones.length} instrucciones extraídas:`, result.instrucciones);

  return result;
}

/**
 * Ejecuta las instrucciones extraídas del MD
 */
export async function executeMarkdownInstructions(
  instrucciones: string[],
  formData: Record<string, any>,
  user: any
): Promise<{ resultado: string; validaciones: any[]; pasosEjecutados: string[] }> {

  const validaciones: any[] = [];
  const pasosEjecutados: string[] = [];
  let resultado = '✅ Solución ejecutada exitosamente';

  console.log(`[MD-EXECUTOR] 🔧 Ejecutando ${instrucciones.length} instrucciones:`, instrucciones);

  for (const instruccion of instrucciones) {
    console.log(`[MD-EXECUTOR] ▶️  Ejecutando: ${instruccion}`);

    switch (instruccion.toLowerCase()) {
      case 'validar_usuario':
        validaciones.push({
          paso: 'Validación de usuario',
          estado: 'completado',
          detalles: `Usuario ${user.username} validado correctamente`
        });
        pasosEjecutados.push('Verificación de credenciales de usuario');
        break;

      case 'reset_password':
        validaciones.push({
          paso: 'Reset de contraseña',
          estado: 'completado',
          detalles: 'Contraseña reseteada exitosamente'
        });
        pasosEjecutados.push('Generación de nueva contraseña temporal');
        pasosEjecutados.push('Envío de email de recuperación');
        break;

      case 'limpiar_cache':
        validaciones.push({
          paso: 'Limpieza de caché',
          estado: 'completado',
          archivosLimpiados: 15
        });
        pasosEjecutados.push('Caché del sistema limpiado (15 archivos)');
        break;

      case 'validar_saldo':
        validaciones.push({
          paso: 'Validación de saldo',
          estado: 'completado',
          saldo: '$2,500.00'
        });
        pasosEjecutados.push('Verificación de saldo disponible');
        break;

      case 'verificar_destinatario':
        validaciones.push({
          paso: 'Verificación de destinatario',
          estado: 'completado',
          cuenta: `****${Math.random().toString().slice(2, 6)}`
        });
        pasosEjecutados.push('Validación de datos del destinatario');
        break;

      case 'procesar_transferencia':
        validaciones.push({
          paso: 'Procesamiento de transferencia',
          estado: 'completado',
          referencia: `TRX-${Date.now()}`
        });
        pasosEjecutados.push('Transferencia procesada exitosamente');
        break;

      case 'enviar_confirmacion':
        validaciones.push({
          paso: 'Envío de confirmación',
          estado: 'completado',
          email: `${user.username}@example.com`
        });
        pasosEjecutados.push('Confirmación enviada por email');
        break;

      case 'reiniciar_app':
        validaciones.push({
          paso: 'Reinicio de aplicación',
          estado: 'completado'
        });
        pasosEjecutados.push('Aplicación reiniciada correctamente');
        break;

      case 'limpiar_cache_app':
        validaciones.push({
          paso: 'Limpieza de caché de app',
          estado: 'completado',
          cacheSize: '45MB'
        });
        pasosEjecutados.push('Caché de aplicación limpiado (45MB)');
        break;

      case 'verificar_conectividad':
        validaciones.push({
          paso: 'Verificación de conectividad',
          estado: 'completado',
          ping: '23ms'
        });
        pasosEjecutados.push('Conectividad a internet verificada');
        break;

      case 'actualizar_app':
        validaciones.push({
          paso: 'Actualización de app',
          estado: 'completado',
          version: '2.1.4'
        });
        pasosEjecutados.push('Aplicación actualizada a versión 2.1.4');
        break;

      default:
        console.warn(`[MD-EXECUTOR] ⚠️  Instrucción no reconocida: ${instruccion}`);
        validaciones.push({
          paso: instruccion,
          estado: 'saltado',
          detalles: 'Instrucción no implementada'
        });
        break;
    }

    // Simular tiempo de ejecución
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  console.log(`[MD-EXECUTOR] ✅ Ejecución completada: ${pasosEjecutados.length} pasos ejecutados`);

  return {
    resultado,
    validaciones,
    pasosEjecutados
  };
}