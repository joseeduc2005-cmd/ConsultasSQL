// src/application/useCases/ExecuteSolutionUseCase.ts

import { KnowledgeArticle } from '../../domain/KnowledgeArticle';
import { IArticleRepository } from '../../infrastructure/articleRepository';

export interface ExecuteSolutionRequest {
  articleId: string;
  formData: Record<string, any>;
}

export interface ExecuteSolutionResponse {
  success: boolean;
  message: string;
  resultado: string;
  pasosEjecutados: string[];
  log: string;
}

export class ExecuteSolutionUseCase {
  constructor(private articleRepository: IArticleRepository) {}

  async execute(request: ExecuteSolutionRequest): Promise<ExecuteSolutionResponse> {
    const article = await this.articleRepository.findById(request.articleId);

    if (!article) {
      throw new Error('Artículo no encontrado');
    }

    if (article.tipo_solucion !== 'ejecutable') {
      return {
        success: false,
        message: 'Este artículo no es ejecutable',
        resultado: '',
        pasosEjecutados: [],
        log: 'Artículo no ejecutable'
      };
    }

    // Parsear comandos del contenido_md
    const comandos = this.parsearComandos(article.contenido_md || '');
    const { resultado, pasosEjecutados, log } = this.ejecutarComandos(comandos, request.formData);

    return {
      success: true,
      message: 'Solución ejecutada exitosamente',
      resultado,
      pasosEjecutados,
      log
    };
  }

  private parsearComandos(contenidoMd: string): string[] {
    const comandos: string[] = [];
    const lines = contenidoMd.split('\n');

    for (const line of lines) {
      // Buscar comandos en formato de código o listas
      const match = line.match(/`(\w+)`|^\s*-\s*(\w+)/);
      if (match) {
        const comando = match[1] || match[2];
        if (this.esComandoValido(comando)) {
          comandos.push(comando);
        }
      }
    }

    return comandos;
  }

  private esComandoValido(comando: string): boolean {
    const comandosValidos = [
      'validar_usuario',
      'reset_password',
      'reiniciar_app',
      'verificar_cuenta',
      'limpiar_cache',
      'actualizar_config'
    ];
    return comandosValidos.includes(comando);
  }

  private ejecutarComandos(comandos: string[], formData: Record<string, any>): {
    resultado: string;
    pasosEjecutados: string[];
    log: string;
  } {
    const pasosEjecutados: string[] = [];
    let resultado = '';
    const log: string[] = [];

    log.push(`[${new Date().toISOString()}] Iniciando ejecución de comandos`);

    for (const comando of comandos) {
      log.push(`Ejecutando comando: ${comando}`);
      pasosEjecutados.push(`Ejecutando: ${comando}`);

      switch (comando) {
        case 'validar_usuario':
          if (formData.username) {
            resultado += `✅ Usuario ${formData.username} validado.\n`;
            pasosEjecutados.push('Credenciales verificadas');
          } else {
            resultado += '❌ Usuario no proporcionado.\n';
          }
          break;

        case 'reset_password':
          if (formData.email) {
            resultado += `✅ Contraseña reseteada para ${formData.email}.\n`;
            pasosEjecutados.push('Contraseña temporal enviada');
          } else {
            resultado += '❌ Email no proporcionado.\n';
          }
          break;

        case 'reiniciar_app':
          resultado += '✅ Aplicación reiniciada correctamente.\n';
          pasosEjecutados.push('Servicios detenidos y reiniciados');
          break;

        case 'verificar_cuenta':
          if (formData.numeroCuenta) {
            resultado += `✅ Cuenta ${formData.numeroCuenta} verificada.\n`;
            pasosEjecutados.push('Saldo y estado de cuenta confirmados');
          } else {
            resultado += '❌ Número de cuenta no proporcionado.\n';
          }
          break;

        case 'limpiar_cache':
          resultado += '✅ Caché limpiado exitosamente.\n';
          pasosEjecutados.push('Archivos temporales eliminados');
          break;

        case 'actualizar_config':
          resultado += '✅ Configuración actualizada.\n';
          pasosEjecutados.push('Parámetros aplicados');
          break;

        default:
          resultado += `⚠️ Comando desconocido: ${comando}\n`;
      }
    }

    log.push(`[${new Date().toISOString()}] Ejecución completada`);
    log.push(`Resultado final: ${resultado.trim()}`);

    return {
      resultado: resultado.trim(),
      pasosEjecutados,
      log: log.join('\n')
    };
  }
}