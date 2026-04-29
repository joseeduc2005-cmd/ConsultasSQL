// src/routes/execute.ts
import { Router, Request, Response } from 'express';
import { ExecuteSolutionUseCase } from '../application/usecases/ExecuteSolutionUseCase';
import { ArticleRepository } from '../infrastructure/articleRepository';
import multer from 'multer';
import { promises as fs } from 'fs';
import path from 'path';

const router = Router();
const articleRepository = new ArticleRepository();
const executeUseCase = new ExecuteSolutionUseCase(articleRepository);

// Configurar multer para archivos .md
const upload = multer({
  dest: path.join(process.cwd(), 'temp'),
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'text/markdown' || path.extname(file.originalname).toLowerCase() === '.md') {
      cb(null, true);
    } else {
      cb(new Error('Solo se permiten archivos .md'));
    }
  },
  limits: {
    fileSize: 5 * 1024 * 1024, // 5MB máximo
  }
});

// Función para procesar archivo .md
async function processMarkdownFile(filePath: string): Promise<string> {
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    await fs.unlink(filePath); // Eliminar archivo temporal
    return content;
  } catch (error) {
    console.error('Error procesando archivo .md:', error);
    throw new Error('Error al procesar el archivo .md');
  }
}

// Función para simular ejecución del script
function simulateScriptExecution(script: string, formData: Record<string, any>, mdContent: string): {
  resultado: string;
  pasosEjecutados: string[];
  log: string;
} {
  const pasosEjecutados: string[] = [];
  let resultado = '';
  const log: string[] = [];

  log.push(`[${new Date().toISOString()}] Iniciando ejecución del script`);
  log.push(`Script recibido: ${script}`);
  log.push(`Datos del formulario: ${JSON.stringify(formData)}`);
  log.push(`Contenido del archivo .md: ${mdContent.substring(0, 100)}...`);

  // Simular diferentes tipos de reparaciones basadas en el script
  if (script.toLowerCase().includes('login')) {
    pasosEjecutados.push('Verificando credenciales del usuario');
    pasosEjecutados.push('Validando sesión activa');
    pasosEjecutados.push('Restableciendo contraseña temporal');

    if (formData.username && formData.password) {
      resultado = `✅ Usuario ${formData.username} validado correctamente. Contraseña restablecida.`;
    } else {
      resultado = '❌ Error: Credenciales incompletas';
    }
  } else if (script.toLowerCase().includes('transferencia') || script.toLowerCase().includes('banco')) {
    pasosEjecutados.push('Verificando cuenta bancaria');
    pasosEjecutados.push('Validando monto de transferencia');
    pasosEjecutados.push('Procesando transacción');

    resultado = `✅ Transferencia procesada exitosamente. Monto: ${formData.monto || 'N/A'}`;
  } else if (script.toLowerCase().includes('app') || script.toLowerCase().includes('reinicio')) {
    pasosEjecutados.push('Deteniendo servicios de la aplicación');
    pasosEjecutados.push('Limpiando caché');
    pasosEjecutados.push('Reiniciando aplicación');

    resultado = '✅ Aplicación reiniciada correctamente. Todos los servicios operativos.';
  } else if (script.toLowerCase().includes('config') || script.toLowerCase().includes('configuración')) {
    pasosEjecutados.push('Analizando configuración actual');
    pasosEjecutados.push('Aplicando cambios de configuración');
    pasosEjecutados.push('Validando nueva configuración');

    resultado = '✅ Configuración actualizada correctamente.';
  } else {
    // Reparación genérica
    pasosEjecutados.push('Analizando problema');
    pasosEjecutados.push('Aplicando solución automática');
    pasosEjecutados.push('Verificando resultado');

    resultado = '✅ Reparación completada exitosamente.';
  }

  // Procesar contenido del archivo .md si contiene instrucciones
  if (mdContent) {
    const lines = mdContent.split('\n');
    for (const line of lines) {
      if (line.trim().startsWith('#')) {
        pasosEjecutados.push(`Ejecutando: ${line.trim().substring(1).trim()}`);
      }
    }
  }

  log.push(`[${new Date().toISOString()}] Ejecución completada`);
  log.push(`Resultado: ${resultado}`);

  return {
    resultado,
    pasosEjecutados,
    log: log.join('\n')
  };
}

// POST /api/repair - Endpoint principal para reparaciones
router.post('/repair', upload.single('mdFile'), async (req: Request, res: Response) => {
  try {
    const { articleId, ...formData } = req.body;

    // Validaciones
    if (!articleId) {
      return res.status(400).json({
        success: false,
        message: 'ID del artículo es requerido'
      });
    }

    if (!req.file) {
      return res.status(400).json({
        success: false,
        message: 'Archivo .md es requerido'
      });
    }

    // Validar campos del formulario
    const article = await articleRepository.findById(articleId);
    if (!article) {
      return res.status(404).json({
        success: false,
        message: 'Artículo no encontrado'
      });
    }

    // Procesar archivo .md
    let mdContent = '';
    try {
      mdContent = await processMarkdownFile(req.file.path);
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: 'Error al procesar el archivo .md'
      });
    }

    // Ejecutar simulación del script
    const { resultado, pasosEjecutados, log } = simulateScriptExecution(
      article.script || '',
      formData,
      mdContent
    );

    // Registrar en consola
    console.log('🔧 REPARACIÓN EJECUTADA');
    console.log('========================');
    console.log(`Artículo: ${article.titulo}`);
    console.log(`Usuario: ${formData.username || 'N/A'}`);
    console.log(`Resultado: ${resultado}`);
    console.log(`Pasos ejecutados: ${pasosEjecutados.length}`);
    console.log('========================');

    // Respuesta exitosa
    res.json({
      success: true,
      message: 'Reparación ejecutada exitosamente',
      resultado,
      pasosEjecutados,
      log,
      timestamp: new Date().toISOString(),
      articleId,
      categoria: article.categoria,
      subcategoria: article.subcategoria
    });

  } catch (error) {
    console.error('Error en reparación:', error);
    res.status(500).json({
      success: false,
      message: 'Error interno del servidor durante la reparación',
      error: error instanceof Error ? error.message : 'Error desconocido'
    });
  }
});

// POST /api/execute - Endpoint legacy para compatibilidad
router.post('/execute', async (req: Request, res: Response) => {
  try {
    const { articleId, formData } = req.body;

    if (!articleId) {
      return res.status(400).json({
        success: false,
        message: 'ID del artículo es requerido'
      });
    }

    const result = await executeUseCase.execute({ articleId, formData });

    res.json(result);
  } catch (error) {
    console.error('Error ejecutando solución:', error);
    res.status(500).json({
      success: false,
      message: 'Error interno del servidor',
      error: error instanceof Error ? error.message : 'Error desconocido'
    });
  }
});

export default router;