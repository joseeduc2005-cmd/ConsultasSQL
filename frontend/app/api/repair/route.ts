// app/api/repair/route.ts
import { NextRequest, NextResponse } from 'next/server';
import pool from '../../lib/db';
import bcrypt from 'bcryptjs';
import { parseMarkdownInstructions, executeMarkdownInstructions } from '../../lib/markdownParser';

export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData();

    const articleId = formData.get('articleId')?.toString() || '';
    const usuario = formData.get('username')?.toString() || '';
    const password = formData.get('password')?.toString() || '';
    const file = formData.get('mdFile');

    if (!articleId || !usuario || !password) {
      return NextResponse.json({ success: false, error: 'Campos obligatorios faltantes' }, { status: 400 });
    }

    let contenidoMD = '';

    if (file) {
      if (!(file instanceof File)) {
        return NextResponse.json({ success: false, error: 'Archivo .md inválido' }, { status: 400 });
      }

      const ext = file.name.split('.').pop()?.toLowerCase();
      if (ext !== 'md') {
        return NextResponse.json({ success: false, error: 'Solo se aceptan archivos .md' }, { status: 400 });
      }

      contenidoMD = await file.text();
    }

    // ===== 1. VALIDAR USUARIO REAL DESDE BD =====
    console.log(`[REPAIR] 🔍 Iniciando reparación para usuario: ${usuario}`);
    
    const userQuery = 'SELECT id, username, password, role FROM users WHERE username = $1';
    const userResult = await pool.query(userQuery, [usuario]);

    if (!userResult.rows || userResult.rows.length === 0) {
      console.log(`[REPAIR] ❌ Usuario no encontrado: ${usuario}`);
      return NextResponse.json({ success: false, error: 'Usuario no válido' }, { status: 401 });
    }

    const user = userResult.rows[0];
    const passwordMatch = await bcrypt.compare(password, user.password);

    if (!passwordMatch) {
      console.log(`[REPAIR] ❌ Contraseña incorrecta para usuario: ${usuario}`);
      return NextResponse.json({ success: false, error: 'Contraseña incorrecta' }, { status: 401 });
    }

    console.log(`[REPAIR] ✅ Usuario validado: ${usuario} (rol: ${user.role})`);

    // ===== 2. OBTENER ARTÍCULO REAL CON DATOS =====
    console.log(`[REPAIR] 🔍 Buscando artículo con ID: ${articleId} (tipo: ${typeof articleId})`);
    
    let articleResult;
    
    // Intentar como UUID primero
    try {
      const articleQuery = 'SELECT * FROM knowledge_base WHERE id = $1::uuid';
      articleResult = await pool.query(articleQuery, [articleId]);
      console.log(`[REPAIR] ✅ Búsqueda por UUID: ${articleResult.rows.length} resultados`);
    } catch (uuidError) {
      // Si falla la conversión UUID, intentar como string
      console.log(`[REPAIR] ⚠️  UUID inválido, intentando búsqueda flexible...`);
      const articleQuery = 'SELECT * FROM knowledge_base WHERE id::text = $1 OR CAST(id AS TEXT) LIKE $2';
      articleResult = await pool.query(articleQuery, [articleId, `%${articleId}%`]);
      console.log(`[REPAIR] ℹ️  Búsqueda flexible: ${articleResult.rows.length} resultados`);
    }

    if (!articleResult.rows || articleResult.rows.length === 0) {
      console.log(`[REPAIR] ❌ Artículo no encontrado: ${articleId}`);
      return NextResponse.json({ 
        success: false, 
        error: `Artículo no encontrado (ID: ${articleId})`,
        debug: { articleId, type: typeof articleId }
      }, { status: 404 });
    }

    const article = articleResult.rows[0];
    const { categoria, subcategoria, pasos, script, titulo } = article;

    console.log(`[REPAIR] 📖 Artículo obtenido - ${titulo} [${categoria}/${subcategoria}]`);

    // ===== 3. PARSEAR INSTRUCCIONES DEL CONTENIDO MD =====
    let pasosEjecutados: string[] = [];
    let validaciones: any[] = [];
    let resultado = '✅ Reparación completada exitosamente';

    const contenidoParaParsear = article.contenido_md || contenidoMD;

    if (contenidoParaParsear) {
      console.log(`[REPAIR] 📄 Parseando contenido MD...`);

      const parsed = parseMarkdownInstructions(contenidoParaParsear);

      if (parsed.instrucciones.length > 0) {
        console.log(`[REPAIR] 🔧 Ejecutando ${parsed.instrucciones.length} instrucciones dinámicas`);

        const executionResult = await executeMarkdownInstructions(
          parsed.instrucciones,
          { username: usuario, password },
          user
        );

        resultado = executionResult.resultado;
        validaciones = executionResult.validaciones;
        pasosEjecutados = executionResult.pasosEjecutados;
      } else {
        console.log(`[REPAIR] ℹ️  No se encontraron instrucciones ejecutables en el MD`);
        resultado = 'ℹ️ Solución de solo lectura - no se ejecutaron comandos';
        pasosEjecutados = ['Contenido revisado - solución informativa'];
        validaciones = [{
          paso: 'Revisión de contenido',
          estado: 'completado',
          detalles: 'Solución de solo lectura consultada'
        }];
      }
    } else {
      console.log(`[REPAIR] ⚠️  No hay contenido MD, usando lógica legacy`);

      if (Array.isArray(pasos)) {
        pasosEjecutados = pasos.map((p: any) =>
          typeof p === 'string' ? p : p.descripcion || p.nombre || String(p)
        );
      }

      if (subcategoria?.toLowerCase().includes('login') || subcategoria?.toLowerCase().includes('autenticación')) {
        validaciones.push({
          paso: 'Validación de usuario',
          estado: 'completado',
          detalles: `Usuario ${usuario} autenticado correctamente`
        });
        resultado = `✅ Usuario ${usuario} validado. Sesión activa.`;
      } else {
        validaciones.push({
          paso: 'Análisis completado',
          estado: 'completado'
        });
      }
    }

    console.log(`[REPAIR] ✅ Resultado: ${resultado}`);
    console.log(`[REPAIR] 📊 Pasos ejecutados: ${pasosEjecutados.length} | Validaciones: ${validaciones.length}`);

    return NextResponse.json({
      success: true,
      resultado,
      pasosEjecutados,
      validaciones,
      articulo: {
        titulo,
        categoria,
        subcategoria
      },
      usuario: user.username,
      rol: user.role,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('[REPAIR] ❌ Error en reparación:', error);
    return NextResponse.json({ 
      success: false, 
      error: error instanceof Error ? error.message : 'Error en reparación'
    }, { status: 500 });
  }
}
