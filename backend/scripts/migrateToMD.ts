import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

dotenv.config({ path: '.env' });

function resolvePrimaryDatabaseUrl() {
  const configFile = String(
    process.env.MULTI_DB_CONFIG_FILE
    || process.env.DATABASES_CONFIG_FILE
    || './config/multidb.databases.json'
    || ''
  ).trim();

  if (configFile) {
    try {
      const resolvedPath = path.isAbsolute(configFile)
        ? configFile
        : path.resolve(process.cwd(), configFile);

      if (fs.existsSync(resolvedPath)) {
        const parsed = JSON.parse(fs.readFileSync(resolvedPath, 'utf8') || '{}');
        const databases = Array.isArray(parsed?.databases) ? parsed.databases : [];
        const primary = databases.find((entry: any) => entry?.enabled !== false && (entry?.primary === true || entry?.isPrimary === true || String(entry?.role || '').toLowerCase() === 'primary'))
          || (databases.length === 1 ? databases[0] : null);

        if (primary) {
          const direct = String(primary.connectionString || primary.url || '').trim();
          if (direct) {
            return direct;
          }

          const host = String(primary.host || '').trim();
          const database = String(primary.database || '').trim();
          const user = String(primary.user || primary.username || '').trim();
          const password = String(primary.password || '').trim();
          const port = Number(primary.port) || 5432;

          if (host && database && user) {
            const auth = password
              ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}`
              : encodeURIComponent(user);
            return `postgresql://${auth}@${host}:${port}/${database}`;
          }
        }
      }
    } catch (error) {
      console.error('⚠ No se pudo resolver la base primaria desde MULTI_DB_CONFIG_FILE:', error);
    }
  }

  return process.env.DATABASE_URL;
}

const DATABASE_URL = resolvePrimaryDatabaseUrl();

if (!DATABASE_URL) {
  console.error('❌ No se encontró base primaria en multidb.databases.json ni DATABASE_URL en .env');
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
});

async function migrateDatabase() {
  try {
    console.log('🔄 Conectando a la base de datos...');
    await pool.query('SELECT NOW()');
    console.log('✓ Conexión exitosa\n');

    // Agregar campos para soluciones MD
    console.log('📋 Agregando campos para soluciones MD...');

    // Verificar si las columnas ya existen
    const columnsResult = await pool.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = 'knowledge_base' AND column_name IN ('contenido_md', 'tipo_solucion')
    `);

    const existingColumns = columnsResult.rows.map((row: any) => row.column_name);

    if (!existingColumns.includes('contenido_md')) {
      console.log('➕ Agregando columna contenido_md...');
      await pool.query(`
        ALTER TABLE knowledge_base
        ADD COLUMN contenido_md TEXT
      `);
    }

    if (!existingColumns.includes('tipo_solucion')) {
      console.log('➕ Agregando columna tipo_solucion...');
      await pool.query(`
        ALTER TABLE knowledge_base
        ADD COLUMN tipo_solucion VARCHAR(20) DEFAULT 'lectura' CHECK (tipo_solucion IN ('lectura', 'ejecutable'))
      `);
    }

    console.log('✓ Campos agregados\n');

    // Actualizar artículos existentes con contenido MD de ejemplo
    console.log('📝 Actualizando artículos con contenido MD...');

    const updateQueries = [
      {
        titulo: 'Problemas con login',
        contenido_md: `# Problemas con Login

## Descripción
Los usuarios no pueden iniciar sesión debido a credenciales inválidas.

## Diagnóstico
- Verificar que el usuario exista en el sistema
- Validar contraseña
- Revisar bloqueos de cuenta

## Solución Ejecutable
\`\`\`yaml
instrucciones:
  - validar_usuario
  - reset_password
  - limpiar_cache
\`\`\`

## Pasos Manuales
1. Verificar credenciales
2. Limpiar caché del navegador
3. Intentar reset de contraseña`,
        tipo_solucion: 'ejecutable'
      },
      {
        titulo: 'Recuperar contraseña',
        contenido_md: `# Recuperación de Contraseña

## Descripción
Proceso para recuperar contraseña olvidada.

## Información General
Este es un proceso de solo lectura que explica cómo recuperar la contraseña.

## Pasos a Seguir
1. Hacer clic en "Olvidé mi contraseña"
2. Ingresar email registrado
3. Seguir instrucciones del email

## Nota
Este proceso no requiere ejecución automática.`,
        tipo_solucion: 'lectura'
      },
      {
        titulo: 'Error en transferencia bancaria',
        contenido_md: `# Error en Transferencia Bancaria

## Descripción
Falla al confirmar monto en envíos desde la app móvil.

## Diagnóstico
- Verificar saldo disponible
- Confirmar datos del destinatario
- Validar límites de transferencia

## Solución Ejecutable
\`\`\`yaml
instrucciones:
  - validar_saldo
  - verificar_destinatario
  - procesar_transferencia
  - enviar_confirmacion
\`\`\`

## Pasos de Verificación
1. Revisar límite diario
2. Confirmar saldo disponible
3. Verificar datos del destinatario`,
        tipo_solucion: 'ejecutable'
      },
      {
        titulo: 'App no abre',
        contenido_md: `# Aplicación No Abre

## Descripción
La aplicación móvil se cierra al intentar iniciar.

## Solución Ejecutable
\`\`\`yaml
instrucciones:
  - reiniciar_app
  - limpiar_cache_app
  - verificar_conectividad
  - actualizar_app
\`\`\`

## Pasos de Diagnóstico
1. Reiniciar el dispositivo
2. Limpiar caché de la aplicación
3. Verificar conexión a internet
4. Actualizar la aplicación`,
        tipo_solucion: 'ejecutable'
      }
    ];

    for (const update of updateQueries) {
      await pool.query(
        'UPDATE knowledge_base SET contenido_md = $1, tipo_solucion = $2 WHERE titulo = $3',
        [update.contenido_md, update.tipo_solucion, update.titulo]
      );
    }

    console.log('✓ Artículos actualizados con contenido MD\n');

    console.log('✨ ¡Migración completada exitosamente!');
    console.log('\n📝 Nuevos campos disponibles:');
    console.log('   - contenido_md: Contenido en formato Markdown');
    console.log('   - tipo_solucion: "lectura" o "ejecutable"');

  } catch (error) {
    console.error('❌ Error en migración:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

migrateDatabase();