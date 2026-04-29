// scripts/setupDatabase.ts

import { Pool } from 'pg';
import bcrypt from 'bcryptjs';
import { randomUUID } from 'crypto';
import * as dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

dotenv.config({ path: '.env.local' });

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
  console.error('❌ No se encontró base primaria en multidb.databases.json ni DATABASE_URL en .env.local');
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
});

async function setupDatabase() {
  try {
    console.log('🔄 Conectando a la base de datos...');
    await pool.query('SELECT NOW()');
    console.log('✓ Conexión exitosa\n');

    // Crear tabla de usuarios (no borrar datos existentes)
    console.log('📋 Creando tabla users si no existe...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        username VARCHAR(50) UNIQUE NOT NULL,
        password VARCHAR(255) NOT NULL,
        role VARCHAR(20) NOT NULL DEFAULT 'user',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('✓ Tabla users asegurada\n');

    // Crear tabla de artículos (no borrar datos existentes)
    console.log('📋 Creando tabla knowledge_base si no existe...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS knowledge_base (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        titulo VARCHAR(200) NOT NULL,
        descripcion TEXT,
        tags TEXT[] NOT NULL DEFAULT '{}',
        contenido TEXT NOT NULL,
        categoria VARCHAR(100),
        subcategoria VARCHAR(100),
        pasos JSONB,
        campos_formulario JSONB,
        script TEXT,
        script_json JSONB,
        creado_por UUID NOT NULL REFERENCES users(id),
        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        actualizado TIMESTAMP
      );
    `);
    console.log('✓ Tabla knowledge_base asegurada\n');

    await pool.query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS script_json JSONB`);

    // Crear índices
    console.log('⚡ Creando índices...');
    await pool.query('CREATE INDEX idx_usuarios_username ON users(username);');
    await pool.query('CREATE INDEX idx_articulos_tags ON knowledge_base USING GIN(tags);');
    await pool.query('CREATE INDEX idx_articulos_creado_por ON knowledge_base(creado_por);');
    console.log('✓ Índices creados\n');

    // Insertar usuarios de prueba
    console.log('👥 Insertando usuarios de prueba...');

    const adminId = randomUUID();
    const userId = randomUUID();

    const adminPassword = await bcrypt.hash('password123', 10);
    const userPassword = await bcrypt.hash('password123', 10);

    await pool.query(
      `INSERT INTO users (id, username, password, role) VALUES ($1, $2, $3, $4) ON CONFLICT (username) DO NOTHING`,
      [adminId, 'admin', adminPassword, 'admin']
    );

    await pool.query(
      `INSERT INTO users (id, username, password, role) VALUES ($1, $2, $3, $4) ON CONFLICT (username) DO NOTHING`,
      [userId, 'user', userPassword, 'user']
    );

    console.log('✓ Usuarios creados:');
    console.log('  - admin (password: password123)');
    console.log('  - user (password: password123)\n');

    // Insertar artículos de prueba
    console.log('📚 Insertando artículos de prueba...');

    const articles = [
      {
        titulo: 'Problemas con login',
        descripcion: 'Cuando los usuarios no logran iniciar sesión, revisa credenciales y sesiones activas.',
        tags: ['login', 'autenticacion', 'error'],
        contenido: 'Solución para problemas de login en la aplicación.',
        categoria: 'Usuarios',
        subcategoria: 'Problemas login',
        pasos: [
          { paso: 1, descripcion: 'Verificar credenciales' },
          { paso: 2, descripcion: 'Limpiar cache del navegador' },
          { paso: 3, descripcion: 'Intentar reset de contraseña' }
        ],
        camposFormulario: [
          { name: 'username', label: 'Usuario', type: 'text', required: true },
          { name: 'errorCode', label: 'Código de error', type: 'text', required: true }
        ],
        script: 'echo "Simulando verificación de login"'
      },
      {
        titulo: 'Recuperar contraseña',
        descripcion: 'Flujo de recuperación para usuarios que olvidan su contraseña.',
        tags: ['password', 'recuperacion', 'seguridad'],
        contenido: 'Proceso para recuperar contraseña olvidada.',
        categoria: 'Usuarios',
        subcategoria: 'Recuperar contraseña',
        pasos: [
          { paso: 1, descripcion: 'Hacer clic en "Olvidé mi contraseña"' },
          { paso: 2, descripcion: 'Ingresar email registrado' },
          { paso: 3, descripcion: 'Seguir instrucciones del email' }
        ],
        camposFormulario: [
          { name: 'email', label: 'Correo electrónico', type: 'email', required: true }
        ],
        script: 'echo "Simulando envío de email de recuperación"'
      },
      {
        titulo: 'Error en transferencia bancaria',
        descripcion: 'Diagnostico de errores comunes en transferencias desde la app móvil.',
        tags: ['transferencia', 'banca', 'error'],
        contenido: 'Solución para errores en transferencias bancarias.',
        categoria: 'Banca móvil',
        subcategoria: 'Error transferencia',
        pasos: [
          { paso: 1, descripcion: 'Verificar saldo disponible' },
          { paso: 2, descripcion: 'Confirmar datos del destinatario' },
          { paso: 3, descripcion: 'Reintentar la operación' }
        ],
        camposFormulario: [
          { name: 'accountNumber', label: 'Número de cuenta', type: 'text', required: true },
          { name: 'amount', label: 'Monto', type: 'number', required: true },
          { name: 'errorCode', label: 'Código de error', type: 'text', required: false }
        ],
        script: 'echo "Simulando verificación de transferencia"'
      },
      {
        titulo: 'App no abre',
        descripcion: 'Pasos de diagnóstico cuando la aplicación no inicia en dispositivos móviles.',
        tags: ['app', 'movil', 'error'],
        contenido: 'Solución cuando la app móvil no se abre.',
        categoria: 'Banca móvil',
        subcategoria: 'App no abre',
        pasos: [
          { paso: 1, descripcion: 'Reiniciar el dispositivo' },
          { paso: 2, descripcion: 'Actualizar la app' },
          { paso: 3, descripcion: 'Limpiar datos de la app' }
        ],
        camposFormulario: [
          { name: 'deviceModel', label: 'Modelo dispositivo', type: 'text', required: true },
          { name: 'appVersion', label: 'Versión app', type: 'text', required: true }
        ],
        script: 'echo "Simulando reinicio de app"'
      }
    ];

    for (const article of articles) {
      await pool.query(
        'INSERT INTO knowledge_base (titulo, tags, contenido, categoria, subcategoria, pasos, script, creado_por) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
        [article.titulo, article.tags, article.contenido, article.categoria, article.subcategoria, JSON.stringify(article.pasos), article.script, adminId]
      );
    }

    console.log(`✓ ${articles.length} artículos creados\n`);

    console.log('✨ ¡Base de datos configurada exitosamente!');
    console.log('\n📝 Instrucciones para ejecutar la aplicación:');
    console.log('   1. npm install');
    console.log('   2. npm run dev');
    console.log('\n🌐 La aplicación estará disponible en: http://localhost:3000');
  } catch (error) {
    console.error('❌ Error configurando la base de datos:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

setupDatabase();
