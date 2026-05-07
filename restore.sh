#!/bin/bash
# Restaura la base de datos desde el backup real (pg_dump custom format)
set -e

echo "🔄 Restaurando base de datos desde backup..."
pg_restore \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  --no-owner \
  --no-privileges \
  /backup/knowledge_base.backup && echo "✅ Base de datos restaurada correctamente." || echo "⚠️  pg_restore terminó con advertencias (normal si ya existen objetos)."
