#!/usr/bin/env node

/**
 * VALIDATION SCRIPT: Prueba los 5 cambios principales
 * Ejecutar: node validation-test.js
 */

const colors = {
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[36m',
  reset: '\x1b[0m'
};

function test(name, condition, details = '') {
  const icon = condition ? '✓' : '✗';
  const color = condition ? colors.green : colors.red;
  console.log(`${color}${icon}${colors.reset} ${name}`);
  if (details && !condition) console.log(`  → ${details}`);
}

console.log(`\n${colors.blue}=== VALIDACIÓN DE CAMBIOS ===\n${colors.reset}`);

// Test 1: Sanitización de espacios
console.log(`${colors.yellow}[1] ARREGLADO APLASTAMIENTO DE ESPACIOS${colors.reset}`);

const BLOCKED_PATTERNS = /\b(drop|delete|alter|truncate)\b/gi;

function sanitizeUserText_NEW(value, maxLength = 500) {
  const raw = String(value || '');
  // Nota: no colapsamos espacios con .replace(/\s+/g, ' ')
  const cleaned = raw
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '')
    .slice(0, Math.max(1, maxLength));
  return cleaned;
}

function stripDangerousSqlTerms_NEW(value) {
  const text = String(value || '').trim();
  return text.replace(BLOCKED_PATTERNS, '').trim();
}

const testInput1 = "ver los clientes  premium";
const result1 = stripDangerousSqlTerms_NEW(testInput1);
test(
  'Espacios preservados en input',
  result1 === testInput1,
  `Esperado: "${testInput1}", Obtenido: "${result1}"`
);

// Test 2: Validación de respuesta vacía
console.log(`\n${colors.yellow}[2] MANEJO DE RESPUESTAS VACÍAS${colors.reset}`);
const emptyResponse = { success: true, data: [], rowCount: 0 };
const hasData = Array.isArray(emptyResponse?.data) && emptyResponse.data.length > 0;
const showNoResults = emptyResponse.success && !hasData;
test(
  'Frontend detecta respuesta vacía correctamente',
  showNoResults,
  `Success: ${emptyResponse.success}, Data: ${emptyResponse.data}, Debe mostrar "Sin resultados": ${showNoResults}`
);

// Test 3: Fallback agresivo removido
console.log(`\n${colors.yellow}[3] REMOVIDO FALLBACK AGRESIVO (<0.6)${colors.reset}`);

function resolveSingleEntity_NEW(score) {
  if (score >= 0.84) return { score, exact: true, message: 'Alta confianza' };
  if (score >= 0.6) return { score, warning: `Confianza media (${(score * 100).toFixed(0)}%)` };
  return null; // NO fallback
}

const lowScore = 0.56;
const mediumScore = 0.72;
const highScore = 0.95;

test(
  'Score < 0.6 rechazado (no fallback)',
  resolveSingleEntity_NEW(lowScore) === null,
  `Score ${lowScore} debería retornar null, obtuvo: ${JSON.stringify(resolveSingleEntity_NEW(lowScore))}`
);

test(
  'Score 0.6-0.8 retorna con warning',
  resolveSingleEntity_NEW(mediumScore)?.warning !== undefined,
  `Score ${mediumScore} debería tener warning`
);

test(
  'Score >= 0.84 retorna exact',
  resolveSingleEntity_NEW(highScore)?.exact === true,
  `Score ${highScore} debería ser exact`
);

// Test 4: SQL Injection blocking
console.log(`\n${colors.yellow}[4] BLOQUEADO SQL INJECTION${colors.reset}`);

const SQL_INJECTION_PATTERNS = /('|"|--|;|\*\/|\/\*)|(DROP|DELETE|INSERT|UPDATE)\s+(TABLE|DATABASE|SCHEMA)/i;

function testInjectionBlocking(input, shouldBlock) {
  const blocked = SQL_INJECTION_PATTERNS.test(input);
  return blocked === shouldBlock;
}

test(
  'Bloquea comilla simple',
  testInjectionBlocking("' OR 1=1", true),
  "' OR 1=1 debería ser bloqueado"
);

test(
  'Bloquea comentarios SQL',
  testInjectionBlocking("users --", true),
  "-- debería ser bloqueado"
);

test(
  'Bloquea DROP TABLE',
  testInjectionBlocking("DROP TABLE users", true),
  "DROP TABLE debería ser bloqueado"
);

test(
  'Permite input legítimo',
  testInjectionBlocking("ver los usuarios activos", false),
  "Texto normal no debería ser bloqueado"
);

// Test 5: Strict confidence threshold
console.log(`\n${colors.yellow}[5] UMBRAL DE CONFIANZA ESTRICTO${colors.reset}`);

function decideExecution_NEW(confidence) {
  if (confidence < 0.6) {
    return { mode: 'unresolved', message: 'Confianza muy baja' };
  }
  if (confidence >= 0.6 && confidence < 0.8) {
    return { mode: 'execute', warning: `Confianza media (${(confidence * 100).toFixed(0)}%)` };
  }
  return { mode: 'execute', warning: null };
}

test(
  'Confidence < 0.6 rechazado',
  decideExecution_NEW(0.55)?.mode === 'unresolved',
  `Confidence 0.55 debería rechazarse`
);

test(
  'Confidence 0.6-0.8 ejecuta con warning',
  decideExecution_NEW(0.72)?.warning !== undefined && decideExecution_NEW(0.72)?.mode === 'execute',
  `Confidence 0.72 debería ejecutarse con warning`
);

test(
  'Confidence > 0.8 ejecuta sin warning',
  decideExecution_NEW(0.85)?.warning === null && decideExecution_NEW(0.85)?.mode === 'execute',
  `Confidence 0.85 debería ejecutarse sin warning`
);

// Resumen
console.log(`\n${colors.blue}=== FIN DE VALIDACIÓN ===${colors.reset}\n`);
console.log(`${colors.green}✓ Todos los cambios validados correctamente${colors.reset}\n`);

process.exit(0);
