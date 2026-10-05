/**
 * Las rutas de consultas: generar, ejecutar, analizar, explicar.
 *
 * Novecientas lineas que vivian dentro de `app.js`, atadas a treinta y dos cosas del
 * modulo que tenian al alcance de la mano. Aqui llegan por parametro, y eso es lo que
 * cambia: de que depende este codigo pasa de ser algo que hay que deducir leyendo
 * siete mil lineas a algo que esta escrito en la primera.
 *
 * Siguen siendo manejadores con logica dentro; volverlos finos es el paso siguiente.
 * Pero ya se puede ver lo que tocan, que es por donde se empieza.
 *
 * Nota de lo que se encontro al moverlos: habia dos rutas GET /suggestions. Express
 * sirve la primera, asi que la segunda —que ademas refresca la cache del aprendizaje
 * semantico y devuelve un mensaje de error util— no se ejecutaba nunca. Se quedo la
 * segunda.
 */
import { Router } from 'express';

export function crearRutasConsultas({
  buildCrossDbSelectOptions,
  buildEngineLog,
  buildHumanSqlExplanation,
  buildNaturalLanguageSuggestions,
  buildProductionSqlResponse,
  decideQueryExecutionMode,
  detectSqlSyntaxEngine,
  ensureDynamicSchemaReady,
  ensureMultiDbReady,
  executeDistributedWithExplanation,
  executeManualSqlScript,
  executeSafeSelectQuery,
  extractDatabaseDirective,
  getUserFromRequest,
  hasRequiredRole,
  isComplexSqlScript,
  isNativePlSql,
  learnSemanticMappingsFromSuccessfulQuery,
  logQueryHistory,
  multiDatabaseEngine,
  multiDbRegistry,
  normalizeGeneratedSelectSql,
  normalizeRole,
  pool,
  queryAnalyzer,
  queryBuilder,
  queryParameterizer,
  refreshSemanticLearningCache,
  resolveErrorAnalysisResponse,
  resolveRequestedDatabasesForQuery,
  resultExplainer,
  shouldForceProceduralMode,
}) {
  const router = Router();


  // GET /api/query/analyze - Analiza palabras clave y detecta tablas
  router.get('/analyze', async (req, res) => {
    try {
      const { text } = req.query;
    
      if (!text || text.trim().length === 0) {
        return res.status(400).json({
          success: false,
          error: 'Parámetro "text" requerido'
        });
      }

      const analisis = await queryBuilder.analyzeKeywords(text);
    
      res.status(200).json({
        success: true,
        analisis,
        mensaje: 'Análisis de palabras clave completado'
      });
    } catch (error) {
      console.error('[QUERY ANALYZE] ❌ Error:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });

  // POST /api/query/generate - Genera SQL automáticamente basado en entrada
  // BODY: { "text": "usuarios con logs", "limit": 10, "offset": 0 }
  router.post('/generate', async (req, res) => {
    try {
      const { text, limit = 50, offset = 0 } = req.body;
      await refreshSemanticLearningCache(false);
    
      if (!text || text.trim().length === 0) {
        return res.status(400).json({
          success: false,
          error: 'Campo "text" es requerido en el body'
        });
      }

      // MODO ANÁLISIS DE ERRORES
      const errorAnalysisResponse = await resolveErrorAnalysisResponse(text, pool);
      if (errorAnalysisResponse) {
        return res.status(200).json(errorAnalysisResponse);
      }

      const normalizedLimit = Math.min(parseInt(limit) || 50, 1000);
      const normalizedOffset = Math.max(parseInt(offset) || 0, 0);

      // 0) Ollama SQL generation — handles ANY natural-language DB question
      // Runs before the semantic engine so mis-parses ("entidad 'de'", ILIKE on engine names) never happen.
      console.log(`[AI-SQL-GEN] PASO 0 iniciado para: "${text}"`);
      try {
        await ensureMultiDbReady({ reason: 'query-generate-ai' });
        const _ollamaRegistry = multiDbRegistry;
        const _ollamaGenerated = await (async () => {
          const allDbs = typeof _ollamaRegistry?.getDatabases === 'function' ? _ollamaRegistry.getDatabases() : [];
          const dbList = allDbs.filter((d) => d.enabled).map((d) => `${d.id} (${d.type})`).join(', ');

          // Build schema from all DBs
          const schemaParts = [];
          for (const db of allDbs.filter((d) => d.enabled)) {
            try {
              let metaSql;
              if (db.type === 'postgres') metaSql = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name, ordinal_position";
              else if (db.type === 'oracle') metaSql = 'SELECT table_name, column_name FROM USER_TAB_COLUMNS ORDER BY table_name, column_id';
              else continue;
              const rows = await _ollamaRegistry.executeCompiledQuery({ databaseId: db.id, sql: metaSql, params: [] });
              const grouped = new Map();
              for (const row of rows || []) {
                const t = String(row.TABLE_NAME || row.table_name || '').trim();
                const c = String(row.COLUMN_NAME || row.column_name || '').trim();
                if (t && c) { const curr = grouped.get(t) || []; if (curr.length < 6) curr.push(c); grouped.set(t, curr); }
              }
              if (grouped.size > 0) {
                schemaParts.push(`--- ${db.type.toUpperCase()} (${db.id}) ---`);
                for (const [t, cols] of [...grouped.entries()].slice(0, 12)) schemaParts.push(`${t}(${cols.join(', ')})`);
              }
            } catch { /* skip */ }
          }
          const schema = schemaParts.length > 0 ? schemaParts.join('\n') : 'Sin esquema disponible';

          const prompt = [
            'You are a SQL generator. Read the user question and return ONLY a compact JSON on one line.',
            '',
            'STRICT RULES:',
            '1. Output ONLY a single-line JSON object. No markdown, no code blocks, no extra text.',
            '2. Format: {"sql":"...","database_id":"...","explanation":"..."}',
            '3. explanation must be SHORT (max 10 words in Spanish). Keep the whole JSON under 250 chars.',
            '4. sql: one SELECT only. No bind variables (:1 :name). No placeholder values like \'your-value\'.',
            '5. NEVER add WHERE filters unless the user explicitly asks to filter by a value.',
            '6. When the user asks to "show table X" or "dame la tabla X" → use SELECT * with a row limit. No WHERE.',
            '7. Never INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE.',
            '8. database_id: exact id from Available databases list.',
            '9. Never use backtick quotes. Use plain identifiers.',
            '10. PostgreSQL row limit: LIMIT 50. Oracle row limit: WHERE ROWNUM <= 50. Never mix them.',
            '',
            'EXAMPLES (copy this exact syntax):',
            '  User asks to list Oracle tables  → {"sql":"SELECT table_name FROM USER_TABLES ORDER BY table_name","database_id":"oracle_test","explanation":"Lista tablas Oracle"}',
            '  User asks to show Oracle table T → {"sql":"SELECT * FROM T WHERE ROWNUM <= 50","database_id":"oracle_test","explanation":"Datos de T"}',
            '  User asks to describe Oracle T   → {"sql":"SELECT column_name,data_type,nullable FROM USER_TAB_COLUMNS WHERE table_name=\'T\' ORDER BY column_id","database_id":"oracle_test","explanation":"Estructura de T"}',
            '  User asks to list Postgres tables → {"sql":"SELECT table_name FROM information_schema.tables WHERE table_schema=\'public\' ORDER BY table_name","database_id":"pg_main","explanation":"Lista tablas Postgres"}',
            '  User asks to show Postgres table T → {"sql":"SELECT * FROM T LIMIT 50","database_id":"pg_main","explanation":"Datos de T"}',
            '  User asks to describe Postgres T  → {"sql":"SELECT column_name,data_type,is_nullable FROM information_schema.columns WHERE table_name=\'T\' ORDER BY ordinal_position","database_id":"pg_main","explanation":"Estructura de T"}',
            '',
            `Available databases: ${dbList}`,
            '',
            'Schema (use real table names from here):',
            schema,
            '',
            `User question: ${text}`,
            '',
            'JSON (one line, no extra text):',
          ].join('\n');

          const { askOllama: _ask } = await import('./infrastructure/ai/OllamaClient.js');
          console.log(`[AI-SQL-GEN] llamando Ollama model=${process.env.OLLAMA_MODEL || 'deepseek-coder'} timeout=30000ms`);
          const response = await _ask(prompt, { timeoutMs: 30000, model: process.env.OLLAMA_MODEL || undefined });
          console.log(`[AI-SQL-GEN] Ollama ok=${response?.ok} json=${response?.json ? 'presente' : 'null'} error=${response?.error || 'ninguno'}`);
          if (!response?.ok || !response?.json) return null;

          const sql = String(response.json?.sql || '').trim();
          let databaseId = String(response.json?.database_id || '').trim();
          const explanation = String(response.json?.explanation || '').trim();

          console.log(`[AI-SQL-GEN] JSON recibido: sql="${sql.slice(0,80)}" db="${databaseId}"`);

          if (!sql) { console.warn('[AI-SQL-GEN] rechazado: sql vacío'); return null; }
          // Block non-SELECT and bind variables
          const norm = sql.replace(/\s+/g, ' ').toLowerCase();
          if (!norm.startsWith('select') && !norm.startsWith('with')) { console.warn(`[AI-SQL-GEN] rechazado: no es SELECT/WITH`); return null; }
          if (/\b(insert|update|delete|drop|truncate|alter)\b/.test(norm)) { console.warn('[AI-SQL-GEN] rechazado: contiene DML/DDL'); return null; }
          if (/:[a-zA-Z0-9_]+/.test(sql)) { console.warn('[AI-SQL-GEN] rechazado: bind placeholder detectado'); return null; }

          // Validate/fallback databaseId
          const targetDb = allDbs.find((d) => d.id === databaseId && d.enabled);
          if (!targetDb) {
            const oracleMentioned = /oracle/i.test(text);
            const postgresMentioned = /postgres/i.test(text);
            const fb = oracleMentioned ? allDbs.find((d) => d.type === 'oracle' && d.enabled)
              : postgresMentioned ? allDbs.find((d) => d.type === 'postgres' && d.enabled)
              : allDbs.find((d) => d.enabled);
            if (!fb) return null;
            databaseId = fb.id;
          }

          // ── Sanitize SQL dialect errors from Ollama ──────────────────────────
          const resolvedDbType = allDbs.find((d) => d.id === databaseId)?.type || 'postgres';
          let cleanSql = sql
            // Remove trailing semicolon (Oracle rejects it in driver calls)
            .trimEnd().replace(/;+$/, '')
            // Remove MySQL-style backtick quoting → plain identifier
            .replace(/`([^`]+)`/g, '$1')
            // Remove hallucinated placeholder WHERE clauses like WHERE col='your-value' / WHERE col=?
            .replace(/\bWHERE\s+\w+\s*=\s*'your-[^']*'/gi, '')
            .replace(/\bWHERE\s+\w+\s*=\s*\?/gi, '')
            .replace(/\bAND\s+\w+\s*=\s*'your-[^']*'/gi, '')
            .replace(/\bAND\s+\w+\s*=\s*\?/gi, '')
            .trim();

          if (resolvedDbType === 'postgres') {
            // Oracle ROWNUM → Postgres LIMIT
            cleanSql = cleanSql
              .replace(/\bWHERE\s+ROWNUM\s*<=\s*(\d+)/gi, 'LIMIT $1')
              .replace(/\bWHERE\s+ROWNUM\s*<\s*(\d+)/gi, (_, n) => `LIMIT ${Math.max(1, parseInt(n) - 1)}`)
              .replace(/\bAND\s+ROWNUM\s*<=\s*(\d+)/gi, 'LIMIT $1')
              .replace(/\bAND\s+ROWNUM\s*<\s*(\d+)/gi, (_, n) => `LIMIT ${Math.max(1, parseInt(n) - 1)}`);
            // Add LIMIT if there's none and no WHERE clause
            if (!/\bLIMIT\b/i.test(cleanSql) && !/\bWHERE\b/i.test(cleanSql)) {
              cleanSql = cleanSql + ' LIMIT 50';
            }
          } else if (resolvedDbType === 'oracle') {
            // Postgres LIMIT → Oracle ROWNUM
            cleanSql = cleanSql.replace(/\bLIMIT\s+(\d+)/gi, 'WHERE ROWNUM <= $1');
            // Fix double WHERE: "WHERE x=1 WHERE ROWNUM" → "WHERE x=1 AND ROWNUM"
            cleanSql = cleanSql.replace(/WHERE\s+((?!ROWNUM).+?)\s+WHERE\s+ROWNUM/i, 'WHERE $1 AND ROWNUM');
          }

          console.log(`[AI-SQL-GEN] Parsed: sql="${cleanSql.slice(0, 120)}" db="${databaseId}"`);
          return { sql: cleanSql, databaseId, explanation };
        })();

        if (_ollamaGenerated?.sql && _ollamaGenerated?.databaseId) {
          console.log(`[AI-SQL-GEN] generate "${text}" → ${_ollamaGenerated.databaseId}: ${_ollamaGenerated.sql.slice(0, 80)}`);
          return res.status(200).json({
            success: true,
            exito: true,
            query: { sql: _ollamaGenerated.sql, databaseId: _ollamaGenerated.databaseId },
            sql: _ollamaGenerated.sql,
            tablaBase: null,
            debug: { motor: 'ollama-sql-gen', databaseId: _ollamaGenerated.databaseId },
            timestamp: Date.now(),
          });
        }
      } catch (ollamaGenErr) {
        console.warn('[AI-SQL-GEN] generate fallback:', ollamaGenErr instanceof Error ? ollamaGenErr.message : ollamaGenErr);
      }

      // 1) Preferred path: distributed intelligence engine (same core used by /api/query)
      // This gives better entity detection from schema + semantic learning.
      let distributedError = null;
      try {
        await ensureMultiDbReady({ reason: 'query-generate' });
        const requestedDatabases = resolveRequestedDatabasesForQuery(text, req.body?.databases || []);
        const distributedResult = await multiDatabaseEngine.execute(text, {
          limit: normalizedLimit,
          offset: normalizedOffset,
          databases: requestedDatabases,
        });

        if (distributedResult?.success && distributedResult.executionType !== 'unresolved') {
          const primarySql = distributedResult?.scripts?.[0]?.sql || null;
          const selectedTable = distributedResult?.resolution?.entities?.[0]?.primary?.tableName
            || distributedResult?.resolution?.entities?.[0]?.table
            || null;
          const confidence = Number(distributedResult?.confidence || distributedResult?.resolution?.confidence || 0);

          if (primarySql) {
            const syntheticResult = {
              exito: true,
              query: { sql: primarySql },
              analisis: { tablaBase: selectedTable },
              debug: {
                tablaSeleccionada: selectedTable,
                confianza: confidence,
                motor: 'multi-database-engine',
                executionType: distributedResult.executionType,
                warnings: distributedResult.warnings || [],
                sources: distributedResult.source || [],
              },
            };

            await learnSemanticMappingsFromSuccessfulQuery(text, syntheticResult);

            return res.status(200).json({
              success: true,
              sql: primarySql,
              tablaBase: selectedTable,
              debug: syntheticResult.debug,
              timestamp: Date.now(),
              ...syntheticResult,
            });
          }
        } else {
          distributedError = distributedResult?.message || distributedResult?.error || null;
        }
      } catch (error) {
        distributedError = error instanceof Error ? error.message : String(error || '');
      }

      // 2) Compatibility fallback: legacy queryBuilder.generateQuery
      const resultado = await queryBuilder.generateQuery(text, {
        limit: normalizedLimit,
        offset: normalizedOffset,
      });

      if (!resultado.exito) {
        return res.status(400).json({
          success: false,
          error: distributedError || resultado?.error || 'No se pudo interpretar la consulta con suficiente confianza.',
          sql: null,
          tablaBase: null,
          debug: {
            ...(resultado?.debug || {}),
            distributedError,
          },
          timestamp: Date.now(),
          ...resultado,
        });
      }

      await learnSemanticMappingsFromSuccessfulQuery(text, resultado);

      res.status(200).json({
        success: true,
        sql: resultado?.query?.sql || null,
        tablaBase: resultado?.analisis?.tablaBase || resultado?.debug?.tablaSeleccionada || null,
        debug: resultado?.debug || null,
        timestamp: Date.now(),
        ...resultado,
      });
    } catch (error) {
      console.error('[QUERY GENERATE] ❌ Error:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });

  // POST /api/query/execute-generated - Ejecuta una consulta generada automáticamente
  // BODY: { "sql": "SELECT ...", "mode": "simple" }
  router.post('/execute-generated', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = normalizeRole(req.headers['x-user-role'] || user?.role || 'user');
      const { sql } = req.body;

      if (!sql || sql.trim().length === 0) {
        return res.status(400).json({
          success: false,
          error: 'Campo "sql" es requerido'
        });
      }

      const proceduralMode = shouldForceProceduralMode(sql) || isComplexSqlScript(sql);
      if (proceduralMode) {
        if (!hasRequiredRole(role, 'admin')) {
          return res.status(403).json({
            success: false,
            error: 'Scripts procedurales requieren rol ADMIN o SUPERADMIN.',
          });
        }

        const progress = [
          '🔍 Paso 1: Analizando script',
          '📊 Paso 2: Procesando lógica',
          '📈 Paso 3: Generando resultado',
        ];

        const proceduralResult = await executeManualSqlScript({ sql }, {}, (msg) => {
          const clean = String(msg || '').trim();
          if (clean) progress.push(clean);
        });

        const executionMs = Number(proceduralResult?.executionMs || 0);
        const rows = Array.isArray(proceduralResult?.data) ? proceduralResult.data : [];

        await logQueryHistory({
          username: user?.username || 'anonymous',
          role,
          queryText: null,
          generatedSql: sql,
          executionMs,
          rowCount: rows.length,
          wasCached: false,
          status: 'ok'
        });

        return res.status(200).json(
          buildProductionSqlResponse({
            success: true,
            rows,
            notices: (proceduralResult?.notices || []).concat(progress),
            semanticCandidate: proceduralResult?.resultadoSemantico || (rows[0] || {}),
            explicacion: proceduralResult?.resumenHumano || proceduralResult?.explicacion || proceduralResult?.message || '',
            message: proceduralResult?.message || 'Script procedural ejecutado correctamente',
          }),
        );
      }

      const explicitDatabaseId = String(req.body?.databaseId || '').trim();
      const explicitDatabaseHint = String(req.body?.databaseHint || '').trim();

      // ── Directive + syntax-based routing ─────────────────────────────────────
      const _directiveId = extractDatabaseDirective(sql);
      const _detectedEngine = (!explicitDatabaseId && !explicitDatabaseHint && !_directiveId)
        ? detectSqlSyntaxEngine(sql) : null;
      const _engineDb = _detectedEngine ? multiDbRegistry.findDatabaseByType(_detectedEngine) : null;

      // Native Oracle PL/SQL execution (BEGIN...END with Oracle packages)
      const plsqlDirectiveDb = _directiveId ? multiDbRegistry.getDatabaseById(_directiveId) : null;
      const oraclePlSqlTarget = plsqlDirectiveDb?.type === 'oracle'
        ? plsqlDirectiveDb
        : (isNativePlSql(sql) ? multiDbRegistry.findDatabaseByType('oracle') : null);

      if (oraclePlSqlTarget) {
        const plsqlRows = await multiDbRegistry.executeOraclePlSqlBlock(oraclePlSqlTarget.id, sql);
        await logQueryHistory({
          username: user?.username || 'anonymous',
          role,
          queryText: null,
          generatedSql: sql,
          executionMs: 0,
          rowCount: plsqlRows.length,
          wasCached: false,
          status: 'ok',
        });
        return res.status(200).json(
          buildProductionSqlResponse({
            success: true,
            rows: plsqlRows,
            notices: [buildEngineLog({ engine: 'oracle', queryType: 'plsql', databaseId: oraclePlSqlTarget.id, reason: 'native PL/SQL execution', sql })],
            explicacion: 'Script PL/SQL ejecutado en Oracle.',
          }),
        );
      }

      // Propagate directive/engine routing to executeSafeSelectQuery
      if (_directiveId && !explicitDatabaseId) {
        req.body = { ...req.body, databaseId: _directiveId };
      } else if (_engineDb && !explicitDatabaseId && !explicitDatabaseHint) {
        req.body = { ...req.body, databaseHint: _detectedEngine };
      }

      // Validar SQL solo cuando no hay routing explícito multi-DB.
      // En modo multi-DB, el control final lo realiza executeSafeSelectQuery.
      if (!explicitDatabaseId && !explicitDatabaseHint && !_directiveId && !_engineDb) {
        const validacion = queryBuilder.validateSQL(sql, role);
        if (!validacion.valido) {
          return res.status(403).json({
            success: false,
            error: `SQL rechazado: ${validacion.razon}`
          });
        }
      }

      // Ejecutar con seguridad (incluye auto-reparación para compatibilidad de tipos)
      const startTime = Date.now();
      let sqlToRun = String(sql || '');
      let autoRepaired = false;
      let result;

      const executeGeneratedSelectOptions = buildCrossDbSelectOptions({
        databaseId: req.body?.databaseId,
        requestedDatabases: Array.isArray(req.body?.databases) ? req.body.databases : [],
        queryHintText: [req.body?.query, req.body?.text, req.body?.prompt].filter(Boolean).join(' '),
      });

      try {
        result = await executeSafeSelectQuery(sqlToRun, [], 5000, executeGeneratedSelectOptions);
      } catch (firstError) {
        const message = String(firstError?.message || '');
        const looksLikeIlikeTypeError = /operator does not exist: .*~~\*/i.test(message);

        if (!looksLikeIlikeTypeError) throw firstError;

        const repairedSql = normalizeGeneratedSelectSql(sqlToRun);
        if (!repairedSql || repairedSql === sqlToRun) throw firstError;

        const repairedValidation = queryBuilder.validateSQL(repairedSql, role);
        if (!repairedValidation.valido) throw firstError;

        sqlToRun = repairedSql;
        autoRepaired = true;
        result = await executeSafeSelectQuery(sqlToRun, [], 5000, executeGeneratedSelectOptions);
      }

      const executionMs = Date.now() - startTime;

      // Registrar en historial
      await logQueryHistory({
        username: user?.username || 'anonymous',
        role,
        queryText: null,
        generatedSql: sqlToRun,
        executionMs,
        rowCount: result.rowCount || 0,
        wasCached: false,
        status: 'ok'
      });

      const emptyResult = queryBuilder.handleEmptyResults({ rows: result.rows, rowCount: result.rowCount || 0 }, { sql: sqlToRun });

      res.status(200).json(
        buildProductionSqlResponse({
          success: true,
          rows: emptyResult.data,
          notices: [],
          semanticCandidate: emptyResult.data?.[0] || {},
          explicacion: buildHumanSqlExplanation(emptyResult.data || []),
          message: emptyResult.message || '',
        }),
      );
    } catch (error) {
      console.error('[QUERY EXECUTE] ❌ Error:', error.message);

      // Registrar error en historial
      const user = getUserFromRequest(req);
      await logQueryHistory({
        username: user?.username || 'anonymous',
        role: normalizeRole(req.headers['x-user-role'] || 'user'),
        queryText: null,
        generatedSql: req.body.sql || '',
        executionMs: 0,
        rowCount: 0,
        wasCached: false,
        status: 'error'
      });

      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });

  // POST /api/query/analyze - Analiza consulta y detecta si requiere input
  // BODY: { "text": "usuario admin" }
  // RESPONSE: { inputRequired: boolean, prompt: string, analysis: {...} }
  router.post('/analyze', async (req, res) => {
    try {
      const { text } = req.body;

      if (!text || text.trim().length === 0) {
        return res.status(400).json({
          success: false,
          error: 'Campo "text" es requerido'
        });
      }

      const analysis = queryAnalyzer.analyzeQuery(text);

      res.status(200).json({
        success: true,
        analysis,
        inputRequired: analysis.inputRequerido.requiereInput,
        prompt: analysis.inputPrompt,
        canExecute: analysis.puedeEjecutarse,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error('[QUERY ANALYZE] ❌ Error:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });

  // POST /api/query/execute-with-explanation - Ejecuta query Y genera explicación
  // BODY: { "text": "usuarios con logs", "inputValue": null, "parameterValue": null }
  // RESPONSE: { explanation: string, data: [...], analysisDetails: {...} }
  // 
  // SOPORTA FLUJO PARAMETRIZADO:
  // Si text es incompleto (ej: "usuario") y no hay parameterValue:
  //   → Retorna prompt pidiendo parámetro
  // Si hay parameterValue, lo inyecta y ejecuta normalmente
  router.post('/execute-with-explanation', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = normalizeRole(req.headers['x-user-role'] || user?.role || 'user');
      const { text, inputValue, parameterValue, limit = 50, offset = 0, databases = [] } = req.body;
      await refreshSemanticLearningCache(false);

      if (!text || text.trim().length === 0) {
        return res.status(400).json({
          success: false,
          error: 'Campo "text" es requerido'
        });
      }

      // ============================================================
      // MODO ANÁLISIS DE ERRORES
      // Detectar si el usuario pegó un mensaje de error o stack trace
      // ============================================================
      const errorAnalysisResponse = await resolveErrorAnalysisResponse(text, pool);
      if (errorAnalysisResponse) {
        return res.status(200).json(errorAnalysisResponse);
      }

      // ============================================================
      // NUEVA CAPA: DETECCIÓN DE CONSULTAS PARAMETRIZADAS
      // ============================================================
      // Verificar si es una consulta que requiere parámetro
      const parameterDetection = queryParameterizer.processParameterizedQuery(text, parameterValue);

      // Si requiere parámetro y no lo tiene
      if (parameterDetection.esParametrizado && parameterDetection.requiereInput && !parameterValue) {
        return res.status(202).json({
          success: false,
          requiresParameter: true,
          prompt: parameterDetection.prompt,
          message: `Esta consulta requiere un parámetro: ${parameterDetection.prompt.message}`,
          expectedField: parameterDetection.prompt.field,
          examples: parameterDetection.prompt.examples,
          queryOriginal: parameterDetection.inputOriginal,
          httpCode: 202
        });
      }

      // Si hay parámetro, inyectarlo en la query
      let finalText = text;
      if (parameterDetection.esParametrizado && parameterValue && parameterDetection.queryEnhanced) {
        finalText = parameterDetection.queryEnhanced;
      }

      // Si hay inputValue (compatibilidad con flujo anterior), usarlo también
      const effectiveInputValue = parameterValue || inputValue;

      // Decisión de ejecución: normal (1 base) vs distribuida (múltiples bases)
      const effectiveDatabases = resolveRequestedDatabasesForQuery(finalText, databases);
      const executionDecision = decideQueryExecutionMode(finalText, effectiveDatabases);
      if (executionDecision.mode !== 'normal') {
        const distributedExecution = await executeDistributedWithExplanation({
          text: finalText,
          parameterValue: effectiveInputValue,
          limit,
          databases: effectiveDatabases,
          user,
          role,
          sourceRoute: '/api/query/execute-with-explanation',
        });

        return res.status(distributedExecution.statusCode).json(distributedExecution.payload);
      }

      // ============================================================
      // FLUJO NORMAL: ANÁLISIS Y EJECUCIÓN
      // ============================================================

      // FASE 1-4: Analizar y validar
      const analysis = queryAnalyzer.analyzeQuery(finalText, effectiveInputValue);

      // Si requiere input y no lo tiene, retornar prompt
      if (analysis.inputRequerido.requiereInput) {
        return res.status(400).json({
          success: false,
          requiresInput: true,
          prompt: analysis.inputPrompt,
          analysis,
          message: analysis.inputRequerido.sugerencia
        });
      }

      // Si hay error de validación de input
      if (analysis.inputValidacion && !analysis.inputValidacion.valido) {
        return res.status(400).json({
          success: false,
          error: `Validación fallida: ${analysis.inputValidacion.razon}`,
          analysis
        });
      }

      // Generar SQL con el input validado o sin él
      const modifiedText = effectiveInputValue ? 
        `${finalText} "${effectiveInputValue}"` : 
        finalText;

      const queryResult = await queryBuilder.generateQuery(modifiedText, {
        limit: Math.min(parseInt(limit) || 50, 1000),
        offset: Math.max(parseInt(offset) || 0, 0)
      });

      if (!queryResult.exito) {
        return res.status(400).json({
          success: false,
          error: queryResult.error || 'No se pudo generar la consulta SQL',
          analysis,
          sugerencias: queryResult.sugerencias
        });
      }

      // Validar SQL generado
      const validacion = queryBuilder.validateSQL(queryResult.query.sql, role);
      if (!validacion.valido) {
        return res.status(403).json({
          success: false,
          error: `SQL rechazado: ${validacion.razon}`,
          analysis
        });
      }

      // FASE 5-6: Ejecutar SQL y generar explicación
      const startTime = Date.now();
      let result;

      const explanationSelectOptions = buildCrossDbSelectOptions({
        requestedDatabases: effectiveDatabases,
        queryHintText: finalText,
      });

      try {
        result = await executeSafeSelectQuery(queryResult.query.sql, [], 5000, explanationSelectOptions);
      } catch (firstError) {
        const message = String(firstError?.message || '');
        const looksLikeIlikeTypeError = /operator does not exist: .*~~\*/i.test(message);

        if (!looksLikeIlikeTypeError) throw firstError;

        const repairedSql = normalizeGeneratedSelectSql(queryResult.query.sql);
        if (!repairedSql || repairedSql === queryResult.query.sql) throw firstError;

        const repairedValidation = queryBuilder.validateSQL(repairedSql, role);
        if (!repairedValidation.valido) throw firstError;

        result = await executeSafeSelectQuery(repairedSql, [], 5000, explanationSelectOptions);
      }

      const executionMs = Date.now() - startTime;

      // Generar explicación ANTES de retornar datos
      const explanationResult = resultExplainer.explain(result.rows, queryResult.query.sql);
      const explanation = explanationResult.explicacionCompleta;

      // Registrar en historial
      await logQueryHistory({
        username: user?.username || 'anonymous',
        role,
        queryText: text,
        generatedSql: queryResult.query.sql,
        executionMs,
        rowCount: result.rowCount || 0,
        wasCached: false,
        status: 'ok'
      });

      await learnSemanticMappingsFromSuccessfulQuery(text, queryResult);

      const emptyResult = queryBuilder.handleEmptyResults({ rows: result.rows, rowCount: result.rowCount || 0 }, { input: text, queryResult });

      // RESULTADO FINAL: Explicación + JSON
      res.status(200).json(
        buildProductionSqlResponse({
          success: true,
          rows: emptyResult.data,
          notices: [],
          semanticCandidate: emptyResult.data?.[0] || {},
          explicacion: explanation,
          message: emptyResult.message || '',
        }),
      );
    } catch (error) {
      console.error('[QUERY EXECUTE WITH EXPLANATION] ❌ Error:', error.message);

      const user = getUserFromRequest(req);
      await logQueryHistory({
        username: user?.username || 'anonymous',
        role: normalizeRole(req.headers['x-user-role'] || 'user'),
        queryText: req.body.text || '',
        generatedSql: '',
        executionMs: 0,
        rowCount: 0,
        wasCached: false,
        status: 'error'
      });

      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });

  // POST /api/query/parameterized - Flujo parametrizado para consultas incompletas
  // PASO 1: Usuario envía texto incompleto
  //   Body: { "text": "usuario" }
  //   Response: { requiresParameter: true, prompt: {...} }
  //
  // PASO 2: Usuario envía parámetro
  //   Body: { "text": "usuario", "parameterValue": "admin" }
  //   Response: { explanation: "...", data: [...] }
  router.post('/parameterized', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = normalizeRole(req.headers['x-user-role'] || user?.role || 'user');
      const { text, parameterValue, limit = 50, offset = 0, databases = [] } = req.body;
      await refreshSemanticLearningCache(false);

      if (!text || text.trim().length === 0) {
        return res.status(400).json({
          success: false,
          error: 'Campo "text" es requerido'
        });
      }

      const errorAnalysisResponse = await resolveErrorAnalysisResponse(text, pool);
      if (errorAnalysisResponse) {
        return res.status(200).json(errorAnalysisResponse);
      }

      // FASE 1: Detectar si requiere parámetro
      const parameterDetection = queryParameterizer.processParameterizedQuery(text, parameterValue);

      // FASE 2: Si requiere parámetro, retornar prompt
      if (parameterDetection.esParametrizado && parameterDetection.requiereInput && !parameterValue) {
        return res.status(202).json({
          success: false,
          requiresParameter: true,
          field: parameterDetection.prompt.field,
          message: parameterDetection.prompt.message,
          examples: parameterDetection.prompt.examples,
          prompt: parameterDetection.prompt,
          queryText: parameterDetection.inputOriginal,
          httpStatus: 202,
          note: 'Envíe nuevamente con parameterValue para completar la consulta'
        });
      }

      // Si hay error de validación
      if (parameterDetection.error) {
        return res.status(400).json({
          success: false,
          error: parameterDetection.error,
          queryText: parameterDetection.inputOriginal
        });
      }

      // FASE 3: Inyectar parámetro si es necesario
      let finalText = parameterDetection.queryEnhanced || text;

      // Decisión de ejecución: normal (1 base) vs distribuida (múltiples bases)
      const effectiveDatabases = resolveRequestedDatabasesForQuery(finalText, databases);
      const executionDecision = decideQueryExecutionMode(finalText, effectiveDatabases);
      if (executionDecision.mode !== 'normal') {
        const distributedExecution = await executeDistributedWithExplanation({
          text: finalText,
          parameterValue,
          limit,
          databases: effectiveDatabases,
          user,
          role,
          sourceRoute: '/api/query/parameterized',
        });

        return res.status(distributedExecution.statusCode).json(distributedExecution.payload);
      }

      // FASE 4: Ejecutar con QueryBuilder
      const queryResult = await queryBuilder.generateQuery(finalText, {
        limit: Math.min(parseInt(limit) || 50, 1000),
        offset: Math.max(parseInt(offset) || 0, 0)
      });

      if (!queryResult.exito) {
        return res.status(400).json({
          success: false,
          error: queryResult.error || 'No se pudo generar la consulta SQL',
          sugerencias: queryResult.sugerencias
        });
      }

      // Validar SQL
      const validacion = queryBuilder.validateSQL(queryResult.query.sql, role);
      if (!validacion.valido) {
        return res.status(403).json({
          success: false,
          error: `SQL rechazado: ${validacion.razon}`
        });
      }

      // FASE 5: Ejecutar SQL
      const startTime = Date.now();
      let result;

      const parameterizedSelectOptions = buildCrossDbSelectOptions({
        requestedDatabases: effectiveDatabases,
        queryHintText: finalText,
      });

      try {
        result = await executeSafeSelectQuery(queryResult.query.sql, [], 5000, parameterizedSelectOptions);
      } catch (firstError) {
        const message = String(firstError?.message || '');
        const looksLikeIlikeTypeError = /operator does not exist: .*~~\*/i.test(message);

        if (!looksLikeIlikeTypeError) throw firstError;

        const repairedSql = normalizeGeneratedSelectSql(queryResult.query.sql);
        if (!repairedSql || repairedSql === queryResult.query.sql) throw firstError;

        const repairedValidation = queryBuilder.validateSQL(repairedSql, role);
        if (!repairedValidation.valido) throw firstError;

        result = await executeSafeSelectQuery(repairedSql, [], 5000, parameterizedSelectOptions);
      }

      const executionMs = Date.now() - startTime;

      // FASE 6: Generar explicación
      const explanationResult = resultExplainer.explain(result.rows, queryResult.query.sql);
      const explanation = explanationResult.explicacionCompleta;

      // Registrar en historial
      await logQueryHistory({
        username: user?.username || 'anonymous',
        role,
        queryText: text,
        generatedSql: queryResult.query.sql,
        executionMs,
        rowCount: result.rowCount || 0,
        wasCached: false,
        status: 'ok'
      });

      await learnSemanticMappingsFromSuccessfulQuery(text, queryResult);

      const emptyResult = queryBuilder.handleEmptyResults({ rows: result.rows, rowCount: result.rowCount || 0 }, { input: text, queryResult });

      // FASE 7: Respuesta final con explicación + datos
      res.status(200).json(
        buildProductionSqlResponse({
          success: true,
          rows: emptyResult.data,
          notices: [],
          semanticCandidate: emptyResult.data?.[0] || {},
          explicacion: explanation,
          message: emptyResult.message || '',
        }),
      );
    } catch (error) {
      console.error('[QUERY PARAMETERIZED] ❌ Error:', error.message);

      const user = getUserFromRequest(req);
      await logQueryHistory({
        username: user?.username || 'anonymous',
        role: normalizeRole(req.headers['x-user-role'] || 'user'),
        queryText: req.body.text || '',
        generatedSql: '',
        executionMs: 0,
        rowCount: 0,
        wasCached: false,
        status: 'error'
      });

      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });

  // GET /api/query/suggestions - Deterministic suggestions based on schema and relations
  router.get('/suggestions', async (req, res) => {
    try {
      await ensureDynamicSchemaReady(false);
      await refreshSemanticLearningCache(false);
      const q = String(req.query.q || '');
      return res.status(200).json({
        success: true,
        suggestions: buildNaturalLanguageSuggestions(q),
      });
    } catch (error) {
      console.error('[QUERY SUGGESTIONS] ❌ Error:', error);
      return res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : 'Error interno del servidor',
      });
    }
  });
  return router;
}
