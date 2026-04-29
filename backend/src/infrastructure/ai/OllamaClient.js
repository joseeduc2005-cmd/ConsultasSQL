// Normalize URL: accept both base URL (http://host:port) and full path (.../api/generate)
const _rawUrl = process.env.OLLAMA_URL || 'http://localhost:11434/api/generate';
const OLLAMA_URL = _rawUrl.endsWith('/api/generate') ? _rawUrl : _rawUrl.replace(/\/$/, '') + '/api/generate';

const OLLAMA_MODEL = process.env.OLLAMA_MODEL || 'deepseek-coder';
const DEFAULT_TIMEOUT_MS = Math.max(500, Number(process.env.OLLAMA_TIMEOUT_MS) || 3000);

// ---------------------------------------------------------------------------
// JSON extraction — tolerates truncated / partially generated responses.
// Strategy (in order of strictness):
//   1. Direct JSON.parse
//   2. Fenced code block  ```json ... ```
//   3. Brace-slice JSON.parse
//   4. Regex field extraction (handles truncated explanation)
// ---------------------------------------------------------------------------
function extractJsonFromText(text = '') {
  const source = String(text || '').trim();
  if (!source) return null;

  // 1. Direct parse
  try { return JSON.parse(source); } catch { /* continue */ }

  // 2. Fenced code block
  const fenced = source.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  if (fenced?.[1]) {
    try { return JSON.parse(fenced[1]); } catch { /* continue */ }
  }

  // 3. Brace-slice — finds outermost { ... }
  const start = source.indexOf('{');
  const end = source.lastIndexOf('}');
  if (start >= 0 && end > start) {
    try { return JSON.parse(source.slice(start, end + 1)); } catch { /* continue */ }
  }

  // 4. Regex field extraction — works even when the JSON is truncated mid-explanation
  //    We only need "sql" and "database_id"; explanation is optional.
  const sqlMatch = source.match(/"sql"\s*:\s*"((?:[^"\\]|\\.)*)"/);
  const dbMatch = source.match(/"database_id"\s*:\s*"([^"]+)"/);
  if (sqlMatch?.[1] && dbMatch?.[1]) {
    const explanationMatch = source.match(/"explanation"\s*:\s*"((?:[^"\\]|\\.)*)"/);
    console.log('[Ollama] extractJson: used regex fallback (truncated response)');
    return {
      sql: sqlMatch[1],
      database_id: dbMatch[1],
      explanation: explanationMatch?.[1] || '',
    };
  }

  return null;
}

export async function askOllama(prompt, options = {}) {
  const timeoutMs = Math.max(300, Number(options?.timeoutMs) || DEFAULT_TIMEOUT_MS);
  const model = String(options?.model || OLLAMA_MODEL).trim() || 'deepseek-coder';

  console.log(`[Ollama] → model=${model} timeout=${timeoutMs}ms url=${OLLAMA_URL}`);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(OLLAMA_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model,
        prompt: String(prompt || ''),
        stream: false,
        options: {
          // Enough tokens for the JSON (sql + database_id + short explanation)
          num_predict: 300,
          // Low temperature → more deterministic, fewer hallucinations
          temperature: 0.1,
        },
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      let errBody = '';
      try { errBody = await response.text(); } catch { /* ignore */ }
      console.warn(`[Ollama] ✗ HTTP ${response.status} — ${errBody.slice(0, 200)}`);
      return { ok: false, text: '', json: null, error: `Ollama respondió ${response.status}: ${errBody.slice(0, 120)}` };
    }

    const payload = await response.json();
    const text = String(payload?.response || '').trim();
    const parsed = extractJsonFromText(text);

    console.log(`[Ollama] ✓ text[0..150]="${text.slice(0, 150)}" json=${parsed ? 'ok' : 'null'}`);

    return { ok: true, text, json: parsed, error: null };
  } catch (error) {
    const msg = error instanceof Error ? error.message : 'Error de conexión con Ollama';
    console.warn(`[Ollama] ✗ catch: ${msg}`);
    return { ok: false, text: '', json: null, error: msg };
  } finally {
    clearTimeout(timer);
  }
}

export default askOllama;
