async function testAiFirstArchitecture() {
  const queries = [
    'dame las tablas que se encuentran en la base de datos',
    'dame los usuarios',
    'usuarios con nombre admin',
    'cuántos usuarios hay',
    'usuarios activos con más logs',
    'lista todos los logs',
    'columnas de users',
    'dame el usuario admin',
    'filtro por rol admin',
  ];

  console.log('🧪 TESTING AI-FIRST ARCHITECTURE\n');

  for (const q of queries) {
    try {
      const r = await fetch('http://localhost:3002/api/query', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ query: q, limit: 10 }),
      });
      const j = await r.json();
      const rows = Array.isArray(j?.resultado?.data) ? j.resultado.data.length : 0;
      const tr = j?.resultado?.trazabilidad || {};

      console.log(`✓ Q: "${q}"`);
      console.log(`  → STATUS=${r.status} | ROWS=${rows} | AI=${tr.interpretadoPor} | INTENT=${tr.intencion}`);
      if (rows > 0) {
        console.log(`  → SAMPLE: ${JSON.stringify(j.resultado.data[0]).substring(0, 70)}...`);
      }
      console.log();
    } catch (e) {
      console.log(`✗ Q: "${q}" → ERROR: ${e.message}\n`);
    }
  }
}

testAiFirstArchitecture().catch(e => {
  console.error(e);
  process.exit(1);
});
