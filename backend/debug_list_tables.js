async function debugListTables() {
  const queries = [
    'dame las tablas que se encuentran en la base de datos',
    'listar las tablas',
    'todas las tablas',
    'tablas',
    'qué tablas existen',
  ];

  for (const q of queries) {
    try {
      const r = await fetch('http://localhost:3002/api/query', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ query: q }),
      });
      const j = await r.json();
      const rows = Array.isArray(j?.resultado?.data) ? j.resultado.data.length : 0;
      const msg = j?.resumenHumano || '';
      const intent = j?.resultado?.trazabilidad?.intencion || 'N/A';

      console.log(`\nQ: "${q}"`);
      console.log(`→ ROWS=${rows} | INTENT=${intent}`);
      console.log(`→ MSG=${msg.substring(0, 80)}`);
      if (rows > 0) {
        const data = j.resultado.data.map(row => row.tabla || row.columna || Object.values(row)[0]).join(', ');
        console.log(`→ DATA: ${data}`);
      }
    } catch (e) {
      console.log(`✗ "${q}" → ${e.message}`);
    }
  }
}

debugListTables().catch(e => {
  console.error(e);
  process.exit(1);
});
