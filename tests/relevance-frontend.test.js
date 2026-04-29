/**
 * Manual test for Database Relevance Validator integration
 * Run this in the browser console or as a test script
 */

const API_URL = 'http://localhost:3002/api/query';

const testQueries = [
  // Database queries (should be accepted)
  { query: 'mostrar usuarios', expected: 'ACCEPT', category: 'DB Query' },
  { query: 'usuarios activos', expected: 'ACCEPT', category: 'DB Query' },
  { query: 'usuarios con más de 5 logs', expected: 'ACCEPT', category: 'DB Query' },
  { query: 'contar registros de usuarios', expected: 'ACCEPT', category: 'DB Query' },
  { query: 'listar permisos de roles', expected: 'ACCEPT', category: 'DB Query' },
  
  // Non-database queries (should be rejected)
  { query: 'hola', expected: 'REJECT', category: 'Greeting' },
  { query: 'como estás', expected: 'REJECT', category: 'Small Talk' },
  { query: 'dame un chiste', expected: 'REJECT', category: 'Entertainment' },
  { query: 'cuéntame una historia', expected: 'REJECT', category: 'Story' },
  { query: 'información sobre películas', expected: 'REJECT', category: 'General Knowledge' },
];

async function runTest(testCase) {
  try {
    const response = await fetch(API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: testCase.query }),
    });
    
    const data = await response.json();
    
    // Determine if it was accepted or rejected based on executionType
    // 'not-db-related' means the relevance validator rejected it
    // Any other executionType means it passed the relevance gate (even if query failed later)
    const wasRejectedByValidator = data.executionType === 'not-db-related';
    const result = wasRejectedByValidator ? 'REJECT' : 'ACCEPT';
    const actualExecution = data.executionType || 'unknown';
    const isCorrect = result === testCase.expected;
    
    return {
      query: testCase.query,
      category: testCase.category,
      expected: testCase.expected,
      actual: result,
      executionType: actualExecution,
      success: data.success,
      message: data.message || data.error,
      isCorrect,
    };
  } catch (error) {
    return {
      query: testCase.query,
      category: testCase.category,
      expected: testCase.expected,
      actual: 'ERROR',
      error: error.message,
      isCorrect: false,
    };
  }
}

async function runAllTests() {
  console.log('\n=== DATABASE RELEVANCE VALIDATOR - FRONTEND TEST ===\n');
  
  const results = [];
  
  for (const testCase of testQueries) {
    console.log(`Testing: "${testCase.query}"...`);
    const result = await runTest(testCase);
    results.push(result);
    
    if (result.isCorrect) {
      console.log(`✅ ${result.actual} (${result.executionType})`);
    } else {
      console.log(`❌ Expected ${result.expected}, got ${result.actual}`);
      if (result.message) console.log(`   Message: ${result.message}`);
    }
  }
  
  // Summary
  const passed = results.filter(r => r.isCorrect).length;
  const total = results.length;
  
  console.log(`\n=== RESULTS ===`);
  console.log(`✅ Passed: ${passed}/${total}`);
  console.log(`❌ Failed: ${total - passed}/${total}`);
  console.log(`Success Rate: ${((passed / total) * 100).toFixed(1)}%\n`);
  
  // Detailed results
  console.log('=== DETAILED RESULTS ===');
  for (const result of results) {
    const status = result.isCorrect ? '✅' : '❌';
    console.log(`${status} [${result.category}] "${result.query}"`);
    console.log(`   Expected: ${result.expected}, Got: ${result.actual}`);
    if (result.message) console.log(`   Message: ${result.message}`);
  }
}

// Export for use in Node.js or browser
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { runAllTests, runTest, testQueries };
}

// Auto-run if called directly
if (typeof window === 'undefined') {
  runAllTests().then(() => process.exit(0));
}
