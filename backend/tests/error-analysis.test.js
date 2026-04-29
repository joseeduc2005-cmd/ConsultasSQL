import {
  analyzeErrorInput,
  buildErrorAnalysisResponse,
  detectErrorAnalysisInput,
} from '../src/infrastructure/query/ErrorAnalysisEngine.js';

describe('ErrorAnalysisEngine', () => {
  test('detects backend error-like input', () => {
    expect(detectErrorAnalysisInput('Error: connection refused at UserRepository.js:120')).toBe(true);
    expect(detectErrorAnalysisInput('usuarios con logs')).toBe(false);
  });

  test('extracts module, file, line and inferred cause', () => {
    const analysis = analyzeErrorInput('Error: connection refused at UserRepository.js:120');

    expect(analysis.tipo_error).toBe('connection refused');
    expect(analysis.modulo).toBe('UserRepository');
    expect(analysis.archivo).toBe('UserRepository.js');
    expect(analysis.linea).toBe(120);
    expect(analysis.capa).toBe('Repository');
    expect(String(analysis.causa || '').toLowerCase()).toContain('conexión rechazada');
  });

  test('builds clean production response with frequency and solution', () => {
    const analysis = analyzeErrorInput('password authentication failed in AuthService.ts:42');
    const response = buildErrorAnalysisResponse(analysis, {
      frequency: 5,
      rows: [{ modulo: 'AuthService', capa: 'Service', archivo: 'AuthService.ts', linea: 42, contexto: 'login' }],
    });

    expect(response).toEqual({
      resumenHumano: expect.any(String),
      resultado: expect.objectContaining({
        tipo_error: 'password authentication failed',
        modulo: 'AuthService',
        capa: 'Service',
        frecuencia: 5,
        archivo: 'AuthService.ts',
        linea: 42,
        solucion: expect.any(String),
      }),
    });
  });

  test('detects stack trace levels, origin and propagation flow', () => {
    const stackTraceInput = `Error: connection refused\n at UserRepository.findById (src/repository/UserRepository.js:120:17)\n at UserService.getUser (src/service/UserService.js:45:12)\n at UserController.getUser (src/controller/UserController.js:10:5)`;

    const analysis = analyzeErrorInput(stackTraceInput);

    expect(analysis.stack_detectado).toBe(true);
    expect(analysis.niveles_traza).toBe(3);
    expect(analysis.archivo_origen).toBe('UserRepository.js');
    expect(analysis.linea_origen).toBe(120);
    expect(analysis.capa_origen).toBe('Repository');
    expect(analysis.nivel_fallo).toBe(1);
    expect(analysis.flujo_detectado).toContain('Controller -> Service -> Repository');
    expect(analysis.propagacion_detectada).toBe(true);
  });

  test('includes debugging-focused origin fields in final response', () => {
    const analysis = analyzeErrorInput(`Error: timeout\n at SqlRepository.query (SqlRepository.ts:88:3)\n at ReportService.run (ReportService.ts:30:2)\n at ReportController.get (ReportController.ts:12:1)`);
    const response = buildErrorAnalysisResponse(analysis, {
      frequency: 2,
      rows: [],
    });

    expect(response).toEqual({
      resumenHumano: expect.any(String),
      resultado: expect.objectContaining({
        tipo_error: 'timeout',
        archivo_origen: 'SqlRepository.ts',
        linea: 88,
        capa: 'Repository',
        nivel_fallo: 1,
        flujo_detectado: ['Controller -> Service -> Repository'],
        causa: expect.any(String),
        solucion: expect.any(String),
      }),
    });
  });
});
