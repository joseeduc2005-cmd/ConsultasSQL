import { isComplexSqlScript, shouldForceProceduralMode, parsePlSqlScript } from '../src/infrastructure/query/PlSqlInterpreter.js';

describe('PL/SQL interpreter - procedural mode', () => {
  test('should force procedural mode when script contains procedural keywords', () => {
    const sql = `
      DO $$
      DECLARE v_total integer := 0;
      BEGIN
        FOR rec IN (SELECT id FROM users) LOOP
          v_total := v_total + 1;
        END LOOP;
      END $$;
    `;

    expect(shouldForceProceduralMode(sql)).toBe(true);
    expect(isComplexSqlScript(sql)).toBe(true);
  });

  test('should force procedural mode when script only contains RAISE NOTICE block', () => {
    const sql = `
      BEGIN
        RAISE NOTICE 'Procesando usuario: %', 'admin';
      END;
    `;

    expect(shouldForceProceduralMode(sql)).toBe(true);
    expect(isComplexSqlScript(sql)).toBe(true);
  });

  test('SELECT ... INTO should be transformed into executable SELECT with semantic alias', () => {
    const sql = `
      DECLARE v_logs_usuario integer;
      BEGIN
        SELECT COUNT(*) INTO v_logs_usuario FROM logs;
      END;
    `;

    const parsed = parsePlSqlScript(sql);
    expect(Array.isArray(parsed.steps)).toBe(true);
    expect(parsed.steps.length).toBeGreaterThan(0);

    const selectStep = parsed.steps.find((s) => s.operation === 'select');
    expect(selectStep).toBeTruthy();
    expect(String(selectStep.sql || '').toUpperCase()).toContain('COUNT(*) AS logs_usuario'.toUpperCase());
  });

  test('assignment accumulator should generate executable assignment step', () => {
    const sql = `
      DECLARE v_total_logs integer := 0;
      DECLARE v_logs_usuario integer := 1;
      BEGIN
        v_total_logs := v_total_logs + v_logs_usuario;
      END;
    `;

    const parsed = parsePlSqlScript(sql);
    const assignmentStep = parsed.steps.find((s) => s.operation === 'assignment');

    expect(assignmentStep).toBeTruthy();
    expect(String(assignmentStep.sql || '').toUpperCase()).toContain('SELECT');
    expect(String(assignmentStep.sql || '')).toContain('AS total_logs');
  });

  test('must never fallback to SELECT 1 when script has no direct SQL statements', () => {
    const sql = `
      DECLARE v_dummy integer;
      BEGIN
        NULL;
      END;
    `;

    const parsed = parsePlSqlScript(sql);
    const renderedSql = parsed.steps.map((s) => String(s.sql || '')).join('\n');

    expect(renderedSql.toUpperCase()).not.toContain('SELECT 1 AS RESULTADO_FINAL');
    expect(renderedSql.toUpperCase()).not.toContain('SELECT NULL');
    expect(parsed.steps.length).toBeGreaterThan(0);
  });

  test('procedural scripts should end with a real final SELECT projection', () => {
    const sql = `
      DO $$
      DECLARE v_total_logs integer := 0;
      DECLARE v_contador integer := 0;
      BEGIN
        v_total_logs := v_total_logs + 1;
        v_contador := v_contador + 1;
        RAISE NOTICE 'debug %', v_total_logs;
      END $$;
    `;

    const parsed = parsePlSqlScript(sql);
    const lastStep = parsed.steps[parsed.steps.length - 1];
    const finalSql = String(lastStep?.sql || '').toUpperCase();

    expect(String(lastStep?.operation || '')).toBe('select');
    expect(finalSql.startsWith('SELECT ')).toBe(true);
    expect(finalSql).toContain('TOTAL_LOGS');
    expect(finalSql).toContain('TOTAL_USUARIOS');
    expect(finalSql).not.toContain('NULL');
  });

  test('RAISE NOTICE should be parsed as notice step and not ignored', () => {
    const sql = `
      DO $$
      DECLARE v_total_logs integer := 0;
      BEGIN
        v_total_logs := v_total_logs + 1;
        RAISE NOTICE 'Total logs: %', v_total_logs;
      END $$;
    `;

    const parsed = parsePlSqlScript(sql);
    const noticeStep = parsed.steps.find((s) => s.operation === 'notice');
    const lastStep = parsed.steps[parsed.steps.length - 1];

    expect(noticeStep).toBeTruthy();
    expect(String(noticeStep.noticeTemplate || '').toLowerCase()).toContain('total logs');
    expect(String(lastStep.operation || '')).toBe('select');
    expect(String(lastStep.sql || '').toUpperCase().startsWith('SELECT ')).toBe(true);
  });
});
