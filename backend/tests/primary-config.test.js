import MultiDbRegistry from '../src/infrastructure/distributed/MultiDbRegistry.js';

describe('Primary database resolution', () => {
  test('uses explicit primary database from registry config', () => {
    const registry = new MultiDbRegistry({
      databases: [
        {
          id: 'analytics',
          type: 'postgres',
          host: 'analytics.local',
          port: 5432,
          database: 'analytics',
          user: 'reader',
          password: 'secret',
          enabled: true,
        },
        {
          id: 'core',
          type: 'postgres',
          host: 'localhost',
          port: 5432,
          database: 'knowledge_base',
          user: 'postgres',
          password: 'admin123',
          primary: true,
          enabled: true,
        },
      ],
    });

    const resolved = registry.resolvePrimaryConnection({ expectedType: 'postgres', allowEnvFallback: false });

    expect(resolved.source).toBe('registry');
    expect(resolved.database.id).toBe('core');
    expect(resolved.connectionString).toBe('postgresql://postgres:admin123@localhost:5432/knowledge_base');
  });

  test('falls back to the only configured database when none is marked primary', () => {
    const registry = new MultiDbRegistry({
      databases: [
        {
          id: 'core',
          type: 'postgres',
          host: 'localhost',
          port: 5432,
          database: 'knowledge_base',
          user: 'postgres',
          password: 'admin123',
          enabled: true,
        },
      ],
    });

    const resolved = registry.resolvePrimaryConnection({ expectedType: 'postgres', allowEnvFallback: false });

    expect(resolved.source).toBe('registry');
    expect(resolved.database.id).toBe('core');
  });

  test('when only oracle exists and postgres is requested, throws not-found (no postgres available)', () => {
    const registry = new MultiDbRegistry({
      databases: [
        {
          id: 'oracle_rrhh',
          type: 'oracle',
          host: '10.0.0.25',
          port: 1521,
          database: 'XEPDB1',
          user: 'hr',
          password: 'secret',
          primary: true,
          enabled: true,
        },
      ],
    });

    // Ya no lanza por tipo incorrecto — lanza porque no hay postgres disponible
    expect(() => registry.resolvePrimaryConnection({ expectedType: 'postgres', allowEnvFallback: false }))
      .toThrow('No se encontró ninguna base configurada');
  });

  test('when oracle is primary but postgres also exists, postgres is returned for internal pool', () => {
    const registry = new MultiDbRegistry({
      databases: [
        {
          id: 'oracle_rrhh',
          type: 'oracle',
          host: '10.0.0.25',
          port: 1521,
          database: 'XEPDB1',
          user: 'hr',
          password: 'secret',
          primary: true,
          enabled: true,
        },
        {
          id: 'pg_internal',
          type: 'postgres',
          host: 'localhost',
          port: 5432,
          database: 'knowledge_base',
          user: 'postgres',
          password: 'admin123',
          primary: false,
          enabled: true,
        },
      ],
    });

    const resolved = registry.resolvePrimaryConnection({ expectedType: 'postgres', allowEnvFallback: false });
    expect(resolved.database.id).toBe('pg_internal');
    expect(resolved.database.type).toBe('postgres');
  });
});