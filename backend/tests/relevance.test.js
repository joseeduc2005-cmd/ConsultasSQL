import { DatabaseRelevanceValidator } from '../src/infrastructure/query/DatabaseRelevanceValidator.js';

const validator = new DatabaseRelevanceValidator();

describe('DatabaseRelevanceValidator - DB queries should be accepted', () => {
  const dbCases = [
    ['mostrar usuarios', 'Basic list query'],
    ['usuarios activos', 'Users with status filter'],
    ['usuarios con más de 5 logs', 'Aggregation query'],
    ['contar registros de la tabla usuarios', 'Count query'],
    ['buscar usuario por id', 'Search query'],
    ['rol del usuario', 'Field reference'],
    ['logs del sistema', 'System logs query'],
    ['listar permisos de roles', 'Permission query'],
    ['auditoría de sesiones', 'Audit query'],
    ['usuarios con estado activo', 'Status filter'],
    ['columnas de la tabla usuarios', 'Column reference'],
    ['base de datos de usuarios', 'Explicit DB reference'],
    ['query de usuarios', 'Query keyword'],
  ];

  test.each(dbCases)('%s should be DB-related (%s)', (query) => {
    const result = validator.validateRelevance(query);
    expect(result.isDbRelated).toBe(true);
  });
});

describe('DatabaseRelevanceValidator - non-DB queries should be rejected', () => {
  const nonDbCases = [
    ['hola', 'Greeting'],
    ['como estás', 'How are you'],
    ['me puedes ayudar', 'General help request'],
    ['gracias', 'Thanks'],
    ['dame un chiste', 'Joke request'],
    ['qué es la música', 'General knowledge'],
    ['cuéntame una historia', 'Story request'],
    ['información sobre películas', 'Movie info'],
    ['receta de comida', 'Recipe request'],
    ['información', 'Ambiguous generic info'],
    ['búsqueda', 'Ambiguous generic search'],
    ['detalles', 'Ambiguous generic details'],
  ];

  test.each(nonDbCases)('%s should NOT be DB-related (%s)', (query) => {
    const result = validator.validateRelevance(query);
    expect(result.isDbRelated).toBe(false);
  });
});

describe('DatabaseRelevanceValidator - rejection response format', () => {
  test('createNonDbQueryResponse returns expected contract', () => {
    const validation = validator.validateRelevance('hola');
    const response = validator.createNonDbQueryResponse('hola', validation);

    expect(response).toBeDefined();
    expect(typeof response).toBe('object');
  });
});
