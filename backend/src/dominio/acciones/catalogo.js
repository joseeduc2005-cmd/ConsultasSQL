/**
 * Qué acciones de negocio existen y qué parámetros exige cada una.
 *
 * Es una lista cerrada, y eso es la garantía: un artículo solo puede pedir una acción
 * que esté aquí, y añadir una nueva obliga a escribirla a mano. Nada de lo que llegue
 * en una petición acaba siendo el nombre de algo que se ejecuta.
 *
 * Estaba en actionsRegistry.js, que es el fichero donde vive también la parte que
 * recibe el pool y habla con la base. Por eso se mueve: dominio/scripts/validacion.js
 * necesita estas dos búsquedas, y un módulo de dominio no debe mirar hacia un fichero
 * de infraestructura. Aquí es código puro — una lista y dos consultas sobre ella.
 */

export const BUSINESS_ACTION_CATALOG = [
  {
    nombre: 'getUser',
    descripcion: 'Obtiene usuario por username',
    categoria: 'usuarios',
    requiredParams: ['usuario'],
  },
  {
    nombre: 'unlockUser',
    descripcion: 'Desbloquea usuario',
    categoria: 'usuarios',
    requiredParams: [],
  },
  {
    nombre: 'validateUser',
    descripcion: 'Valida existencia de usuario',
    categoria: 'usuarios',
    requiredParams: [],
  },
  {
    nombre: 'logAction',
    descripcion: 'Registra auditoría',
    categoria: 'auditoria',
    requiredParams: ['usuario'],
  },
];

const ACTION_METADATA_MAP = BUSINESS_ACTION_CATALOG.reduce((acc, item) => {
  acc[item.nombre] = item;
  return acc;
}, {});

export function getBusinessActionMetadata(actionName) {
  return ACTION_METADATA_MAP[actionName] || null;
}

export function getRequiredParamsForActions(actionNames = []) {
  const required = new Set();
  for (const actionName of actionNames) {
    const metadata = getBusinessActionMetadata(actionName);
    const requiredParams = Array.isArray(metadata?.requiredParams) ? metadata.requiredParams : [];
    requiredParams.forEach((paramName) => required.add(paramName));
  }
  return Array.from(required);
}
