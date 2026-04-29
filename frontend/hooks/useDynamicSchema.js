'use client';

import { useState, useEffect } from 'react';

const SCHEMA_CACHE_BY_DB = new Map();
const SCHEMA_CACHE_TTL_MS = 30 * 1000;

/**
 * Hook personalizado para cargar schema dinámico de la BD
 * 
 * Características:
 * - Auto-detección de tablas, columnas, PKs, FKs
 * - Cache inteligente
 * - Detección de relaciones automáticas
 */
export function useDynamicSchema(options = false) {
  const normalizedOptions = typeof options === 'boolean'
    ? { forceRefresh: options, databaseId: '' }
    : {
        forceRefresh: Boolean(options?.forceRefresh),
        databaseId: String(options?.databaseId || '').trim(),
      };

  const forceRefresh = normalizedOptions.forceRefresh;
  const databaseId = normalizedOptions.databaseId;
  const cacheKey = databaseId || '__default__';

  const [schema, setSchema] = useState(null);
  const [tablas, setTablas] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [source, setSource] = useState('auto-detected');
  const [stats, setStats] = useState(null);
  const [refreshNonce, setRefreshNonce] = useState(0);

  useEffect(() => {
    const loadSchema = async () => {
      try {
        const cached = SCHEMA_CACHE_BY_DB.get(cacheKey);
        const stillValid = cached && (Date.now() - Number(cached.loadedAt || 0) <= SCHEMA_CACHE_TTL_MS);
        if (!forceRefresh && stillValid) {
          setSchema(cached.schema || null);
          setTablas(cached.tablas || []);
          setSource(cached.source || 'cache');
          setStats(cached.stats || null);
          setError(null);
          setLoading(false);
          return;
        }

        setLoading(true);
        setError(null);

        const token = localStorage.getItem('token');
        const userRole = localStorage.getItem('userRole') || 'user';

        const params = new URLSearchParams();
        if (forceRefresh) params.set('refresh', 'true');
        if (databaseId) params.set('databaseId', databaseId);
        const url = `/api/db/schema-full${params.toString() ? `?${params.toString()}` : ''}`;

        console.info('[useDynamicSchema] load', { databaseId: databaseId || '', forceRefresh });

        const response = await fetch(url, {
          headers: {
            Authorization: `Bearer ${token}`,
            'x-user-role': userRole,
          },
        });

        if (!response.ok) {
          throw new Error(`HTTP Error: ${response.status}`);
        }

        const data = await response.json();

        if (data.success && data.schema) {
          const { schema: fullSchema, tablas: tablasList, detectadoEn, totalTablas } = data.schema;
          const nextStats = {
            totalTablas,
            detectadoEn,
            cacheInfo: data.cacheStats,
          };
          
          setSchema(fullSchema);
          setTablas(tablasList || []);
          setSource(data.source || 'auto-detected');
          setStats(nextStats);

          SCHEMA_CACHE_BY_DB.set(cacheKey, {
            schema: fullSchema,
            tablas: tablasList || [],
            source: data.source || 'auto-detected',
            stats: nextStats,
            loadedAt: Date.now(),
          });
        } else {
          throw new Error(data.error || 'Error cargando schema');
        }
      } catch (err) {
        console.error('Error loading schema:', err);
        setError(err.message);
        setSchema(null);
        setTablas([]);
      } finally {
        setLoading(false);
      }
    };

    loadSchema();
  }, [forceRefresh, databaseId, cacheKey, refreshNonce]);

  /**
   * Obtiene información de una tabla específica
   */
  const getTableInfo = (tableName) => {
    if (!schema) return null;
    return schema[tableName] || null;
  };

  /**
   * Obtiene todas las columnas de una tabla
   */
  const getTableColumns = (tableName) => {
    const tableInfo = getTableInfo(tableName);
    return tableInfo?.columnas || [];
  };

  /**
   * Obtiene la clave primaria de una tabla
   */
  const getPrimaryKey = (tableName) => {
    const tableInfo = getTableInfo(tableName);
    return tableInfo?.pkPrincipal || null;
  };

  /**
   * Obtiene las claves foráneas
   */
  const getForeignKeys = (tableName) => {
    const tableInfo = getTableInfo(tableName);
    return tableInfo?.clavesForaneas || [];
  };

  return {
    schema,
    tablas,
    loading,
    error,
    source,
    stats,
    getTableInfo,
    getTableColumns,
    getPrimaryKey,
    getForeignKeys,
    // Función para recargar el schema manualmente
    refresh: () => {
      SCHEMA_CACHE_BY_DB.delete(cacheKey);
      setLoading(true);
      setError(null);
      setRefreshNonce((prev) => prev + 1);
    }
  };
}

export default useDynamicSchema;
