'use client';

import React, { useDeferredValue, useEffect, useMemo, useRef, useState } from 'react';
import {
  SOLUTION_TYPES,
  ACTION_TYPES,
  createEmptyWorkflowRow,
  getAllowedActionTypes,
  validateWorkflow,
  generateScriptJson,
} from '@/lib/workflowBuilder';
import ConditionBuilder from './ConditionBuilder';
import JoinBuilder from './JoinBuilder';
import QueryPreview from './QueryPreview';
import AnalyticBuilder from './AnalyticBuilder';
import QueryHistoryPanel from './QueryHistoryPanel';
import { useTheme } from '../../app/components/ThemeProvider';

const EMPTY_INSERT_PAIR = { columna: '', valor: '' };
const EMPTY_CONDITION_PAIR = {
  tabla: '',
  columna: '',
  operador: '=',
  valor: '',
  compare_type: 'value',
  right_tabla: '',
  right_columna: '',
};
export default function WorkflowBuilder({
  value = { tipo_solucion: SOLUTION_TYPES.DATABASE, workflow_rows: [] },
  onChange = () => {},
  assistantQuery = null,
  onAssistantQueryConsumed = () => {},
  showHistoryPanel = true,
}) {
  const { theme } = useTheme();
  const [rows, setRows] = useState(() => {
    const initialRows = value.workflow_rows || [];
    return initialRows.length > 0 ? initialRows : [createEmptyWorkflowRow(1)];
  });
  const [solutionType, setSolutionType] = useState(value.tipo_solucion || SOLUTION_TYPES.DATABASE);
  const [schema, setSchema] = useState({});
  const [schemaRelations, setSchemaRelations] = useState([]);
  const [schemaFull, setSchemaFull] = useState({});
  const [schemaSource, setSchemaSource] = useState('empty');
  const [schemaLoading, setSchemaLoading] = useState(false);
  const [schemaError, setSchemaError] = useState('');
  const [userRole, setUserRole] = useState('user');
  const [advancedRowsOpen, setAdvancedRowsOpen] = useState({});
  const [joinRowsOpen, setJoinRowsOpen] = useState({});
  const onChangeRef = useRef(onChange);
  // Tracks the rows array we last pushed to the parent so we can detect bounce-back
  const lastPushedRowsRef = useRef(null);
  const lastPushedTypeRef = useRef(null);
  const deferredRows = useDeferredValue(rows);
  const deferredSolutionType = useDeferredValue(solutionType);

  const hasAdvancedContent = (row, currentSolutionType) => {
    const config = row?.config || {};

    if (row?.tipo === ACTION_TYPES.VALIDACION) {
      return true;
    }

    if (row?.tipo === ACTION_TYPES.SELECT) {
      return (
        config.query_mode === 'compuesta'
        || (Array.isArray(config.join_pairs) && config.join_pairs.length > 0)
        || (currentSolutionType === SOLUTION_TYPES.SCRIPT && Boolean(config.guardar_en))
      );
    }

    return currentSolutionType === SOLUTION_TYPES.SCRIPT && Boolean(config.guardar_en);
  };

  const buildInitialAdvancedState = (workflowRows, currentSolutionType) => (
    (workflowRows || []).reduce((acc, row, index) => {
      if (hasAdvancedContent(row, currentSolutionType)) {
        acc[index] = true;
      }
      return acc;
    }, {})
  );

  const buildInitialJoinState = (workflowRows) => (
    (workflowRows || []).reduce((acc, row, index) => {
      if (row?.config?.query_mode === 'compuesta' || (Array.isArray(row?.config?.join_pairs) && row.config.join_pairs.length > 0)) {
        acc[index] = true;
      }
      return acc;
    }, {})
  );

  const shiftIndexedState = (currentState, removedIndex) => (
    Object.entries(currentState).reduce((acc, [key, stateValue]) => {
      const numericKey = Number(key);
      if (numericKey < removedIndex) {
        acc[numericKey] = stateValue;
      } else if (numericKey > removedIndex) {
        acc[numericKey - 1] = stateValue;
      }
      return acc;
    }, {})
  );

  useEffect(() => {
    onChangeRef.current = onChange;
  }, [onChange]);

  useEffect(() => {
    const incomingSolutionType = value.tipo_solucion || SOLUTION_TYPES.DATABASE;
    const rawIncoming = value.workflow_rows;

    // Skip if this is our own data bouncing back from the parent's onChange handler.
    // Only sync when genuinely new external data arrives (e.g. opening a different article).
    if (
      rawIncoming === lastPushedRowsRef.current
      && incomingSolutionType === lastPushedTypeRef.current
    ) {
      return;
    }

    const incomingRows = rawIncoming && rawIncoming.length > 0
      ? rawIncoming
      : [createEmptyWorkflowRow(1)];

    setRows(incomingRows);
    setSolutionType(incomingSolutionType);
    setAdvancedRowsOpen(buildInitialAdvancedState(incomingRows, incomingSolutionType));
    setJoinRowsOpen(buildInitialJoinState(incomingRows));
  }, [value.tipo_solucion, value.workflow_rows]);

  useEffect(() => {
    setUserRole(String(localStorage.getItem('userRole') || 'user').toLowerCase());

    const loadSchema = async () => {
      try {
        setSchemaLoading(true);
        const token = localStorage.getItem('token');
        const response = await fetch('/api/db/schema', {
          headers: {
            Authorization: `Bearer ${token}`,
            'x-user-role': localStorage.getItem('userRole') || 'admin',
          },
        });

        const result = await response.json();

        const payload = result?.schema || result?.data;
        const detectedTables = payload?.tablas && typeof payload.tablas === 'object' && !Array.isArray(payload.tablas)
          ? payload.tablas
          : payload && typeof payload === 'object' && !Array.isArray(payload)
            ? payload
            : null;

        if (response.ok && result?.success && detectedTables) {
          setSchema(detectedTables);
          setSchemaRelations(Array.isArray(payload?.relaciones) ? payload.relaciones : []);
          setSchemaSource(result.source || 'database');
          setSchemaError('');

          try {
            const fullResponse = await fetch('/api/db/schema-full', {
              headers: {
                Authorization: `Bearer ${token}`,
                'x-user-role': localStorage.getItem('userRole') || 'admin',
              },
            });
            const fullResult = await fullResponse.json();
            const fullPayload = fullResult?.schema || fullResult?.data || {};
            const fullSchemaTables = fullPayload?.schema && typeof fullPayload.schema === 'object' && !Array.isArray(fullPayload.schema)
              ? fullPayload.schema
              : fullPayload && typeof fullPayload === 'object' && !Array.isArray(fullPayload)
                ? fullPayload
                : {};

            if (fullResponse.ok && fullResult?.success && Object.keys(fullSchemaTables).length > 0) {
              setSchemaFull(fullSchemaTables);
            } else {
              setSchemaFull({});
            }
          } catch {
            setSchemaFull({});
          }

          return;
        }

        setSchema({});
        setSchemaRelations([]);
        setSchemaFull({});
        setSchemaSource('empty');
        setSchemaError('No se pudo cargar el schema dinámico');
      } catch (error) {
        console.error('Schema load error:', error);
        setSchema({});
        setSchemaRelations([]);
        setSchemaFull({});
        setSchemaSource('empty');
        setSchemaError('No se pudo cargar el schema dinámico');
      } finally {
        setSchemaLoading(false);
      }
    };

    loadSchema();
  }, []);


  const availableTables = useMemo(() => {
    const tables = Object.keys(schema || {});
    return tables;
  }, [schema]);

  const getColumnsForTable = (tableName) => {
    if (!tableName) return [];
    return Array.isArray(schema?.[tableName]) ? schema[tableName] : [];
  };

  const parseSqlForBuilder = (sqlText) => {
    const normalized = String(sqlText || '').replace(/\s+/g, ' ').trim();
    if (!normalized) return null;

    const fromMatch = normalized.match(/\bfrom\s+"?([a-zA-Z0-9_]+)"?(?:\s+([a-zA-Z_][a-zA-Z0-9_]*))?/i);
    const baseTable = fromMatch?.[1] || '';
    const baseAlias = fromMatch?.[2] || baseTable;

    const selectMatch = normalized.match(/\bselect\s+(.+?)\s+from\b/i);
    const selectedColumns = selectMatch?.[1]
      ? selectMatch[1]
        .split(',')
        .map((col) => col.trim())
        .filter(Boolean)
        .map((col) => col.replace(/^"|"$/g, ''))
        .map((col) => col.split(/\s+as\s+/i)[0].trim())
        .map((col) => col.includes('.') ? col.split('.').pop() : col)
        .map((col) => col.replace(/^"|"$/g, ''))
      : [];

    const joins = [];
    const joinRegex = /\bjoin\s+"?([a-zA-Z0-9_]+)"?(?:\s+([a-zA-Z_][a-zA-Z0-9_]*))?\s+on\s+([a-zA-Z0-9_".]+)\s*=\s*([a-zA-Z0-9_".]+)/gi;
    let joinMatch = joinRegex.exec(normalized);
    while (joinMatch) {
      const joinTable = joinMatch[1];
      const joinAlias = joinMatch[2] || joinTable;
      const left = joinMatch[3].replace(/"/g, '');
      const right = joinMatch[4].replace(/"/g, '');

      const [leftAlias, leftColumn] = left.includes('.') ? left.split('.') : ['', left];
      const [rightAlias, rightColumn] = right.includes('.') ? right.split('.') : ['', right];

      if (leftAlias === baseAlias && leftColumn && rightColumn) {
        joins.push({ tabla: joinTable, base_columna: leftColumn, join_columna: rightColumn });
      } else if (rightAlias === baseAlias && leftColumn && rightColumn) {
        joins.push({ tabla: joinTable, base_columna: rightColumn, join_columna: leftColumn });
      }
      joinMatch = joinRegex.exec(normalized);
    }

    const wherePairs = [];
    const whereMatch = normalized.match(/\bwhere\s+(.+?)(?:\border\s+by\b|\bgroup\s+by\b|\blimit\b|\boffset\b|$)/i);
    if (whereMatch?.[1]) {
      const clauses = whereMatch[1].split(/\s+and\s+/i).map((x) => x.trim()).filter(Boolean);
      clauses.forEach((clause) => {
        const m = clause.match(/^([a-zA-Z0-9_".]+)\s*(=|!=|<>|>=|<=|>|<|like|ilike)\s*(.+)$/i);
        if (!m) return;
        const left = m[1].replace(/"/g, '');
        const operator = m[2];
        const rawValue = m[3].trim();
        const leftParts = left.split('.');
        const column = leftParts.length > 1 ? leftParts[1] : leftParts[0];
        wherePairs.push({
          ...EMPTY_CONDITION_PAIR,
          tabla: baseTable,
          columna: column,
          operador: operator,
          valor: rawValue.replace(/^'|'$/g, ''),
        });
      });
    }

    return {
      baseTable,
      selectedColumns: selectedColumns.filter((c) => c !== '*'),
      joins,
      wherePairs,
    };
  };

  useEffect(() => {
    if (!assistantQuery?.sql) return;
    const parsed = parseSqlForBuilder(assistantQuery.sql);
    if (!parsed?.baseTable) {
      onAssistantQueryConsumed();
      return;
    }

    setRows((prev) => {
      const next = [...prev];
      let targetIndex = next.findIndex((row) => row.tipo === ACTION_TYPES.SELECT);
      if (targetIndex < 0) {
        next.push(createEmptyWorkflowRow(next.length + 1));
        targetIndex = next.length - 1;
      }

      const target = {
        ...next[targetIndex],
        tipo: ACTION_TYPES.SELECT,
        tabla: parsed.baseTable,
        descripcion: next[targetIndex].descripcion || `Consulta auto generada: ${parsed.baseTable}`,
        config: {
          ...(next[targetIndex].config || {}),
          select_mode: parsed.joins.length > 0 ? 'relacional' : 'simple',
          query_mode: parsed.joins.length > 0 ? 'compuesta' : 'normal',
          join_pairs: parsed.joins,
          where_pairs: parsed.wherePairs.length > 0
            ? parsed.wherePairs
            : [{ ...EMPTY_CONDITION_PAIR, tabla: parsed.baseTable }],
          columna: parsed.wherePairs[0]?.columna || '',
          valor: parsed.wherePairs[0]?.valor || '',
          columnas: parsed.selectedColumns,
        },
      };

      next[targetIndex] = target;
      return next;
    });

    onAssistantQueryConsumed();
  }, [assistantQuery, onAssistantQueryConsumed]);

  const validation = useMemo(
    () => validateWorkflow(deferredRows, deferredSolutionType, schema),
    [deferredRows, deferredSolutionType, schema]
  );

  const generatedJSON = useMemo(() => {
    if (!validation.isValid) return null;
    return generateScriptJson(deferredRows, deferredSolutionType);
  }, [deferredRows, deferredSolutionType, validation]);

  const addRow = () => {
    const newOrder = Math.max(...rows.map((r) => r.orden), 0) + 1;
    setRows([...rows, createEmptyWorkflowRow(newOrder)]);
  };

  const deleteRow = (index) => {
    const nextRows = rows.filter((_, i) => i !== index).map((row, i) => ({ ...row, orden: i + 1 }));
    setRows(nextRows.length === 0 ? [createEmptyWorkflowRow(1)] : nextRows);
    setAdvancedRowsOpen((prev) => shiftIndexedState(prev, index));
    setJoinRowsOpen((prev) => shiftIndexedState(prev, index));
  };

  const updateRow = (index, field, value) => {
    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[index], config: { ...(next[index].config || {}) } };

      if (field === 'tipo') {
        current.tipo = value;
        current.tabla = '';
        current.config = {};
        setAdvancedRowsOpen((prev) => ({
          ...prev,
          [index]: value === ACTION_TYPES.VALIDACION,
        }));
        setJoinRowsOpen((prev) => ({
          ...prev,
          [index]: false,
        }));
      } else if (field === 'tabla') {
        current.tabla = value;
        current.config = {
          ...current.config,
          columna: '',
          valor: '',
          where_pairs: [{ ...EMPTY_CONDITION_PAIR, tabla: value }],
          columna_actualizar: '',
          columna_condicion: '',
          insert_pairs: [EMPTY_INSERT_PAIR],
          join_pairs: [],
        };
      } else if (field === 'descripcion') {
        current.descripcion = value;
      }

      next[index] = current;
      return next;
    });
  };

  const updateConfig = (index, field, value) => {
    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[index], config: { ...(next[index].config || {}) } };
      current.config[field] = value;
      next[index] = current;
      return next;
    });
  };

  const getResolvedSelectMode = (config = {}) => {
    const explicit = String(config?.select_mode || '').trim();
    if (['simple', 'relacional', 'analitico'].includes(explicit)) return explicit;

    const queryMode = String(config?.query_mode || '').trim();
    if (queryMode === 'analitico') return 'analitico';
    if (queryMode === 'compuesta') return 'relacional';
    if (queryMode === 'normal') return 'simple';

    return 'relacional';
  };

  const updateSelectMode = (rowIndex, mode) => {
    const normalizedMode = ['simple', 'relacional', 'analitico'].includes(mode) ? mode : 'relacional';
    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[rowIndex], config: { ...(next[rowIndex].config || {}) } };
      current.config.select_mode = normalizedMode;

      if (normalizedMode === 'simple') {
        current.config.query_mode = 'normal';
        current.config.join_pairs = [];
      } else if (normalizedMode === 'analitico') {
        current.config.query_mode = 'analitico';
      } else {
        const hasJoinPairs = Array.isArray(current.config.join_pairs) && current.config.join_pairs.length > 0;
        current.config.query_mode = hasJoinPairs ? 'compuesta' : 'normal';
      }

      next[rowIndex] = current;
      return next;
    });

    if (normalizedMode === 'simple') {
      setJoinRowsOpen((prev) => ({ ...prev, [rowIndex]: false }));
    }
  };

  const updateAnalyticConfig = (rowIndex, field, value) => {
    updateConfig(rowIndex, field, value);
  };

  const updateInsertPair = (rowIndex, pairIndex, field, value) => {
    const config = rows[rowIndex]?.config || {};
    const currentPairs = Array.isArray(config.insert_pairs)
      ? [...config.insert_pairs]
      : Object.entries(config.columnas_valores || {}).map(([columna, valor]) => ({ columna, valor: String(valor ?? '') }));

    if (currentPairs.length === 0) currentPairs.push({ ...EMPTY_INSERT_PAIR });

    currentPairs[pairIndex] = { ...currentPairs[pairIndex], [field]: value };
    updateConfig(rowIndex, 'insert_pairs', currentPairs);
  };

  const addInsertPair = (rowIndex) => {
    const config = rows[rowIndex]?.config || {};
    const currentPairs = Array.isArray(config.insert_pairs)
      ? [...config.insert_pairs]
      : [];
    updateConfig(rowIndex, 'insert_pairs', [...currentPairs, { ...EMPTY_INSERT_PAIR }]);
  };

  const removeInsertPair = (rowIndex, pairIndex) => {
    const config = rows[rowIndex]?.config || {};
    const currentPairs = Array.isArray(config.insert_pairs) ? [...config.insert_pairs] : [];
    const nextPairs = currentPairs.filter((_, i) => i !== pairIndex);
    updateConfig(rowIndex, 'insert_pairs', nextPairs.length > 0 ? nextPairs : [{ ...EMPTY_INSERT_PAIR }]);
  };

  const updateWherePairs = (rowIndex, pairs, logic) => {
    const normalizedPairs = Array.isArray(pairs) && pairs.length > 0
      ? pairs
      : [{ ...EMPTY_CONDITION_PAIR }];

    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[rowIndex], config: { ...(next[rowIndex].config || {}) } };
      current.config.where_pairs = normalizedPairs;
      if (logic !== undefined) current.config.where_logic = logic;
      // Legacy sync for first pair
      const first = normalizedPairs[0] || EMPTY_CONDITION_PAIR;
      current.config.columna = first.columna || '';
      current.config.valor = first.valor || '';
      next[rowIndex] = current;
      return next;
    });
  };

  const updateSelectedColumns = (rowIndex, nextSelectedColumns) => {
    const normalized = Array.isArray(nextSelectedColumns)
      ? Array.from(new Set(nextSelectedColumns.map((entry) => String(entry || '').trim()).filter(Boolean)))
      : [];
    updateConfig(rowIndex, 'selected_columns', normalized);
  };

  const handleJoinPairsChange = (rowIndex, pairs) => {
    const rawPairs = Array.isArray(pairs)
      ? pairs.map((pair) => ({
          tabla: String(pair?.tabla || '').trim(),
          base_columna: String(pair?.base_columna || '').trim(),
          join_columna: String(pair?.join_columna || '').trim(),
        }))
      : [];

    const completePairs = rawPairs.filter((pair) => pair.tabla && pair.base_columna && pair.join_columna);

    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[rowIndex], config: { ...(next[rowIndex].config || {}) } };
      const joinMode = current.config.join_mode === 'manual' ? 'manual' : 'auto';
      const selectMode = getResolvedSelectMode(current.config);
      current.config.join_pairs = joinMode === 'manual' ? rawPairs : completePairs;
      if (current.tipo === ACTION_TYPES.SELECT && selectMode === 'analitico') {
        current.config.query_mode = 'analitico';
      } else {
        current.config.query_mode = rawPairs.length > 0 ? 'compuesta' : 'normal';
      }
      next[rowIndex] = current;
      return next;
    });

    setJoinRowsOpen((prev) => ({
      ...prev,
      [rowIndex]: rawPairs.length > 0,
    }));
  };

  const updateJoinPairMapping = (rowIndex, pairIndex, field, value) => {
    const normalizedValue = String(value || '').trim();
    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[rowIndex], config: { ...(next[rowIndex].config || {}) } };
      const pairs = Array.isArray(current.config.join_pairs)
        ? current.config.join_pairs.map((pair) => ({ ...pair }))
        : [];

      current.config.join_pairs = pairs.map((pair, idx) => {
        if (idx !== pairIndex) return pair;
        return {
          ...pair,
          [field]: normalizedValue,
        };
      });

      next[rowIndex] = current;
      return next;
    });
  };

  const addJoinPairMapping = (rowIndex, tableName, baseColumn = '', joinColumn = '') => {
    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[rowIndex], config: { ...(next[rowIndex].config || {}) } };
      const pairs = Array.isArray(current.config.join_pairs)
        ? current.config.join_pairs.map((pair) => ({ ...pair }))
        : [];

      pairs.push({
        tabla: String(tableName || '').trim(),
        base_columna: String(baseColumn || '').trim(),
        join_columna: String(joinColumn || '').trim(),
      });

      current.config.join_pairs = pairs;
      current.config.query_mode = pairs.length > 0 ? 'compuesta' : 'normal';
      next[rowIndex] = current;
      return next;
    });
  };

  const removeJoinPairMapping = (rowIndex, pairIndex) => {
    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[rowIndex], config: { ...(next[rowIndex].config || {}) } };
      const pairs = Array.isArray(current.config.join_pairs)
        ? current.config.join_pairs.map((pair) => ({ ...pair }))
        : [];
      const nextPairs = pairs.filter((_, idx) => idx !== pairIndex);

      current.config.join_pairs = nextPairs;
      current.config.query_mode = nextPairs.length > 0 ? 'compuesta' : 'normal';
      next[rowIndex] = current;
      return next;
    });
  };

  const handleJoinModeChange = (rowIndex, mode) => {
    const normalizedMode = mode === 'manual' ? 'manual' : 'auto';
    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[rowIndex], config: { ...(next[rowIndex].config || {}) } };
      current.config.join_mode = normalizedMode;
      current.config.join_pairs = [];
      next[rowIndex] = current;
      return next;
    });
  };

  const updateQueryMode = (index, modeValue) => {
    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[index], config: { ...(next[index].config || {}) } };
      current.config.query_mode = modeValue;
      if (modeValue === 'normal') {
        current.config.join_pairs = [];
      }
      next[index] = current;
      return next;
    });

    if (modeValue === 'normal') {
      setJoinRowsOpen((prev) => ({
        ...prev,
        [index]: false,
      }));
    }
  };

  const toggleAdvancedRow = (rowIndex) => {
    setAdvancedRowsOpen((prev) => ({
      ...prev,
      [rowIndex]: !prev[rowIndex],
    }));
  };

  const toggleJoinSection = (rowIndex) => {
    const nextOpenState = !joinRowsOpen[rowIndex];

    setAdvancedRowsOpen((prev) => ({
      ...prev,
      [rowIndex]: true,
    }));

    setJoinRowsOpen((prev) => ({
      ...prev,
      [rowIndex]: nextOpenState,
    }));
  };

  const setRowJoinEnabled = (rowIndex, enabled) => {
    const current = rows[rowIndex];
    const selectMode = current?.tipo === ACTION_TYPES.SELECT
      ? getResolvedSelectMode(current?.config || {})
      : '';

    if (enabled) {
      if (selectMode === 'analitico') {
        updateConfig(rowIndex, 'query_mode', 'analitico');
      } else {
      updateQueryMode(rowIndex, 'compuesta');
      }
      setJoinRowsOpen((prev) => ({
        ...prev,
        [rowIndex]: true,
      }));
      return;
    }

    if (selectMode === 'analitico') {
      updateConfig(rowIndex, 'join_pairs', []);
      updateConfig(rowIndex, 'query_mode', 'analitico');
    } else {
      updateQueryMode(rowIndex, 'normal');
    }
    setJoinRowsOpen((prev) => ({
      ...prev,
      [rowIndex]: false,
    }));
  };

  const autoCompleteJoinMappings = (rowIndex) => {
    const row = rows[rowIndex];
    const baseTable = String(row?.tabla || '').trim();
    if (!baseTable) return;

    const related = (Array.isArray(schemaRelations) ? schemaRelations : [])
      .filter((rel) => rel?.from?.tabla === baseTable || rel?.to?.tabla === baseTable)
      .map((rel) => {
        if (rel.from.tabla === baseTable) {
          return {
            tabla: rel.to.tabla,
            base_columna: rel.from.columna,
            join_columna: rel.to.columna,
          };
        }
        return {
          tabla: rel.from.tabla,
          base_columna: rel.to.columna,
          join_columna: rel.from.columna,
        };
      });

    const dedup = [];
    const keys = new Set();
    related.forEach((pair) => {
      const key = `${pair.tabla}|${pair.base_columna}|${pair.join_columna}`;
      if (pair.tabla && pair.base_columna && pair.join_columna && !keys.has(key)) {
        keys.add(key);
        dedup.push(pair);
      }
    });

    if (dedup.length === 0) return;

    setRows((prev) => {
      const next = [...prev];
      const current = { ...next[rowIndex], config: { ...(next[rowIndex].config || {}) } };

      if (current.tipo === ACTION_TYPES.SELECT && getResolvedSelectMode(current.config) === 'simple') {
        current.config.select_mode = 'relacional';
      }

      current.config.join_mode = 'auto';
      current.config.join_pairs = dedup;
      current.config.query_mode = current.tipo === ACTION_TYPES.SELECT && getResolvedSelectMode(current.config) === 'analitico'
        ? 'analitico'
        : 'compuesta';

      next[rowIndex] = current;
      return next;
    });

    setJoinRowsOpen((prev) => ({
      ...prev,
      [rowIndex]: true,
    }));
  };

  useEffect(() => {
    const syncTimer = window.setTimeout(() => {
      // Record what we're pushing so the sync effect can skip the bounce-back.
      lastPushedRowsRef.current = deferredRows;
      lastPushedTypeRef.current = deferredSolutionType;
      onChangeRef.current({
        tipo_solucion: deferredSolutionType,
        workflow_rows: deferredRows,
        script_json: generatedJSON,
        workflow_valid: validation.isValid,
        workflow_errors: validation.errors,
        workflow_row_errors: validation.rowErrors,
      });
    }, 80);

    return () => window.clearTimeout(syncTimer);
  }, [deferredRows, deferredSolutionType, generatedJSON, validation]);

  const renderInlineRowErrors = (rowIndex) => {
    const errors = Array.from(new Set(validation?.rowErrors?.[rowIndex] || []));
    if (errors.length === 0) return null;

    return (
      <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2">
        <p className="text-xs font-semibold text-amber-800">Qué falta completar</p>
        <div className="mt-1 space-y-1">
        {errors.map((message, idx) => (
          <p key={`${rowIndex}-${idx}`} className="text-xs text-amber-800">
            {message}
          </p>
        ))}
        </div>
      </div>
    );
  };

  const renderConfigEditor = (row, index) => {
    const config = row.config || {};
    const columns = getColumnsForTable(row.tabla);

    if (!row.tipo) {
      return <div className="text-gray-400 text-sm">Selecciona un tipo de acción</div>;
    }

    if (row.tipo === ACTION_TYPES.SELECT) {
      const joinPairs = Array.isArray(config.join_pairs) ? config.join_pairs : [];
      const joinMode = config.join_mode === 'manual' ? 'manual' : 'auto';
      const selectMode = getResolvedSelectMode(config);
      const isSimpleMode = selectMode === 'simple';
      const isAnalyticMode = selectMode === 'analitico';
      const isJoinEnabled = !isSimpleMode && (joinPairs.length > 0 || config.query_mode === 'compuesta' || config.query_mode === 'analitico');
      const isJoinSectionOpen = Boolean(joinRowsOpen[index]) && isJoinEnabled;
      const hasBaseTable = Boolean(row.tabla);
      const wherePairs = Array.isArray(config.where_pairs) && config.where_pairs.length > 0
        ? config.where_pairs
        : [{ ...EMPTY_CONDITION_PAIR, tabla: row.tabla || '', columna: config.columna || '', valor: config.valor || '' }];
      const whereLogic = config.where_logic || 'AND';
      const groupByCols = Array.isArray(config.group_by) ? config.group_by : [];
      const aggregates = Array.isArray(config.aggregates) ? config.aggregates : [];
      const orderBy = Array.isArray(config.order_by) ? config.order_by : [];
      const having = Array.isArray(config.having) ? config.having : [];
      const selectedColumns = Array.isArray(config.selected_columns)
        ? config.selected_columns.map((entry) => String(entry || '').trim()).filter(Boolean)
        : [];
      const selectedColumnsByTable = selectedColumns.reduce((acc, entry) => {
        const [tableName, columnName] = String(entry).split('.');
        if (!tableName || !columnName) return acc;
        if (!acc[tableName]) acc[tableName] = [];
        if (!acc[tableName].includes(columnName)) acc[tableName].push(columnName);
        return acc;
      }, {});

      // Tables available inside conditions = base table + all JOIN tables
      const joinedTableNames = Array.from(new Set(joinPairs.map((p) => p.tabla).filter(Boolean)));
      const relationColumnsByTable = joinPairs.reduce((acc, pair) => {
        if (row.tabla && pair.base_columna) {
          acc[row.tabla] = Array.from(new Set([...(acc[row.tabla] || []), pair.base_columna]));
        }
        if (pair.tabla && pair.join_columna) {
          acc[pair.tabla] = Array.from(new Set([...(acc[pair.tabla] || []), pair.join_columna]));
        }
        return acc;
      }, {});

      const conditionPreviewText = hasBaseTable && wherePairs.some((p) => p.columna && ((p.compare_type || 'value') === 'column' ? p.right_columna : p.valor))
        ? wherePairs
            .filter((p) => p.columna && ((p.compare_type || 'value') === 'column' ? p.right_columna : p.valor))
            .map((p) => {
              const tablePrefix = p.tabla || row.tabla;
              const colRef = isJoinEnabled && tablePrefix ? `${tablePrefix}.${p.columna}` : p.columna;
              if ((p.compare_type || 'value') === 'column') {
                const rightTable = p.right_tabla || row.tabla;
                const rightRef = isJoinEnabled && rightTable ? `${rightTable}.${p.right_columna}` : p.right_columna;
                return `${colRef} ${p.operador || '='} ${rightRef}`;
              }
              return `${colRef} ${p.operador || '='} ${p.valor}`;
            })
            .join(` ${whereLogic} `)
        : 'Aquí verás las condiciones cuando completes las columnas y/o valores.';

      const selectionTables = [row.tabla, ...joinedTableNames].filter(Boolean);
      const toggleTableColumnSelection = (tableName, columnName) => {
        const key = `${tableName}.${columnName}`;
        if (selectedColumns.includes(key)) {
          updateSelectedColumns(index, selectedColumns.filter((entry) => entry !== key));
          return;
        }
        updateSelectedColumns(index, [...selectedColumns, key]);
      };

      return (
        <div className="space-y-3">
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-800">1. Acción</p>
            <p className="mt-1 text-xs text-slate-500">
              {row.tabla ? `[ SELECT ] desde [ ${row.tabla} ]` : '[ SELECT ] desde [ tabla principal ]'}
            </p>
          </div>

          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-800">Modo de consulta</p>
            <div className="mt-3 inline-flex rounded-xl border border-slate-200 bg-white p-1">
              {[
                { key: 'simple', label: 'Simple' },
                { key: 'relacional', label: 'Relacional' },
                { key: 'analitico', label: 'Analítico' },
              ].map((modeItem) => {
                const active = selectMode === modeItem.key;
                return (
                  <button
                    key={`select-mode-${modeItem.key}`}
                    type="button"
                    onClick={() => updateSelectMode(index, modeItem.key)}
                    className={`rounded-lg px-3 py-1.5 text-xs font-semibold ${active ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'}`}
                  >
                    {modeItem.label}
                  </button>
                );
              })}
            </div>
          </div>

          {!isSimpleMode && (
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 space-y-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <p className="text-sm font-semibold text-slate-800">2. Relaciones disponibles</p>
              <button
                type="button"
                onClick={() => autoCompleteJoinMappings(index)}
                disabled={!hasBaseTable}
                className="rounded-md border border-blue-200 bg-white px-2.5 py-1 text-xs font-semibold text-blue-700 hover:bg-blue-50 disabled:opacity-50"
              >
                Auto completar consulta
              </button>
            </div>

            <label className="flex items-start gap-3 rounded-lg border border-slate-200 bg-white px-3 py-3">
              <input
                type="checkbox"
                checked={isJoinEnabled}
                onChange={(e) => setRowJoinEnabled(index, e.target.checked)}
                disabled={!hasBaseTable}
                className="mt-1 h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
              />
              <div>
                <p className="text-sm font-medium text-slate-800">¿Consulta relacional?</p>
                <p className="text-xs text-slate-500">Actívalo solo si necesitas traer datos de otra tabla.</p>
              </div>
            </label>

            {isJoinEnabled ? (
              <JoinBuilder
                baseTable={row.tabla || ''}
                baseColumns={columns}
                availableTables={availableTables}
                relations={schemaRelations}
                mode={joinMode}
                onModeChange={(mode) => handleJoinModeChange(index, mode)}
                joinPairs={joinPairs}
                onChange={(pairs) => handleJoinPairsChange(index, pairs)}
                open={isJoinSectionOpen}
                onToggle={() => toggleJoinSection(index)}
                disabled={!hasBaseTable}
                getColumnsForTable={getColumnsForTable}
              />
            ) : (
              <div className="rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-500">
                Sin relación: la consulta usará solo la tabla base.
              </div>
            )}
          </div>
          )}

          {isAnalyticMode && (
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 space-y-3">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <p className="text-sm font-semibold text-slate-800">3. Configuración analítica</p>
                <p className="mt-1 text-xs text-slate-500">Define GROUP BY, agregaciones, ORDER BY y HAVING.</p>
              </div>

              {solutionType === SOLUTION_TYPES.SCRIPT && (
                <div className="w-full lg:w-72">
                  <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">Guardar resultado en</p>
                  <input
                    type="text"
                    value={config.guardar_en || ''}
                    onChange={(e) => updateConfig(index, 'guardar_en', e.target.value)}
                    placeholder="Ejemplo: reporte_ventas"
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  />
                </div>
              )}
            </div>

            <AnalyticBuilder
              groupByCols={groupByCols}
              aggregates={aggregates}
              orderBy={orderBy}
              having={having}
              baseTable={row.tabla || ''}
              joinedTables={joinedTableNames}
              getColumnsForTable={getColumnsForTable}
              onGroupByChange={(next) => updateAnalyticConfig(index, 'group_by', next)}
              onAggregatesChange={(next) => updateAnalyticConfig(index, 'aggregates', next)}
              onOrderByChange={(next) => updateAnalyticConfig(index, 'order_by', next)}
              onHavingChange={(next) => updateAnalyticConfig(index, 'having', next)}
            />

            {renderInlineRowErrors(index)}
          </div>
          )}

          {!isAnalyticMode && (
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <p className="text-sm font-semibold text-slate-800">{!isSimpleMode ? '4. Selección de campos' : '3. Selección de campos'}</p>
                <p className="mt-1 text-xs text-slate-500">Elige qué columnas quieres traer del resultado.</p>
              </div>

              {solutionType === SOLUTION_TYPES.SCRIPT && (
                <div className="w-full lg:w-72">
                  <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">Guardar resultado en</p>
                  <input
                    type="text"
                    value={config.guardar_en || ''}
                    onChange={(e) => updateConfig(index, 'guardar_en', e.target.value)}
                    placeholder="Ejemplo: usuario_encontrado"
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  />
                </div>
              )}
            </div>

            <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-2">
              {selectionTables.map((tableName) => {
                const tableColumns = getColumnsForTable(tableName);
                return (
                  <div key={`select-columns-${index}-${tableName}`} className="rounded-lg border border-slate-200 bg-white p-3">
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">{tableName}</p>
                    <div className="mt-2 max-h-40 space-y-1 overflow-y-auto pr-1">
                      {tableColumns.map((columnName) => {
                        const key = `${tableName}.${columnName}`;
                        const checked = selectedColumns.includes(key);
                        return (
                          <label key={key} className="flex items-center gap-2 rounded px-2 py-1 text-sm hover:bg-slate-50">
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={() => toggleTableColumnSelection(tableName, columnName)}
                              className="h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
                            />
                            <span className="text-slate-700">{columnName}</span>
                          </label>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>

            <div className="mt-3 rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-500">
              {selectedColumns.length > 0
                ? `Columnas seleccionadas: ${selectedColumns.join(', ')}`
                : 'Selecciona al menos una columna para generar un SELECT real.'}
            </div>

            {isJoinEnabled && joinedTableNames.length > 0 && (
              <div className="mt-3 rounded-lg border border-slate-200 bg-white p-3">
                <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">Mapeo de relación (columna ↔ columna)</p>
                <p className="mt-1 text-xs text-slate-500">Define aquí cómo se une cada tabla, usando las columnas que elegiste en esta opción.</p>

                <div className={`mt-3 grid grid-cols-1 gap-3 ${joinedTableNames.length > 1 ? 'lg:grid-cols-2' : ''}`}>
                  {joinedTableNames.map((tableName) => {
                    const tablePairs = joinPairs
                      .map((pair, pairIndex) => ({ pair, pairIndex }))
                      .filter(({ pair }) => pair.tabla === tableName);

                    const selectedBase = Array.isArray(selectedColumnsByTable?.[row.tabla])
                      ? selectedColumnsByTable[row.tabla].filter(Boolean)
                      : [];
                    const selectedJoin = Array.isArray(selectedColumnsByTable?.[tableName])
                      ? selectedColumnsByTable[tableName].filter(Boolean)
                      : [];

                    const baseOptions = selectedBase.length > 0 ? selectedBase : columns;
                    const joinOptions = selectedJoin.length > 0 ? selectedJoin : getColumnsForTable(tableName);

                    const usedBaseColumns = tablePairs.map(({ pair }) => pair.base_columna).filter(Boolean);
                    const usedJoinColumns = tablePairs.map(({ pair }) => pair.join_columna).filter(Boolean);
                    const suggestedBase = baseOptions.find((columnName) => !usedBaseColumns.includes(columnName)) || baseOptions[0] || '';
                    const suggestedJoin = joinOptions.find((columnName) => !usedJoinColumns.includes(columnName)) || joinOptions[0] || '';

                    return (
                      <div key={`mapping-${index}-${tableName}`} className={`rounded-lg border border-indigo-100 bg-gradient-to-br from-indigo-50 to-cyan-50 p-3 ${joinedTableNames.length === 1 ? 'max-w-3xl' : ''}`}>
                        <div className="flex items-center justify-between gap-2">
                          <div className="flex items-center gap-2 text-sm font-semibold text-slate-800">
                            <span className="rounded bg-blue-600 px-2 py-0.5 text-xs font-bold uppercase tracking-wide text-white">Tabla A</span>
                            <span>{row.tabla}</span>
                            <span className="text-slate-400">↔</span>
                            <span className="rounded bg-teal-600 px-2 py-0.5 text-xs font-bold uppercase tracking-wide text-white">Tabla B</span>
                            <span>{tableName}</span>
                          </div>
                          <button
                            type="button"
                            onClick={() => addJoinPairMapping(index, tableName, suggestedBase, suggestedJoin)}
                            className="rounded-md border border-indigo-200 bg-white px-2.5 py-1 text-xs font-semibold text-indigo-700 hover:bg-indigo-50"
                          >
                            + Vincular otra
                          </button>
                        </div>

                        <div className="mt-2 space-y-2">
                          {tablePairs.map(({ pair, pairIndex }, relationIndex) => {
                            const baseOptionsWithCurrent = pair.base_columna && !baseOptions.includes(pair.base_columna)
                              ? [pair.base_columna, ...baseOptions]
                              : baseOptions;
                            const joinOptionsWithCurrent = pair.join_columna && !joinOptions.includes(pair.join_columna)
                              ? [pair.join_columna, ...joinOptions]
                              : joinOptions;

                            return (
                              <div key={`pair-${tableName}-${pairIndex}`} className="rounded-md border border-slate-200 bg-white p-2 shadow-sm">
                                <div className="mb-2 flex items-center justify-between">
                                  <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Relación {relationIndex + 1}</p>
                                  <button
                                    type="button"
                                    onClick={() => removeJoinPairMapping(index, pairIndex)}
                                    disabled={tablePairs.length === 1}
                                    className="rounded border border-red-200 px-2 py-0.5 text-[11px] font-semibold text-red-700 hover:bg-red-50 disabled:opacity-40"
                                  >
                                    Quitar
                                  </button>
                                </div>

                                <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-2">
                                  <select
                                    value={pair.base_columna || ''}
                                    onChange={(e) => updateJoinPairMapping(index, pairIndex, 'base_columna', e.target.value)}
                                    className="w-full rounded-md border border-blue-200 bg-blue-50 px-2 py-1.5 text-xs font-semibold text-blue-900"
                                  >
                                    <option value="">Columna {row.tabla}</option>
                                    {baseOptionsWithCurrent.map((columnName) => (
                                      <option key={`map-base-${tableName}-${pairIndex}-${columnName}`} value={columnName}>{columnName}</option>
                                    ))}
                                  </select>

                                  <span className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-indigo-200 bg-indigo-50 text-sm font-bold text-indigo-600">→</span>

                                  <select
                                    value={pair.join_columna || ''}
                                    onChange={(e) => updateJoinPairMapping(index, pairIndex, 'join_columna', e.target.value)}
                                    className="w-full rounded-md border border-teal-200 bg-teal-50 px-2 py-1.5 text-xs font-semibold text-teal-900"
                                  >
                                    <option value="">Columna {tableName}</option>
                                    {joinOptionsWithCurrent.map((columnName) => (
                                      <option key={`map-join-${tableName}-${pairIndex}-${columnName}`} value={columnName}>{columnName}</option>
                                    ))}
                                  </select>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
          )}

          {!isAnalyticMode && (
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <p className="text-sm font-semibold text-slate-800">{!isSimpleMode ? '5. Condiciones' : '4. Condiciones'}</p>
                <p className="mt-1 text-xs text-slate-500">Define al final el filtro que usará esta consulta.</p>
              </div>
            </div>

            <div className="mt-3">
              <ConditionBuilder
                conditions={wherePairs}
                baseTable={row.tabla || ''}
                joinedTables={joinedTableNames}
                relationColumnsByTable={relationColumnsByTable}
                selectedColumnsByTable={selectedColumnsByTable}
                getColumnsForTable={getColumnsForTable}
                logic={whereLogic}
                onLogicChange={(newLogic) => updateWherePairs(index, wherePairs, newLogic)}
                disabled={!hasBaseTable}
                onChange={(pairs) => updateWherePairs(index, pairs)}
              />
            </div>

            <div className="mt-3 rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-500">
              {conditionPreviewText}
            </div>

            <div className="mt-3">
              {renderInlineRowErrors(index)}
            </div>
          </div>
          )}
        </div>
      );
    }

    if (row.tipo === ACTION_TYPES.UPDATE) {
      const joinPairs = Array.isArray(config.join_pairs) ? config.join_pairs : [];
      const joinMode = config.join_mode === 'manual' ? 'manual' : 'auto';
      const isJoinEnabled = config.query_mode === 'compuesta';
      const isJoinSectionOpen = Boolean(joinRowsOpen[index]) && isJoinEnabled;
      const hasBaseTable = Boolean(row.tabla);
      const joinedTableNames = Array.from(new Set(joinPairs.map((p) => p.tabla).filter(Boolean)));
      const wherePairs = Array.isArray(config.where_pairs) && config.where_pairs.length > 0
        ? config.where_pairs
        : [{ ...EMPTY_CONDITION_PAIR, tabla: row.tabla || '' }];
      const whereLogic = config.where_logic || 'AND';
      const relationColumnsByTable = joinPairs.reduce((acc, pair) => {
        if (row.tabla && pair.base_columna) {
          acc[row.tabla] = Array.from(new Set([...(acc[row.tabla] || []), pair.base_columna]));
        }
        if (pair.tabla && pair.join_columna) {
          acc[pair.tabla] = Array.from(new Set([...(acc[pair.tabla] || []), pair.join_columna]));
        }
        return acc;
      }, {});
      const conditionStep = isJoinEnabled ? 4 : 3;

      return (
        <div className="space-y-3">
          {/* 1. Actualización */}
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-800">1. Actualización</p>
            <p className="mt-1 text-xs text-slate-500">
              {row.tabla ? `[ UPDATE ] en [ ${row.tabla} ]` : '[ UPDATE ] en [ tabla principal ]'}
            </p>
            <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-2">
              <select
                value={config.columna_actualizar || ''}
                onChange={(e) => updateConfig(index, 'columna_actualizar', e.target.value)}
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
              >
                <option value="">Columna a actualizar</option>
                {columns.map((column) => (
                  <option key={column} value={column}>{column}</option>
                ))}
              </select>
              <input
                type="text"
                value={config.valor_nuevo || ''}
                onChange={(e) => updateConfig(index, 'valor_nuevo', e.target.value)}
                placeholder="Valor nuevo"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
              />
            </div>
            {solutionType === SOLUTION_TYPES.SCRIPT && (
              <div className="mt-3">
                <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">Guardar resultado en</p>
                <input
                  type="text"
                  value={config.guardar_en || ''}
                  onChange={(e) => updateConfig(index, 'guardar_en', e.target.value)}
                  placeholder="Opcional: ej. filas_actualizadas"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                />
              </div>
            )}
          </div>

          {/* 2. Relaciones */}
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 space-y-3">
            <p className="text-sm font-semibold text-slate-800">2. Relaciones disponibles</p>
            <label className="flex items-start gap-3 rounded-lg border border-slate-200 bg-white px-3 py-3">
              <input
                type="checkbox"
                checked={isJoinEnabled}
                onChange={(e) => setRowJoinEnabled(index, e.target.checked)}
                disabled={!hasBaseTable}
                className="mt-1 h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
              />
              <div>
                <p className="text-sm font-medium text-slate-800">¿Consulta relacional?</p>
                <p className="text-xs text-slate-500">Actívalo si necesitas filtrar usando columnas de otra tabla.</p>
              </div>
            </label>
            {isJoinEnabled ? (
              <JoinBuilder
                baseTable={row.tabla || ''}
                baseColumns={columns}
                availableTables={availableTables}
                relations={schemaRelations}
                mode={joinMode}
                onModeChange={(mode) => handleJoinModeChange(index, mode)}
                joinPairs={joinPairs}
                onChange={(pairs) => handleJoinPairsChange(index, pairs)}
                open={isJoinSectionOpen}
                onToggle={() => toggleJoinSection(index)}
                disabled={!hasBaseTable}
                getColumnsForTable={getColumnsForTable}
              />
            ) : (
              <div className="rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-500">
                Sin relación: el UPDATE usará solo la tabla base.
              </div>
            )}
          </div>

          {/* 3. Mapeo (solo si JOIN activo) */}
          {isJoinEnabled && joinedTableNames.length > 0 && (
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-sm font-semibold text-slate-800">3. Mapeo de relación (columna ↔ columna)</p>
              <p className="mt-1 text-xs text-slate-500">Define cómo se une cada tabla relacionada.</p>
              <div className={`mt-3 grid grid-cols-1 gap-3 ${joinedTableNames.length > 1 ? 'lg:grid-cols-2' : ''}`}>
                {joinedTableNames.map((tableName) => {
                  const tablePairs = joinPairs
                    .map((pair, pairIndex) => ({ pair, pairIndex }))
                    .filter(({ pair }) => pair.tabla === tableName);
                  const baseOptions = columns;
                  const joinOptions = getColumnsForTable(tableName);
                  const usedBaseColumns = tablePairs.map(({ pair }) => pair.base_columna).filter(Boolean);
                  const usedJoinColumns = tablePairs.map(({ pair }) => pair.join_columna).filter(Boolean);
                  const suggestedBase = baseOptions.find((c) => !usedBaseColumns.includes(c)) || baseOptions[0] || '';
                  const suggestedJoin = joinOptions.find((c) => !usedJoinColumns.includes(c)) || joinOptions[0] || '';

                  return (
                    <div key={`mapping-upd-${index}-${tableName}`} className={`rounded-lg border border-indigo-100 bg-gradient-to-br from-indigo-50 to-cyan-50 p-3 ${joinedTableNames.length === 1 ? 'max-w-3xl' : ''}`}>
                      <div className="flex items-center justify-between gap-2">
                        <div className="flex items-center gap-2 text-sm font-semibold text-slate-800">
                          <span className="rounded bg-blue-600 px-2 py-0.5 text-xs font-bold uppercase tracking-wide text-white">Tabla A</span>
                          <span>{row.tabla}</span>
                          <span className="text-slate-400">↔</span>
                          <span className="rounded bg-teal-600 px-2 py-0.5 text-xs font-bold uppercase tracking-wide text-white">Tabla B</span>
                          <span>{tableName}</span>
                        </div>
                        <button
                          type="button"
                          onClick={() => addJoinPairMapping(index, tableName, suggestedBase, suggestedJoin)}
                          className="rounded-md border border-indigo-200 bg-white px-2.5 py-1 text-xs font-semibold text-indigo-700 hover:bg-indigo-50"
                        >
                          + Vincular otra
                        </button>
                      </div>
                      <div className="mt-2 space-y-2">
                        {tablePairs.map(({ pair, pairIndex }, relationIndex) => {
                          const baseOptionsWithCurrent = pair.base_columna && !baseOptions.includes(pair.base_columna)
                            ? [pair.base_columna, ...baseOptions] : baseOptions;
                          const joinOptionsWithCurrent = pair.join_columna && !joinOptions.includes(pair.join_columna)
                            ? [pair.join_columna, ...joinOptions] : joinOptions;
                          return (
                            <div key={`pair-upd-${tableName}-${pairIndex}`} className="rounded-md border border-slate-200 bg-white p-2 shadow-sm">
                              <div className="mb-2 flex items-center justify-between">
                                <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Relación {relationIndex + 1}</p>
                                <button
                                  type="button"
                                  onClick={() => removeJoinPairMapping(index, pairIndex)}
                                  disabled={tablePairs.length === 1}
                                  className="rounded border border-red-200 px-2 py-0.5 text-[11px] font-semibold text-red-700 hover:bg-red-50 disabled:opacity-40"
                                >
                                  Quitar
                                </button>
                              </div>
                              <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-2">
                                <select
                                  value={pair.base_columna || ''}
                                  onChange={(e) => updateJoinPairMapping(index, pairIndex, 'base_columna', e.target.value)}
                                  className="w-full rounded-md border border-blue-200 bg-blue-50 px-2 py-1.5 text-xs font-semibold text-blue-900"
                                >
                                  <option value="">Columna {row.tabla}</option>
                                  {baseOptionsWithCurrent.map((c) => (
                                    <option key={`map-base-upd-${tableName}-${pairIndex}-${c}`} value={c}>{c}</option>
                                  ))}
                                </select>
                                <span className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-indigo-200 bg-indigo-50 text-sm font-bold text-indigo-600">→</span>
                                <select
                                  value={pair.join_columna || ''}
                                  onChange={(e) => updateJoinPairMapping(index, pairIndex, 'join_columna', e.target.value)}
                                  className="w-full rounded-md border border-teal-200 bg-teal-50 px-2 py-1.5 text-xs font-semibold text-teal-900"
                                >
                                  <option value="">Columna {tableName}</option>
                                  {joinOptionsWithCurrent.map((c) => (
                                    <option key={`map-join-upd-${tableName}-${pairIndex}-${c}`} value={c}>{c}</option>
                                  ))}
                                </select>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Condiciones (siempre visible) */}
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-800">{conditionStep}. Condiciones</p>
            <p className="mt-1 text-xs text-slate-500">Define el filtro WHERE del UPDATE.</p>
            <div className="mt-3">
              <ConditionBuilder
                conditions={wherePairs}
                baseTable={row.tabla || ''}
                joinedTables={joinedTableNames}
                relationColumnsByTable={relationColumnsByTable}
                selectedColumnsByTable={{}}
                getColumnsForTable={getColumnsForTable}
                logic={whereLogic}
                onLogicChange={(newLogic) => updateWherePairs(index, wherePairs, newLogic)}
                disabled={!hasBaseTable}
                onChange={(pairs) => updateWherePairs(index, pairs)}
              />
            </div>
            <div className="mt-3">
              {renderInlineRowErrors(index)}
            </div>
          </div>
        </div>
      );
    }

    if (row.tipo === ACTION_TYPES.DELETE) {
      const joinPairs = Array.isArray(config.join_pairs) ? config.join_pairs : [];
      const joinMode = config.join_mode === 'manual' ? 'manual' : 'auto';
      const isJoinEnabled = config.query_mode === 'compuesta';
      const isJoinSectionOpen = Boolean(joinRowsOpen[index]) && isJoinEnabled;
      const hasBaseTable = Boolean(row.tabla);
      const joinedTableNames = Array.from(new Set(joinPairs.map((p) => p.tabla).filter(Boolean)));
      const wherePairs = Array.isArray(config.where_pairs) && config.where_pairs.length > 0
        ? config.where_pairs
        : [{ ...EMPTY_CONDITION_PAIR, tabla: row.tabla || '' }];
      const whereLogic = config.where_logic || 'AND';
      const relationColumnsByTable = joinPairs.reduce((acc, pair) => {
        if (row.tabla && pair.base_columna) {
          acc[row.tabla] = Array.from(new Set([...(acc[row.tabla] || []), pair.base_columna]));
        }
        if (pair.tabla && pair.join_columna) {
          acc[pair.tabla] = Array.from(new Set([...(acc[pair.tabla] || []), pair.join_columna]));
        }
        return acc;
      }, {});
      const conditionStep = isJoinEnabled ? 4 : 3;

      return (
        <div className="space-y-3">
          {/* 1. Acción */}
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-800">1. Acción</p>
            <p className="mt-1 text-xs text-slate-500">
              {row.tabla ? `[ DELETE ] de [ ${row.tabla} ]` : '[ DELETE ] de [ tabla principal ]'}
            </p>
            {solutionType === SOLUTION_TYPES.SCRIPT && (
              <div className="mt-3">
                <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">Guardar resultado en</p>
                <input
                  type="text"
                  value={config.guardar_en || ''}
                  onChange={(e) => updateConfig(index, 'guardar_en', e.target.value)}
                  placeholder="Opcional: ej. filas_eliminadas"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                />
              </div>
            )}
          </div>

          {/* 2. Relaciones */}
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 space-y-3">
            <p className="text-sm font-semibold text-slate-800">2. Relaciones disponibles</p>
            <label className="flex items-start gap-3 rounded-lg border border-slate-200 bg-white px-3 py-3">
              <input
                type="checkbox"
                checked={isJoinEnabled}
                onChange={(e) => setRowJoinEnabled(index, e.target.checked)}
                disabled={!hasBaseTable}
                className="mt-1 h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
              />
              <div>
                <p className="text-sm font-medium text-slate-800">¿Consulta relacional?</p>
                <p className="text-xs text-slate-500">Actívalo si necesitas filtrar usando columnas de otra tabla.</p>
              </div>
            </label>
            {isJoinEnabled ? (
              <JoinBuilder
                baseTable={row.tabla || ''}
                baseColumns={columns}
                availableTables={availableTables}
                relations={schemaRelations}
                mode={joinMode}
                onModeChange={(mode) => handleJoinModeChange(index, mode)}
                joinPairs={joinPairs}
                onChange={(pairs) => handleJoinPairsChange(index, pairs)}
                open={isJoinSectionOpen}
                onToggle={() => toggleJoinSection(index)}
                disabled={!hasBaseTable}
                getColumnsForTable={getColumnsForTable}
              />
            ) : (
              <div className="rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-500">
                Sin relación: el DELETE usará solo la tabla base.
              </div>
            )}
          </div>

          {/* 3. Mapeo (solo si JOIN activo) */}
          {isJoinEnabled && joinedTableNames.length > 0 && (
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-sm font-semibold text-slate-800">3. Mapeo de relación (columna ↔ columna)</p>
              <p className="mt-1 text-xs text-slate-500">Define cómo se une cada tabla relacionada.</p>
              <div className={`mt-3 grid grid-cols-1 gap-3 ${joinedTableNames.length > 1 ? 'lg:grid-cols-2' : ''}`}>
                {joinedTableNames.map((tableName) => {
                  const tablePairs = joinPairs
                    .map((pair, pairIndex) => ({ pair, pairIndex }))
                    .filter(({ pair }) => pair.tabla === tableName);
                  const baseOptions = columns;
                  const joinOptions = getColumnsForTable(tableName);
                  const usedBaseColumns = tablePairs.map(({ pair }) => pair.base_columna).filter(Boolean);
                  const usedJoinColumns = tablePairs.map(({ pair }) => pair.join_columna).filter(Boolean);
                  const suggestedBase = baseOptions.find((c) => !usedBaseColumns.includes(c)) || baseOptions[0] || '';
                  const suggestedJoin = joinOptions.find((c) => !usedJoinColumns.includes(c)) || joinOptions[0] || '';

                  return (
                    <div key={`mapping-del-${index}-${tableName}`} className={`rounded-lg border border-indigo-100 bg-gradient-to-br from-indigo-50 to-cyan-50 p-3 ${joinedTableNames.length === 1 ? 'max-w-3xl' : ''}`}>
                      <div className="flex items-center justify-between gap-2">
                        <div className="flex items-center gap-2 text-sm font-semibold text-slate-800">
                          <span className="rounded bg-blue-600 px-2 py-0.5 text-xs font-bold uppercase tracking-wide text-white">Tabla A</span>
                          <span>{row.tabla}</span>
                          <span className="text-slate-400">↔</span>
                          <span className="rounded bg-teal-600 px-2 py-0.5 text-xs font-bold uppercase tracking-wide text-white">Tabla B</span>
                          <span>{tableName}</span>
                        </div>
                        <button
                          type="button"
                          onClick={() => addJoinPairMapping(index, tableName, suggestedBase, suggestedJoin)}
                          className="rounded-md border border-indigo-200 bg-white px-2.5 py-1 text-xs font-semibold text-indigo-700 hover:bg-indigo-50"
                        >
                          + Vincular otra
                        </button>
                      </div>
                      <div className="mt-2 space-y-2">
                        {tablePairs.map(({ pair, pairIndex }, relationIndex) => {
                          const baseOptionsWithCurrent = pair.base_columna && !baseOptions.includes(pair.base_columna)
                            ? [pair.base_columna, ...baseOptions] : baseOptions;
                          const joinOptionsWithCurrent = pair.join_columna && !joinOptions.includes(pair.join_columna)
                            ? [pair.join_columna, ...joinOptions] : joinOptions;
                          return (
                            <div key={`pair-del-${tableName}-${pairIndex}`} className="rounded-md border border-slate-200 bg-white p-2 shadow-sm">
                              <div className="mb-2 flex items-center justify-between">
                                <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Relación {relationIndex + 1}</p>
                                <button
                                  type="button"
                                  onClick={() => removeJoinPairMapping(index, pairIndex)}
                                  disabled={tablePairs.length === 1}
                                  className="rounded border border-red-200 px-2 py-0.5 text-[11px] font-semibold text-red-700 hover:bg-red-50 disabled:opacity-40"
                                >
                                  Quitar
                                </button>
                              </div>
                              <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-2">
                                <select
                                  value={pair.base_columna || ''}
                                  onChange={(e) => updateJoinPairMapping(index, pairIndex, 'base_columna', e.target.value)}
                                  className="w-full rounded-md border border-blue-200 bg-blue-50 px-2 py-1.5 text-xs font-semibold text-blue-900"
                                >
                                  <option value="">Columna {row.tabla}</option>
                                  {baseOptionsWithCurrent.map((c) => (
                                    <option key={`map-base-del-${tableName}-${pairIndex}-${c}`} value={c}>{c}</option>
                                  ))}
                                </select>
                                <span className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-indigo-200 bg-indigo-50 text-sm font-bold text-indigo-600">→</span>
                                <select
                                  value={pair.join_columna || ''}
                                  onChange={(e) => updateJoinPairMapping(index, pairIndex, 'join_columna', e.target.value)}
                                  className="w-full rounded-md border border-teal-200 bg-teal-50 px-2 py-1.5 text-xs font-semibold text-teal-900"
                                >
                                  <option value="">Columna {tableName}</option>
                                  {joinOptionsWithCurrent.map((c) => (
                                    <option key={`map-join-del-${tableName}-${pairIndex}-${c}`} value={c}>{c}</option>
                                  ))}
                                </select>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Condiciones (siempre visible) */}
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-800">{conditionStep}. Condiciones</p>
            <p className="mt-1 text-xs text-slate-500">Define el filtro WHERE del DELETE.</p>
            <div className="mt-3">
              <ConditionBuilder
                conditions={wherePairs}
                baseTable={row.tabla || ''}
                joinedTables={joinedTableNames}
                relationColumnsByTable={relationColumnsByTable}
                selectedColumnsByTable={{}}
                getColumnsForTable={getColumnsForTable}
                logic={whereLogic}
                onLogicChange={(newLogic) => updateWherePairs(index, wherePairs, newLogic)}
                disabled={!hasBaseTable}
                onChange={(pairs) => updateWherePairs(index, pairs)}
              />
            </div>
            <div className="mt-3">
              {renderInlineRowErrors(index)}
            </div>
          </div>
        </div>
      );
    }

    if (row.tipo === ACTION_TYPES.INSERT) {
      const insertPairs = Array.isArray(config.insert_pairs)
        ? config.insert_pairs
        : Object.entries(config.columnas_valores || {}).map(([columna, valor]) => ({ columna, valor: String(valor ?? '') }));
      const pairs = insertPairs.length > 0 ? insertPairs : [{ ...EMPTY_INSERT_PAIR }];

      return (
        <div className="space-y-3">
          {/* 1. Acción */}
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-sm font-semibold text-slate-800">1. Acción</p>
            <p className="mt-1 text-xs text-slate-500">
              {row.tabla ? `[ INSERT ] en [ ${row.tabla} ]` : '[ INSERT ] en [ tabla principal ]'}
            </p>
          </div>

          {/* 2. Columnas y valores */}
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 space-y-3">
            <p className="text-sm font-semibold text-slate-800">2. Columnas y valores</p>
            <p className="mt-1 text-xs text-slate-500">Define qué columnas insertar y con qué valores.</p>
            <div className="space-y-2">
              {pairs.map((pair, pairIndex) => (
                <div key={`${index}-${pairIndex}`} className="grid grid-cols-1 gap-3 lg:grid-cols-[1fr_1fr_auto]">
                  <select
                    value={pair.columna || ''}
                    onChange={(e) => updateInsertPair(index, pairIndex, 'columna', e.target.value)}
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  >
                    <option value="">Columna</option>
                    {columns.map((column) => (
                      <option key={column} value={column}>{column}</option>
                    ))}
                  </select>
                  <input
                    type="text"
                    value={pair.valor || ''}
                    onChange={(e) => updateInsertPair(index, pairIndex, 'valor', e.target.value)}
                    placeholder="Valor"
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  />
                  <button
                    type="button"
                    onClick={() => removeInsertPair(index, pairIndex)}
                    className="rounded-lg border border-red-200 px-3 py-2 text-sm text-red-700 hover:bg-red-50"
                  >
                    Eliminar
                  </button>
                </div>
              ))}
            </div>
            <button
              type="button"
              onClick={() => addInsertPair(index)}
              className="rounded-lg border border-blue-200 bg-white px-3 py-2 text-sm font-medium text-blue-700 hover:bg-blue-50"
            >
              + Agregar columna
            </button>
          </div>

          {/* guardar_en (SCRIPT) + errores */}
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 space-y-3">
            {solutionType === SOLUTION_TYPES.SCRIPT && (
              <div>
                <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">Guardar resultado en</p>
                <input
                  type="text"
                  value={config.guardar_en || ''}
                  onChange={(e) => updateConfig(index, 'guardar_en', e.target.value)}
                  placeholder="Opcional: ej. id_insertado"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                />
              </div>
            )}
            {renderInlineRowErrors(index)}
          </div>
        </div>
      );
    }

    return (
      <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 space-y-3">
        <p className="text-sm font-medium text-slate-700">Validación avanzada</p>
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-2 xl:grid-cols-4">
          <input
            type="text"
            value={config.variable || ''}
            onChange={(e) => updateConfig(index, 'variable', e.target.value)}
            placeholder="Variable"
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
          />
          <select
            value={config.condicion || ''}
            onChange={(e) => updateConfig(index, 'condicion', e.target.value)}
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
          >
            <option value="">Condición</option>
            <option value="existe">existe</option>
            <option value="no_existe">no_existe</option>
            <option value="igual">igual</option>
          </select>
          <input
            type="text"
            value={config.valor || ''}
            onChange={(e) => updateConfig(index, 'valor', e.target.value)}
            placeholder="Valor opcional"
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
          />
          <input
            type="text"
            value={config.mensaje_error || ''}
            onChange={(e) => updateConfig(index, 'mensaje_error', e.target.value)}
            placeholder="Mensaje para el usuario"
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
          />
        </div>

        {renderInlineRowErrors(index)}
      </div>
    );
  };

  const isLightTheme = theme === 'light';
  const baseControlClass = isLightTheme
    ? 'w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-800 placeholder:text-slate-400'
    : 'w-full rounded-lg border border-slate-600 bg-slate-800 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-400';
  const shellClass = isLightTheme
    ? 'w-full overflow-hidden rounded-2xl border border-sky-200 bg-gradient-to-br from-sky-100 via-blue-100 to-cyan-100 text-slate-900 shadow-[0_18px_40px_rgba(2,132,199,0.20)] backdrop-blur-sm [&_button]:transition-all [&_button]:duration-200 [&_button:hover]:-translate-y-[1px] [&_input]:rounded-xl [&_input]:border [&_input]:border-sky-200 [&_input]:bg-white [&_input]:text-slate-900 [&_input]:placeholder:text-slate-400 [&_select]:rounded-xl [&_select]:border [&_select]:border-sky-200 [&_select]:bg-white [&_select]:text-slate-900'
    : 'w-full overflow-hidden rounded-2xl border border-slate-700/80 bg-[#0f172a] text-slate-100 shadow-[0_18px_40px_rgba(2,6,23,0.45)] backdrop-blur-sm [&_button]:transition-all [&_button]:duration-200 [&_button:hover]:-translate-y-[1px] [&_input]:rounded-xl [&_input]:border [&_input]:border-slate-700 [&_input]:bg-slate-900/70 [&_input]:text-slate-100 [&_input]:placeholder:text-slate-500 [&_select]:rounded-xl [&_select]:border [&_select]:border-slate-700 [&_select]:bg-slate-900/70 [&_select]:text-slate-100';
  const cardClass = isLightTheme ? 'rounded-xl border border-sky-200 bg-white/90 p-3 shadow-sm' : 'rounded-xl border border-slate-700 bg-slate-900/80 p-3 shadow-[0_10px_30px_rgba(0,0,0,0.35)] backdrop-blur-sm';

  return (
    <div className={shellClass}>

      <div className={`border-b px-3 py-2 text-xs ${isLightTheme ? 'border-sky-200 text-slate-600 bg-white/60' : 'border-slate-700 text-slate-300'}`}>
        {schemaLoading
          ? 'Cargando schema...'
          : schemaSource === 'database'
            ? '🟢 Conectado a base real'
            : `🟡 Sin schema activo${schemaError ? ` · ${schemaError}` : ''}`}
      </div>

      <div className={`space-y-3 border-b p-3 ${isLightTheme ? 'border-sky-200 bg-gradient-to-br from-sky-100 via-blue-100 to-cyan-100' : 'border-slate-200 bg-gradient-to-br from-slate-950 via-slate-900 to-slate-800'}`}>

        {Object.keys(schemaFull || {}).length > 0 && (
          <div className={cardClass}>
            <p className={`text-xs font-semibold uppercase tracking-wide ${isLightTheme ? 'text-slate-600' : 'text-slate-300'}`}>Hints de schema (PK/FK)</p>
            <div className="mt-2 flex flex-wrap gap-2">
              {Object.entries(schemaFull).slice(0, 8).map(([tableName, info]) => (
                <div key={`schema-full-${tableName}`} className={`rounded-lg border px-2.5 py-1.5 text-xs ${isLightTheme ? 'border-sky-200 bg-sky-50 text-slate-800' : 'border-slate-700 bg-slate-800 text-slate-100'}`}>
                  <span className="font-semibold">{tableName}</span>
                  <span className={`ml-2 ${isLightTheme ? 'text-slate-500' : 'text-slate-300'}`}>
                    PK: {Array.isArray(info?.pkPrincipal) ? info.pkPrincipal.join(', ') : (info?.pkPrincipal || '-')}
                  </span>
                  <span className={`ml-2 ${isLightTheme ? 'text-slate-500' : 'text-slate-300'}`}>
                    FK: {Array.isArray(info?.clavesForaneas) ? info.clavesForaneas.length : 0}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {showHistoryPanel && <QueryHistoryPanel userRole={userRole} />}
      </div>

      <div className="space-y-3 p-3">
        {schemaLoading && (
          <div className="grid grid-cols-1 gap-3">
            {[1, 2].map((item) => (
              <div key={`wf-skeleton-${item}`} className={`rounded-2xl border p-4 backdrop-blur-md ${isLightTheme ? 'border-sky-200 bg-white/80' : 'border-slate-700 bg-slate-900/60'}`}>
                <div className={`h-4 w-40 animate-pulse rounded ${isLightTheme ? 'bg-sky-100' : 'bg-slate-700'}`} />
                <div className={`mt-3 h-10 animate-pulse rounded-xl ${isLightTheme ? 'bg-sky-100' : 'bg-slate-800'}`} />
                <div className={`mt-2 h-10 animate-pulse rounded-xl ${isLightTheme ? 'bg-sky-100' : 'bg-slate-800'}`} />
              </div>
            ))}
          </div>
        )}

        {rows.map((row, idx) => (
          <div key={idx} className={`rounded-2xl border p-4 shadow-[0_12px_24px_rgba(2,6,23,0.20)] backdrop-blur-md ${isLightTheme ? 'border-sky-200 bg-white/90' : 'border-slate-700 bg-slate-900/65'}`}>
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <span className={`rounded-full px-2.5 py-1 text-xs font-semibold ${isLightTheme ? 'bg-sky-100 text-sky-700' : 'bg-slate-800 text-cyan-200'}`}>Paso {row.orden}</span>
                {row.tipo && (
                  <span className={`rounded-full px-2.5 py-1 text-xs font-medium ${isLightTheme ? 'bg-sky-50 text-slate-600' : 'bg-slate-800 text-slate-300'}`}>
                    {row.tipo.toUpperCase()}
                  </span>
                )}
              </div>

              <button
                type="button"
                onClick={() => deleteRow(idx)}
                className={`rounded-lg border px-3 py-2 text-sm ${isLightTheme ? 'border-red-300 text-red-700 hover:bg-red-50' : 'border-red-500/40 text-red-300 hover:bg-red-500/15'}`}
              >
                Eliminar
              </button>
            </div>

            <div className={`rounded-2xl border p-4 ${isLightTheme ? 'border-sky-200 bg-sky-50/60' : 'border-slate-700 bg-slate-900/70'}`}>
              <p className={`text-sm font-semibold ${isLightTheme ? 'text-sky-700' : 'text-cyan-200'}`}>Configuración base</p>
              <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,1.3fr)_220px_220px]">
                <input
                  type="text"
                  value={row.descripcion || ''}
                  onChange={(e) => updateRow(idx, 'descripcion', e.target.value)}
                  placeholder="Descripción del paso"
                  className={baseControlClass}
                />

                <select
                  value={row.tipo || ''}
                  onChange={(e) => updateRow(idx, 'tipo', e.target.value)}
                  className={baseControlClass}
                >
                  <option value="">Selecciona tipo</option>
                  {getAllowedActionTypes(solutionType).map((type) => (
                    <option key={type} value={type}>{type.toUpperCase()}</option>
                  ))}
                </select>

                {row.tipo === ACTION_TYPES.VALIDACION ? (
                  <div className="flex items-center rounded-lg border border-dashed border-slate-300 px-3 py-2 text-sm text-slate-400">
                    Este paso no usa tabla
                  </div>
                ) : (
                  <select
                    value={row.tabla || ''}
                    onChange={(e) => updateRow(idx, 'tabla', e.target.value)}
                    className={baseControlClass}
                  >
                    <option value="">Selecciona tabla</option>
                    {availableTables.map((table) => (
                      <option key={table} value={table}>{table}</option>
                    ))}
                  </select>
                )}
              </div>
            </div>

            <div className="mt-4">
              {renderConfigEditor(row, idx)}
            </div>
          </div>
        ))}
      </div>

      <div className={`border-t p-3 ${isLightTheme ? 'border-sky-200 bg-white/70' : 'border-slate-700 bg-slate-900/70'}`}>
        <button
          type="button"
          onClick={addRow}
          className={`w-full rounded-xl px-4 py-2 font-semibold ${isLightTheme ? 'bg-sky-500 text-white hover:bg-sky-600' : 'bg-cyan-500 text-slate-950 hover:bg-cyan-400'}`}
        >
          + Agregar Paso
        </button>
      </div>

      <QueryPreview generatedJSON={generatedJSON} isValid={validation.isValid} />
    </div>
  );
}
