'use client';

import { useCallback, useEffect, useMemo, useState, useTransition } from 'react';
import { useRouter } from 'next/navigation';
import { withAuth } from '../components/ProtectedRoute';
import ThemeToggle from '../components/ThemeToggle';
import { useTheme } from '../components/ThemeProvider';
import UserIdentityBadge from '../components/UserIdentityBadge';
import Sidebar from '../components/Sidebar';
import ContentPanel from '../components/ContentPanel';
import CommentsSection from '../components/CommentsSection';
import WorkflowBuilder from '@/components/WorkflowBuilder/WorkflowBuilder';
import { KnowledgeArticle } from '../types';

// ─── Lib imports ──────────────────────────────────────────────────────────────
import {
  SolutionType, ScriptCreationMode, WorkflowActionType, WorkflowTableName,
  WorkflowValidationCondition, WorkflowRow, WorkflowStepConfig, BuilderFormState,
  WorkflowValidationResult, WORKFLOW_TABLES, VALIDATION_CONDITIONS, SCRIPT_CREATION_MODES,
} from '../lib/admin/adminTypes';
import {
  normalizeSolutionType, isDatabaseSolutionType, isScriptSolutionType,
  isExecutableSolutionType, getAllowedActionTypes, getCreationModesForSolutionType,
  createEmptyWorkflowConfig, createEmptyWorkflowRow, normalizeWorkflowRows,
  adaptRowsForSolutionType, updateRows, updateRowConfig,
  getPanelPreferenceKey, createInitialFormState,
} from '../lib/admin/workflowUtils';
import { parseScriptSource, parseWorkflowRowsFromScript } from '../lib/admin/scriptParser';
import {
  validateWorkflowRows, buildWorkflowScriptJson,
  computeScriptForMode, inferCreationModeFromScript,
} from '../lib/admin/scriptUtils';

// ─────────────────────────────────────────────────────────────────────────────

function AdminPage() {
  const router = useRouter();
  const { theme } = useTheme();
  const isLightTheme = theme === 'light';

  const [articles, setArticles] = useState<KnowledgeArticle[]>([]);
  const [loading, setLoading] = useState(true);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [selectedArticle, setSelectedArticle] = useState<KnowledgeArticle | null>(null);
  const [selectedCategory, setSelectedCategory] = useState('');
  const [selectedSubcategory, setSelectedSubcategory] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [commentsCollapsed, setCommentsCollapsed] = useState(false);
  const [currentUser, setCurrentUser] = useState<{ username: string; role: string } | null>(null);
  const [categories, setCategories] = useState<Array<{ categoria: string; subcategorias: string[] }>>([]);
  const [articleCount, setArticleCount] = useState(0);
  const [uploadForm, setUploadForm] = useState<BuilderFormState>(createInitialFormState());
  const [editForm, setEditForm] = useState<BuilderFormState>({ ...createInitialFormState(), id: '' });
  const [uploadAttempted, setUploadAttempted] = useState(false);
  const [editAttempted, setEditAttempted] = useState(false);
  const [uploadSaving, setUploadSaving] = useState(false);
  const [editSaving, setEditSaving] = useState(false);
  const [uploadError, setUploadError] = useState('');
  const [editError, setEditError] = useState('');
  const [assistantQuery, setAssistantQuery] = useState<any | null>(null);
  const [availableDatabases, setAvailableDatabases] = useState<Array<{ id: string; label: string; type: string; connected?: boolean; primary?: boolean }>>([]);
  const [showDbModal, setShowDbModal] = useState(false);
  const [dbForm, setDbForm] = useState({ id: '', label: '', type: 'postgres', host: '', port: '5432', database: '', user: '', password: '' });
  const [dbSaving, setDbSaving] = useState(false);
  const [dbError, setDbError] = useState('');
  const [allDatabases, setAllDatabases] = useState<Array<{ id: string; label: string; type: string; connected?: boolean; primary?: boolean }>>([]);
  const [, startWorkflowSyncTransition] = useTransition();

  const handleUploadWorkflowChange = useCallback((data: any) => {
    startWorkflowSyncTransition(() => {
      setUploadForm((prev) => ({
        ...prev,
        tipo_solucion: data.tipo_solucion,
        workflow_rows: data.workflow_rows,
        script_json: data.script_json ?? null,
      }));
    });
  }, []);

  const handleEditWorkflowChange = useCallback((data: any) => {
    startWorkflowSyncTransition(() => {
      setEditForm((prev) => ({
        ...prev,
        tipo_solucion: data.tipo_solucion,
        workflow_rows: data.workflow_rows,
        script_json: data.script_json ?? null,
      }));
    });
  }, []);

  const handleAssistantQuerySelected = useCallback((query: any) => {
    setAssistantQuery({ ...query, _ts: Date.now() });
  }, []);

  const clearAssistantQuery = useCallback(() => {
    setAssistantQuery(null);
  }, []);

  const buildCategoriesFromArticles = (source: KnowledgeArticle[]) => {
    const categoryMap = new Map<string, Set<string>>();

    source.forEach((article) => {
      const cat = article.categoria || 'Sin categoria';
      const sub = article.subcategoria || 'General';
      if (!categoryMap.has(cat)) categoryMap.set(cat, new Set());
      categoryMap.get(cat)?.add(sub);
    });

    return Array.from(categoryMap.entries()).map(([categoria, subcats]) => ({
      categoria,
      subcategorias: Array.from(subcats),
    }));
  };

  useEffect(() => {
    const userJson = localStorage.getItem('user');
    if (userJson) {
      try {
        setCurrentUser(JSON.parse(userJson));
      } catch (error) {
        console.error('Error parseando usuario:', error);
      }
    }

    const fetchArticles = async () => {
      setLoading(true);
      try {
        const res = await fetch('/api/articles');
        const data = await res.json();
        const fetched: KnowledgeArticle[] = data?.success && Array.isArray(data.data) ? data.data : Array.isArray(data) ? data : [];
        setArticles(fetched);
        setArticleCount(fetched.length);
        setSelectedArticle(null);
        const computedCategories = buildCategoriesFromArticles(fetched);
        setCategories(computedCategories);
        if (computedCategories.length > 0) {
          setSelectedCategory(computedCategories[0].categoria);
          setSelectedSubcategory(computedCategories[0].subcategorias[0] || '');
        }
      } catch (error) {
        console.error('Error cargando articulos:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchArticles();

    // Cargar bases de datos: para el selector de artículos solo las conectadas/no primarias.
    // Para el panel de gestión de admin se carga con showAll=true.
    const refreshDatabases = () => {
      fetch('/api/distributed/databases')
        .then((r) => r.json())
        .then((d) => { if (Array.isArray(d?.databases)) setAvailableDatabases(d.databases); })
        .catch(() => {});
      fetch('/api/distributed/databases?showAll=true')
        .then((r) => r.json())
        .then((d) => { if (Array.isArray(d?.databases)) setAllDatabases(d.databases); })
        .catch(() => {});
    };
    refreshDatabases();
  }, []);

  useEffect(() => {
    if (!currentUser?.username || !currentUser?.role) return;
    const key = getPanelPreferenceKey(currentUser.username, currentUser.role);
    const raw = localStorage.getItem(key);
    if (!raw) return;

    try {
      const parsed = JSON.parse(raw);
      setSidebarCollapsed(Boolean(parsed.sidebarCollapsed));
      setCommentsCollapsed(Boolean(parsed.commentsCollapsed));
    } catch {
      // Ignore malformed localStorage values
    }
  }, [currentUser]);

  useEffect(() => {
    if (!currentUser?.username || !currentUser?.role) return;
    const key = getPanelPreferenceKey(currentUser.username, currentUser.role);
    localStorage.setItem(key, JSON.stringify({ sidebarCollapsed, commentsCollapsed }));
  }, [sidebarCollapsed, commentsCollapsed, currentUser]);

  useEffect(() => {
    setCategories(buildCategoriesFromArticles(articles));
    setArticleCount(articles.length);
  }, [articles]);

  const visibleArticles = useMemo(() => {
    if (!searchQuery.trim()) return articles;
    const query = searchQuery.toLowerCase();
    return articles.filter((article) => {
      return (
        article.titulo.toLowerCase().includes(query) ||
        (article.descripcion?.toLowerCase().includes(query) ?? false) ||
        (article.contenido_md?.toLowerCase().includes(query) ?? false) ||
        article.subcategoria.toLowerCase().includes(query) ||
        article.categoria.toLowerCase().includes(query) ||
        article.tags?.some((tag) => tag.toLowerCase().includes(query))
      );
    });
  }, [articles, searchQuery]);

  const categoryOptions = useMemo(() => {
    return Array.from(new Set(articles.map((article) => (article.categoria || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
  }, [articles]);

  const uploadWorkflowValidation = useMemo(() => validateWorkflowRows(uploadForm.workflow_rows, uploadForm.tipo_solucion), [uploadForm.workflow_rows, uploadForm.tipo_solucion]);
  const editWorkflowValidation = useMemo(() => validateWorkflowRows(editForm.workflow_rows, editForm.tipo_solucion), [editForm.workflow_rows, editForm.tipo_solucion]);

  const uploadGeneratedScript = useMemo(() => {
    if (!isExecutableSolutionType(uploadForm.tipo_solucion) || !uploadWorkflowValidation.valid) return null;
    return buildWorkflowScriptJson(uploadForm.workflow_rows, uploadForm.tipo_solucion);
  }, [uploadForm.workflow_rows, uploadForm.tipo_solucion, uploadWorkflowValidation.valid]);

  const editGeneratedScript = useMemo(() => {
    if (!isExecutableSolutionType(editForm.tipo_solucion) || !editWorkflowValidation.valid) return null;
    return buildWorkflowScriptJson(editForm.workflow_rows, editForm.tipo_solucion);
  }, [editForm.workflow_rows, editForm.tipo_solucion, editWorkflowValidation.valid]);

  const uploadEnterpriseValidation = useMemo(() => {
    if (!isExecutableSolutionType(uploadForm.tipo_solucion)) {
      return { isValid: true, errors: [] as string[] };
    }
    const result = computeScriptForMode(uploadForm);
    return { isValid: result.valid, errors: result.error ? [result.error] : [] as string[] };
  }, [uploadForm]);

  const editEnterpriseValidation = useMemo(() => {
    if (!isExecutableSolutionType(editForm.tipo_solucion)) {
      return { isValid: true, errors: [] as string[] };
    }
    const result = computeScriptForMode(editForm);
    return { isValid: result.valid, errors: result.error ? [result.error] : [] as string[] };
  }, [editForm]);

  const uploadModeResult = useMemo(() => {
    if (!isExecutableSolutionType(uploadForm.tipo_solucion)) {
      return { valid: true, error: '', scriptJson: null as unknown };
    }
    return computeScriptForMode(uploadForm);
  }, [uploadForm]);

  const editModeResult = useMemo(() => {
    if (!isExecutableSolutionType(editForm.tipo_solucion)) {
      return { valid: true, error: '', scriptJson: null as unknown };
    }
    return computeScriptForMode(editForm);
  }, [editForm]);

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    router.push('/login');
  };

  const openUploadModal = () => {
    if (currentUser?.role !== 'admin') {
      alert('Solo admin puede subir soluciones.');
      return;
    }
    setUploadAttempted(false);
    setUploadSaving(false);
    setUploadError('');
    setUploadForm(createInitialFormState());
    setShowUploadModal(true);
  };

  const openEditModal = (article: KnowledgeArticle) => {
    if (currentUser?.role !== 'admin') {
      alert('Solo admin puede editar soluciones.');
      return;
    }

    const normalizedType = normalizeSolutionType(article.tipo_solucion);
    const parsedSource = parseScriptSource(article.script_json);
    const inferredCreationMode = inferCreationModeFromScript(parsedSource);
    const hasWorkflow = Boolean(parsedSource && typeof parsedSource === 'object' && !Array.isArray(parsedSource) && Array.isArray((parsedSource as Record<string, unknown>).workflow));
    const effectiveType: SolutionType = normalizedType === 'lectura' ? 'lectura' : normalizedType === 'script' || hasWorkflow ? normalizedType : 'database';
    const availableModes = getCreationModesForSolutionType(effectiveType);
    const normalizedCreationMode = availableModes.some((mode) => mode.value === inferredCreationMode)
      ? inferredCreationMode
      : (availableModes[0]?.value || 'codigo_manual');

    setSelectedArticle(article);
    setEditAttempted(false);
    setEditSaving(false);
    setEditError('');
    const existingTargetDb = String(
      (parsedSource as Record<string, unknown> | null)?.target_database || ''
    ).trim();

    setEditForm({
      id: article.id,
      titulo: article.titulo || '',
      categoria: article.categoria || '',
      subcategoria: article.subcategoria || '',
      tags: (article.tags || []).join(', '),
      descripcion: article.descripcion || '',
      contenido_md: article.contenido_md || '',
      tipo_solucion: effectiveType,
      target_database: existingTargetDb,
      workflow_rows: adaptRowsForSolutionType(parseWorkflowRowsFromScript(article.script_json), effectiveType),
      script_json: parsedSource,
      script_creation_mode: normalizedCreationMode,
      unified_json_text: normalizedCreationMode === 'json_unificado' ? JSON.stringify(parsedSource, null, 2) : '',
      manual_json_text: normalizedCreationMode === 'codigo_manual' ? JSON.stringify(parsedSource, null, 2) : '',
    });
    setShowEditModal(true);
  };

  const handleArticleSelect = (article: KnowledgeArticle) => {
    setSelectedArticle(article);
    setSelectedCategory(article.categoria);
    setSelectedSubcategory(article.subcategoria);
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploadForm((prev) => ({ ...prev, archivo_md: file }));
    const reader = new FileReader();
    reader.onload = (evt) => {
      const content = evt.target?.result as string;
      const updates: Partial<BuilderFormState> = { contenido_md: content };

      // Auto-extraer título del .md si aún no hay título
      const titleMatch = content.match(/^#\s+(.+)$/m);
      if (titleMatch && !uploadForm.titulo) updates.titulo = titleMatch[1].trim();

      // Auto-detectar directiva -- database: <id>
      const dbMatch = content.match(/^--\s*database\s*:\s*(\S+)/im);
      if (dbMatch) updates.target_database = dbMatch[1].trim();

      setUploadForm((prev) => ({ ...prev, ...updates }));
    };
    reader.readAsText(file);
  };

  const addWorkflowRow = (rows: WorkflowRow[]) => normalizeWorkflowRows([...rows, createEmptyWorkflowRow(rows.length + 1)]);

  const removeWorkflowRow = (rows: WorkflowRow[], index: number) => {
    const next = rows.filter((_, rowIndex) => rowIndex !== index);
    return normalizeWorkflowRows(next.length > 0 ? next : [createEmptyWorkflowRow(1)]);
  };

  const submitArticle = async (mode: 'create' | 'edit') => {
    const isEdit = mode === 'edit';
    const form = isEdit ? editForm : uploadForm;
    const validation = isEdit ? editWorkflowValidation : uploadWorkflowValidation;
    const setAttempted = isEdit ? setEditAttempted : setUploadAttempted;
    const setSaving = isEdit ? setEditSaving : setUploadSaving;
    const setError = isEdit ? setEditError : setUploadError;

    setAttempted(true);
    setError('');

    if (currentUser?.role !== 'admin') {
      alert(isEdit ? 'No tiene permisos para editar soluciones.' : 'No tiene permisos para crear soluciones.');
      return;
    }

    if (!form.titulo.trim() || !form.contenido_md.trim()) {
      alert('Titulo y contenido MD son obligatorios.');
      return;
    }

    if (!form.categoria.trim()) {
      alert('Categoria es obligatoria.');
      return;
    }

    const modeResult = isExecutableSolutionType(form.tipo_solucion)
      ? computeScriptForMode(form)
      : { valid: true, error: '', scriptJson: null };

    if (isExecutableSolutionType(form.tipo_solucion) && !modeResult.valid) {
      alert(modeResult.error || validation.generalError || 'Script inválido para el modo seleccionado.');
      return;
    }

    // Inyectar target_database en script_json para que el motor lo use al ejecutar
    const baseScriptJson = modeResult.scriptJson as Record<string, unknown> | null;
    const scriptJson = form.target_database
      ? { ...(baseScriptJson || {}), target_database: form.target_database }
      : baseScriptJson;

    setSaving(true);
    try {
      const token = localStorage.getItem('token');
      const normalizedSubcategory = form.subcategoria.trim() || 'General';
      const payload = {
        titulo: form.titulo.trim(),
        categoria: form.categoria.trim(),
        subcategoria: normalizedSubcategory,
        tags: form.tags,
        descripcion: form.descripcion,
        contenido_md: form.contenido_md,
        tipo_solucion: form.tipo_solucion,
        script_json: scriptJson,
      };

      const response = await fetch(isEdit ? `/api/articles/${form.id}` : '/api/articles', {
        method: isEdit ? 'PUT' : 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-user-role': currentUser?.role || 'user',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorBody = await response.json();
        const message = errorBody.error || 'Error desconocido';
        setError(message);
        alert(`Error: ${message}`);
        return;
      }

      const data = await response.json();
      if (data.success && data.data) {
        if (isEdit) {
          setArticles((prev) => prev.map((item) => (item.id === data.data.id ? data.data : item)));
          setSelectedArticle(data.data);
          setShowEditModal(false);
        } else {
          setArticles((prev) => [data.data, ...prev]);
          setShowUploadModal(false);
          setUploadForm(createInitialFormState());
          setUploadAttempted(false);
        }
      }
    } catch (error) {
      console.error(isEdit ? 'Error editando articulo:' : 'Error subiendo articulo:', error);
      const message = isEdit ? 'Error al editar el articulo' : 'Error al subir el articulo';
      setError(message);
      alert(message);
    } finally {
      setSaving(false);
    }
  };

  const handleUpload = async (e: React.FormEvent) => {
    e.preventDefault();
    await submitArticle('create');
  };

  const handleEdit = async (e: React.FormEvent) => {
    e.preventDefault();
    await submitArticle('edit');
  };

  const handleDelete = async (articleId: string | number) => {
    if (!confirm('Estas seguro de eliminar este articulo?')) return;
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`/api/articles/${articleId}`, {
        method: 'DELETE',
        headers: {
          'x-user-role': currentUser?.role || 'user',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
      });
      if (response.ok) setArticles((prev) => prev.filter((item) => item.id !== articleId));
      else alert('Error al eliminar el articulo');
    } catch (error) {
      console.error('Error eliminando articulo:', error);
      alert('Error al eliminar el articulo');
    }
  };

  const handleAddDatabase = async (e: React.FormEvent) => {
    e.preventDefault();
    setDbSaving(true);
    setDbError('');
    try {
      const token = localStorage.getItem('token');
      const res = await fetch('/api/distributed/databases', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-user-role': currentUser?.role || 'user',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({ ...dbForm, port: Number(dbForm.port) }),
      });
      const data = await res.json();
      if (!res.ok) { setDbError(data.error || 'Error al agregar'); return; }
      setShowDbModal(false);
      setDbForm({ id: '', label: '', type: 'postgres', host: '', port: '5432', database: '', user: '', password: '' });
      // Esperar ~3s para que el watcher detecte el cambio y haga el probe
      setTimeout(() => {
        fetch('/api/distributed/databases').then(r => r.json()).then(d => { if (Array.isArray(d?.databases)) setAvailableDatabases(d.databases); });
        fetch('/api/distributed/databases?showAll=true').then(r => r.json()).then(d => { if (Array.isArray(d?.databases)) setAllDatabases(d.databases); });
      }, 3500);
    } catch { setDbError('Error de red'); }
    finally { setDbSaving(false); }
  };

  const handleDeleteDatabase = async (dbId: string) => {
    if (!confirm(`¿Eliminar la base "${dbId}"? Esta acción no se puede deshacer.`)) return;
    try {
      const token = localStorage.getItem('token');
      const res = await fetch(`/api/distributed/databases/${dbId}`, {
        method: 'DELETE',
        headers: {
          'x-user-role': currentUser?.role || 'user',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
      });
      const data = await res.json();
      if (!res.ok) { alert(data.error || 'Error al eliminar'); return; }
      setTimeout(() => {
        fetch('/api/distributed/databases').then(r => r.json()).then(d => { if (Array.isArray(d?.databases)) setAvailableDatabases(d.databases); });
        fetch('/api/distributed/databases?showAll=true').then(r => r.json()).then(d => { if (Array.isArray(d?.databases)) setAllDatabases(d.databases); });
      }, 3500);
    } catch { alert('Error de red'); }
  };

  const handleSearchChange = (value: string) => {
    setSearchQuery(value);
    if (!value.trim()) setSelectedArticle(null);
  };

  const normalizeNavValue = (value: string) => value.trim().toLowerCase();

  const handleCategorySelect = (categoria: string, subcategoria?: string) => {
    setSelectedCategory(categoria);
    setSelectedSubcategory(subcategoria || '');
    const source = searchQuery.trim() ? visibleArticles : articles;
    const article = source.find((a) => {
      if (normalizeNavValue(a.categoria || '') !== normalizeNavValue(categoria || '')) return false;
      if (!subcategoria) return true;
      return normalizeNavValue(a.subcategoria || '') === normalizeNavValue(subcategoria || '');
    }) || source.find((a) => normalizeNavValue(a.categoria || '') === normalizeNavValue(categoria || ''));
    setSelectedArticle(article || null);
  };

  const renderConfigFields = (
    row: WorkflowRow,
    rowIndex: number,
    solutionType: SolutionType,
    updateFormRows: (updater: (rows: WorkflowRow[]) => WorkflowRow[]) => void,
  ) => {
    const commonInputClass = isLightTheme
      ? 'w-full rounded-md border border-[color:var(--line)] bg-white px-3 py-2 text-sm text-[color:var(--ink-900)] placeholder:text-[color:var(--ink-500)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-strong)]'
      : 'w-full rounded-md border border-slate-700 bg-slate-900/80 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-cyan-400';
    const enterpriseMode = isScriptSolutionType(solutionType);

    if (row.tipo === 'select') {
      return (
        <div className={`grid grid-cols-1 gap-2 ${enterpriseMode ? 'lg:grid-cols-3' : 'lg:grid-cols-2'}`}>
          <input value={row.config.campo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { campo: e.target.value }))} placeholder="Campo" className={commonInputClass} />
          <input value={row.config.valor} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { valor: e.target.value }))} placeholder="Valor" className={commonInputClass} />
          {enterpriseMode && <input value={row.config.guardarEn} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { guardarEn: e.target.value }))} placeholder="guardar_en" className={commonInputClass} />}
        </div>
      );
    }

    if (row.tipo === 'insert') {
      return (
        <div className={`grid grid-cols-1 gap-2 ${enterpriseMode ? 'lg:grid-cols-3' : 'lg:grid-cols-2'}`}>
          <input value={row.config.campo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { campo: e.target.value }))} placeholder="Campo" className={commonInputClass} />
          <input value={row.config.valor} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { valor: e.target.value }))} placeholder="Valor" className={commonInputClass} />
          {enterpriseMode && <input value={row.config.guardarEn} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { guardarEn: e.target.value }))} placeholder="guardar_en" className={commonInputClass} />}
        </div>
      );
    }

    if (row.tipo === 'update') {
      return (
        <div className={`grid grid-cols-1 gap-2 ${enterpriseMode ? 'lg:grid-cols-5' : 'lg:grid-cols-4'}`}>
          <input value={row.config.campoObjetivo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { campoObjetivo: e.target.value }))} placeholder="Campo a actualizar" className={commonInputClass} />
          <input value={row.config.valorNuevo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { valorNuevo: e.target.value }))} placeholder="Valor nuevo" className={commonInputClass} />
          <input value={row.config.condicionCampo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicionCampo: e.target.value }))} placeholder="Where campo" className={commonInputClass} />
          <input value={row.config.condicionValor} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicionValor: e.target.value }))} placeholder="Where valor" className={commonInputClass} />
          {enterpriseMode && <input value={row.config.guardarEn} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { guardarEn: e.target.value }))} placeholder="guardar_en" className={commonInputClass} />}
        </div>
      );
    }

    if (row.tipo === 'delete') {
      return (
        <div className={`grid grid-cols-1 gap-2 ${enterpriseMode ? 'lg:grid-cols-3' : 'lg:grid-cols-2'}`}>
          <input value={row.config.condicionCampo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicionCampo: e.target.value }))} placeholder="Campo condición" className={commonInputClass} />
          <input value={row.config.condicionValor} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicionValor: e.target.value }))} placeholder="Valor condición" className={commonInputClass} />
          {enterpriseMode && <input value={row.config.guardarEn} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { guardarEn: e.target.value }))} placeholder="guardar_en" className={commonInputClass} />}
        </div>
      );
    }

    if (enterpriseMode && row.tipo === 'validacion') {
      return (
        <div className="grid grid-cols-1 gap-2 lg:grid-cols-4">
          <input value={row.config.variable} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { variable: e.target.value }))} placeholder="Variable" className={commonInputClass} />
          <select value={row.config.condicion} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicion: e.target.value as WorkflowValidationCondition }))} className={commonInputClass}>
            {VALIDATION_CONDITIONS.map((condition) => (
              <option key={`${rowIndex}-${condition}`} value={condition}>{condition}</option>
            ))}
          </select>
          {row.config.condicion === 'igual' ? (
            <input value={row.config.valorValidacion} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { valorValidacion: e.target.value }))} placeholder="Valor esperado" className={commonInputClass} />
          ) : (
            <div className="flex items-center rounded-md border border-dashed border-[color:var(--line)] px-3 py-2 text-sm text-[color:var(--ink-600)]">Sin valor esperado</div>
          )}
          <input value={row.config.mensajeError} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { mensajeError: e.target.value }))} placeholder="Mensaje error (opcional)" className={commonInputClass} />
        </div>
      );
    }

    return <div className="text-sm text-[color:var(--ink-600)]">Selecciona un tipo para configurar este paso.</div>;
  };

  const renderWorkflowBuilder = (
    form: BuilderFormState,
    setForm: React.Dispatch<React.SetStateAction<BuilderFormState>>,
    validation: WorkflowValidationResult,
    attempted: boolean,
    generatedScript: Record<string, unknown> | null,
  ) => {
    const enterpriseMode = isScriptSolutionType(form.tipo_solucion);
    const allowedTypes = getAllowedActionTypes(form.tipo_solucion);
    const updateFormRows = (updater: (rows: WorkflowRow[]) => WorkflowRow[]) => {
      setForm((prev) => ({ ...prev, workflow_rows: updater(prev.workflow_rows) }));
    };

    return (
      <div className="space-y-4 rounded-2xl border border-[color:var(--line)] bg-gradient-to-b from-white to-[color:var(--surface-2)] p-4">
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div>
            <p className="text-sm font-semibold text-[color:var(--ink-900)]">Workflow Builder</p>
            <p className="text-xs text-[color:var(--ink-700)]">Una sola tabla visual. El modo de ejecución solo cambia reglas, campos habilitados y payload generado.</p>
          </div>
          <div className="flex items-center gap-3">
            <span className={`rounded-full px-3 py-1 text-xs font-semibold ${enterpriseMode ? 'bg-[#e7f0ff] text-[#1746a2]' : 'bg-[#e8f7ef] text-[#17633b]'}`}>
              {enterpriseMode ? 'Script' : 'Base de Datos'}
            </span>
            <button type="button" onClick={() => updateFormRows((rows) => addWorkflowRow(rows))} className="inline-flex items-center justify-center rounded-lg border border-[color:var(--line)] bg-white px-3 py-2 text-sm font-medium text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]">
              + Agregar paso
            </button>
          </div>
        </div>

        <div className="rounded-xl border border-[color:var(--line)] bg-[color:var(--surface-2)] px-4 py-3 text-xs text-[color:var(--ink-700)]">
          {enterpriseMode
            ? 'Permite validaciones, guardar_en, contexto entre pasos y variables reutilizables.'
            : 'Permite solo operaciones select, update, insert y delete. No usa validaciones complejas ni contexto entre pasos.'}
        </div>

        {attempted && !validation.valid && <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{validation.generalError}</div>}

        <div className="overflow-x-auto rounded-2xl border border-[color:var(--line)] bg-white">
          <table className="min-w-full border-collapse">
            <thead className="bg-[color:var(--surface-2)] text-left text-xs uppercase tracking-[0.08em] text-[color:var(--ink-700)]">
              <tr>
                <th className="px-4 py-3">Orden</th>
                <th className="px-4 py-3">Descripción</th>
                <th className="px-4 py-3">Tipo</th>
                <th className="px-4 py-3">Tabla</th>
                <th className="px-4 py-3">Configuración</th>
                <th className="px-4 py-3 text-right">Eliminar</th>
              </tr>
            </thead>
            <tbody>
              {form.workflow_rows.map((row, rowIndex) => {
                const rowErrors = attempted ? validation.rowErrors[rowIndex] || [] : [];
                const hasErrors = rowErrors.length > 0;

                return (
                  <tr key={`workflow-row-${rowIndex}`} className={hasErrors ? 'bg-red-50/40' : 'bg-white'}>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      <div className="flex h-10 w-10 items-center justify-center rounded-full bg-[color:var(--surface-2)] text-sm font-semibold text-[color:var(--ink-900)]">{rowIndex + 1}</div>
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      <input value={row.descripcion} onChange={(e) => setForm((prev) => ({ ...prev, workflow_rows: updateRows(prev.workflow_rows, rowIndex, { descripcion: e.target.value }) }))} placeholder="Descripción del paso" className="w-full rounded-md border border-[color:var(--line)] bg-white px-3 py-2 text-sm text-[color:var(--ink-900)] placeholder:text-[color:var(--ink-500)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-strong)]" />
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      <select
                        value={row.tipo}
                        onChange={(e) => {
                          const nextType = e.target.value as WorkflowActionType | '';
                          setForm((prev) => ({
                            ...prev,
                            workflow_rows: normalizeWorkflowRows(
                              prev.workflow_rows.map((currentRow, currentIndex) => {
                                if (currentIndex !== rowIndex) return currentRow;
                                return {
                                  ...currentRow,
                                  tipo: nextType,
                                  tabla: nextType === 'validacion' ? '' : currentRow.tabla,
                                  config: {
                                    ...createEmptyWorkflowConfig(),
                                    condicion: 'existe',
                                  },
                                };
                              })
                            ),
                          }));
                        }}
                        className="w-full rounded-md border border-[color:var(--line)] bg-white px-3 py-2 text-sm text-[color:var(--ink-900)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-strong)]"
                      >
                        <option value="">Selecciona un tipo</option>
                        {allowedTypes.map((type) => (
                          <option key={`type-${rowIndex}-${type}`} value={type}>{type}</option>
                        ))}
                      </select>
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      {row.tipo === 'validacion' ? (
                        <div className="flex h-10 items-center rounded-md border border-dashed border-[color:var(--line)] px-3 text-sm text-[color:var(--ink-600)]">No aplica</div>
                      ) : (
                        <select value={row.tabla} onChange={(e) => setForm((prev) => ({ ...prev, workflow_rows: updateRows(prev.workflow_rows, rowIndex, { tabla: e.target.value as WorkflowTableName | '' }) }))} className="w-full rounded-md border border-[color:var(--line)] bg-white px-3 py-2 text-sm text-[color:var(--ink-900)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-strong)]">
                          <option value="">Selecciona tabla</option>
                          {WORKFLOW_TABLES.map((tableName) => (
                            <option key={`table-${rowIndex}-${tableName}`} value={tableName}>{tableName}</option>
                          ))}
                        </select>
                      )}
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      {renderConfigFields(row, rowIndex, form.tipo_solucion, updateFormRows)}
                      {hasErrors && (
                        <div className="mt-2 space-y-1 text-xs text-red-700">
                          {rowErrors.map((message, errorIndex) => (
                            <p key={`row-${rowIndex}-error-${errorIndex}`}>{message}</p>
                          ))}
                        </div>
                      )}
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top text-right">
                      <button type="button" onClick={() => updateFormRows((rows) => removeWorkflowRow(rows, rowIndex))} className="rounded-lg border border-red-200 bg-white px-3 py-2 text-sm font-medium text-red-700 hover:bg-red-50">
                        Eliminar
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <div className="rounded-2xl border border-[#d8def5] bg-[#0f172a] p-4">
          <div className="mb-2 flex items-center justify-between">
            <p className="text-sm font-semibold text-white">JSON generado automáticamente</p>
            <span className="rounded-full bg-white/10 px-3 py-1 text-xs text-slate-200">Solo lectura</span>
          </div>
          <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs leading-6 text-slate-200">
            {generatedScript ? JSON.stringify(generatedScript, null, 2) : 'Completa el workflow para generar el script_json.'}
          </pre>
        </div>
      </div>
    );
  };

  const modalInputClass = isLightTheme
    ? 'w-full rounded-lg border border-[color:var(--line)] bg-white p-3 text-[color:var(--ink-900)] placeholder:text-[color:var(--ink-500)]'
    : 'w-full rounded-lg border border-slate-700 bg-slate-900/80 p-3 text-slate-100 placeholder:text-slate-400';
  const modalFileClass = isLightTheme
    ? 'w-full rounded-lg border border-[color:var(--line)] bg-white p-2 text-[color:var(--ink-800)]'
    : 'w-full rounded-lg border border-slate-700 bg-slate-900/80 p-2 text-slate-100';
  const modalTopSectionClass = isLightTheme
    ? 'space-y-4 rounded-2xl border border-[color:var(--line)] bg-[color:var(--surface-2)] p-4'
    : 'space-y-4 rounded-2xl border border-slate-700 bg-slate-900/60 p-4';
  const modalExecSectionClass = isLightTheme
    ? 'space-y-4 rounded-2xl border border-[color:var(--line)] bg-white p-4'
    : 'space-y-4 rounded-2xl border border-slate-700 bg-slate-900/70 p-4';
  const modeSwitchContainerClass = isLightTheme
    ? 'inline-flex rounded-xl border border-[color:var(--line)] bg-white p-1'
    : 'inline-flex rounded-xl border border-slate-700 bg-slate-900/80 p-1';

  return (
    <div className="h-screen flex flex-col overflow-hidden px-3 py-3 sm:px-5 sm:py-5">
      <div className="glass-panel-strong sticky top-0 z-40 overflow-visible rounded-2xl border px-4 sm:px-6">
        <div className="w-full py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-[#2363eb] to-[#1aa0c8] flex items-center justify-center text-white font-bold shadow-md">S</div>
            <div>
              <h1 className="text-xl font-bold text-[color:var(--ink-900)]">Panel de Administracion - Soluciones MD</h1>
              <p className="text-xs text-[color:var(--ink-700)]">Gestiona articulos, workflows y ejecuciones desde una sola vista.</p>
            </div>
          </div>
          <div className="ml-auto flex items-center gap-3">
            <ThemeToggle className="glass-pill" />
            <span className="glass-pill rounded-full px-3 py-1 text-sm text-[color:var(--ink-700)]">Articulos: {articleCount}</span>
            <UserIdentityBadge
              username={currentUser?.username || 'usuario'}
              role={currentUser?.role || 'user'}
              isLightTheme={isLightTheme}
              primaryActionLabel="Ir a Panel SQL Automático"
              onPrimaryAction={() => router.push('/dashboard')}
              onLogout={handleLogout}
            />
          </div>
        </div>
        <div className="w-full py-3 border-t border-[color:var(--line)]">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <input value={searchQuery} onChange={(e) => handleSearchChange(e.target.value)} placeholder="Buscar..." className="input-glass flex-1 min-w-[220px] p-2 rounded-xl" />
            {currentUser?.role === 'admin' ? (
              <div className="flex flex-wrap gap-2">
                <button onClick={openUploadModal} className="btn-accent px-4 py-2 rounded-xl text-sm font-semibold">Subir Nueva Solucion MD</button>
                {selectedArticle && (
                  <>
                    <button onClick={() => openEditModal(selectedArticle)} className="glass-pill px-4 py-2 rounded-xl text-[color:var(--ink-900)] transition-colors hover:bg-white/85">Editar articulo</button>
                    <button onClick={() => handleDelete(selectedArticle.id)} className="bg-[#fff1f1]/90 text-red-700 border border-red-200 hover:bg-[#ffe3e3] px-4 py-2 rounded-xl transition-colors">Eliminar articulo</button>
                  </>
                )}
              </div>
            ) : (
              <button className="hidden" />
            )}
          </div>
        </div>
      </div>

      <div className="flex-1 min-h-0 w-full py-4">
        <div className="flex gap-4 h-full">
          <aside className={`glass-panel glass-panel-hover rounded-2xl p-3 h-full min-h-0 overflow-hidden transition-[width] duration-300 ease-in-out ${sidebarCollapsed ? 'w-[68px]' : 'w-[320px]'}`}>
            <Sidebar
              categories={categories}
              articles={visibleArticles}
              onSelectCategory={handleCategorySelect}
              onSelectArticle={handleArticleSelect}
              selectedCategory={selectedCategory}
              selectedSubcategory={selectedSubcategory}
              selectedArticle={selectedArticle || undefined}
              collapsed={sidebarCollapsed}
              onToggleCollapse={() => setSidebarCollapsed((prev) => !prev)}
            />
          </aside>

          {!selectedArticle ? (
            <section className="glass-panel-strong glass-panel-hover flex-1 rounded-2xl p-4 h-full min-h-0 flex items-center justify-center">
              <p className="text-lg text-[color:var(--ink-700)]">Selecciona un articulo del menu</p>
            </section>
          ) : (
            <>
              <section className="glass-panel-strong glass-panel-hover flex-1 rounded-2xl p-4 h-full min-h-0 overflow-auto">
                <ContentPanel selectedArticle={selectedArticle} loading={loading} error="" onExecuteSolution={async () => 'OK'} />
              </section>
              <section className={`glass-panel-strong glass-panel-hover rounded-2xl p-4 h-full min-h-0 overflow-auto flex flex-col transition-[width] duration-300 ease-in-out ${commentsCollapsed ? 'w-[68px]' : 'w-[360px]'}`}>
                <CommentsSection articleId={selectedArticle.id} currentUser={currentUser} collapsed={commentsCollapsed} onToggleCollapse={() => setCommentsCollapsed((prev) => !prev)} />
              </section>
            </>
          )}
        </div>
      </div>

      {showUploadModal && (
        <div className="fixed inset-0 bg-black/45 flex items-center justify-center z-50 p-4">
          <div className="glass-panel-strong w-full max-w-7xl rounded-3xl border max-h-[92vh] overflow-hidden">
            <div className="border-b border-[#e6eaf8] px-6 py-5">
              <h2 className="text-2xl font-bold text-[color:var(--ink-900)]">Subir Solucion MD</h2>
              <p className="mt-1 text-sm text-[color:var(--ink-700)]">Configura el workflow visualmente. El sistema generará el script automáticamente.</p>
            </div>
            <form onSubmit={handleUpload} className="flex h-[calc(92vh-96px)] flex-col">
              <div className="space-y-5 overflow-y-auto px-6 py-5">
                {uploadError && <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{uploadError}</div>}
                <div className={modalTopSectionClass}>
                  <input type="file" accept=".md" onChange={handleFileChange} required className={modalFileClass} />
                  <input type="text" value={uploadForm.titulo} onChange={(e) => setUploadForm((prev) => ({ ...prev, titulo: e.target.value }))} placeholder="Título" className={modalInputClass} required />
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                      <div>
                        <input type="text" value={uploadForm.categoria} onChange={(e) => setUploadForm((prev) => ({ ...prev, categoria: e.target.value }))} placeholder="Categoría" list="upload-category-options" className={modalInputClass} required />
                        <datalist id="upload-category-options">
                          {categoryOptions.map((category) => (
                            <option key={category} value={category} />
                          ))}
                        </datalist>
                      </div>
                      <div>
                        <input type="text" value={uploadForm.subcategoria} onChange={(e) => setUploadForm((prev) => ({ ...prev, subcategoria: e.target.value }))} placeholder="Subcategoría (ej: Consultas, Reportes...)" className={modalInputClass} />
                      </div>
                    </div>
                    {/* Base de datos destino */}
                    <div className="space-y-1.5">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Base de datos destino</p>
                      <p className="text-xs text-[color:var(--ink-600)]">Solo aparecen las BDs con conexión activa. Si el .md contiene <code className="rounded bg-slate-100 px-1 text-xs">-- database: id</code> se detecta automáticamente.</p>
                      <div className="flex items-center gap-2">
                        <select
                          value={uploadForm.target_database}
                          onChange={(e) => setUploadForm((prev) => ({ ...prev, target_database: e.target.value }))}
                          className={modalInputClass}
                        >
                          <option value="">— Sin base específica (usa la principal) —</option>
                          {availableDatabases.length === 0
                            ? <option disabled value="">Sin bases externas conectadas en este momento</option>
                            : availableDatabases.map((db) => (
                                <option key={db.id} value={db.id}>{db.label} ({db.type} · {db.id})</option>
                              ))
                          }
                        </select>
                        {uploadForm.target_database && (
                          <span className="shrink-0 rounded-full bg-emerald-100 px-2 py-1 text-xs font-semibold text-emerald-700">✓ detectada</span>
                        )}
                      </div>
                    </div>
                    <textarea value={uploadForm.contenido_md} onChange={(e) => setUploadForm((prev) => ({ ...prev, contenido_md: e.target.value }))} placeholder="Contenido MD — puedes incluir SQL con {{parametros}} para que el usuario los complete" className={`min-h-[180px] ${modalInputClass}`} required />
                    <div className="space-y-2">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Nivel 1 · Modo de ejecución</p>
                      <div className={modeSwitchContainerClass}>
                        <button
                          type="button"
                          onClick={() => setUploadForm((prev) => {
                            const nextType: SolutionType = 'database';
                            const availableModes = getCreationModesForSolutionType(nextType);
                            const currentModeAllowed = availableModes.some((mode) => mode.value === prev.script_creation_mode);
                            return {
                              ...prev,
                              tipo_solucion: nextType,
                              workflow_rows: adaptRowsForSolutionType(prev.workflow_rows, nextType),
                              script_creation_mode: currentModeAllowed ? prev.script_creation_mode : availableModes[0].value,
                            };
                          })}
                          className={`rounded-lg px-4 py-2 text-sm font-semibold ${isDatabaseSolutionType(uploadForm.tipo_solucion) ? 'bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                        >
                          Base de Datos
                        </button>
                        <button
                          type="button"
                          onClick={() => setUploadForm((prev) => {
                            const nextType: SolutionType = 'script';
                            const availableModes = getCreationModesForSolutionType(nextType);
                            const currentModeAllowed = availableModes.some((mode) => mode.value === prev.script_creation_mode);
                            return {
                              ...prev,
                              tipo_solucion: nextType,
                              workflow_rows: adaptRowsForSolutionType(prev.workflow_rows, nextType),
                              script_creation_mode: currentModeAllowed ? prev.script_creation_mode : availableModes[0].value,
                            };
                          })}
                          className={`rounded-lg px-4 py-2 text-sm font-semibold ${isScriptSolutionType(uploadForm.tipo_solucion) ? 'bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                        >
                          Script
                        </button>
                      </div>
                    </div>
                </div>

                {isExecutableSolutionType(uploadForm.tipo_solucion) && (
                  <div className={modalExecSectionClass}>
                    <div className="space-y-2">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Nivel 2 · Forma de creación</p>
                      <div className="flex flex-wrap gap-2">
                        {getCreationModesForSolutionType(uploadForm.tipo_solucion).map((mode) => (
                          <button
                            key={`upload-mode-${mode.value}`}
                            type="button"
                            onClick={() => setUploadForm((prev) => ({ ...prev, script_creation_mode: mode.value }))}
                            className={`rounded-lg border px-3 py-2 text-sm font-semibold ${uploadForm.script_creation_mode === mode.value ? 'border-[color:var(--accent-strong)] bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'border-[color:var(--line)] bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'border-slate-700 bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                          >
                            {mode.label} · {mode.level}
                          </button>
                        ))}
                      </div>
                      <p className="text-xs text-[color:var(--ink-700)]">
                        {isDatabaseSolutionType(uploadForm.tipo_solucion)
                          ? 'Base de Datos: Visual Builder y JSON. Código Manual está deshabilitado.'
                          : 'Script: solo Código Manual (JSON workflow).'}
                      </p>
                    </div>

                    {uploadForm.script_creation_mode === 'visual_builder' && (
                      <div>
                        <WorkflowBuilder
                          value={{
                            tipo_solucion: uploadForm.tipo_solucion,
                            workflow_rows: uploadForm.workflow_rows
                          } as any}
                          onChange={handleUploadWorkflowChange as any}
                          assistantQuery={assistantQuery}
                          onAssistantQueryConsumed={clearAssistantQuery}
                          showHistoryPanel={false}
                        />
                      </div>
                    )}

                    {uploadForm.script_creation_mode === 'json_unificado' && (
                      <textarea
                        value={uploadForm.unified_json_text}
                        onChange={(e) => setUploadForm((prev) => ({ ...prev, unified_json_text: e.target.value }))}
                        placeholder={JSON.stringify({ workflow: [{ tipo: 'select', tabla: 'users', columnas: ['id', 'username'], join: [{ tabla: 'employees', on: { 'users.id': 'employees.user_id' }, columnas: ['name'] }], where: { 'users.id': '{{usuario_id}}' }, guardar_en: 'usuario_info' }] }, null, 2)}
                        className="min-h-[260px] w-full rounded-lg border border-[color:var(--line)] bg-[color:var(--surface-2)] p-3 font-mono text-sm text-[color:var(--ink-900)]"
                      />
                    )}

                    {uploadForm.script_creation_mode === 'codigo_manual' && (
                      <div className="space-y-2">
                        <div className="rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                          Modo avanzado. Puedes escribir JSON de workflow, SQL SELECT multi-paso, o scripts PL/SQL procedurales (DECLARE / BEGIN…END, FOR LOOP, IF/THEN).
                        </div>
                        <textarea
                          value={uploadForm.manual_json_text}
                          onChange={(e) => setUploadForm((prev) => ({ ...prev, manual_json_text: e.target.value }))}
                          placeholder={JSON.stringify({ workflow: [{ tipo: 'select', tabla: 'users', columnas: ['id', 'username'], join: [{ tabla: 'employees', on: { 'users.id': 'employees.user_id' }, columnas: ['cargo'] }], where: { 'users.username': '{{usuario}}' }, guardar_en: 'user_data' }] }, null, 2)}
                          className="min-h-[260px] w-full rounded-lg border border-[color:var(--line)] bg-[#fff9f0] p-3 font-mono text-sm text-[color:var(--ink-900)]"
                        />
                      </div>
                    )}

                    <div className="rounded-xl border border-[color:var(--line)] bg-[#0f172a] p-4">
                      <div className="mb-2 flex items-center justify-between">
                        <p className="text-sm font-semibold text-white">Preview JSON unificado</p>
                        <span className={`rounded px-2 py-1 text-xs ${uploadModeResult.valid ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-700'}`}>
                          {uploadModeResult.valid ? 'Válido' : 'Error'}
                        </span>
                      </div>
                      {!uploadModeResult.valid && (
                        <p className="mb-2 text-xs text-red-300">{uploadModeResult.error}</p>
                      )}
                      <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs leading-6 text-slate-200">
                        {uploadModeResult.scriptJson ? JSON.stringify(uploadModeResult.scriptJson, null, 2) : 'Completa el contenido según el modo seleccionado.'}
                      </pre>
                    </div>
                  </div>
                )}
              </div>
              <div className="flex justify-end gap-3 border-t border-[color:var(--line)] px-6 py-4">
                <button type="button" onClick={() => setShowUploadModal(false)} className="rounded-lg border border-[color:var(--line)] px-4 py-2 text-[color:var(--ink-800)]">Cancelar</button>
                <button type="submit" disabled={uploadSaving || !uploadEnterpriseValidation.isValid} className="rounded-lg bg-[#25295c] px-5 py-2 text-white disabled:cursor-not-allowed disabled:opacity-60">{uploadSaving ? 'Guardando...' : 'Subir'}</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {showEditModal && selectedArticle && (
        <div className="fixed inset-0 bg-black/45 flex items-center justify-center z-50 p-4">
          <div className="glass-panel-strong w-full max-w-7xl rounded-3xl border max-h-[92vh] overflow-hidden">
            <div className="border-b border-[color:var(--line)] px-6 py-5">
              <h2 className="text-2xl font-bold text-[color:var(--ink-900)]">Editar articulo</h2>
              <p className="mt-1 text-sm text-[color:var(--ink-700)]">Actualiza el workflow visual y el sistema regenerará el script automáticamente.</p>
            </div>
            <form onSubmit={handleEdit} className="flex h-[calc(92vh-96px)] flex-col">
              <div className="space-y-5 overflow-y-auto px-6 py-5">
                {editError && <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{editError}</div>}
                <div className={modalTopSectionClass}>
                  <input type="text" value={editForm.titulo} onChange={(e) => setEditForm((prev) => ({ ...prev, titulo: e.target.value }))} placeholder="Título" className={modalInputClass} required />
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                      <div>
                        <input type="text" value={editForm.categoria} onChange={(e) => setEditForm((prev) => ({ ...prev, categoria: e.target.value }))} placeholder="Categoría" list="edit-category-options" className={modalInputClass} required />
                        <datalist id="edit-category-options">
                          {categoryOptions.map((category) => (
                            <option key={category} value={category} />
                          ))}
                        </datalist>
                      </div>
                      <div>
                        <input type="text" value={editForm.subcategoria} onChange={(e) => setEditForm((prev) => ({ ...prev, subcategoria: e.target.value }))} placeholder="Subcategoría" className={modalInputClass} />
                      </div>
                    </div>
                    {/* Base de datos destino */}
                    <div className="space-y-1.5">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Base de datos destino</p>
                      <p className="text-xs text-[color:var(--ink-600)]">Solo aparecen las BDs con conexión activa.</p>
                      <select
                        value={editForm.target_database}
                        onChange={(e) => setEditForm((prev) => ({ ...prev, target_database: e.target.value }))}
                        className={modalInputClass}
                      >
                        <option value="">— Sin base específica (usa la principal) —</option>
                        {availableDatabases.length === 0
                          ? <option disabled value="">Sin bases externas conectadas en este momento</option>
                          : availableDatabases.map((db) => (
                              <option key={db.id} value={db.id}>{db.label} ({db.type} · {db.id})</option>
                            ))
                        }
                      </select>
                    </div>
                    <textarea value={editForm.contenido_md} onChange={(e) => setEditForm((prev) => ({ ...prev, contenido_md: e.target.value }))} placeholder="Contenido MD" className={`min-h-[180px] ${modalInputClass}`} required />
                    <div className="space-y-2">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Nivel 1 · Modo de ejecución</p>
                      <div className={modeSwitchContainerClass}>
                        <button
                          type="button"
                          onClick={() => setEditForm((prev) => {
                            const nextType: SolutionType = 'database';
                            const availableModes = getCreationModesForSolutionType(nextType);
                            const currentModeAllowed = availableModes.some((mode) => mode.value === prev.script_creation_mode);
                            return {
                              ...prev,
                              tipo_solucion: nextType,
                              workflow_rows: adaptRowsForSolutionType(prev.workflow_rows, nextType),
                              script_creation_mode: currentModeAllowed ? prev.script_creation_mode : availableModes[0].value,
                            };
                          })}
                          className={`rounded-lg px-4 py-2 text-sm font-semibold ${isDatabaseSolutionType(editForm.tipo_solucion) ? 'bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                        >
                          Base de Datos
                        </button>
                        <button
                          type="button"
                          onClick={() => setEditForm((prev) => {
                            const nextType: SolutionType = 'script';
                            const availableModes = getCreationModesForSolutionType(nextType);
                            const currentModeAllowed = availableModes.some((mode) => mode.value === prev.script_creation_mode);
                            return {
                              ...prev,
                              tipo_solucion: nextType,
                              workflow_rows: adaptRowsForSolutionType(prev.workflow_rows, nextType),
                              script_creation_mode: currentModeAllowed ? prev.script_creation_mode : availableModes[0].value,
                            };
                          })}
                          className={`rounded-lg px-4 py-2 text-sm font-semibold ${isScriptSolutionType(editForm.tipo_solucion) ? 'bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                        >
                          Script
                        </button>
                      </div>
                    </div>
                </div>

                {isExecutableSolutionType(editForm.tipo_solucion) && (
                  <div className={modalExecSectionClass}>
                    <div className="space-y-2">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Nivel 2 · Forma de creación</p>
                      <div className="flex flex-wrap gap-2">
                        {getCreationModesForSolutionType(editForm.tipo_solucion).map((mode) => (
                          <button
                            key={`edit-mode-${mode.value}`}
                            type="button"
                            onClick={() => setEditForm((prev) => ({ ...prev, script_creation_mode: mode.value }))}
                            className={`rounded-lg border px-3 py-2 text-sm font-semibold ${editForm.script_creation_mode === mode.value ? 'border-[color:var(--accent-strong)] bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'border-[color:var(--line)] bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'border-slate-700 bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                          >
                            {mode.label} · {mode.level}
                          </button>
                        ))}
                      </div>
                      <p className="text-xs text-[color:var(--ink-700)]">
                        {isDatabaseSolutionType(editForm.tipo_solucion)
                          ? 'Base de Datos: Visual Builder y JSON. Código Manual está deshabilitado.'
                          : 'Script: solo Código Manual (JSON workflow).'}
                      </p>
                    </div>

                    {editForm.script_creation_mode === 'visual_builder' && (
                      <div>
                        <WorkflowBuilder
                          value={{
                            tipo_solucion: editForm.tipo_solucion,
                            workflow_rows: editForm.workflow_rows
                          } as any}
                          onChange={handleEditWorkflowChange as any}
                          assistantQuery={assistantQuery}
                          onAssistantQueryConsumed={clearAssistantQuery}
                          showHistoryPanel={false}
                        />
                      </div>
                    )}

                    {editForm.script_creation_mode === 'json_unificado' && (
                      <textarea
                        value={editForm.unified_json_text}
                        onChange={(e) => setEditForm((prev) => ({ ...prev, unified_json_text: e.target.value }))}
                        placeholder={JSON.stringify({ workflow: [{ tipo: 'select', tabla: 'users', columnas: ['id', 'username'], join: [{ tabla: 'employees', on: { 'users.id': 'employees.user_id' }, columnas: ['name'] }], where: { 'users.id': '{{usuario_id}}' }, guardar_en: 'usuario_info' }] }, null, 2)}
                        className="min-h-[260px] w-full rounded-lg border border-[color:var(--line)] bg-[color:var(--surface-2)] p-3 font-mono text-sm text-[color:var(--ink-900)]"
                      />
                    )}

                    {editForm.script_creation_mode === 'codigo_manual' && (
                      <div className="space-y-2">
                        <div className="rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                          Modo avanzado. Puedes escribir JSON de workflow, SQL SELECT multi-paso, o scripts PL/SQL procedurales (DECLARE / BEGIN…END, FOR LOOP, IF/THEN).
                        </div>
                        <textarea
                          value={editForm.manual_json_text}
                          onChange={(e) => setEditForm((prev) => ({ ...prev, manual_json_text: e.target.value }))}
                          placeholder={JSON.stringify({ workflow: [{ tipo: 'select', tabla: 'users', columnas: ['id', 'username'], join: [{ tabla: 'employees', on: { 'users.id': 'employees.user_id' }, columnas: ['cargo'] }], where: { 'users.username': '{{usuario}}' }, guardar_en: 'user_data' }] }, null, 2)}
                          className="min-h-[260px] w-full rounded-lg border border-[color:var(--line)] bg-[#fff9f0] p-3 font-mono text-sm text-[color:var(--ink-900)]"
                        />
                      </div>
                    )}

                    <div className="rounded-xl border border-[color:var(--line)] bg-[#0f172a] p-4">
                      <div className="mb-2 flex items-center justify-between">
                        <p className="text-sm font-semibold text-white">Preview JSON unificado</p>
                        <span className={`rounded px-2 py-1 text-xs ${editModeResult.valid ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-700'}`}>
                          {editModeResult.valid ? 'Válido' : 'Error'}
                        </span>
                      </div>
                      {!editModeResult.valid && (
                        <p className="mb-2 text-xs text-red-300">{editModeResult.error}</p>
                      )}
                      <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs leading-6 text-slate-200">
                        {editModeResult.scriptJson ? JSON.stringify(editModeResult.scriptJson, null, 2) : 'Completa el contenido según el modo seleccionado.'}
                      </pre>
                    </div>
                  </div>
                )}
              </div>
              <div className="flex justify-end gap-3 border-t border-[color:var(--line)] px-6 py-4">
                <button type="button" onClick={() => setShowEditModal(false)} className="rounded-lg border border-[color:var(--line)] px-4 py-2 text-[color:var(--ink-800)]">Cancelar</button>
                <button type="submit" disabled={editSaving || !editEnterpriseValidation.isValid} className="rounded-lg bg-[#25295c] px-5 py-2 text-white disabled:cursor-not-allowed disabled:opacity-60">{editSaving ? 'Guardando...' : 'Guardar'}</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── Modal Gestión de Bases de Datos ───────────────────────────────── */}
      {showDbModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm p-4">
          <div className={`w-full max-w-2xl rounded-2xl shadow-2xl border overflow-hidden ${isLightTheme ? 'bg-white border-slate-200' : 'bg-slate-900 border-slate-700'}`}>
            <div className={`px-6 py-4 border-b flex items-center justify-between ${isLightTheme ? 'border-slate-200 bg-slate-50' : 'border-slate-700 bg-slate-800'}`}>
              <div>
                <h2 className="text-lg font-bold text-[color:var(--ink-900)]">🗄 Gestión de Bases de Datos</h2>
                <p className="text-xs text-[color:var(--ink-600)] mt-0.5">Los cambios se aplican automáticamente — sin reiniciar el sistema.</p>
              </div>
              <button onClick={() => setShowDbModal(false)} className="text-[color:var(--ink-600)] hover:text-[color:var(--ink-900)] text-xl font-bold leading-none">×</button>
            </div>

            <div className="p-6 space-y-5 max-h-[70vh] overflow-y-auto">
              {/* Lista de BDs actuales */}
              <div className="space-y-2">
                <p className="text-sm font-semibold text-[color:var(--ink-900)]">Bases configuradas</p>
                <div className="space-y-2">
                  {allDatabases.map((db) => (
                    <div key={db.id} className={`flex items-center gap-3 rounded-xl border px-4 py-3 ${isLightTheme ? 'border-slate-200 bg-slate-50' : 'border-slate-700 bg-slate-800'}`}>
                      <span className={`shrink-0 w-2 h-2 rounded-full ${db.connected ? 'bg-emerald-400' : 'bg-slate-400'}`} title={db.connected ? 'Conectada' : 'Sin conexión'} />
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-semibold text-[color:var(--ink-900)] truncate">{db.label}</p>
                        <p className="text-xs text-[color:var(--ink-600)]">{db.type} · {db.id} {db.primary ? '· BD primaria del sistema' : ''}</p>
                      </div>
                      <span className={`text-xs rounded-full px-2 py-0.5 font-medium ${db.connected ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-500'}`}>
                        {db.connected ? 'conectada' : 'sin conexión'}
                      </span>
                      {!db.primary && (
                        <button
                          onClick={() => handleDeleteDatabase(db.id)}
                          className="shrink-0 text-xs text-red-500 hover:text-red-700 hover:bg-red-50 rounded-lg px-2 py-1 transition-colors"
                          title="Eliminar esta base de datos"
                        >
                          Eliminar
                        </button>
                      )}
                    </div>
                  ))}
                  {allDatabases.length === 0 && (
                    <p className="text-sm text-[color:var(--ink-600)] text-center py-4">No hay bases configuradas aún.</p>
                  )}
                </div>
              </div>

              {/* Formulario para agregar nueva BD */}
              <div className={`rounded-xl border p-4 space-y-4 ${isLightTheme ? 'border-slate-200 bg-slate-50' : 'border-slate-700 bg-slate-800'}`}>
                <p className="text-sm font-semibold text-[color:var(--ink-900)]">Agregar nueva base de datos</p>
                {dbError && <p className="text-sm text-red-600 bg-red-50 rounded-lg px-3 py-2">{dbError}</p>}
                <form onSubmit={handleAddDatabase} className="space-y-3">
                  <div className="grid grid-cols-2 gap-3">
                    <input required placeholder="ID único (ej: oracle_prod)" value={dbForm.id} onChange={e => setDbForm(p => ({...p, id: e.target.value.toLowerCase().replace(/\s/g,'_')}))} className={modalInputClass} />
                    <input placeholder="Nombre visible (ej: Oracle Producción)" value={dbForm.label} onChange={e => setDbForm(p => ({...p, label: e.target.value}))} className={modalInputClass} />
                  </div>
                  <div className="grid grid-cols-3 gap-3">
                    <select value={dbForm.type} onChange={e => setDbForm(p => ({...p, type: e.target.value, port: e.target.value === 'oracle' ? '1521' : e.target.value === 'mysql' ? '3306' : '5432'}))} className={modalInputClass}>
                      <option value="postgres">PostgreSQL</option>
                      <option value="oracle">Oracle</option>
                      <option value="mysql">MySQL</option>
                    </select>
                    <input required placeholder="Host / IP" value={dbForm.host} onChange={e => setDbForm(p => ({...p, host: e.target.value}))} className={modalInputClass} />
                    <input required placeholder="Puerto" type="number" value={dbForm.port} onChange={e => setDbForm(p => ({...p, port: e.target.value}))} className={modalInputClass} />
                  </div>
                  <div className="grid grid-cols-3 gap-3">
                    <input required placeholder="Base de datos / SID / Service" value={dbForm.database} onChange={e => setDbForm(p => ({...p, database: e.target.value}))} className={modalInputClass} />
                    <input required placeholder="Usuario" value={dbForm.user} onChange={e => setDbForm(p => ({...p, user: e.target.value}))} className={modalInputClass} />
                    <input required placeholder="Contraseña" type="password" value={dbForm.password} onChange={e => setDbForm(p => ({...p, password: e.target.value}))} className={modalInputClass} />
                  </div>
                  <p className="text-xs text-[color:var(--ink-600)]">
                    💡 Al guardar, el sistema conectará automáticamente en segundos. El estado aparecerá actualizado sin reiniciar nada.
                  </p>
                  <div className="flex justify-end">
                    <button type="submit" disabled={dbSaving} className="rounded-lg bg-[#25295c] px-5 py-2 text-sm font-semibold text-white disabled:opacity-60">
                      {dbSaving ? 'Guardando...' : 'Agregar base de datos'}
                    </button>
                  </div>
                </form>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default withAuth(AdminPage);
