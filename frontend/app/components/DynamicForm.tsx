// app/components/DynamicForm.tsx

'use client';

import { useState } from 'react';
import { KnowledgeArticle } from '../types';

interface FormField {
  name: string;
  label: string;
  type: string;
  required?: boolean;
}

interface DynamicFormProps {
  article: KnowledgeArticle;
  onSubmit?: (formData: Record<string, any>) => Promise<string>;
  buttonLabel?: string;
  showTitle?: boolean;
  title?: string;
}

export default function DynamicForm({
  article,
  onSubmit,
  buttonLabel = 'Reparar',
  showTitle = false,
  title = 'Formulario dinámico',
}: DynamicFormProps) {
  const [formData, setFormData] = useState<Record<string, any>>({});
  const [loading, setLoading] = useState(false);
  const [mdFilename, setMdFilename] = useState('');
  const [mdContent, setMdContent] = useState('');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [resultMessage, setResultMessage] = useState('');
  const [repairResult, setRepairResult] = useState<any>(null);

  // Obtener articleId del artículo
  const articleId = article.id;

  const getDynamicFields = (): FormField[] => {
    if (article.camposFormulario && Array.isArray(article.camposFormulario) && article.camposFormulario.length > 0) {
      return article.camposFormulario;
    }

    // Fallback simple lógica basada en título y tags
    const fields: FormField[] = [];

    if (article.titulo.toLowerCase().includes('login') || article.tags.includes('login')) {
      fields.push({ name: 'username', label: 'Usuario', type: 'text', required: true });
      fields.push({ name: 'password', label: 'Contraseña', type: 'password', required: true });
    }

    if (article.titulo.toLowerCase().includes('transferencia') || article.tags.includes('transferencia')) {
      fields.push({ name: 'accountNumber', label: 'Número de cuenta', type: 'text', required: true });
      fields.push({ name: 'amount', label: 'Monto', type: 'number', required: true });
    }

    if (article.titulo.toLowerCase().includes('error') || article.tags.includes('error')) {
      fields.push({ name: 'errorCode', label: 'Código de error', type: 'text', required: true });
      fields.push({ name: 'problemDescription', label: 'Descripción del problema', type: 'textarea', required: false });
    }

    if (fields.length === 0) {
      fields.push({ name: 'comment', label: 'Detalle', type: 'textarea', required: false });
    }

    return fields;
  };

  const fields = getDynamicFields();

  const validateForm = (): boolean => {
    const newErrors: Record<string, string> = {};

    // Validar campos dinámicos
    fields.forEach((field) => {
      if (field.required && !formData[field.name]?.trim()) {
        if (field.name === 'username') {
          newErrors[field.name] = 'El usuario es obligatorio';
        } else if (field.name === 'password') {
          newErrors[field.name] = 'La contraseña es obligatoria';
        } else {
          newErrors[field.name] = `${field.label} es obligatorio`;
        }
      }
    });

    // Validar archivo .md solo si no hay contenido MD ya disponible en el artículo
    if (!article.contenido_md) {
      if (!mdFilename) {
        newErrors.mdFile = 'Debe subir un archivo .md';
      } else if (!mdFilename.toLowerCase().endsWith('.md')) {
        newErrors.mdFile = 'Solo se permiten archivos .md';
      }
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setResultMessage('');
    setRepairResult(null); // Resetear resultado anterior

    if (!validateForm()) {
      return;
    }

    // ====== VALIDACIÓN EXTRA DEL ID ======
    console.log('[REPAIR-FORM] 🔍 Información del artículo:', {
      articleId: articleId,
      type: typeof articleId,
      isUUID: typeof articleId === 'string' && articleId.length > 10,
      titulo: article.titulo,
    });

    if (!articleId || (typeof articleId === 'string' && articleId.length < 10)) {
      setResultMessage('❌ ID del artículo inválido. Por favor, recarga la página.');
      console.error('[REPAIR-FORM] ❌ ID inválido:', articleId);
      return;
    }

    setLoading(true);
    try {
      // Crear FormData para enviar archivo y datos
      const formDataToSend = new FormData();

      // Agregar datos del formulario
      Object.entries(formData).forEach(([key, value]) => {
        formDataToSend.append(key, value as string);
      });

      // Agregar ID del artículo
      formDataToSend.append('articleId', articleId.toString());
      
      console.log('[REPAIR-FORM] 📤 ID enviado:', articleId.toString());

      // Agregar archivo .md
      if (mdFilename && mdContent) {
        const mdBlob = new Blob([mdContent], { type: 'text/markdown' });
        const mdFile = new File([mdBlob], mdFilename, { type: 'text/markdown' });
        formDataToSend.append('mdFile', mdFile);
      }

      // Enviar a nuevo endpoint de reparación
      const response = await fetch('/api/repair', {
        method: 'POST',
        body: formDataToSend,
      });

      const result = await response.json();

      if (response.ok && result.success) {
        setResultMessage(`✅ ${result.message}\n\n${result.resultado}`);
        setRepairResult(result); // Guardar resultado completo
      } else {
        setResultMessage(`❌ ${result.message || result.error || 'Error en la reparación'}`);
        console.error('[REPAIR-FORM] ❌ Error:', result);
        setRepairResult(null);
      }
    } catch (error) {
      setResultMessage('❌ Error al ejecutar la reparación');
      console.error('[REPAIR-FORM] ❌ Error:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (name: string, value: any) => {
    setFormData((prev) => ({ ...prev, [name]: value }));
  };

  return (
    <div className="w-full">
      {showTitle && (
        <h3 className="text-lg font-semibold mb-4 text-gray-900">{title}</h3>
      )}
      <form onSubmit={handleSubmit} className="space-y-4">
        {/* Campos dinámicos */}
        {fields.length > 0 && (
          <div className="bg-slate-50 rounded-lg p-4 border border-slate-200">
            {fields.map((field) => (
              <div key={field.name} className="mb-4 last:mb-0">
                <label className="block text-sm font-semibold text-gray-900 mb-2">
                  {field.label}
                  {field.required && <span className="text-red-500 ml-1">*</span>}
                </label>
                {field.type === 'textarea' ? (
                  <textarea
                    value={formData[field.name] || ''}
                    onChange={(e) => handleChange(field.name, e.target.value)}
                    required={field.required}
                    placeholder={`Ingresa ${field.label.toLowerCase()}`}
                    className={`w-full px-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none transition-colors text-sm ${
                      errors[field.name] ? 'border-red-500 bg-red-50' : 'border-slate-300 bg-white'
                    }`}
                    rows={3}
                  />
                ) : (
                  <input
                    type={field.type}
                    value={formData[field.name] || ''}
                    onChange={(e) => handleChange(field.name, e.target.value)}
                    required={field.required}
                    placeholder={`Ingresa ${field.label.toLowerCase()}`}
                    className={`w-full px-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-colors text-sm ${
                      errors[field.name] ? 'border-red-500 bg-red-50' : 'border-slate-300 bg-white'
                    }`}
                  />
                )}
                {errors[field.name] && (
                  <p className="mt-1 text-xs text-red-600 flex items-center">
                    <span className="mr-1">⚠️</span>{errors[field.name]}
                  </p>
                )}
              </div>
            ))}
          </div>
        )}

        {/* Archivo Markdown */}
        {!article.contenido_md && (
          <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
            <label className="block text-sm font-semibold text-gray-900 mb-2">Archivo Markdown (.md)</label>
            <input
              type="file"
              accept=".md"
              onChange={async (e) => {
                const file = e.target.files?.[0];
                if (!file) {
                  setMdFilename('');
                  setMdContent('');
                  return;
                }
                setMdFilename(file.name);
                const text = await file.text();
                setMdContent(text);
              }}
              className={`w-full px-4 py-2 border rounded-lg cursor-pointer text-sm block file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-semibold file:bg-blue-600 file:text-white hover:file:bg-blue-700 transition-colors ${
                errors.mdFile ? 'border-red-500 bg-red-50' : 'border-blue-300'
              }`}
            />
            {mdFilename && (
              <p className="mt-2 text-xs text-[#25295c] flex items-center">
                <span className="mr-1">✓</span>
                <span className="font-medium">{mdFilename}</span>
              </p>
            )}
            {errors.mdFile && (
              <p className="mt-2 text-xs text-red-600 flex items-center">
                <span className="mr-1">⚠️</span>{errors.mdFile}
              </p>
            )}
            {mdContent && (
              <div className="mt-3 p-3 border border-slate-300 rounded-lg bg-white max-h-32 overflow-y-auto text-xs font-mono text-gray-700 whitespace-pre-wrap">
                {mdContent.substring(0, 200)}
                {mdContent.length > 200 && '...'}
              </div>
            )}
          </div>
        )}

        {/* Botón envío */}
        <button
          type="submit"
          disabled={loading}
          className="w-full bg-gradient-to-r from-blue-600 to-blue-700 text-white py-3 px-4 rounded-lg hover:from-blue-700 hover:to-blue-800 disabled:opacity-50 disabled:cursor-not-allowed font-semibold transition-all duration-200 flex items-center justify-center space-x-2"
        >
          {loading ? (
            <>
              <span className="animate-spin">⏳</span>
              <span>Procesando...</span>
            </>
          ) : (
            <>
              <span>{buttonLabel}</span>
            </>
          )}
        </button>

        {/* Resultado */}
        {resultMessage && (
          <div className={`mt-4 p-4 rounded-lg border-l-4 text-sm ${
            resultMessage.includes('❌') || resultMessage.includes('Error')
              ? 'bg-red-50 text-red-800 border-l-red-500'
              : 'bg-indigo-50 text-indigo-800 border-l-indigo-500'
          }`}>
            <p className="font-semibold mb-2">{resultMessage.split('\n')[0]}</p>
            
            {repairResult?.pasosEjecutados && repairResult.pasosEjecutados.length > 0 && (
              <div className="mt-3 pt-3 border-t border-current border-opacity-20">
                <p className="font-semibold text-xs uppercase tracking-wider mb-2">Pasos ejecutados:</p>
                <ol className="list-decimal list-inside space-y-1 text-xs">
                  {repairResult.pasosEjecutados.map((paso: string, index: number) => (
                    <li key={index} className="ml-2">{paso}</li>
                  ))}
                </ol>
              </div>
            )}
            
            {repairResult?.categoria && (
              <p className="text-xs opacity-75 mt-2">
                📁 {repairResult.categoria}
                {repairResult.subcategoria && ` > ${repairResult.subcategoria}`}
              </p>
            )}
          </div>
        )}
      </form>
    </div>
  );
}
