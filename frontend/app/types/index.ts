// app/types/index.ts

export interface KnowledgeArticle {
  id: string | number;
  titulo: string;
  categoria: string;
  subcategoria: string;
  tags: string[];
  descripcion: string;
  contenido?: string;
  pasos?: string[];
  camposFormulario?: any[];
  accionScript?: string;
  script_json?: Record<string, unknown> | string | null;
  contenido_md?: string;
  tipo_solucion?: 'lectura' | 'ejecutable' | 'database' | 'script';
  creado_en?: string;
}

export interface User {
  id: number;
  username: string;
  name: string;
  email: string;
  role: 'admin' | 'user';
}

export interface LoginResponse {
  success: boolean;
  token: string;
  user: User;
}

export interface ApiResponse<T> {
  success?: boolean;
  data?: T;
  error?: string;
  message?: string;
}
