// src/interfaces/types.ts

// Authentication Types
export interface AuthTokenPayload {
  id: string;
  username: string;
  role: 'admin' | 'user';
  iat?: number;
  exp?: number;
}

export interface LoginRequest {
  username: string;
  password: string;
}

export interface LoginResponse {
  success: boolean;
  user?: {
    id: string;
    username: string;
    role: string;
  };
  token?: string;
  error?: string;
}

// Article Types
export interface ArticleResponse {
  id: string;
  titulo: string;
  tags: string[];
  contenido?: string;
  creadoPor: string;
  fecha: string;
  actualizado?: string;
}

export interface CreateArticleRequest {
  titulo: string;
  tags: string[];
  contenido: string;
}

export interface UpdateArticleRequest {
  titulo: string;
  tags: string[];
  contenido: string;
}

export interface SearchArticlesRequest {
  tags: string[];
}

// API Response Types
export interface ApiSuccessResponse<T> {
  success: true;
  data?: T;
  message?: string;
}

export interface ApiErrorResponse {
  success: false;
  error: string;
  code?: string;
}

export type ApiResponse<T> = ApiSuccessResponse<T> | ApiErrorResponse;

// Pagination
export interface PaginationParams {
  page: number;
  limit: number;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pages: number;
}
