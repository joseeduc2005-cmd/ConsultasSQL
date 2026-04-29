// app/lib/api.ts

import { sanitizeApiPayload } from './sanitize';

export async function apiCall(
  url: string,
  options: RequestInit = {}
): Promise<Response> {
  const token = localStorage.getItem('token');
  
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string>),
  };

  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  return fetch(url, {
    ...options,
    headers,
  });
}

export async function apiGet<T>(url: string): Promise<T | null> {
  try {
    const response = await apiCall(url);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('API Error:', error);
    return null;
  }
}

export async function apiPost<T>(url: string, data: any): Promise<T | null> {
  try {
    const safeData = sanitizeApiPayload(data);
    const response = await apiCall(url, {
      method: 'POST',
      body: JSON.stringify(safeData),
    });
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('API Error:', error);
    return null;
  }
}

export async function apiPut<T>(url: string, data: any): Promise<T | null> {
  try {
    const safeData = sanitizeApiPayload(data);
    const response = await apiCall(url, {
      method: 'PUT',
      body: JSON.stringify(safeData),
    });
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('API Error:', error);
    return null;
  }
}

export async function apiDelete(url: string): Promise<boolean> {
  try {
    const response = await apiCall(url, {
      method: 'DELETE',
    });
    return response.ok;
  } catch (error) {
    console.error('API Error:', error);
    return false;
  }
}
