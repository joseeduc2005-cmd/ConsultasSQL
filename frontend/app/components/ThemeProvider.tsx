'use client';

import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';

type ThemeMode = 'light' | 'dark';

type ThemeContextValue = {
  theme: ThemeMode;
  setTheme: (next: ThemeMode) => void;
  toggleTheme: () => void;
};

const STORAGE_KEY = 'appTheme';

const ThemeContext = createContext<ThemeContextValue | null>(null);

function normalizeTheme(value: unknown): ThemeMode {
  return String(value || '').toLowerCase() === 'dark' ? 'dark' : 'light';
}

function applyThemeToDocument(theme: ThemeMode) {
  document.documentElement.setAttribute('data-theme', theme);
  document.body.setAttribute('data-theme', theme);
}

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [theme, setThemeState] = useState<ThemeMode>('light');

  useEffect(() => {
    const saved = normalizeTheme(localStorage.getItem(STORAGE_KEY));
    setThemeState(saved);
    applyThemeToDocument(saved);
  }, []);

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, theme);
    localStorage.setItem('workflowBuilderTheme', theme);
    applyThemeToDocument(theme);
  }, [theme]);

  const value = useMemo<ThemeContextValue>(() => ({
    theme,
    setTheme: (next) => setThemeState(normalizeTheme(next)),
    toggleTheme: () => setThemeState((prev) => (prev === 'dark' ? 'light' : 'dark')),
  }), [theme]);

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) {
    throw new Error('useTheme must be used inside ThemeProvider');
  }
  return ctx;
}
