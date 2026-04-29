// app/login/page.tsx

'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';

export default function LoginPage() {
  const router = useRouter();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      const controller = new AbortController();
      const timeoutId = window.setTimeout(() => controller.abort(), 12000);

      const response = await fetch('/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password }),
        signal: controller.signal,
      });

      window.clearTimeout(timeoutId);

      const data = await response.json();

      if (!response.ok) {
        setError(data.error || 'Error en login');
        return;
      }

      // Guardar token en localStorage
      localStorage.setItem('token', data.token);
      localStorage.setItem('user', JSON.stringify(data.user));
      localStorage.setItem('userRole', data.user.role);
      document.cookie = `token=${encodeURIComponent(data.token)}; Path=/; SameSite=Lax`;

      // Redirigir según el rol
      if (data.user.role === 'admin') {
        router.replace('/admin');
      } else {
        router.replace('/admin');
      }
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') {
        setError('El servidor tardó demasiado en responder. Intenta nuevamente.');
      } else {
        setError('Error conectando con el servidor');
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="relative min-h-screen overflow-hidden px-4 py-6 sm:px-8 sm:py-10">
      <div className="pointer-events-none absolute inset-0">
        <div className="absolute left-[-90px] top-[-70px] h-72 w-72 rounded-full bg-[#4f7dff]/25 blur-3xl" />
        <div className="absolute bottom-[-120px] right-[-120px] h-80 w-80 rounded-full bg-[#22a6cf]/20 blur-3xl" />
      </div>

      <div className="relative mx-auto grid min-h-[calc(100vh-3rem)] max-w-7xl grid-cols-1 gap-6 lg:grid-cols-[1.2fr_0.9fr]">
        <section className="glass-panel-strong glass-panel-hover hidden rounded-3xl p-10 lg:flex lg:flex-col lg:justify-between">
          <div>
            <div className="mb-7 flex items-center gap-3">
              <span className="flex h-11 w-11 items-center justify-center rounded-2xl bg-gradient-to-br from-[color:var(--accent)] to-[color:var(--accent-2)] text-lg font-extrabold text-white shadow-md">S</span>
              <div>
                <p className="text-sm font-semibold uppercase tracking-[0.16em] text-[color:var(--ink-800)]">Sistema de Soporte</p>
                <p className="text-xs text-[color:var(--ink-700)]">Workspace inteligente de soluciones</p>
              </div>
            </div>

            <h1 className="text-5xl font-extrabold leading-tight text-[color:var(--accent-strong)]">
              Portal de Soporte
            </h1>
            <p className="mt-4 max-w-2xl text-lg text-[color:var(--ink-800)]">
              Aplicación interna para consultar soluciones, ejecutar flujos y gestionar artículos de conocimiento según tu rol.
            </p>
          </div>

          <div className="glass-panel rounded-2xl border border-[color:var(--line)] p-5">
            <p className="text-sm font-semibold text-[color:var(--ink-900)]">Información de acceso</p>
            <p className="mt-1 text-sm text-[color:var(--ink-700)]">
              Inicia sesión con tus credenciales para abrir tu panel personalizado.
            </p>
          </div>
        </section>

        <section className="flex items-center justify-center">
          <div className="glass-panel-strong glass-panel-hover w-full max-w-md rounded-3xl p-7 sm:p-8">
            <div className="mb-6">
              <div>
                <h2 className="text-3xl font-extrabold text-[color:var(--ink-900)]">Iniciar sesion</h2>
                <p className="mt-1 text-sm text-[color:var(--ink-700)]">Accede a tu panel de trabajo.</p>
              </div>
            </div>

            {error && (
              <div className="mb-4 rounded-xl border border-red-200 bg-red-50/90 px-4 py-3 text-sm text-red-700">
                {error}
              </div>
            )}

            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label className="mb-2 block text-sm font-semibold text-[color:var(--ink-800)]">Usuario</label>
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  className="input-glass w-full rounded-xl px-4 py-2.5"
                  required
                />
              </div>

              <div>
                <label className="mb-2 block text-sm font-semibold text-[color:var(--ink-800)]">Contrasena</label>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="input-glass w-full rounded-xl px-4 py-2.5"
                  required
                />
              </div>

              <button
                type="submit"
                disabled={loading}
                className="btn-accent w-full rounded-xl px-4 py-2.5 text-sm font-semibold disabled:cursor-not-allowed disabled:opacity-60"
              >
                {loading ? 'Ingresando...' : 'Entrar al sistema'}
              </button>
            </form>

            <div className="glass-panel mt-6 rounded-2xl p-4 text-sm text-[color:var(--ink-700)]">
              <p className="mb-2 font-semibold text-[color:var(--ink-900)]">Usuarios de prueba</p>
              <p>Admin: admin / password123</p>
              <p>User: user / password123</p>
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}
