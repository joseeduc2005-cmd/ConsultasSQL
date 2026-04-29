// app/components/ProtectedRoute.tsx

'use client';

import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';

interface User {
  id: string;
  username: string;
  role: string;
}

export function withAuth<P extends object>(
  Component: React.ComponentType<P>,
  requiredRole?: string | string[]
) {
  return function ProtectedComponent(props: P) {
    const router = useRouter();
    const [user, setUser] = useState<User | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
      const verifyUser = async () => {
        try {
          const token = localStorage.getItem('token');
          const userStr = localStorage.getItem('user');

          if (!token || !userStr) {
            router.push('/login');
            return;
          }

          const localUser = JSON.parse(userStr);

          // Confirmar con el backend para evitar role spoofing local
          const controller = new AbortController();
          const timeoutId = window.setTimeout(() => controller.abort(), 12000);
          const response = await fetch('/api/auth/me', {
            headers: {
              Authorization: `Bearer ${token}`,
            },
            signal: controller.signal,
          });
          window.clearTimeout(timeoutId);

          if (!response.ok) {
            localStorage.removeItem('token');
            localStorage.removeItem('user');
            router.push('/login');
            return;
          }

          const data = await response.json();
          const parsedUser = data.user || localUser;
          setUser(parsedUser);

          const roleMatches =
            requiredRole == null ||
            (typeof requiredRole === 'string'
              ? parsedUser.role === requiredRole
              : requiredRole.includes(parsedUser.role));

          if (!roleMatches) {
            router.push(parsedUser.role === 'admin' ? '/admin' : '/dashboard');
            return;
          }

          setLoading(false);
        } catch (error) {
          console.error('ProtectedRoute auth error:', error);
          localStorage.removeItem('token');
          localStorage.removeItem('user');
          router.push('/login');
        }
      };

      verifyUser();
    }, [router, requiredRole]);

    if (loading) {
      return (
        <div className="min-h-screen flex items-center justify-center">
          <div className="text-xl text-gray-600">Cargando...</div>
        </div>
      );
    }

    const WrappedComponent = Component as any;
    return <WrappedComponent {...props} user={user} />;
  };
}
