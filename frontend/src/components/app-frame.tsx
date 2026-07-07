'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { ReactNode, useEffect, useState } from 'react';
import { logout } from '@/lib/api';
import { getToken } from '@/lib/tokens';
import { cn } from '@/components/ui';

const NAV = [
  { href: '/dashboard', label: 'Dashboard',     icon: '▣' },
  { href: '/watchlist', label: 'Watchlist',     icon: '◎' },
  { href: '/agent',     label: 'AI Agent',      icon: '◆' },
  { href: '/ai',        label: 'AI Analytics',  icon: '◇' },
  { href: '/data',      label: 'Data Lake',     icon: '⬡' },
  { href: '/account',   label: 'Account',       icon: '◉' },
];

const SKIP_AUTH = ['/login'];

export function AppFrame({ children }: { children: ReactNode }) {
  const pathname = usePathname() ?? '/';
  const router   = useRouter();
  const [ready,    setReady]    = useState(false);
  const [leaving,  setLeaving]  = useState(false);

  useEffect(() => {
    const tok = getToken();
    if (!tok && !SKIP_AUTH.some(p => pathname.startsWith(p))) {
      router.replace('/login');
    } else {
      setReady(true);
    }
  }, [pathname, router]);

  if (SKIP_AUTH.some(p => pathname.startsWith(p))) return <>{children}</>;
  if (!ready) return null;

  const handleLogout = async () => {
    setLeaving(true);
    try { await logout(); } catch { /* ignore */ }
    router.replace('/login');
  };

  return (
    <div className="flex h-screen overflow-hidden bg-[#030712]">
      {/* ─── Sidebar ─────────────────────────────────────────────── */}
      <aside className="w-56 flex-shrink-0 flex flex-col bg-[#0a0f1a] border-r border-slate-800/60">
        {/* Wordmark */}
        <div className="px-4 py-5 border-b border-slate-800/60">
          <div className="flex items-center gap-2.5">
            <div className="w-7 h-7 rounded-lg bg-blue-600 flex items-center justify-center text-white font-bold text-[11px] tracking-tight select-none">
              FS
            </div>
            <div>
              <p className="text-sm font-semibold text-slate-100 leading-tight">FinStreamAI</p>
              <p className="text-[10px] text-slate-500 leading-tight">Analytics workspace</p>
            </div>
          </div>
        </div>

        {/* Nav links */}
        <nav className="flex-1 px-3 py-4 space-y-0.5 overflow-y-auto">
          {NAV.map(item => {
            const active = pathname === item.href || pathname.startsWith(item.href + '/');
            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  'flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors',
                  active
                    ? 'bg-blue-600/15 text-blue-400 border border-blue-600/25'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50 border border-transparent',
                )}
              >
                <span className={cn('text-base', active ? 'opacity-100' : 'opacity-50')}>{item.icon}</span>
                {item.label}
              </Link>
            );
          })}
        </nav>

        {/* Sign-out */}
        <div className="px-3 py-4 border-t border-slate-800/60">
          <button
            onClick={handleLogout}
            disabled={leaving}
            className="flex items-center gap-3 w-full px-3 py-2 rounded-lg text-sm font-medium text-slate-400 hover:text-red-400 hover:bg-red-950/30 border border-transparent transition-colors disabled:opacity-50"
          >
            <span className="text-base opacity-50">⎋</span>
            {leaving ? 'Signing out…' : 'Sign out'}
          </button>
        </div>
      </aside>

      {/* ─── Content ─────────────────────────────────────────────── */}
      <main className="flex-1 overflow-auto">
        <div className="max-w-6xl mx-auto px-6 py-8">
          {children}
        </div>
      </main>
    </div>
  );
}
