'use client';

import { useState, FormEvent } from 'react';
import { useRouter } from 'next/navigation';
import { login, register } from '@/lib/api';
import { Button, Input, Alert, cn } from '@/components/ui';

type Mode = 'login' | 'register';

export default function LoginPage() {
  const router   = useRouter();
  const [mode,   setMode]   = useState<Mode>('login');
  const [email,  setEmail]  = useState('');
  const [pass,   setPass]   = useState('');
  const [name,   setName]   = useState('');
  const [error,  setError]  = useState('');
  const [busy,   setBusy]   = useState(false);

  const switchMode = (m: Mode) => { setMode(m); setError(''); };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError('');
    setBusy(true);
    try {
      if (mode === 'register') await register(email, pass, name || undefined);
      await login(email, pass);
      router.replace('/dashboard');
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Authentication failed');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-[#030712] px-4">
      <div className="w-full max-w-sm">
        {/* Brand */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-12 h-12 rounded-2xl bg-blue-600 text-white font-bold text-lg mb-4 select-none">
            FS
          </div>
          <h1 className="text-2xl font-bold text-slate-100">FinStreamAI</h1>
          <p className="text-sm text-slate-500 mt-1">Financial intelligence workspace</p>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-2xl">
          {/* Mode toggle */}
          <div className="flex rounded-lg bg-slate-800/60 p-1 mb-6">
            {(['login', 'register'] as Mode[]).map(m => (
              <button
                key={m}
                type="button"
                onClick={() => switchMode(m)}
                className={cn(
                  'flex-1 py-1.5 text-sm font-medium rounded-md transition-colors',
                  mode === m ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-slate-200',
                )}
              >
                {m === 'login' ? 'Sign in' : 'Sign up'}
              </button>
            ))}
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            {mode === 'register' && (
              <Input
                label="Full name"
                type="text"
                placeholder="Jane Doe"
                value={name}
                onChange={e => setName(e.target.value)}
                autoComplete="name"
              />
            )}
            <Input
              label="Email"
              type="email"
              placeholder="you@example.com"
              value={email}
              onChange={e => setEmail(e.target.value)}
              autoComplete="email"
              required
            />
            <Input
              label="Password"
              type="password"
              placeholder="••••••••"
              value={pass}
              onChange={e => setPass(e.target.value)}
              autoComplete={mode === 'login' ? 'current-password' : 'new-password'}
              required
            />

            {error && <Alert message={error} variant="error" onClose={() => setError('')} />}

            <Button type="submit" loading={busy} className="w-full mt-2">
              {mode === 'login' ? 'Sign in' : 'Create account'}
            </Button>
          </form>
        </div>

        <p className="text-center text-xs text-slate-600 mt-6">
          All traffic is encrypted. Tokens stored locally only.
        </p>
      </div>
    </div>
  );
}
