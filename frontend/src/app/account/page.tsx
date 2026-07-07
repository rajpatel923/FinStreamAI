'use client';

import { useState, useEffect, useCallback, FormEvent } from 'react';
import {
  updateProfile, getPreferences, updatePreferences,
  listApiKeys, createApiKey, revokeApiKey,
  type ApiKeyResponse, type UserPreferenceResponse,
} from '@/lib/api';
import {
  PageHeader, Card, CardHeader, CardContent,
  Button, Input, Select, Alert, Badge,
  Table, Thead, Tbody, Tr, Th, Td,
  EmptyState, Spinner, Mono, SectionTitle,
  Tabs, cn,
} from '@/components/ui';

// ── Page ──────────────────────────────────────────────────────────────────────
export default function AccountPage() {
  const [tab, setTab] = useState('profile');

  // ── Profile ───────────────────────────────────────────────────────────────
  const [name,       setName]       = useState('');
  const [profileBusy,setProfileBusy]= useState(false);
  const [profileOk,  setProfileOk]  = useState('');
  const [profileErr, setProfileErr] = useState('');

  const handleProfileUpdate = async (e: FormEvent) => {
    e.preventDefault();
    setProfileBusy(true);
    setProfileOk('');
    setProfileErr('');
    try {
      await updateProfile({ full_name: name });
      setProfileOk('Profile updated.');
    } catch (err: unknown) {
      setProfileErr(err instanceof Error ? err.message : 'Update failed');
    } finally {
      setProfileBusy(false);
    }
  };

  // ── Preferences ───────────────────────────────────────────────────────────
  const [prefs,     setPrefs]     = useState<UserPreferenceResponse | null>(null);
  const [prefsBusy, setPrefsBusy] = useState(false);
  const [prefsOk,   setPrefsOk]   = useState('');
  const [prefsErr,  setPrefsErr]  = useState('');
  const [theme,     setTheme]     = useState('dark');
  const [digest,    setDigest]    = useState('daily');
  const [alerts,    setAlerts]    = useState(true);

  const loadPrefs = useCallback(async () => {
    try {
      const p = await getPreferences() as UserPreferenceResponse;
      setPrefs(p);
      const raw = p as Record<string, unknown>;
      setTheme(String(raw.theme ?? 'dark'));
      setDigest(String(raw.digest_frequency ?? 'daily'));
      setAlerts(raw.notifications_enabled !== false);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { if (tab === 'preferences') loadPrefs(); }, [tab, loadPrefs]);

  const handlePrefsUpdate = async (e: FormEvent) => {
    e.preventDefault();
    setPrefsBusy(true);
    setPrefsOk('');
    setPrefsErr('');
    try {
      await updatePreferences({ theme, digest_frequency: digest, notifications_enabled: alerts });
      setPrefsOk('Preferences saved.');
    } catch (err: unknown) {
      setPrefsErr(err instanceof Error ? err.message : 'Failed to save preferences');
    } finally {
      setPrefsBusy(false); }
  };

  // ── API Keys ──────────────────────────────────────────────────────────────
  const [apiKeys,    setApiKeys]    = useState<ApiKeyResponse[]>([]);
  const [keysLoading,setKeysLoading]= useState(false);
  const [keyName,    setKeyName]    = useState('');
  const [createBusy, setCreateBusy] = useState(false);
  const [newKey,     setNewKey]     = useState<string | null>(null);
  const [keysErr,    setKeysErr]    = useState('');

  const refreshKeys = useCallback(async () => {
    setKeysLoading(true);
    try { setApiKeys(await listApiKeys()); } catch { /* ignore */ }
    setKeysLoading(false);
  }, []);

  useEffect(() => { if (tab === 'apikeys') refreshKeys(); }, [tab, refreshKeys]);

  const handleCreateKey = async (e: FormEvent) => {
    e.preventDefault();
    if (!keyName.trim()) return;
    setCreateBusy(true);
    setNewKey(null);
    setKeysErr('');
    try {
      const res = await createApiKey(keyName) as Record<string, unknown>;
      setKeyName('');
      setNewKey(String(res.key ?? res.raw_key ?? ''));
      await refreshKeys();
    } catch (err: unknown) {
      setKeysErr(err instanceof Error ? err.message : 'Failed to create key');
    } finally {
      setCreateBusy(false);
    }
  };

  const handleRevoke = async (id: string) => {
    try { await revokeApiKey(id); await refreshKeys(); } catch { /* ignore */ }
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div>
      <PageHeader
        title="Account"
        subtitle="Profile, preferences, and API keys"
        action={
          <Tabs
            tabs={[
              { id: 'profile', label: 'Profile' },
              { id: 'preferences', label: 'Preferences' },
              { id: 'apikeys', label: 'API Keys' },
            ]}
            active={tab}
            onSelect={setTab}
          />
        }
      />

      {/* ── PROFILE ──────────────────────────────────────────────────── */}
      {tab === 'profile' && (
        <Card className="max-w-md">
          <CardHeader title="Profile" subtitle="Update your display name" />
          <CardContent>
            <form onSubmit={handleProfileUpdate} className="space-y-4">
              <Input
                label="Full name"
                placeholder="Jane Doe"
                value={name}
                onChange={e => setName(e.target.value)}
                autoComplete="name"
              />
              {profileErr && <Alert message={profileErr} onClose={() => setProfileErr('')} />}
              {profileOk  && <Alert message={profileOk}  variant="success" onClose={() => setProfileOk('')} />}
              <Button type="submit" loading={profileBusy}>Save changes</Button>
            </form>
          </CardContent>
        </Card>
      )}

      {/* ── PREFERENCES ──────────────────────────────────────────────── */}
      {tab === 'preferences' && (
        <Card className="max-w-md">
          <CardHeader title="Preferences" subtitle="Customize notifications and display" />
          <CardContent>
            {!prefs ? (
              <div className="flex items-center justify-center py-8"><Spinner /></div>
            ) : (
              <form onSubmit={handlePrefsUpdate} className="space-y-4">
                <Select label="Theme" value={theme} onChange={e => setTheme(e.target.value)}>
                  <option value="dark">Dark</option>
                  <option value="light">Light</option>
                  <option value="system">System</option>
                </Select>

                <Select label="Digest frequency" value={digest} onChange={e => setDigest(e.target.value)}>
                  <option value="daily">Daily</option>
                  <option value="weekly">Weekly</option>
                  <option value="none">None</option>
                </Select>

                <div>
                  <label className="flex items-center gap-3 cursor-pointer group">
                    <button
                      type="button"
                      role="switch"
                      aria-checked={alerts}
                      onClick={() => setAlerts(v => !v)}
                      className={cn(
                        'relative inline-flex h-5 w-9 flex-shrink-0 rounded-full border-2 border-transparent transition-colors',
                        alerts ? 'bg-blue-600' : 'bg-slate-700',
                      )}
                    >
                      <span
                        className={cn(
                          'pointer-events-none inline-block h-4 w-4 rounded-full bg-white shadow-sm transform transition-transform',
                          alerts ? 'translate-x-4' : 'translate-x-0',
                        )}
                      />
                    </button>
                    <span className="text-sm text-slate-300 group-hover:text-slate-100 transition-colors">
                      Enable price alert notifications
                    </span>
                  </label>
                </div>

                {prefsErr && <Alert message={prefsErr} onClose={() => setPrefsErr('')} />}
                {prefsOk  && <Alert message={prefsOk}  variant="success" onClose={() => setPrefsOk('')} />}
                <Button type="submit" loading={prefsBusy}>Save preferences</Button>
              </form>
            )}
          </CardContent>
        </Card>
      )}

      {/* ── API KEYS ─────────────────────────────────────────────────── */}
      {tab === 'apikeys' && (
        <div className="space-y-4">
          {/* Create */}
          <Card className="max-w-md">
            <CardHeader title="Create API Key" subtitle="Keys use the fsk_ prefix and grant full API access" />
            <CardContent>
              <form onSubmit={handleCreateKey} className="flex gap-3 items-end">
                <Input
                  label="Key name"
                  placeholder="My integration"
                  value={keyName}
                  onChange={e => setKeyName(e.target.value)}
                />
                <Button type="submit" loading={createBusy}>Create</Button>
              </form>
              {keysErr && <div className="mt-3"><Alert message={keysErr} onClose={() => setKeysErr('')} /></div>}

              {newKey && (
                <div className="mt-4 p-3 bg-green-950/40 border border-green-800/50 rounded-xl">
                  <p className="text-xs text-green-400 mb-2 font-medium">
                    Copy this key now — it won&apos;t be shown again.
                  </p>
                  <div className="flex items-center gap-2">
                    <Mono className="flex-1 break-all">{newKey}</Mono>
                    <button
                      onClick={() => navigator.clipboard.writeText(newKey)}
                      className="text-xs text-green-400 hover:text-green-300 border border-green-800/60 rounded px-2 py-1 flex-shrink-0"
                    >
                      Copy
                    </button>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Keys list */}
          <Card>
            <CardHeader
              title="Active Keys"
              subtitle={`${apiKeys.length} key${apiKeys.length !== 1 ? 's' : ''}`}
              action={<Button variant="secondary" size="xs" onClick={refreshKeys} loading={keysLoading}>Refresh</Button>}
            />
            <CardContent className="p-0">
              {keysLoading ? (
                <div className="flex items-center justify-center py-8"><Spinner /></div>
              ) : apiKeys.length === 0 ? (
                <EmptyState message="No API keys — create one above" icon="◌" />
              ) : (
                <Table>
                  <Thead>
                    <Tr>
                      <Th>Name</Th>
                      <Th>Prefix</Th>
                      <Th>Scopes</Th>
                      <Th>Created</Th>
                      <Th>Last used</Th>
                      <Th></Th>
                    </Tr>
                  </Thead>
                  <Tbody>
                    {apiKeys.map(k => {
                      const key = k as Record<string, unknown>;
                      return (
                        <Tr key={String(key.id)}>
                          <Td className="font-medium text-slate-200">{String(key.name)}</Td>
                          <Td><Mono>{String(key.key_prefix ?? key.prefix ?? 'fsk_****')}</Mono></Td>
                          <Td>
                            <div className="flex flex-wrap gap-1">
                              {Array.isArray(key.scopes) && (key.scopes as string[]).length > 0
                                ? (key.scopes as string[]).map(s => <Badge key={s} variant="blue">{s}</Badge>)
                                : <Badge variant="gray">full</Badge>}
                            </div>
                          </Td>
                          <Td className="text-slate-500 text-xs">
                            {key.created_at ? new Date(String(key.created_at)).toLocaleDateString() : '—'}
                          </Td>
                          <Td className="text-slate-500 text-xs">
                            {key.last_used_at ? new Date(String(key.last_used_at)).toLocaleDateString() : 'Never'}
                          </Td>
                          <Td>
                            <Button variant="danger" size="xs" onClick={() => handleRevoke(String(key.id))}>Revoke</Button>
                          </Td>
                        </Tr>
                      );
                    })}
                  </Tbody>
                </Table>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
