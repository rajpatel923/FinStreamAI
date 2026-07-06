"use client";

import { FormEvent, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { Button, EmptyState, ErrorBanner, Field, JsonBlock, PageShell, Section, TextArea, getErrorMessage } from "@/components/ui";
import { createApiKey, getPreferences, listApiKeys, revokeApiKey, updatePreferences, updateProfile, type ApiKeyResponse, type JsonRecord } from "@/lib/api";
import { getToken } from "@/lib/tokens";

export default function AccountPage() {
  const router = useRouter();
  const [preferences, setPreferences] = useState<JsonRecord | null>(null);
  const [preferenceJson, setPreferenceJson] = useState("{}");
  const [apiKeys, setApiKeys] = useState<ApiKeyResponse[]>([]);
  const [apiKeyName, setApiKeyName] = useState("");
  const [profileName, setProfileName] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(true);

  async function load() {
    setError("");
    setLoading(true);
    try {
      const [pref, keys] = await Promise.all([getPreferences(), listApiKeys()]);
      setPreferences(pref);
      setPreferenceJson(JSON.stringify(pref || {}, null, 2));
      setApiKeys(keys || []);
    } catch (err) {
      setError(getErrorMessage(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!getToken()) router.replace("/login");
    else void load();
  }, [router]);

  async function savePreferences(event: FormEvent) {
    event.preventDefault();
    setError("");
    setMessage("");
    try {
      const payload = JSON.parse(preferenceJson) as JsonRecord;
      const updated = await updatePreferences(payload);
      setPreferences(updated);
      setMessage("Preferences saved.");
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function saveProfile(event: FormEvent) {
    event.preventDefault();
    setError("");
    setMessage("");
    try {
      await updateProfile({ full_name: profileName });
      setMessage("Profile updated.");
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function addKey(event: FormEvent) {
    event.preventDefault();
    setError("");
    setMessage("");
    try {
      await createApiKey(apiKeyName);
      setApiKeyName("");
      await load();
      setMessage("API key created.");
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function removeKey(id: string) {
    setError("");
    setMessage("");
    try {
      await revokeApiKey(id);
      await load();
      setMessage("API key revoked.");
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  return (
    <PageShell title="Account" description="Manage profile details, preferences, and gateway API keys.">
      <ErrorBanner message={error} />
      {message ? <div className="rounded border border-emerald-900 bg-emerald-950 px-3 py-2 text-sm text-emerald-200">{message}</div> : null}
      <div className="grid gap-4 lg:grid-cols-2">
        <Section title="Profile">
          <form onSubmit={saveProfile} className="space-y-3">
            <Field label="Full name" value={profileName} onChange={setProfileName} />
            <Button type="submit">Update profile</Button>
          </form>
        </Section>
        <Section title="Preferences">
          <form onSubmit={savePreferences} className="space-y-3">
            <TextArea label="Preferences JSON" value={preferenceJson} onChange={setPreferenceJson} rows={8} />
            <Button type="submit">Save preferences</Button>
          </form>
        </Section>
      </div>
      <Section title="API Keys">
        <form onSubmit={addKey} className="mb-4 flex gap-2">
          <input
            className="min-w-0 flex-1 rounded border border-gray-700 bg-gray-900 px-3 py-2 text-gray-100"
            value={apiKeyName}
            placeholder="Key name"
            onChange={(event) => setApiKeyName(event.target.value)}
          />
          <Button type="submit" disabled={!apiKeyName.trim()}>
            Create
          </Button>
        </form>
        {loading ? <EmptyState>Loading account data...</EmptyState> : null}
        {!loading && !apiKeys.length ? <EmptyState>No API keys yet.</EmptyState> : null}
        <div className="space-y-2">
          {apiKeys.map((key, index) => {
            const id = String(key.id || key.key_id || key.name || index);
            return (
              <div key={id} className="flex items-center justify-between gap-3 rounded border border-gray-800 p-3">
                <div className="min-w-0 text-sm">
                  <div className="truncate text-gray-100">{String(key.name || key.id || "API key")}</div>
                  <div className="truncate text-gray-500">{String(key.created_at || key.prefix || "")}</div>
                </div>
                <Button variant="danger" onClick={() => removeKey(id)}>
                  Revoke
                </Button>
              </div>
            );
          })}
        </div>
      </Section>
      {preferences ? (
        <Section title="Current preferences">
          <JsonBlock value={preferences} />
        </Section>
      ) : null}
    </PageShell>
  );
}
