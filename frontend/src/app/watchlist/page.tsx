"use client";

import { FormEvent, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  addWatchlist,
  createAlert,
  deleteAlert,
  getMarketData,
  listAlerts,
  listWatchlist,
  removeWatchlist,
  type AlertResponse,
  type WatchlistItemResponse,
} from "@/lib/api";
import { getToken } from "@/lib/tokens";
import { Button, EmptyState, ErrorBanner, Field, JsonBlock, PageShell, Section, getErrorMessage } from "@/components/ui";

export default function WatchlistPage() {
  const router = useRouter();
  const [items, setItems] = useState<WatchlistItemResponse[]>([]);
  const [alerts, setAlerts] = useState<AlertResponse[]>([]);
  const [symbol, setSymbol] = useState("");
  const [notes, setNotes] = useState("");
  const [alertSymbol, setAlertSymbol] = useState("");
  const [threshold, setThreshold] = useState("");
  const [direction, setDirection] = useState("above");
  const [preview, setPreview] = useState<unknown>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  async function load() {
    setError("");
    setLoading(true);
    try {
      const [watchlist, activeAlerts] = await Promise.all([listWatchlist(), listAlerts()]);
      setItems(watchlist || []);
      setAlerts(activeAlerts || []);
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

  async function addItem(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      await addWatchlist(symbol, notes);
      setSymbol("");
      setNotes("");
      await load();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function removeItem(symbolToRemove: string) {
    setError("");
    try {
      await removeWatchlist(symbolToRemove);
      await load();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function addAlert(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      await createAlert(alertSymbol, Number(threshold), direction);
      setAlertSymbol("");
      setThreshold("");
      await load();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function removeAlert(id?: string) {
    if (!id) return;
    setError("");
    try {
      await deleteAlert(id);
      await load();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function previewSymbol(symbolToPreview: string) {
    setError("");
    try {
      setPreview(await getMarketData(symbolToPreview));
    } catch (err) {
      setPreview({ error: getErrorMessage(err) });
    }
  }

  return (
    <PageShell title="Watchlist" description="Track symbols, create price alerts, and preview gateway market data.">
      <ErrorBanner message={error} />
      <div className="grid gap-4 lg:grid-cols-2">
        <Section title="Add symbol">
          <form onSubmit={addItem} className="space-y-3">
            <Field label="Symbol" value={symbol} onChange={setSymbol} required />
            <Field label="Notes" value={notes} onChange={setNotes} />
            <Button type="submit" disabled={!symbol.trim()}>
              Add to watchlist
            </Button>
          </form>
        </Section>
        <Section title="Create alert">
          <form onSubmit={addAlert} className="grid gap-3 sm:grid-cols-3">
            <Field label="Symbol" value={alertSymbol} onChange={setAlertSymbol} required />
            <Field label="Threshold" type="number" value={threshold} onChange={setThreshold} required />
            <label className="block text-sm text-gray-300">
              <span className="mb-1 block">Direction</span>
              <select
                className="w-full rounded border border-gray-700 bg-gray-900 px-3 py-2 text-gray-100"
                value={direction}
                onChange={(event) => setDirection(event.target.value)}
              >
                <option value="above">Above</option>
                <option value="below">Below</option>
              </select>
            </label>
            <div className="sm:col-span-3">
              <Button type="submit" disabled={!alertSymbol.trim() || !threshold}>
                Create alert
              </Button>
            </div>
          </form>
        </Section>
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <Section title="Symbols">
          {loading ? <EmptyState>Loading watchlist...</EmptyState> : null}
          {!loading && !items.length ? <EmptyState>No symbols yet.</EmptyState> : null}
          <div className="space-y-2">
            {items.map((item) => (
              <div key={item.id || item.symbol} className="rounded border border-gray-800 p-3">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="font-medium">{item.symbol}</div>
                    <div className="text-xs text-gray-500">{item.notes || item.name || "No notes"}</div>
                  </div>
                  <div className="flex gap-2">
                    <Button variant="secondary" onClick={() => previewSymbol(item.symbol)}>
                      Preview
                    </Button>
                    <Button variant="danger" onClick={() => removeItem(item.symbol)}>
                      Remove
                    </Button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Section>
        <Section title="Alerts">
          {!alerts.length ? <EmptyState>No alerts yet.</EmptyState> : null}
          <div className="space-y-2">
            {alerts.map((alert, index) => (
              <div key={alert.id || `${alert.symbol}-${index}`} className="flex items-center justify-between gap-3 rounded border border-gray-800 p-3">
                <div className="text-sm">
                  <div>{alert.symbol}</div>
                  <div className="text-xs text-gray-500">
                    {alert.direction || "direction"} {alert.threshold ?? "n/a"} {alert.status || ""}
                  </div>
                </div>
                <Button variant="danger" onClick={() => removeAlert(alert.id)}>
                  Delete
                </Button>
              </div>
            ))}
          </div>
        </Section>
        <Section title="Market preview">
          <JsonBlock value={preview || "Select Preview for a watchlist symbol."} />
        </Section>
      </div>
    </PageShell>
  );
}
