"use client";

import { useState, useEffect } from "react";
import {
  listWatchlist, addWatchlist, removeWatchlist,
  listAlerts, createAlert, deleteAlert,
  WatchlistItemResponse, AlertResponse,
} from "@/lib/api";

export default function WatchlistPage() {
  const [watchlist, setWatchlist] = useState<WatchlistItemResponse[]>([]);
  const [alerts, setAlerts] = useState<AlertResponse[]>([]);
  const [newSymbol, setNewSymbol] = useState("");
  const [alertSymbol, setAlertSymbol] = useState("");
  const [alertCondition, setAlertCondition] = useState("price_above");
  const [alertThreshold, setAlertThreshold] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    void loadAll();
  }, []);

  async function loadAll() {
    try {
      const [w, a] = await Promise.all([listWatchlist(), listAlerts()]);
      setWatchlist(w);
      setAlerts(a);
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleAddSymbol(e: React.FormEvent) {
    e.preventDefault();
    if (!newSymbol.trim()) return;
    setLoading(true);
    try {
      const item = await addWatchlist(newSymbol.trim().toUpperCase());
      setWatchlist((prev) => [...prev, item]);
      setNewSymbol("");
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  async function handleRemoveSymbol(symbol: string) {
    try {
      await removeWatchlist(symbol);
      setWatchlist((prev) => prev.filter((w) => w.symbol !== symbol));
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleCreateAlert(e: React.FormEvent) {
    e.preventDefault();
    if (!alertSymbol.trim() || !alertThreshold) return;
    setLoading(true);
    try {
      const alert = await createAlert(
        alertSymbol.trim().toUpperCase(),
        alertCondition,
        parseFloat(alertThreshold)
      );
      setAlerts((prev) => [...prev, alert]);
      setAlertSymbol("");
      setAlertThreshold("");
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  async function handleDeleteAlert(id: string) {
    try {
      await deleteAlert(id);
      setAlerts((prev) => prev.filter((a) => a.id !== id));
    } catch (e) {
      setError(String(e));
    }
  }

  return (
    <div className="space-y-8 max-w-2xl">
      <h1 className="text-2xl font-bold">Watchlist & Alerts</h1>

      {error && (
        <div className="text-xs text-red-400 bg-red-900/20 border border-red-800 rounded px-3 py-2">
          {error}
          <button onClick={() => setError("")} className="ml-2 underline">dismiss</button>
        </div>
      )}

      {/* Watchlist */}
      <section className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 className="font-semibold text-lg mb-4">Watchlist</h2>
        <form onSubmit={handleAddSymbol} className="flex gap-2 mb-4">
          <input
            value={newSymbol}
            onChange={(e) => setNewSymbol(e.target.value)}
            placeholder="AAPL"
            className="flex-1 bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-indigo-500 uppercase"
          />
          <button
            type="submit"
            disabled={loading}
            className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-sm font-medium transition-colors"
          >
            Add
          </button>
        </form>
        <ul className="space-y-2">
          {watchlist.length === 0 && (
            <li className="text-sm text-gray-500">No symbols yet.</li>
          )}
          {watchlist.map((w) => (
            <li
              key={w.id}
              className="flex items-center justify-between bg-gray-800 rounded px-3 py-2"
            >
              <span className="font-mono font-bold text-indigo-300">{w.symbol}</span>
              <span className="text-xs text-gray-500 mr-auto ml-3">
                {new Date(w.created_at).toLocaleDateString()}
              </span>
              <button
                onClick={() => handleRemoveSymbol(w.symbol)}
                className="text-xs text-red-400 hover:text-red-300 transition-colors"
              >
                Remove
              </button>
            </li>
          ))}
        </ul>
      </section>

      {/* Alerts */}
      <section className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 className="font-semibold text-lg mb-4">Alerts</h2>
        <form onSubmit={handleCreateAlert} className="grid grid-cols-2 gap-2 mb-4">
          <input
            value={alertSymbol}
            onChange={(e) => setAlertSymbol(e.target.value)}
            placeholder="Symbol (AAPL)"
            className="bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-indigo-500 uppercase"
          />
          <select
            value={alertCondition}
            onChange={(e) => setAlertCondition(e.target.value)}
            className="bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-white focus:outline-none focus:border-indigo-500"
          >
            <option value="price_above">Price Above</option>
            <option value="price_below">Price Below</option>
            <option value="volume_spike">Volume Spike</option>
            <option value="sentiment_drop">Sentiment Drop</option>
          </select>
          <input
            type="number"
            value={alertThreshold}
            onChange={(e) => setAlertThreshold(e.target.value)}
            placeholder="Threshold (e.g. 200)"
            step="any"
            className="bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-indigo-500"
          />
          <button
            type="submit"
            disabled={loading}
            className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-sm font-medium transition-colors"
          >
            Create Alert
          </button>
        </form>
        <ul className="space-y-2">
          {alerts.length === 0 && (
            <li className="text-sm text-gray-500">No alerts yet.</li>
          )}
          {alerts.map((a) => (
            <li
              key={a.id}
              className="flex items-center justify-between bg-gray-800 rounded px-3 py-2 text-sm"
            >
              <span className="font-mono font-bold text-indigo-300 w-16">{a.symbol ?? "—"}</span>
              <span className="text-gray-400 flex-1 truncate mx-2">{a.name}</span>
              <span
                className={`text-xs px-2 py-0.5 rounded-full mr-3 shrink-0 ${
                  a.is_active
                    ? "bg-green-900/50 text-green-400"
                    : "bg-gray-700 text-gray-400"
                }`}
              >
                {a.is_active ? "active" : "inactive"}
              </span>
              <button
                onClick={() => handleDeleteAlert(a.id)}
                className="text-xs text-red-400 hover:text-red-300 transition-colors shrink-0"
              >
                Delete
              </button>
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}
