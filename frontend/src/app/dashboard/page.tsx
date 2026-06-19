"use client";

import { useState, useCallback } from "react";
import { checkHealth, HealthResult } from "@/lib/api";

const SERVICES = [
  { name: "API Gateway", port: 8005, url: process.env.NEXT_PUBLIC_GATEWAY_URL ?? "http://localhost:8005" },
  { name: "AI Services", port: 8003, url: process.env.NEXT_PUBLIC_AI_URL ?? "http://localhost:8003" },
  { name: "Agent Service", port: 8006, url: process.env.NEXT_PUBLIC_AGENT_URL ?? "http://localhost:8006" },
];

interface ServiceStatus {
  name: string;
  port: number;
  result?: HealthResult;
  loading: boolean;
}

export default function DashboardPage() {
  const [statuses, setStatuses] = useState<ServiceStatus[]>(
    SERVICES.map((s) => ({ name: s.name, port: s.port, loading: false }))
  );

  const runChecks = useCallback(async () => {
    setStatuses(SERVICES.map((s) => ({ name: s.name, port: s.port, loading: true })));
    const results = await Promise.all(SERVICES.map((s) => checkHealth(s.url)));
    setStatuses(
      SERVICES.map((s, i) => ({ name: s.name, port: s.port, result: results[i], loading: false }))
    );
  }, []);

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Service Health</h1>
        <button
          onClick={runChecks}
          className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded text-sm font-medium transition-colors"
        >
          Check All
        </button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {statuses.map((s) => (
          <div
            key={s.name}
            className="bg-gray-900 border border-gray-800 rounded-xl p-6"
          >
            <div className="flex items-start justify-between mb-3">
              <div>
                <p className="font-semibold text-white">{s.name}</p>
                <p className="text-xs text-gray-500 mt-0.5">port {s.port}</p>
              </div>
              <StatusBadge result={s.result} loading={s.loading} />
            </div>
            {s.result && !s.loading && (
              <div className="text-xs text-gray-400 space-y-1 mt-2">
                {s.result.status === "ok" && (
                  <p>Latency: <span className="text-green-400">{s.result.latency_ms} ms</span></p>
                )}
                {s.result.detail && (
                  <p className="text-red-400 truncate" title={s.result.detail}>
                    {s.result.detail}
                  </p>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function StatusBadge({ result, loading }: { result?: HealthResult; loading: boolean }) {
  if (loading) {
    return (
      <span className="text-xs px-2 py-1 bg-gray-700 text-gray-300 rounded-full animate-pulse">
        checking…
      </span>
    );
  }
  if (!result) {
    return (
      <span className="text-xs px-2 py-1 bg-gray-800 text-gray-500 rounded-full">idle</span>
    );
  }
  return result.status === "ok" ? (
    <span className="text-xs px-2 py-1 bg-green-900/50 text-green-400 border border-green-800 rounded-full">
      ok
    </span>
  ) : (
    <span className="text-xs px-2 py-1 bg-red-900/50 text-red-400 border border-red-800 rounded-full">
      error
    </span>
  );
}
