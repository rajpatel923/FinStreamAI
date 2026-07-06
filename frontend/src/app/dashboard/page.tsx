"use client";

import { FormEvent, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  checkHealth,
  createExportJob,
  getGatewaySentiment,
  getMarketData,
  getPortfolioAnalytics,
  getRiskAnalytics,
  runBacktest,
  runCustomQuery,
  type JsonRecord,
} from "@/lib/api";
import { getToken } from "@/lib/tokens";
import { Button, ErrorBanner, Field, JsonBlock, PageShell, Section, StatusPill, TextArea, getErrorMessage } from "@/components/ui";

export default function DashboardPage() {
  const router = useRouter();
  const [health, setHealth] = useState<JsonRecord | null>(null);
  const [symbol, setSymbol] = useState("AAPL");
  const [marketData, setMarketData] = useState<unknown>(null);
  const [sentiment, setSentiment] = useState<unknown>(null);
  const [analytics, setAnalytics] = useState<unknown>(null);
  const [customPayload, setCustomPayload] = useState('{"query":"latest market overview","output_format":"json"}');
  const [customResult, setCustomResult] = useState<unknown>(null);
  const [exportPayload, setExportPayload] = useState('{"dataset":"market_data","output_format":"csv","query_params":{}}');
  const [exportResult, setExportResult] = useState<unknown>(null);
  const [backtestPayload, setBacktestPayload] = useState('{"symbol":"AAPL","strategy":"buy_and_hold"}');
  const [backtestResult, setBacktestResult] = useState<unknown>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!getToken()) {
      router.replace("/login");
      return;
    }
    void loadHealth();
    void loadPortfolio();
  }, [router]);

  async function loadHealth() {
    setHealth((await checkHealth()) as unknown as JsonRecord);
  }

  async function loadPortfolio() {
    try {
      setAnalytics(await getPortfolioAnalytics());
    } catch (err) {
      setAnalytics({ error: getErrorMessage(err) });
    }
  }

  async function searchMarket(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      const [market, sent, risk] = await Promise.allSettled([getMarketData(symbol), getGatewaySentiment(symbol), getRiskAnalytics(symbol)]);
      setMarketData(market.status === "fulfilled" ? market.value : { error: getErrorMessage(market.reason) });
      setSentiment(sent.status === "fulfilled" ? sent.value : { error: getErrorMessage(sent.reason) });
      setAnalytics(risk.status === "fulfilled" ? risk.value : { error: getErrorMessage(risk.reason) });
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function submitCustom(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      setCustomResult(await runCustomQuery(JSON.parse(customPayload) as JsonRecord));
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function submitExport(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      setExportResult(await createExportJob(JSON.parse(exportPayload) as JsonRecord));
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function submitBacktest(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      setBacktestResult(await runBacktest(JSON.parse(backtestPayload) as JsonRecord));
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  return (
    <PageShell title="Dashboard" description="Gateway overview for service health, market data, analytics, custom queries, and exports.">
      <ErrorBanner message={error} />
      <Section title="Services" action={<Button variant="secondary" onClick={loadHealth}>Refresh</Button>}>
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {["gateway", "ai", "agent", "dataLake"].map((key) => {
            const entry = health?.[key] as { ok?: boolean; data?: JsonRecord; error?: string } | undefined;
            return (
              <div key={key} className="rounded border border-gray-800 p-3">
                <div className="mb-2 flex items-center justify-between">
                  <span className="capitalize text-gray-200">{key}</span>
                  <StatusPill ok={entry?.ok} label={entry?.ok ? "online" : "check"} />
                </div>
                <div className="truncate text-xs text-gray-500">{entry?.ok ? String(entry.data?.status || "healthy") : entry?.error || "Not checked"}</div>
              </div>
            );
          })}
        </div>
      </Section>

      <div className="grid gap-4 lg:grid-cols-3">
        <Section title="Market lookup">
          <form onSubmit={searchMarket} className="space-y-3">
            <Field label="Symbol" value={symbol} onChange={setSymbol} />
            <Button type="submit">Load</Button>
          </form>
        </Section>
        <Section title="Market data">
          <JsonBlock value={marketData || "Run a lookup."} />
        </Section>
        <Section title="Sentiment / risk">
          <JsonBlock value={{ sentiment, analytics }} />
        </Section>
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <Section title="Custom query">
          <form onSubmit={submitCustom} className="space-y-3">
            <TextArea label="Payload JSON" value={customPayload} onChange={setCustomPayload} rows={6} />
            <Button type="submit">Run</Button>
          </form>
          <div className="mt-3">
            <JsonBlock value={customResult || "No result yet."} />
          </div>
        </Section>
        <Section title="Export">
          <form onSubmit={submitExport} className="space-y-3">
            <TextArea label="Export JSON" value={exportPayload} onChange={setExportPayload} rows={6} />
            <Button type="submit">Create export</Button>
          </form>
          <div className="mt-3">
            <JsonBlock value={exportResult || "No export job yet."} />
          </div>
        </Section>
        <Section title="Backtest">
          <form onSubmit={submitBacktest} className="space-y-3">
            <TextArea label="Backtest JSON" value={backtestPayload} onChange={setBacktestPayload} rows={6} />
            <Button type="submit">Run backtest</Button>
          </form>
          <div className="mt-3">
            <JsonBlock value={backtestResult || "No backtest result yet."} />
          </div>
        </Section>
      </div>
    </PageShell>
  );
}
