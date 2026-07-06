"use client";

import { FormEvent, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { Button, ErrorBanner, Field, JsonBlock, PageShell, Section, TextArea, getErrorMessage } from "@/components/ui";
import { lakeCacheStats, lakeGraphAffected, lakeGraphCompany, lakeGraphPagerank, lakeQuality, lakeQuery, type JsonRecord } from "@/lib/api";
import { getToken } from "@/lib/tokens";

export default function DataPage() {
  const router = useRouter();
  const [quality, setQuality] = useState<unknown>(null);
  const [cache, setCache] = useState<unknown>(null);
  const [graph, setGraph] = useState<unknown>(null);
  const [queryResult, setQueryResult] = useState<unknown>(null);
  const [symbol, setSymbol] = useState("AAPL");
  const [eventId, setEventId] = useState("");
  const [queryJson, setQueryJson] = useState('{"sql":"select * from gold_market_data limit 10"}');
  const [error, setError] = useState("");

  async function loadOverview() {
    setError("");
    try {
      const [q, c, p] = await Promise.allSettled([lakeQuality(), lakeCacheStats(), lakeGraphPagerank()]);
      setQuality(q.status === "fulfilled" ? q.value : { error: getErrorMessage(q.reason) });
      setCache(c.status === "fulfilled" ? c.value : { error: getErrorMessage(c.reason) });
      setGraph(p.status === "fulfilled" ? p.value : { error: getErrorMessage(p.reason) });
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  useEffect(() => {
    if (!getToken()) router.replace("/login");
    else void loadOverview();
  }, [router]);

  async function runCompany(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      setGraph(await lakeGraphCompany(symbol));
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function runAffected(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      setGraph(await lakeGraphAffected(eventId));
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function runQuery(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      setQueryResult(await lakeQuery(JSON.parse(queryJson) as JsonRecord));
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  return (
    <PageShell title="Data" description="Read data-lake quality, query, graph, and cache views.">
      <ErrorBanner message={error} />
      <div className="grid gap-4 lg:grid-cols-2">
        <Section title="Lake quality">
          <JsonBlock value={quality || "Loading..."} />
        </Section>
        <Section title="Cache stats">
          <JsonBlock value={cache || "Loading..."} />
        </Section>
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <Section title="Graph lookup">
          <form onSubmit={runCompany} className="mb-3 flex gap-2">
            <input className="min-w-0 flex-1 rounded border border-gray-700 bg-gray-900 px-3 py-2" value={symbol} onChange={(e) => setSymbol(e.target.value)} />
            <Button type="submit">Company</Button>
          </form>
          <form onSubmit={runAffected} className="mb-3 flex gap-2">
            <input className="min-w-0 flex-1 rounded border border-gray-700 bg-gray-900 px-3 py-2" value={eventId} placeholder="Event ID" onChange={(e) => setEventId(e.target.value)} />
            <Button type="submit" disabled={!eventId.trim()}>
              Affected
            </Button>
          </form>
          <JsonBlock value={graph || "No graph result yet."} />
        </Section>
        <Section title="Query">
          <form onSubmit={runQuery} className="space-y-3">
            <TextArea label="Query payload JSON" value={queryJson} onChange={setQueryJson} rows={7} />
            <Button type="submit">Run query</Button>
          </form>
          <div className="mt-3">
            <JsonBlock value={queryResult || "No query result yet."} />
          </div>
        </Section>
      </div>
    </PageShell>
  );
}
