"use client";

import { FormEvent, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  analyzeSentiment,
  calculateRisk,
  extractEvents,
  getGatewaySentiment,
  getPrediction,
  getPredictionSignals,
  getRiskMetrics,
  type JsonRecord,
} from "@/lib/api";
import { getToken } from "@/lib/tokens";
import { Button, ErrorBanner, Field, JsonBlock, PageShell, Section, TextArea, getErrorMessage } from "@/components/ui";

export default function AiPage() {
  const router = useRouter();
  const [symbol, setSymbol] = useState("AAPL");
  const [text, setText] = useState("Apple reported stronger than expected revenue and improving margins.");
  const [portfolioJson, setPortfolioJson] = useState('{"positions":[{"symbol":"AAPL","quantity":10,"price":190}]}');
  const [sentiment, setSentiment] = useState<unknown>(null);
  const [prediction, setPrediction] = useState<unknown>(null);
  const [signals, setSignals] = useState<unknown>(null);
  const [risk, setRisk] = useState<unknown>(null);
  const [events, setEvents] = useState<unknown>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!getToken()) router.replace("/login");
  }, [router]);

  async function runSentiment(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      const [aiSentiment, gatewaySentiment] = await Promise.allSettled([analyzeSentiment(text, symbol), getGatewaySentiment(symbol)]);
      setSentiment({
        ai: aiSentiment.status === "fulfilled" ? aiSentiment.value : { error: getErrorMessage(aiSentiment.reason) },
        gateway: gatewaySentiment.status === "fulfilled" ? gatewaySentiment.value : { error: getErrorMessage(gatewaySentiment.reason) },
      });
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function runPrediction(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      const [pricePrediction, predictionSignals] = await Promise.allSettled([getPrediction(symbol), getPredictionSignals(symbol)]);
      setPrediction(pricePrediction.status === "fulfilled" ? pricePrediction.value : { error: getErrorMessage(pricePrediction.reason) });
      setSignals(predictionSignals.status === "fulfilled" ? predictionSignals.value : { error: getErrorMessage(predictionSignals.reason) });
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function runRisk(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      const [metrics, calculated] = await Promise.allSettled([getRiskMetrics(symbol), calculateRisk(JSON.parse(portfolioJson) as JsonRecord)]);
      setRisk({
        metrics: metrics.status === "fulfilled" ? metrics.value : { error: getErrorMessage(metrics.reason) },
        calculated: calculated.status === "fulfilled" ? calculated.value : { error: getErrorMessage(calculated.reason) },
      });
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function runEvents(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      setEvents(await extractEvents(text));
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  return (
    <PageShell title="AI" description="Use AI-service sentiment, prediction, risk, and event endpoints.">
      <ErrorBanner message={error} />
      <div className="grid gap-4 lg:grid-cols-2">
        <Section title="Inputs">
          <div className="space-y-3">
            <Field label="Symbol" value={symbol} onChange={setSymbol} />
            <TextArea label="Text" value={text} onChange={setText} rows={5} />
            <TextArea label="Portfolio risk JSON" value={portfolioJson} onChange={setPortfolioJson} rows={5} />
          </div>
        </Section>
        <Section title="Actions">
          <div className="flex flex-wrap gap-2">
            <Button onClick={() => void runSentiment(new Event("submit") as unknown as FormEvent)}>Analyze sentiment</Button>
            <Button onClick={() => void runPrediction(new Event("submit") as unknown as FormEvent)}>Predict</Button>
            <Button onClick={() => void runRisk(new Event("submit") as unknown as FormEvent)}>Risk</Button>
            <Button onClick={() => void runEvents(new Event("submit") as unknown as FormEvent)}>Extract events</Button>
          </div>
        </Section>
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <Section title="Sentiment">
          <form onSubmit={runSentiment} className="mb-3">
            <Button type="submit">Run sentiment</Button>
          </form>
          <JsonBlock value={sentiment || "No sentiment result yet."} />
        </Section>
        <Section title="Prediction">
          <form onSubmit={runPrediction} className="mb-3">
            <Button type="submit">Run prediction</Button>
          </form>
          <JsonBlock value={{ prediction, signals }} />
        </Section>
        <Section title="Risk">
          <form onSubmit={runRisk} className="mb-3">
            <Button type="submit">Run risk</Button>
          </form>
          <JsonBlock value={risk || "No risk result yet."} />
        </Section>
        <Section title="Events">
          <form onSubmit={runEvents} className="mb-3">
            <Button type="submit">Extract events</Button>
          </form>
          <JsonBlock value={events || "No event extraction result yet."} />
        </Section>
      </div>
    </PageShell>
  );
}
