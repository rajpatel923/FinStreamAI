"use client";

import { useState } from "react";
import { analyzeSentiment, getPrediction, getRiskMetrics, SentimentResult, PredictionResult, RiskMetrics } from "@/lib/api";

type Tab = "sentiment" | "prediction" | "risk";

export default function AIPage() {
  const [tab, setTab] = useState<Tab>("sentiment");

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold mb-6">AI Services</h1>
      <div className="flex border border-gray-700 rounded-lg overflow-hidden mb-6">
        {(["sentiment", "prediction", "risk"] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`flex-1 py-2 text-sm font-medium transition-colors capitalize ${
              tab === t ? "bg-indigo-600 text-white" : "text-gray-400 hover:bg-gray-800"
            }`}
          >
            {t}
          </button>
        ))}
      </div>
      {tab === "sentiment" && <SentimentTab />}
      {tab === "prediction" && <PredictionTab />}
      {tab === "risk" && <RiskTab />}
    </div>
  );
}

function SentimentTab() {
  const [text, setText] = useState("");
  const [result, setResult] = useState<SentimentResult | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!text.trim()) return;
    setLoading(true);
    setError("");
    setResult(null);
    try {
      setResult(await analyzeSentiment(text));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 space-y-4">
      <h2 className="font-semibold text-gray-300">Sentiment Analyzer</h2>
      <form onSubmit={handleSubmit} className="space-y-3">
        <textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          rows={4}
          placeholder="Apple earnings beat expectations for Q4..."
          className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-indigo-500 resize-none"
        />
        <button
          type="submit"
          disabled={loading}
          className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-sm font-medium transition-colors"
        >
          {loading ? "Analyzing…" : "Analyze"}
        </button>
      </form>
      {error && <ErrorBox message={error} />}
      {result && (
        <div className="bg-gray-800 rounded-lg p-4 space-y-2">
          <Row label="Label">
            <span className={`font-semibold ${
              result.label === "positive" ? "text-green-400" :
              result.label === "negative" ? "text-red-400" : "text-yellow-400"
            }`}>{result.label}</span>
          </Row>
          <Row label="Confidence">
            <span className="text-white">{(result.score * 100).toFixed(1)}%</span>
          </Row>
          <Row label="Sentiment Score">
            <span className="text-white">{result.sentiment_score.toFixed(4)}</span>
          </Row>
        </div>
      )}
    </div>
  );
}

function PredictionTab() {
  const [symbol, setSymbol] = useState("");
  const [result, setResult] = useState<PredictionResult | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!symbol.trim()) return;
    setLoading(true);
    setError("");
    setResult(null);
    try {
      setResult(await getPrediction(symbol.trim().toUpperCase()));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 space-y-4">
      <h2 className="font-semibold text-gray-300">Price Direction Prediction</h2>
      <form onSubmit={handleSubmit} className="flex gap-2">
        <input
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          placeholder="AAPL"
          className="flex-1 bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-indigo-500 uppercase"
        />
        <button
          type="submit"
          disabled={loading}
          className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-sm font-medium transition-colors"
        >
          {loading ? "…" : "Predict"}
        </button>
      </form>
      {error && <ErrorBox message={error} />}
      {result && (
        <div className="bg-gray-800 rounded-lg p-4 space-y-2">
          <Row label="Symbol"><span className="font-mono font-bold text-indigo-300">{result.symbol}</span></Row>
          <Row label="Direction">
            <span className={`font-semibold ${
              result.direction === "up" ? "text-green-400" :
              result.direction === "down" ? "text-red-400" : "text-yellow-400"
            }`}>{result.direction}</span>
          </Row>
          <Row label="Probability">
            <span className="text-white">{(result.probability * 100).toFixed(1)}%</span>
          </Row>
        </div>
      )}
    </div>
  );
}

function RiskTab() {
  const [symbol, setSymbol] = useState("");
  const [result, setResult] = useState<RiskMetrics | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!symbol.trim()) return;
    setLoading(true);
    setError("");
    setResult(null);
    try {
      setResult(await getRiskMetrics(symbol.trim().toUpperCase()));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 space-y-4">
      <h2 className="font-semibold text-gray-300">Risk Metrics</h2>
      <form onSubmit={handleSubmit} className="flex gap-2">
        <input
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          placeholder="AAPL"
          className="flex-1 bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-indigo-500 uppercase"
        />
        <button
          type="submit"
          disabled={loading}
          className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-sm font-medium transition-colors"
        >
          {loading ? "…" : "Get Metrics"}
        </button>
      </form>
      {error && <ErrorBox message={error} />}
      {result && (
        <div className="bg-gray-800 rounded-lg p-4 space-y-2">
          <Row label="Symbol"><span className="font-mono font-bold text-indigo-300">{result.symbol}</span></Row>
          {result.error ? (
            <p className="text-xs text-yellow-400">
              {result.error === "insufficient_data"
                ? `Insufficient price history (${result.n_observations ?? 0} observations — need ≥20).`
                : result.error}
            </p>
          ) : (
            <>
              <Row label="VaR 95%"><span className="text-white">{result.var_95?.toFixed(4)}</span></Row>
              <Row label="CVaR 95%"><span className="text-white">{result.cvar_95?.toFixed(4)}</span></Row>
              <Row label="Sharpe Ratio"><span className="text-white">{result.sharpe_ratio?.toFixed(4)}</span></Row>
              <Row label="Max Drawdown">
                <span className="text-red-400">{((result.max_drawdown ?? 0) * 100).toFixed(2)}%</span>
              </Row>
            </>
          )}
        </div>
      )}
    </div>
  );
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex justify-between items-center text-sm">
      <span className="text-gray-400">{label}</span>
      {children}
    </div>
  );
}

function ErrorBox({ message }: { message: string }) {
  return (
    <p className="text-xs text-red-400 bg-red-900/20 border border-red-800 rounded px-3 py-2">
      {message}
    </p>
  );
}
