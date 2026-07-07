'use client';

import { useState, FormEvent } from 'react';
import {
  analyzeSentiment, getPredictionSignals, getRiskMetrics,
  calculateRisk, extractEvents,
} from '@/lib/api';
import {
  PageHeader, Card, CardHeader, CardContent, StatCard,
  Button, Input, Textarea, Alert, Badge, ScoreBar,
  Table, Thead, Tbody, Tr, Th, Td,
  EmptyState, Spinner, Tabs, cn, fmt, fmtPct,
} from '@/components/ui';

// ── Types ─────────────────────────────────────────────────────────────────────
interface SentimentResult {
  label: string; score: number;
  entities?: { text: string; label: string }[];
}
interface SignalResult {
  symbol?: string; signals?: unknown[];
  prediction?: unknown; confidence?: number;
}
interface RiskResult {
  var_95?: number; cvar_95?: number; sharpe_ratio?: number;
  max_drawdown?: number; annualized_return?: number; volatility?: number;
  [k: string]: unknown;
}
interface EventResult { events?: unknown[]; raw?: string }

function sentimentBadge(label: string) {
  const l = label.toLowerCase();
  if (l.includes('pos')) return <Badge variant="green">Positive</Badge>;
  if (l.includes('neg')) return <Badge variant="red">Negative</Badge>;
  return <Badge variant="yellow">Neutral</Badge>;
}

function riskColor(val: number, higherIsBetter: boolean): 'green' | 'red' | 'yellow' {
  if (higherIsBetter) return val > 1 ? 'green' : val > 0 ? 'yellow' : 'red';
  return val < 0.05 ? 'green' : val < 0.15 ? 'yellow' : 'red';
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function AIPage() {
  const [tab, setTab] = useState('sentiment');

  // ── Sentiment ─────────────────────────────────────────────────────────────
  const [sentText,   setSentText]   = useState('');
  const [sentResult, setSentResult] = useState<SentimentResult | null>(null);
  const [sentBusy,   setSentBusy]   = useState(false);
  const [sentError,  setSentError]  = useState('');

  const handleSentiment = async (e: FormEvent) => {
    e.preventDefault();
    if (!sentText.trim()) return;
    setSentBusy(true);
    setSentError('');
    setSentResult(null);
    try {
      const res = await analyzeSentiment(sentText) as Record<string, unknown>;
      setSentResult({
        label:    String(res.label ?? res.sentiment_label ?? 'neutral'),
        score:    Number(res.score ?? res.sentiment_score ?? 0),
        entities: (res.entities as { text: string; label: string }[]) ?? [],
      });
    } catch (e: unknown) {
      setSentError(e instanceof Error ? e.message : 'Sentiment analysis failed');
    } finally {
      setSentBusy(false);
    }
  };

  // ── Prediction ────────────────────────────────────────────────────────────
  const [predSymbol, setPredSymbol] = useState('');
  const [predResult, setPredResult] = useState<SignalResult | null>(null);
  const [predBusy,   setPredBusy]   = useState(false);
  const [predError,  setPredError]  = useState('');

  const handlePredict = async (e: FormEvent) => {
    e.preventDefault();
    if (!predSymbol.trim()) return;
    setPredBusy(true);
    setPredError('');
    setPredResult(null);
    try {
      const res = await getPredictionSignals(predSymbol.toUpperCase()) as Record<string, unknown>;
      setPredResult(res as SignalResult);
    } catch (e: unknown) {
      setPredError(e instanceof Error ? e.message : 'Prediction failed');
    } finally {
      setPredBusy(false);
    }
  };

  // ── Risk ──────────────────────────────────────────────────────────────────
  const [riskSymbol, setRiskSymbol] = useState('');
  const [riskResult, setRiskResult] = useState<RiskResult | null>(null);
  const [riskBusy,   setRiskBusy]   = useState(false);
  const [riskError,  setRiskError]  = useState('');
  const [riskMode,   setRiskMode]   = useState<'symbol' | 'portfolio'>('symbol');

  const handleRisk = async (e: FormEvent) => {
    e.preventDefault();
    setRiskBusy(true);
    setRiskError('');
    setRiskResult(null);
    try {
      const res = riskMode === 'portfolio'
        ? await calculateRisk({ symbols: riskSymbol.split(',').map(s => s.trim().toUpperCase()) })
        : await getRiskMetrics(riskSymbol.toUpperCase());
      setRiskResult(res as RiskResult);
    } catch (e: unknown) {
      setRiskError(e instanceof Error ? e.message : 'Risk calculation failed');
    } finally {
      setRiskBusy(false);
    }
  };

  // ── Events ────────────────────────────────────────────────────────────────
  const [evtText,   setEvtText]   = useState('');
  const [evtResult, setEvtResult] = useState<EventResult | null>(null);
  const [evtBusy,   setEvtBusy]   = useState(false);
  const [evtError,  setEvtError]  = useState('');

  const handleEvents = async (e: FormEvent) => {
    e.preventDefault();
    if (!evtText.trim()) return;
    setEvtBusy(true);
    setEvtError('');
    setEvtResult(null);
    try {
      const res = await extractEvents(evtText) as Record<string, unknown>;
      setEvtResult(res as EventResult);
    } catch (e: unknown) {
      setEvtError(e instanceof Error ? e.message : 'Event extraction failed');
    } finally {
      setEvtBusy(false);
    }
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div>
      <PageHeader
        title="AI Analytics"
        subtitle="Sentiment, prediction, risk, and event extraction"
        action={
          <Tabs
            tabs={[
              { id: 'sentiment', label: 'Sentiment' },
              { id: 'prediction', label: 'Prediction' },
              { id: 'risk', label: 'Risk' },
              { id: 'events', label: 'Events' },
            ]}
            active={tab}
            onSelect={setTab}
          />
        }
      />

      {/* ── SENTIMENT ────────────────────────────────────────────────── */}
      {tab === 'sentiment' && (
        <div className="space-y-4">
          <Card>
            <CardHeader title="Sentiment Analysis" subtitle="FinBERT model — financial text classification" />
            <CardContent>
              <form onSubmit={handleSentiment} className="space-y-3">
                <Textarea
                  label="Financial text"
                  rows={4}
                  placeholder="Paste a news article, tweet, or earnings call excerpt…"
                  value={sentText}
                  onChange={e => setSentText(e.target.value)}
                />
                <Button type="submit" loading={sentBusy}>Analyze</Button>
              </form>
              {sentError && <div className="mt-3"><Alert message={sentError} onClose={() => setSentError('')} /></div>}
            </CardContent>
          </Card>

          {sentBusy && (
            <div className="flex items-center justify-center py-8"><Spinner size="lg" /></div>
          )}

          {sentResult && (
            <Card>
              <CardHeader title="Sentiment Result" />
              <CardContent className="space-y-4">
                <div className="flex items-center gap-4">
                  {sentimentBadge(sentResult.label)}
                  <span className="text-sm text-slate-400">Label: <span className="text-slate-200">{sentResult.label}</span></span>
                </div>
                <ScoreBar value={sentResult.score} label="Sentiment Score" />

                {sentResult.entities && sentResult.entities.length > 0 && (
                  <div>
                    <p className="text-xs text-slate-500 mb-2 uppercase tracking-wider">Named Entities</p>
                    <div className="flex flex-wrap gap-2">
                      {sentResult.entities.map((ent, i) => (
                        <div key={i} className="flex items-center gap-1.5 bg-slate-800 rounded-lg px-2.5 py-1">
                          <span className="text-slate-200 text-xs">{ent.text}</span>
                          <Badge variant="purple">{ent.label}</Badge>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      )}

      {/* ── PREDICTION ───────────────────────────────────────────────── */}
      {tab === 'prediction' && (
        <div className="space-y-4">
          <Card>
            <CardHeader title="Price Prediction" subtitle="XGBoost model — signal generation from technical indicators" />
            <CardContent>
              <form onSubmit={handlePredict} className="flex gap-3 items-end">
                <Input
                  label="Symbol"
                  placeholder="AAPL"
                  value={predSymbol}
                  onChange={e => setPredSymbol(e.target.value.toUpperCase())}
                  className="w-32"
                />
                <Button type="submit" loading={predBusy}>Get Signals</Button>
              </form>
              {predError && <div className="mt-3"><Alert message={predError} onClose={() => setPredError('')} /></div>}
            </CardContent>
          </Card>

          {predBusy && (
            <div className="flex items-center justify-center py-8"><Spinner size="lg" /></div>
          )}

          {predResult && (
            <Card>
              <CardHeader title={`Prediction — ${predResult.symbol ?? predSymbol}`} />
              <CardContent>
                {predResult.confidence !== undefined && (
                  <div className="mb-4">
                    <ScoreBar value={predResult.confidence} min={0} max={1} label="Confidence" />
                  </div>
                )}
                {predResult.signals && Array.isArray(predResult.signals) && predResult.signals.length > 0 ? (
                  <Table>
                    <Thead>
                      <Tr>
                        {Object.keys(predResult.signals[0] as Record<string, unknown>).map(k => (
                          <Th key={k}>{k}</Th>
                        ))}
                      </Tr>
                    </Thead>
                    <Tbody>
                      {predResult.signals.map((row, i) => (
                        <Tr key={i}>
                          {Object.entries(row as Record<string, unknown>).map(([k, v]) => (
                            <Td key={k} className="tabular-nums">
                              {typeof v === 'number' ? fmt(v) : v == null ? '—' : String(v)}
                            </Td>
                          ))}
                        </Tr>
                      ))}
                    </Tbody>
                  </Table>
                ) : (
                  <pre className="text-xs text-slate-400 bg-slate-950 rounded-lg p-4 overflow-auto max-h-64">
                    {JSON.stringify(predResult, null, 2)}
                  </pre>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      )}

      {/* ── RISK ─────────────────────────────────────────────────────── */}
      {tab === 'risk' && (
        <div className="space-y-4">
          <Card>
            <CardHeader title="Risk Metrics" subtitle="VaR, CVaR, Sharpe, Max Drawdown (empyrical)" />
            <CardContent>
              <div className="flex gap-2 mb-4">
                {(['symbol', 'portfolio'] as const).map(m => (
                  <button
                    key={m}
                    onClick={() => setRiskMode(m)}
                    className={cn(
                      'px-3 py-1.5 rounded-lg text-sm font-medium transition-colors capitalize',
                      riskMode === m ? 'bg-blue-600 text-white' : 'text-slate-400 hover:bg-slate-800',
                    )}
                  >
                    {m}
                  </button>
                ))}
              </div>
              <form onSubmit={handleRisk} className="flex gap-3 items-end">
                <Input
                  label={riskMode === 'portfolio' ? 'Symbols (comma-separated)' : 'Symbol'}
                  placeholder={riskMode === 'portfolio' ? 'AAPL,MSFT,GOOGL' : 'AAPL'}
                  value={riskSymbol}
                  onChange={e => setRiskSymbol(e.target.value.toUpperCase())}
                  className="w-56"
                />
                <Button type="submit" loading={riskBusy}>Calculate</Button>
              </form>
              {riskError && <div className="mt-3"><Alert message={riskError} onClose={() => setRiskError('')} /></div>}
            </CardContent>
          </Card>

          {riskBusy && (
            <div className="flex items-center justify-center py-8"><Spinner size="lg" /></div>
          )}

          {riskResult && (
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
              {riskResult.var_95 !== undefined && (
                <StatCard label="VaR 95%" value={fmtPct(Number(riskResult.var_95) * 100)} color={riskColor(Number(riskResult.var_95), false)} />
              )}
              {riskResult.cvar_95 !== undefined && (
                <StatCard label="CVaR 95%" value={fmtPct(Number(riskResult.cvar_95) * 100)} color={riskColor(Number(riskResult.cvar_95), false)} />
              )}
              {riskResult.sharpe_ratio !== undefined && (
                <StatCard label="Sharpe Ratio" value={fmt(Number(riskResult.sharpe_ratio))} color={riskColor(Number(riskResult.sharpe_ratio), true)} />
              )}
              {riskResult.max_drawdown !== undefined && (
                <StatCard label="Max Drawdown" value={fmtPct(Number(riskResult.max_drawdown) * 100)} color={riskColor(Math.abs(Number(riskResult.max_drawdown)), false)} />
              )}
              {riskResult.annualized_return !== undefined && (
                <StatCard label="Ann. Return" value={fmtPct(Number(riskResult.annualized_return) * 100)} up={Number(riskResult.annualized_return) >= 0} color={Number(riskResult.annualized_return) >= 0 ? 'green' : 'red'} />
              )}
              {riskResult.volatility !== undefined && (
                <StatCard label="Volatility" value={fmtPct(Number(riskResult.volatility) * 100)} color="yellow" />
              )}
            </div>
          )}
        </div>
      )}

      {/* ── EVENTS ───────────────────────────────────────────────────── */}
      {tab === 'events' && (
        <div className="space-y-4">
          <Card>
            <CardHeader title="Event Extraction" subtitle="Claude + spaCy fallback — extract financial events from text" />
            <CardContent>
              <form onSubmit={handleEvents} className="space-y-3">
                <Textarea
                  label="News text or earnings excerpt"
                  rows={5}
                  placeholder="Apple announced Q4 earnings of $1.46 EPS, beating consensus by $0.12…"
                  value={evtText}
                  onChange={e => setEvtText(e.target.value)}
                />
                <Button type="submit" loading={evtBusy}>Extract Events</Button>
              </form>
              {evtError && <div className="mt-3"><Alert message={evtError} onClose={() => setEvtError('')} /></div>}
            </CardContent>
          </Card>

          {evtBusy && (
            <div className="flex items-center justify-center py-8"><Spinner size="lg" /></div>
          )}

          {evtResult && (
            <Card>
              <CardHeader title="Extracted Events" />
              <CardContent>
                {evtResult.events && Array.isArray(evtResult.events) && evtResult.events.length > 0 ? (
                  <div className="space-y-3">
                    {evtResult.events.map((evt, i) => {
                      const ev = evt as Record<string, unknown>;
                      return (
                        <div key={i} className="bg-slate-800/40 rounded-xl px-4 py-3 border border-slate-700/50">
                          <div className="flex items-center gap-2 mb-1.5">
                            <Badge variant="blue">{String(ev.type ?? ev.event_type ?? 'Event')}</Badge>
                            {Boolean(ev.company) && <span className="text-xs text-slate-300 font-medium">{String(ev.company)}</span>}
                            {Boolean(ev.date) && <span className="text-xs text-slate-500">{String(ev.date)}</span>}
                          </div>
                          <p className="text-sm text-slate-300">{String(ev.description ?? ev.text ?? JSON.stringify(ev))}</p>
                          {Boolean(ev.impact) && (
                            <p className="text-xs text-slate-500 mt-1">Impact: {String(ev.impact)}</p>
                          )}
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <pre className="text-xs text-slate-400 bg-slate-950 rounded-lg p-4 overflow-auto max-h-72">
                    {JSON.stringify(evtResult, null, 2)}
                  </pre>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      )}
    </div>
  );
}
