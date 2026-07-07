'use client';

import { useState, useEffect, useCallback, FormEvent } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, BarChart, Bar,
} from 'recharts';
import {
  checkHealth,
  getMarketData,
  getPortfolioAnalytics,
  runBacktest,
} from '@/lib/api';
import {
  PageHeader, Card, CardHeader, CardContent, StatCard,
  Badge, StatusDot, Button, Input, Select, Alert,
  Spinner, EmptyState, Tabs, cn, fmt, fmtPct, fmtUSD,
} from '@/components/ui';

// ── Types ────────────────────────────────────────────────────────────────────
type ServiceKey = 'gateway' | 'ai' | 'agent' | 'dataLake';
type HealthStatus = 'ok' | 'degraded' | 'down' | 'unknown';

interface HealthMap { gateway: HealthStatus; ai: HealthStatus; agent: HealthStatus; dataLake: HealthStatus }
interface MarketBar { timestamp: string | number; open: number; high: number; low: number; close: number; volume: number }
interface ChartPoint { time: string; close: number; volume: number; open: number; high: number; low: number }

// ── Helpers ──────────────────────────────────────────────────────────────────
function formatTs(ts: string | number): string {
  const d = typeof ts === 'number' ? new Date(ts * 1000) : new Date(ts);
  if (isNaN(d.getTime())) return String(ts);
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function formatTsShort(ts: string | number): string {
  const d = typeof ts === 'number' ? new Date(ts * 1000) : new Date(ts);
  if (isNaN(d.getTime())) return String(ts);
  return `${d.getMonth() + 1}/${d.getDate()} ${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}`;
}

const SERVICES: { key: ServiceKey; label: string }[] = [
  { key: 'gateway', label: 'API Gateway' },
  { key: 'ai',      label: 'AI Service' },
  { key: 'agent',   label: 'Agent Service' },
  { key: 'dataLake',label: 'Data Lake' },
];

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const ChartTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 shadow-xl text-xs">
      <p className="text-slate-400 mb-1">{label}</p>
      <p className="text-slate-100 font-semibold">{fmtUSD(payload[0]?.value ?? 0)}</p>
      {payload[1] && <p className="text-slate-400">Vol: {Number(payload[1]?.value).toLocaleString()}</p>}
    </div>
  );
};

// ── Page ─────────────────────────────────────────────────────────────────────
export default function DashboardPage() {
  const [health,      setHealth]      = useState<HealthMap>({ gateway: 'unknown', ai: 'unknown', agent: 'unknown', dataLake: 'unknown' });
  const [healthLoading, setHealthLoading] = useState(true);

  const [symbol,      setSymbol]      = useState('AAPL');
  const [chartData,   setChartData]   = useState<ChartPoint[]>([]);
  const [chartMeta,   setChartMeta]   = useState<{ symbol: string; first: number; last: number; change: number } | null>(null);
  const [chartLoading,setChartLoading]= useState(false);
  const [chartError,  setChartError]  = useState('');

  const [chartTab,    setChartTab]    = useState('price');
  const [interval,    setInterval]    = useState('1h');

  const [backtestOpen, setBacktestOpen] = useState(false);
  const [btSymbol,    setBtSymbol]    = useState('AAPL');
  const [btShort,     setBtShort]     = useState('20');
  const [btLong,      setBtLong]      = useState('50');
  const [btCapital,   setBtCapital]   = useState('10000');
  const [btResult,    setBtResult]    = useState<Record<string, unknown> | null>(null);
  const [btLoading,   setBtLoading]   = useState(false);
  const [btError,     setBtError]     = useState('');

  // ── Health ─────────────────────────────────────────────────────────────────
  const refreshHealth = useCallback(async () => {
    setHealthLoading(true);
    const results = await checkHealth();
    type SvcResult = { ok: boolean; data?: { status?: string }; error?: string } | null;
    const toStatus = (v: SvcResult): HealthStatus => {
      if (!v) return 'down';
      if (v.ok && v.data?.status === 'ok') return 'ok';
      if (v.ok) return 'degraded';
      return 'down';
    };
    setHealth({
      gateway:  toStatus(results.gateway  as SvcResult),
      ai:       toStatus(results.ai       as SvcResult),
      agent:    toStatus(results.agent    as SvcResult),
      dataLake: toStatus(results.dataLake as SvcResult),
    });
    setHealthLoading(false);
  }, []);

  useEffect(() => { refreshHealth(); }, [refreshHealth]);

  // ── Market data ────────────────────────────────────────────────────────────
  const fetchChart = useCallback(async (sym: string, intv: string) => {
    if (!sym.trim()) return;
    setChartLoading(true);
    setChartError('');
    try {
      const res = await getMarketData(sym.toUpperCase(), intv, 200) as Record<string, unknown>;
      const bars = (res.data as MarketBar[]) || [];
      if (!bars.length) { setChartData([]); setChartMeta(null); setChartLoading(false); return; }
      const pts: ChartPoint[] = bars.map(b => ({
        time:   formatTsShort(b.timestamp),
        close:  Number(b.close),
        open:   Number(b.open),
        high:   Number(b.high),
        low:    Number(b.low),
        volume: Number(b.volume),
      }));
      const first = pts[0].close;
      const last  = pts[pts.length - 1].close;
      setChartData(pts);
      setChartMeta({ symbol: sym.toUpperCase(), first, last, change: ((last - first) / first) * 100 });
    } catch (e: unknown) {
      setChartError(e instanceof Error ? e.message : 'Failed to load market data');
    } finally {
      setChartLoading(false);
    }
  }, []);

  useEffect(() => { fetchChart('AAPL', '1h'); }, [fetchChart]);

  const handleSearch = (e: FormEvent) => {
    e.preventDefault();
    fetchChart(symbol, interval);
  };

  // ── Backtest ───────────────────────────────────────────────────────────────
  const handleBacktest = async (e: FormEvent) => {
    e.preventDefault();
    setBtLoading(true);
    setBtError('');
    setBtResult(null);
    try {
      const res = await runBacktest({
        symbol: btSymbol.toUpperCase(),
        strategy: 'sma_crossover',
        short_window: parseInt(btShort),
        long_window:  parseInt(btLong),
        initial_capital: parseFloat(btCapital),
      }) as Record<string, unknown>;
      setBtResult(res);
    } catch (e: unknown) {
      setBtError(e instanceof Error ? e.message : 'Backtest failed');
    } finally {
      setBtLoading(false);
    }
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  const priceColor = chartMeta ? (chartMeta.change >= 0 ? 'text-green-400' : 'text-red-400') : 'text-slate-400';
  const lineColor  = chartMeta ? (chartMeta.change >= 0 ? '#22c55e' : '#ef4444') : '#3b82f6';

  return (
    <div>
      <PageHeader
        title="Dashboard"
        subtitle="Service health and market overview"
        action={
          <Button variant="secondary" size="sm" onClick={refreshHealth} loading={healthLoading}>
            Refresh
          </Button>
        }
      />

      {/* ── Service health ─────────────────────────────────────────── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-6">
        {SERVICES.map(svc => {
          const st = health[svc.key];
          return (
            <Card key={svc.key}>
              <CardContent className="py-4 flex items-center gap-3">
                <StatusDot status={healthLoading ? 'unknown' : st} />
                <div>
                  <p className="text-sm font-medium text-slate-200">{svc.label}</p>
                  <p className={cn('text-xs mt-0.5', st === 'ok' ? 'text-green-400' : st === 'down' ? 'text-red-400' : 'text-amber-400')}>
                    {healthLoading ? 'Checking…' : st === 'ok' ? 'Healthy' : st === 'down' ? 'Down' : st === 'degraded' ? 'Degraded' : 'Unknown'}
                  </p>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {/* ── Market data chart ──────────────────────────────────────── */}
      <Card className="mb-6">
        <CardHeader
          title={chartMeta ? `${chartMeta.symbol} — Price Chart` : 'Market Data'}
          subtitle="Historical price & volume"
          action={
            <form onSubmit={handleSearch} className="flex items-center gap-2">
              <Input
                placeholder="Symbol"
                value={symbol}
                onChange={e => setSymbol(e.target.value.toUpperCase())}
                className="w-24"
              />
              <Select value={interval} onChange={e => { setInterval(e.target.value); fetchChart(symbol, e.target.value); }} className="w-20">
                <option value="1m">1m</option>
                <option value="5m">5m</option>
                <option value="15m">15m</option>
                <option value="1h">1h</option>
                <option value="1d">1d</option>
              </Select>
              <Button type="submit" size="sm" loading={chartLoading}>Load</Button>
            </form>
          }
        />
        <CardContent>
          {/* Stats row */}
          {chartMeta && (
            <div className="flex items-center gap-6 mb-4 pb-4 border-b border-slate-800">
              <div>
                <p className="text-xs text-slate-500">Last</p>
                <p className="text-xl font-bold text-slate-100 tabular-nums">{fmtUSD(chartMeta.last)}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500">Change</p>
                <p className={cn('text-sm font-semibold tabular-nums', priceColor)}>{fmtPct(chartMeta.change)}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500">Open</p>
                <p className="text-sm text-slate-300 tabular-nums">{fmtUSD(chartMeta.first)}</p>
              </div>
              <div className="ml-auto">
                <Tabs
                  tabs={[{ id: 'price', label: 'Price' }, { id: 'volume', label: 'Volume' }]}
                  active={chartTab}
                  onSelect={setChartTab}
                />
              </div>
            </div>
          )}

          {chartError && <Alert message={chartError} onClose={() => setChartError('')} />}

          {chartLoading ? (
            <div className="flex items-center justify-center h-48"><Spinner size="lg" /></div>
          ) : chartData.length === 0 ? (
            <EmptyState message="Enter a symbol to view price data" icon="📈" />
          ) : (
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                {chartTab === 'price' ? (
                  <LineChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                    <XAxis dataKey="time" tick={{ fill: '#64748b', fontSize: 10 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
                    <YAxis domain={['auto', 'auto']} tick={{ fill: '#64748b', fontSize: 10 }} tickLine={false} axisLine={false} tickFormatter={v => `$${fmt(v, 0)}`} width={56} />
                    <Tooltip content={<ChartTooltip />} />
                    <Line type="monotone" dataKey="close" stroke={lineColor} strokeWidth={1.5} dot={false} activeDot={{ r: 3 }} />
                  </LineChart>
                ) : (
                  <BarChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                    <XAxis dataKey="time" tick={{ fill: '#64748b', fontSize: 10 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
                    <YAxis tick={{ fill: '#64748b', fontSize: 10 }} tickLine={false} axisLine={false} tickFormatter={v => `${(v / 1e6).toFixed(1)}M`} width={56} />
                    <Tooltip content={<ChartTooltip />} />
                    <Bar dataKey="volume" fill="#3b82f6" opacity={0.7} radius={[2, 2, 0, 0]} />
                  </BarChart>
                )}
              </ResponsiveContainer>
            </div>
          )}
        </CardContent>
      </Card>

      {/* ── Backtest ────────────────────────────────────────────────── */}
      <Card>
        <CardHeader
          title="SMA Crossover Backtest"
          subtitle="Premium feature — simulate a simple moving average strategy"
          action={
            <Button variant="ghost" size="sm" onClick={() => setBacktestOpen(o => !o)}>
              {backtestOpen ? 'Collapse' : 'Expand'}
            </Button>
          }
        />
        {backtestOpen && (
          <CardContent>
            <form onSubmit={handleBacktest} className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
              <Input label="Symbol" value={btSymbol} onChange={e => setBtSymbol(e.target.value.toUpperCase())} />
              <Input label="Short window" type="number" value={btShort} onChange={e => setBtShort(e.target.value)} />
              <Input label="Long window"  type="number" value={btLong}  onChange={e => setBtLong(e.target.value)} />
              <Input label="Capital ($)"  type="number" value={btCapital} onChange={e => setBtCapital(e.target.value)} />
              <div className="col-span-2 sm:col-span-4">
                <Button type="submit" loading={btLoading}>Run Backtest</Button>
              </div>
            </form>

            {btError  && <Alert message={btError} onClose={() => setBtError('')} />}

            {btResult && (
              <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 mt-4">
                {[
                  { label: 'Initial Capital', value: fmtUSD(Number(btResult.initial_capital)) },
                  { label: 'Final Value',      value: fmtUSD(Number(btResult.final_value)) },
                  { label: 'Total Return',     value: fmtPct(Number(btResult.total_return_pct)) },
                  { label: 'Sharpe Ratio',     value: fmt(Number(btResult.sharpe_ratio)) },
                  { label: 'Bars Processed',   value: String(btResult.num_bars ?? '—') },
                  { label: 'Strategy',         value: String(btResult.strategy ?? '—') },
                ].map(s => (
                  <StatCard key={s.label} label={s.label} value={s.value} />
                ))}
              </div>
            )}
          </CardContent>
        )}
      </Card>
    </div>
  );
}
