'use client';

import { useState, useEffect, useCallback, FormEvent } from 'react';
import {
  lakeQuality, lakeQuery, lakeGraphCompany,
  lakeCacheStats, lakeGraphAffected,
} from '@/lib/api';
import {
  PageHeader, Card, CardHeader, CardContent, StatCard,
  Button, Input, Select, Alert, Badge,
  Table, Thead, Tbody, Tr, Th, Td,
  EmptyState, Spinner, Tabs, SectionTitle, cn, fmt,
} from '@/components/ui';

// ── Page ──────────────────────────────────────────────────────────────────────
export default function DataPage() {
  const [tab, setTab] = useState('quality');

  // ── Quality ───────────────────────────────────────────────────────────────
  const [quality,   setQuality]   = useState<Record<string, unknown> | null>(null);
  const [qualBusy,  setQualBusy]  = useState(false);
  const [qualErr,   setQualErr]   = useState('');

  const loadQuality = useCallback(async () => {
    setQualBusy(true);
    setQualErr('');
    try {
      const res = await lakeQuality() as Record<string, unknown>;
      setQuality(res);
    } catch (e: unknown) {
      setQualErr(e instanceof Error ? e.message : 'Failed to load quality metrics');
    } finally {
      setQualBusy(false);
    }
  }, []);

  useEffect(() => { if (tab === 'quality') loadQuality(); }, [tab, loadQuality]);

  // ── Cache stats ────────────────────────────────────────────────────────────
  const [cacheStats, setCacheStats] = useState<Record<string, unknown> | null>(null);
  const [cacheBusy,  setCacheBusy]  = useState(false);
  const [cacheErr,   setCacheErr]   = useState('');

  const loadCacheStats = useCallback(async () => {
    setCacheBusy(true);
    setCacheErr('');
    try {
      const res = await lakeCacheStats() as Record<string, unknown>;
      setCacheStats(res);
    } catch (e: unknown) {
      setCacheErr(e instanceof Error ? e.message : 'Failed to load cache stats');
    } finally {
      setCacheBusy(false);
    }
  }, []);

  useEffect(() => { if (tab === 'cache') loadCacheStats(); }, [tab, loadCacheStats]);

  // ── Query ─────────────────────────────────────────────────────────────────
  const [qTable,   setQTable]   = useState('market_ticks_clean');
  const [qSymbol,  setQSymbol]  = useState('AAPL');
  const [qLimit,   setQLimit]   = useState('20');
  const [qResult,  setQResult]  = useState<Record<string, unknown>[] | null>(null);
  const [qCols,    setQCols]    = useState<string[]>([]);
  const [qBusy,    setQBusy]    = useState(false);
  const [qErr,     setQErr]     = useState('');

  const handleQuery = async (e: FormEvent) => {
    e.preventDefault();
    setQBusy(true);
    setQErr('');
    setQResult(null);
    try {
      const res = await lakeQuery({
        table: qTable,
        symbol: qSymbol || undefined,
        limit: parseInt(qLimit),
      }) as Record<string, unknown>;
      const rows = (res.rows as Record<string, unknown>[]) ?? [];
      setQResult(rows);
      setQCols(rows.length > 0 ? Object.keys(rows[0]) : []);
    } catch (e: unknown) {
      setQErr(e instanceof Error ? e.message : 'Query failed');
    } finally {
      setQBusy(false);
    }
  };

  // ── Graph ─────────────────────────────────────────────────────────────────
  const [gSymbol,  setGSymbol]  = useState('AAPL');
  const [gMode,    setGMode]    = useState<'company' | 'event'>('company');
  const [gEvt,     setGEvt]     = useState('');
  const [gResult,  setGResult]  = useState<Record<string, unknown> | null>(null);
  const [gBusy,    setGBusy]    = useState(false);
  const [gErr,     setGErr]     = useState('');

  const handleGraph = async (e: FormEvent) => {
    e.preventDefault();
    setGBusy(true);
    setGErr('');
    setGResult(null);
    try {
      const res = gMode === 'company'
        ? await lakeGraphCompany(gSymbol) as Record<string, unknown>
        : await lakeGraphAffected(gEvt) as Record<string, unknown>;
      setGResult(res);
    } catch (e: unknown) {
      setGErr(e instanceof Error ? e.message : 'Graph lookup failed');
    } finally {
      setGBusy(false);
    }
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div>
      <PageHeader
        title="Data Lake"
        subtitle="Quality metrics, graph lookups, and query interface"
        action={
          <Tabs
            tabs={[
              { id: 'quality', label: 'Quality' },
              { id: 'query',   label: 'Query' },
              { id: 'graph',   label: 'Graph' },
              { id: 'cache',   label: 'Cache' },
            ]}
            active={tab}
            onSelect={setTab}
          />
        }
      />

      {/* ── QUALITY ──────────────────────────────────────────────────── */}
      {tab === 'quality' && (
        <div className="space-y-4">
          <div className="flex items-center gap-2">
            <Button variant="secondary" size="sm" onClick={loadQuality} loading={qualBusy}>Refresh</Button>
          </div>

          {qualErr && <Alert message={qualErr} onClose={() => setQualErr('')} />}

          {qualBusy && !quality ? (
            <div className="flex items-center justify-center py-12"><Spinner size="lg" /></div>
          ) : quality ? (
            <div className="space-y-4">
              {/* Summary stats */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                {[
                  { label: 'Total Records', key: 'total_records',      color: 'default' },
                  { label: 'Quality Score', key: 'quality_score',      color: 'green' },
                  { label: 'Null Rate',     key: 'null_rate',          color: 'yellow' },
                  { label: 'Invalid Rate',  key: 'invalid_rate',       color: 'red' },
                ].map(({ label, key, color }) =>
                  quality[key] !== undefined ? (
                    <StatCard
                      key={key}
                      label={label}
                      value={typeof quality[key] === 'number' ? fmt(quality[key] as number, 4) : String(quality[key])}
                      color={color as 'green' | 'red' | 'yellow' | 'default'}
                    />
                  ) : null
                )}
              </div>

              {/* Full quality JSON */}
              <Card>
                <CardHeader title="Full Quality Report" />
                <CardContent>
                  <pre className="text-xs text-slate-400 overflow-auto max-h-72">
                    {JSON.stringify(quality, null, 2)}
                  </pre>
                </CardContent>
              </Card>
            </div>
          ) : (
            <EmptyState message="Click Refresh to load quality metrics" icon="⬡" />
          )}
        </div>
      )}

      {/* ── QUERY ────────────────────────────────────────────────────── */}
      {tab === 'query' && (
        <div className="space-y-4">
          <Card>
            <CardHeader title="Lake Query" subtitle="Query Delta lake tables (bronze / silver / gold)" />
            <CardContent>
              <form onSubmit={handleQuery} className="flex items-end gap-3 flex-wrap">
                <Select
                  label="Table"
                  value={qTable}
                  onChange={e => setQTable(e.target.value)}
                  className="w-56"
                >
                  <option value="market_ticks_clean">market_ticks_clean</option>
                  <option value="market_bars_1hour">market_bars_1hour</option>
                  <option value="news_articles_scored">news_articles_scored</option>
                  <option value="social_sentiment">social_sentiment</option>
                  <option value="technical_indicators">technical_indicators</option>
                  <option value="predictions_signals">predictions_signals</option>
                </Select>
                <Input label="Symbol" value={qSymbol} onChange={e => setQSymbol(e.target.value.toUpperCase())} className="w-24" />
                <Input label="Limit" type="number" value={qLimit} onChange={e => setQLimit(e.target.value)} className="w-20" />
                <Button type="submit" loading={qBusy}>Run</Button>
              </form>
              {qErr && <div className="mt-3"><Alert message={qErr} onClose={() => setQErr('')} /></div>}
            </CardContent>
          </Card>

          {qBusy && (
            <div className="flex items-center justify-center py-8"><Spinner size="lg" /></div>
          )}

          {qResult && (
            <Card>
              <CardHeader title={`Results — ${qResult.length} row${qResult.length !== 1 ? 's' : ''}`} />
              <CardContent className="p-0">
                {qResult.length === 0 ? (
                  <EmptyState message="No rows returned" icon="○" />
                ) : (
                  <div className="overflow-x-auto">
                    <Table>
                      <Thead>
                        <Tr>
                          {qCols.map(c => <Th key={c}>{c}</Th>)}
                        </Tr>
                      </Thead>
                      <Tbody>
                        {qResult.slice(0, 50).map((row, i) => (
                          <Tr key={i}>
                            {qCols.map(c => (
                              <Td key={c} className="text-xs tabular-nums max-w-[180px] truncate">
                                {row[c] === null || row[c] === undefined ? (
                                  <span className="text-slate-600">null</span>
                                ) : (
                                  String(row[c])
                                )}
                              </Td>
                            ))}
                          </Tr>
                        ))}
                      </Tbody>
                    </Table>
                    {qResult.length > 50 && (
                      <p className="text-xs text-slate-500 px-4 py-2 border-t border-slate-800">
                        Showing 50 of {qResult.length} rows
                      </p>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      )}

      {/* ── GRAPH ────────────────────────────────────────────────────── */}
      {tab === 'graph' && (
        <div className="space-y-4">
          <Card>
            <CardHeader title="Knowledge Graph" subtitle="Neo4j — company entities, events, and relationships" />
            <CardContent>
              <div className="flex gap-2 mb-4">
                {(['company', 'event'] as const).map(m => (
                  <button
                    key={m}
                    onClick={() => { setGMode(m); setGResult(null); setGErr(''); }}
                    className={cn(
                      'px-3 py-1.5 rounded-lg text-sm font-medium transition-colors capitalize',
                      gMode === m ? 'bg-blue-600 text-white' : 'text-slate-400 hover:bg-slate-800',
                    )}
                  >
                    {m === 'company' ? 'Company lookup' : 'Affected by event'}
                  </button>
                ))}
              </div>
              <form onSubmit={handleGraph} className="flex gap-3 items-end">
                {gMode === 'company' ? (
                  <Input label="Symbol" value={gSymbol} onChange={e => setGSymbol(e.target.value.toUpperCase())} className="w-28" />
                ) : (
                  <Input label="Event description" value={gEvt} onChange={e => setGEvt(e.target.value)} className="w-64" />
                )}
                <Button type="submit" loading={gBusy}>Lookup</Button>
              </form>
              {gErr && <div className="mt-3"><Alert message={gErr} onClose={() => setGErr('')} /></div>}
            </CardContent>
          </Card>

          {gBusy && <div className="flex items-center justify-center py-8"><Spinner size="lg" /></div>}

          {gResult && (
            <Card>
              <CardHeader title="Graph Result" />
              <CardContent>
                {/* Render company node nicely */}
                {gMode === 'company' && Boolean(gResult.company) && (
                  <div className="space-y-4">
                    <div className="flex items-center gap-3">
                      <div className="w-10 h-10 rounded-xl bg-blue-600/20 border border-blue-600/30 flex items-center justify-center text-blue-400 font-bold text-sm">
                        {String((gResult.company as Record<string, unknown>).symbol ?? '?')}
                      </div>
                      <div>
                        <p className="text-sm font-semibold text-slate-200">
                          {String((gResult.company as Record<string, unknown>).name ?? gSymbol)}
                        </p>
                        <p className="text-xs text-slate-500">
                          {String((gResult.company as Record<string, unknown>).sector ?? 'Unknown sector')}
                        </p>
                      </div>
                    </div>
                    {Boolean(gResult.related_events) && Array.isArray(gResult.related_events) && (
                      <div>
                        <SectionTitle>Related Events ({(gResult.related_events as unknown[]).length})</SectionTitle>
                        <div className="space-y-2">
                          {(gResult.related_events as Record<string, unknown>[]).slice(0, 10).map((ev, i) => (
                            <div key={i} className="bg-slate-800/40 rounded-lg px-3 py-2 text-xs text-slate-300">
                              {String(ev.description ?? ev.type ?? JSON.stringify(ev))}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}
                {(!gResult.company || gMode === 'event') && (
                  <pre className="text-xs text-slate-400 overflow-auto max-h-72">
                    {JSON.stringify(gResult, null, 2)}
                  </pre>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      )}

      {/* ── CACHE ────────────────────────────────────────────────────── */}
      {tab === 'cache' && (
        <div className="space-y-4">
          <Button variant="secondary" size="sm" onClick={loadCacheStats} loading={cacheBusy}>Refresh Stats</Button>

          {cacheErr && <Alert message={cacheErr} onClose={() => setCacheErr('')} />}

          {cacheBusy && !cacheStats ? (
            <div className="flex items-center justify-center py-12"><Spinner size="lg" /></div>
          ) : cacheStats ? (
            <div className="space-y-4">
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                {[
                  { label: 'Hit Rate',       key: 'hit_rate',      color: 'green' },
                  { label: 'Miss Rate',      key: 'miss_rate',     color: 'red' },
                  { label: 'Total Keys',     key: 'total_keys',    color: 'default' },
                  { label: 'Memory Used',    key: 'memory_used',   color: 'yellow' },
                ].map(({ label, key, color }) =>
                  cacheStats[key] !== undefined ? (
                    <StatCard
                      key={key}
                      label={label}
                      value={typeof cacheStats[key] === 'number' ? fmt(cacheStats[key] as number, 4) : String(cacheStats[key])}
                      color={color as 'green' | 'red' | 'yellow' | 'default'}
                    />
                  ) : null
                )}
              </div>
              <Card>
                <CardHeader title="Full Cache Report" />
                <CardContent>
                  <pre className="text-xs text-slate-400 overflow-auto max-h-64">
                    {JSON.stringify(cacheStats, null, 2)}
                  </pre>
                </CardContent>
              </Card>
            </div>
          ) : (
            <EmptyState message="Click Refresh Stats to load cache metrics" icon="◇" />
          )}
        </div>
      )}
    </div>
  );
}
