'use client';

import { useState, useEffect, useCallback, useRef, FormEvent } from 'react';
import {
  listSessions, deleteSession, listSessionMessages,
  streamChat,
  listPositions, createPosition, deletePosition, getPortfolioSummary,
  proposeTrade, confirmOrder, listOrders,
  type SessionResponse, type MessageResponse, type PositionResponse, type OrderResponse,
} from '@/lib/api';
import {
  PageHeader, Card, CardHeader, CardContent,
  Button, Input, Select, Alert, Badge, StatCard,
  Table, Thead, Tbody, Tr, Th, Td,
  EmptyState, Spinner, Tabs, cn, fmtUSD, fmt,
} from '@/components/ui';

// ── Types ─────────────────────────────────────────────────────────────────────
interface ChatMessage { role: 'user' | 'assistant'; content: string }
interface PortfolioSummary { total_cost_basis: number; total_market_value: number; unrealized_pnl: number }

// ── SSE parser ────────────────────────────────────────────────────────────────
async function readSSE(
  response: Response,
  onChunk: (text: string) => void,
): Promise<void> {
  const reader = response.body?.getReader();
  if (!reader) return;
  const dec = new TextDecoder();
  let buf = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    const lines = buf.split('\n');
    buf = lines.pop() ?? '';
    for (const line of lines) {
      if (!line.startsWith('data: ')) continue;
      const raw = line.slice(6).trim();
      if (!raw || raw === '[DONE]') continue;
      try {
        const parsed = JSON.parse(raw) as Record<string, unknown>;
        const text = (parsed.content ?? parsed.text ?? parsed.delta ?? '') as string;
        if (text) onChunk(text);
      } catch {
        onChunk(raw);
      }
    }
  }
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function AgentPage() {
  const [tab, setTab] = useState('chat');

  // ── Chat ─────────────────────────────────────────────────────────────────
  const [sessions,    setSessions]    = useState<SessionResponse[]>([]);
  const [sessLoading, setSessLoading] = useState(true);
  const [currentSess, setCurrentSess] = useState<string | null>(null);
  const [messages,    setMessages]    = useState<ChatMessage[]>([]);
  const [msgLoading,  setMsgLoading]  = useState(false);
  const [input,       setInput]       = useState('');
  const [streaming,   setStreaming]   = useState(false);
  const [chatError,   setChatError]   = useState('');
  const bottomRef = useRef<HTMLDivElement>(null);

  // ── Portfolio ─────────────────────────────────────────────────────────────
  const [positions,  setPositions]  = useState<PositionResponse[]>([]);
  const [portSummary,setPortSummary]= useState<PortfolioSummary | null>(null);
  const [portLoading,setPortLoading]= useState(false);
  const [posSymbol,  setPosSymbol]  = useState('');
  const [posQty,     setPosQty]     = useState('');
  const [posCost,    setPosCost]    = useState('');
  const [posError,   setPosError]   = useState('');
  const [posBusy,    setPosBusy]    = useState(false);

  // ── Trading ───────────────────────────────────────────────────────────────
  const [orders,     setOrders]     = useState<OrderResponse[]>([]);
  const [ordLoading, setOrdLoading] = useState(false);
  const [trSymbol,   setTrSymbol]   = useState('');
  const [trSide,     setTrSide]     = useState<'buy' | 'sell'>('buy');
  const [trQty,      setTrQty]      = useState('');
  const [trType,     setTrType]     = useState<'market' | 'limit'>('market');
  const [trError,    setTrError]    = useState('');
  const [trBusy,     setTrBusy]     = useState(false);
  const [pendingOrder, setPendingOrder] = useState<string | null>(null);

  // ── Fetch sessions ────────────────────────────────────────────────────────
  const refreshSessions = useCallback(async () => {
    setSessLoading(true);
    try { setSessions(await listSessions()); } catch { /* ignore */ }
    setSessLoading(false);
  }, []);

  useEffect(() => { refreshSessions(); }, [refreshSessions]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // ── Load session messages ─────────────────────────────────────────────────
  const loadSession = async (id: string) => {
    setCurrentSess(id);
    setMsgLoading(true);
    setChatError('');
    try {
      const msgs = await listSessionMessages(id);
      setMessages(msgs.map((m: MessageResponse) => ({
        role:    m.role as 'user' | 'assistant',
        content: String(m.content),
      })));
    } catch (e: unknown) {
      setChatError(e instanceof Error ? e.message : 'Failed to load messages');
    } finally {
      setMsgLoading(false);
    }
  };

  const newChat = () => {
    setCurrentSess(null);
    setMessages([]);
    setChatError('');
  };

  const handleDeleteSession = async (id: string) => {
    try {
      await deleteSession(id);
      if (currentSess === id) newChat();
      await refreshSessions();
    } catch { /* ignore */ }
  };

  // ── Send message (streaming) ──────────────────────────────────────────────
  const sendMessage = async (e: FormEvent) => {
    e.preventDefault();
    const text = input.trim();
    if (!text || streaming) return;
    setInput('');
    setChatError('');
    setMessages(prev => [...prev, { role: 'user', content: text }]);
    setStreaming(true);

    let assistantText = '';
    setMessages(prev => [...prev, { role: 'assistant', content: '' }]);

    try {
      const res = await streamChat(text, currentSess ?? undefined);
      if (!res.ok) throw new Error(`Agent error: ${res.status}`);

      // Extract session id from response headers if new session
      const newSessId = res.headers.get('x-session-id') ?? res.headers.get('X-Session-Id');
      if (newSessId && !currentSess) {
        setCurrentSess(newSessId);
        await refreshSessions();
      }

      await readSSE(res, (chunk) => {
        assistantText += chunk;
        setMessages(prev => {
          const copy = [...prev];
          copy[copy.length - 1] = { role: 'assistant', content: assistantText };
          return copy;
        });
      });

      // If session ID wasn't in headers, refresh sessions to pick up any new one
      if (!newSessId) setTimeout(() => refreshSessions(), 1000);
    } catch (e: unknown) {
      setChatError(e instanceof Error ? e.message : 'Chat error');
      setMessages(prev => prev.slice(0, -1));
    } finally {
      setStreaming(false);
    }
  };

  // ── Portfolio ─────────────────────────────────────────────────────────────
  const refreshPortfolio = useCallback(async () => {
    setPortLoading(true);
    try {
      const [pos, summary] = await Promise.all([listPositions(), getPortfolioSummary()]);
      setPositions(pos);
      setPortSummary(summary as unknown as PortfolioSummary);
    } catch { /* ignore */ }
    setPortLoading(false);
  }, []);

  useEffect(() => { if (tab === 'portfolio') refreshPortfolio(); }, [tab, refreshPortfolio]);

  const handleAddPosition = async (e: FormEvent) => {
    e.preventDefault();
    setPosError('');
    setPosBusy(true);
    try {
      await createPosition({ symbol: posSymbol.toUpperCase(), quantity: parseFloat(posQty), avg_cost_basis: parseFloat(posCost) });
      setPosSymbol(''); setPosQty(''); setPosCost('');
      await refreshPortfolio();
    } catch (err: unknown) {
      setPosError(err instanceof Error ? err.message : 'Failed to add position');
    } finally { setPosBusy(false); }
  };

  const handleRemovePosition = async (id: string) => {
    try { await deletePosition(id); await refreshPortfolio(); } catch { /* ignore */ }
  };

  // ── Trading ───────────────────────────────────────────────────────────────
  const refreshOrders = useCallback(async () => {
    setOrdLoading(true);
    try { setOrders(await listOrders()); } catch { /* ignore */ }
    setOrdLoading(false);
  }, []);

  useEffect(() => { if (tab === 'trading') refreshOrders(); }, [tab, refreshOrders]);

  const handleTrade = async (e: FormEvent) => {
    e.preventDefault();
    setTrError('');
    setTrBusy(true);
    try {
      const res = await proposeTrade({ symbol: trSymbol.toUpperCase(), side: trSide, qty: parseFloat(trQty), order_type: trType }) as Record<string, unknown>;
      if (res.requires_confirmation) {
        setPendingOrder(String(res.order_id));
      }
      setTrSymbol(''); setTrQty('');
      await refreshOrders();
    } catch (err: unknown) {
      setTrError(err instanceof Error ? err.message : 'Trade failed');
    } finally { setTrBusy(false); }
  };

  const handleConfirm = async () => {
    if (!pendingOrder) return;
    try { await confirmOrder(pendingOrder); setPendingOrder(null); await refreshOrders(); } catch { /* ignore */ }
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div>
      <PageHeader
        title="AI Agent"
        subtitle="Chat, manage portfolio, and place orders"
        action={
          <Tabs
            tabs={[{ id: 'chat', label: 'Chat' }, { id: 'portfolio', label: 'Portfolio' }, { id: 'trading', label: 'Trading' }]}
            active={tab}
            onSelect={setTab}
          />
        }
      />

      {/* ── CHAT TAB ─────────────────────────────────────────────────── */}
      {tab === 'chat' && (
        <div className="flex gap-4 h-[calc(100vh-180px)]">
          {/* Sessions sidebar */}
          <Card className="w-52 flex-shrink-0 flex flex-col">
            <div className="px-4 py-3 border-b border-slate-800 flex items-center justify-between">
              <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Sessions</span>
              <Button variant="ghost" size="xs" onClick={newChat}>+ New</Button>
            </div>
            <div className="flex-1 overflow-y-auto">
              {sessLoading ? (
                <div className="flex items-center justify-center py-8"><Spinner size="sm" /></div>
              ) : sessions.length === 0 ? (
                <p className="text-xs text-slate-500 text-center py-8">No sessions yet</p>
              ) : (
                <div className="p-2 space-y-1">
                  {sessions.map(s => (
                    <div
                      key={String(s.id)}
                      className={cn(
                        'group flex items-center justify-between rounded-lg px-2 py-1.5 cursor-pointer transition-colors',
                        currentSess === String(s.id) ? 'bg-blue-600/20 text-blue-400' : 'text-slate-400 hover:bg-slate-800/60 hover:text-slate-200',
                      )}
                      onClick={() => loadSession(String(s.id))}
                    >
                      <p className="text-xs truncate flex-1">{s.title ? String(s.title) : `Session ${String(s.id).slice(0, 8)}`}</p>
                      <button
                        className="ml-1 opacity-0 group-hover:opacity-100 text-slate-600 hover:text-red-400 transition-all text-xs"
                        onClick={ev => { ev.stopPropagation(); handleDeleteSession(String(s.id)); }}
                      >
                        ✕
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </Card>

          {/* Chat area */}
          <Card className="flex-1 flex flex-col min-w-0">
            <div className="px-5 py-3 border-b border-slate-800 flex items-center justify-between">
              <p className="text-sm font-semibold text-slate-200">
                {currentSess ? `Session ${currentSess.slice(0, 8)}…` : 'New conversation'}
              </p>
              {currentSess && (
                <Badge variant="blue">Session active</Badge>
              )}
            </div>

            {/* Messages */}
            <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
              {msgLoading ? (
                <div className="flex items-center justify-center h-full"><Spinner /></div>
              ) : messages.length === 0 ? (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center">
                    <p className="text-4xl mb-3 opacity-20">◆</p>
                    <p className="text-sm text-slate-500">Start a conversation with the AI agent</p>
                    <p className="text-xs text-slate-600 mt-1">Ask about portfolio analysis, market data, or trading strategies</p>
                  </div>
                </div>
              ) : (
                messages.map((msg, i) => (
                  <div key={i} className={cn('flex', msg.role === 'user' ? 'justify-end' : 'justify-start')}>
                    <div
                      className={cn(
                        'max-w-[78%] rounded-2xl px-4 py-2.5 text-sm',
                        msg.role === 'user'
                          ? 'bg-blue-600 text-white rounded-br-sm'
                          : 'bg-slate-800 text-slate-200 rounded-bl-sm',
                      )}
                    >
                      {msg.content || <span className="opacity-40">…</span>}
                    </div>
                  </div>
                ))
              )}
              <div ref={bottomRef} />
            </div>

            {/* Error */}
            {chatError && (
              <div className="px-5 pb-2">
                <Alert message={chatError} onClose={() => setChatError('')} />
              </div>
            )}

            {/* Input */}
            <form onSubmit={sendMessage} className="px-5 py-4 border-t border-slate-800 flex gap-3">
              <input
                type="text"
                value={input}
                onChange={e => setInput(e.target.value)}
                placeholder="Ask the agent anything…"
                disabled={streaming}
                className="flex-1 rounded-xl bg-slate-800 border border-slate-700 text-slate-200 placeholder-slate-500 px-4 py-2.5 text-sm focus:outline-none focus:ring-1 focus:ring-blue-500 transition-colors disabled:opacity-50"
              />
              <Button type="submit" loading={streaming} disabled={!input.trim()}>
                {streaming ? 'Thinking' : 'Send'}
              </Button>
            </form>
          </Card>
        </div>
      )}

      {/* ── PORTFOLIO TAB ────────────────────────────────────────────── */}
      {tab === 'portfolio' && (
        <div className="space-y-4">
          {/* Summary */}
          {portLoading ? (
            <div className="flex items-center justify-center py-8"><Spinner /></div>
          ) : portSummary && (
            <div className="grid grid-cols-3 gap-3">
              <StatCard label="Cost Basis"      value={fmtUSD(portSummary.total_cost_basis)}    color="default" />
              <StatCard label="Market Value"    value={fmtUSD(portSummary.total_market_value)}  color="blue" />
              <StatCard
                label="Unrealized P&L"
                value={fmtUSD(portSummary.unrealized_pnl)}
                color={portSummary.unrealized_pnl >= 0 ? 'green' : 'red'}
                up={portSummary.unrealized_pnl >= 0}
              />
            </div>
          )}

          {/* Add position */}
          <Card>
            <CardHeader title="Add Position" subtitle="Premium — manually add a portfolio position" />
            <CardContent>
              <form onSubmit={handleAddPosition} className="flex items-end gap-3">
                <Input label="Symbol" value={posSymbol} onChange={e => setPosSymbol(e.target.value.toUpperCase())} className="w-28" />
                <Input label="Quantity" type="number" step="0.0001" value={posQty} onChange={e => setPosQty(e.target.value)} className="w-28" />
                <Input label="Avg cost ($)" type="number" step="0.01" value={posCost} onChange={e => setPosCost(e.target.value)} className="w-32" />
                <Button type="submit" loading={posBusy}>Add</Button>
              </form>
              {posError && <div className="mt-3"><Alert message={posError} onClose={() => setPosError('')} /></div>}
            </CardContent>
          </Card>

          {/* Positions table */}
          <Card>
            <CardHeader
              title="Open Positions"
              subtitle={`${positions.length} position${positions.length !== 1 ? 's' : ''}`}
              action={<Button variant="secondary" size="xs" onClick={refreshPortfolio} loading={portLoading}>Refresh</Button>}
            />
            <CardContent className="p-0">
              {positions.length === 0 ? (
                <EmptyState message="No open positions" icon="◌" />
              ) : (
                <Table>
                  <Thead>
                    <Tr>
                      <Th>Symbol</Th>
                      <Th>Quantity</Th>
                      <Th>Avg Cost</Th>
                      <Th>Source</Th>
                      <Th>Opened</Th>
                      <Th></Th>
                    </Tr>
                  </Thead>
                  <Tbody>
                    {positions.map(p => (
                      <Tr key={String(p.id)}>
                        <Td><span className="font-semibold text-slate-100">{String(p.symbol)}</span></Td>
                        <Td className="tabular-nums">{fmt(Number(p.quantity), 4)}</Td>
                        <Td className="tabular-nums">{fmtUSD(Number(p.avg_cost_basis))}</Td>
                        <Td><Badge variant="gray">{String(p.source ?? 'manual')}</Badge></Td>
                        <Td className="text-slate-500 text-xs">{p.opened_at ? new Date(String(p.opened_at)).toLocaleDateString() : '—'}</Td>
                        <Td>
                          <Button variant="danger" size="xs" onClick={() => handleRemovePosition(String(p.id))}>Close</Button>
                        </Td>
                      </Tr>
                    ))}
                  </Tbody>
                </Table>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {/* ── TRADING TAB ──────────────────────────────────────────────── */}
      {tab === 'trading' && (
        <div className="space-y-4">
          {pendingOrder && (
            <Alert
              message={`Order ${pendingOrder} requires confirmation. Click Confirm to proceed.`}
              variant="warning"
            />
          )}
          {pendingOrder && (
            <div className="flex gap-2">
              <Button variant="primary" onClick={handleConfirm}>Confirm Order</Button>
              <Button variant="ghost" onClick={() => setPendingOrder(null)}>Cancel</Button>
            </div>
          )}

          {/* Order form */}
          <Card>
            <CardHeader title="Place Order" subtitle="Orders go through 7-guardrail validation" />
            <CardContent>
              <form onSubmit={handleTrade} className="flex items-end gap-3 flex-wrap">
                <Input label="Symbol" value={trSymbol} onChange={e => setTrSymbol(e.target.value.toUpperCase())} className="w-28" />
                <div className="w-24">
                  <Select label="Side" value={trSide} onChange={e => setTrSide(e.target.value as 'buy' | 'sell')}>
                    <option value="buy">Buy</option>
                    <option value="sell">Sell</option>
                  </Select>
                </div>
                <Input label="Quantity" type="number" step="0.0001" value={trQty} onChange={e => setTrQty(e.target.value)} className="w-28" />
                <div className="w-28">
                  <Select label="Type" value={trType} onChange={e => setTrType(e.target.value as 'market' | 'limit')}>
                    <option value="market">Market</option>
                    <option value="limit">Limit</option>
                  </Select>
                </div>
                <Button type="submit" loading={trBusy} variant={trSide === 'buy' ? 'primary' : 'danger'}>
                  {trSide === 'buy' ? 'Buy' : 'Sell'}
                </Button>
              </form>
              {trError && <div className="mt-3"><Alert message={trError} onClose={() => setTrError('')} /></div>}
            </CardContent>
          </Card>

          {/* Orders list */}
          <Card>
            <CardHeader
              title="Order History"
              action={<Button variant="secondary" size="xs" onClick={refreshOrders} loading={ordLoading}>Refresh</Button>}
            />
            <CardContent className="p-0">
              {ordLoading ? (
                <div className="flex items-center justify-center py-8"><Spinner /></div>
              ) : orders.length === 0 ? (
                <EmptyState message="No orders yet" icon="○" />
              ) : (
                <Table>
                  <Thead>
                    <Tr>
                      <Th>Symbol</Th>
                      <Th>Side</Th>
                      <Th>Qty</Th>
                      <Th>Type</Th>
                      <Th>Status</Th>
                      <Th>Date</Th>
                    </Tr>
                  </Thead>
                  <Tbody>
                    {orders.map(o => (
                      <Tr key={String(o.id)}>
                        <Td><span className="font-semibold text-slate-100">{String(o.symbol)}</span></Td>
                        <Td>
                          <Badge variant={String(o.side) === 'buy' ? 'green' : 'red'}>
                            {String(o.side).toUpperCase()}
                          </Badge>
                        </Td>
                        <Td className="tabular-nums">{fmt(Number(o.qty), 4)}</Td>
                        <Td><Badge variant="gray">{String(o.order_type)}</Badge></Td>
                        <Td>
                          <Badge variant={String(o.status) === 'filled' ? 'green' : String(o.status) === 'rejected' ? 'red' : 'yellow'}>
                            {String(o.status)}
                          </Badge>
                        </Td>
                        <Td className="text-slate-500 text-xs">
                          {o.created_at ? new Date(String(o.created_at)).toLocaleDateString() : '—'}
                        </Td>
                      </Tr>
                    ))}
                  </Tbody>
                </Table>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
