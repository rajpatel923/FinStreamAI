'use client';

import { useState, useEffect, useCallback, FormEvent } from 'react';
import {
  listWatchlist, addWatchlist, removeWatchlist,
  listAlerts, createAlert, deleteAlert,
  type WatchlistItemResponse, type AlertResponse,
} from '@/lib/api';
import {
  PageHeader, Card, CardHeader, CardContent,
  Button, Input, Select, Alert, Badge,
  Table, Thead, Tbody, Tr, Th, Td,
  EmptyState, Spinner, cn,
} from '@/components/ui';

type AlertDir = 'above' | 'below';

function dirBadge(dir: string) {
  return dir === 'above'
    ? <Badge variant="green">↑ Above</Badge>
    : <Badge variant="red">↓ Below</Badge>;
}

export default function WatchlistPage() {
  // ── Watchlist state ────────────────────────────────────────────────────────
  const [items,      setItems]      = useState<WatchlistItemResponse[]>([]);
  const [wlLoading,  setWlLoading]  = useState(true);
  const [addSymbol,  setAddSymbol]  = useState('');
  const [addNotes,   setAddNotes]   = useState('');
  const [addBusy,    setAddBusy]    = useState(false);
  const [wlError,    setWlError]    = useState('');

  // ── Alert state ────────────────────────────────────────────────────────────
  const [alerts,     setAlerts]     = useState<AlertResponse[]>([]);
  const [alLoading,  setAlLoading]  = useState(true);
  const [alSymbol,   setAlSymbol]   = useState('');
  const [threshold,  setThreshold]  = useState('');
  const [direction,  setDirection]  = useState<AlertDir>('above');
  const [alertBusy,  setAlertBusy]  = useState(false);
  const [alError,    setAlError]    = useState('');

  // ── Fetch ──────────────────────────────────────────────────────────────────
  const refreshWatchlist = useCallback(async () => {
    setWlLoading(true);
    try {
      const res = await listWatchlist();
      setItems(res);
    } catch { /* empty on error */ }
    setWlLoading(false);
  }, []);

  const refreshAlerts = useCallback(async () => {
    setAlLoading(true);
    try {
      const res = await listAlerts();
      setAlerts(res);
    } catch { /* empty on error */ }
    setAlLoading(false);
  }, []);

  useEffect(() => { refreshWatchlist(); refreshAlerts(); }, [refreshWatchlist, refreshAlerts]);

  // ── Actions ────────────────────────────────────────────────────────────────
  const handleAddWatchlist = async (e: FormEvent) => {
    e.preventDefault();
    if (!addSymbol.trim()) return;
    setAddBusy(true);
    setWlError('');
    try {
      await addWatchlist(addSymbol.toUpperCase(), addNotes || undefined);
      setAddSymbol('');
      setAddNotes('');
      await refreshWatchlist();
    } catch (err: unknown) {
      setWlError(err instanceof Error ? err.message : 'Failed to add symbol');
    } finally {
      setAddBusy(false);
    }
  };

  const handleRemove = async (symbol: string) => {
    try {
      await removeWatchlist(symbol);
      await refreshWatchlist();
    } catch (err: unknown) {
      setWlError(err instanceof Error ? err.message : 'Failed to remove symbol');
    }
  };

  const handleCreateAlert = async (e: FormEvent) => {
    e.preventDefault();
    if (!alSymbol.trim() || !threshold.trim()) return;
    setAlertBusy(true);
    setAlError('');
    try {
      await createAlert(alSymbol.toUpperCase(), parseFloat(threshold), direction);
      setAlSymbol('');
      setThreshold('');
      await refreshAlerts();
    } catch (err: unknown) {
      setAlError(err instanceof Error ? err.message : 'Failed to create alert');
    } finally {
      setAlertBusy(false);
    }
  };

  const handleDeleteAlert = async (id: string) => {
    try {
      await deleteAlert(id);
      await refreshAlerts();
    } catch (err: unknown) {
      setAlError(err instanceof Error ? err.message : 'Failed to delete alert');
    }
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div>
      <PageHeader
        title="Watchlist"
        subtitle="Track symbols and set price alerts"
      />

      {/* ── Watchlist ────────────────────────────────────────────────── */}
      <Card className="mb-6">
        <CardHeader
          title="Tracked Symbols"
          subtitle={`${items.length} symbol${items.length !== 1 ? 's' : ''}`}
          action={
            <form onSubmit={handleAddWatchlist} className="flex items-center gap-2">
              <Input
                placeholder="Symbol, e.g. TSLA"
                value={addSymbol}
                onChange={e => setAddSymbol(e.target.value.toUpperCase())}
                className="w-28"
              />
              <Input
                placeholder="Notes (optional)"
                value={addNotes}
                onChange={e => setAddNotes(e.target.value)}
                className="w-40"
              />
              <Button type="submit" size="sm" loading={addBusy}>Add</Button>
            </form>
          }
        />
        {wlError && (
          <div className="px-5 pt-3">
            <Alert message={wlError} onClose={() => setWlError('')} />
          </div>
        )}
        <CardContent className="p-0">
          {wlLoading ? (
            <div className="flex items-center justify-center py-12"><Spinner /></div>
          ) : items.length === 0 ? (
            <EmptyState message="No symbols tracked yet — add one above" icon="◎" />
          ) : (
            <Table>
              <Thead>
                <Tr>
                  <Th>Symbol</Th>
                  <Th>Notes</Th>
                  <Th>Added</Th>
                  <Th></Th>
                </Tr>
              </Thead>
              <Tbody>
                {items.map(item => (
                  <Tr key={String(item.id ?? item.symbol)}>
                    <Td>
                      <span className="font-semibold text-slate-100">{String(item.symbol)}</span>
                    </Td>
                    <Td className="text-slate-400">{item.notes ? String(item.notes) : '—'}</Td>
                    <Td className="text-slate-500 text-xs">
                      {item.created_at ? new Date(String(item.created_at)).toLocaleDateString() : '—'}
                    </Td>
                    <Td>
                      <Button
                        variant="danger"
                        size="xs"
                        onClick={() => handleRemove(String(item.symbol))}
                      >
                        Remove
                      </Button>
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* ── Price Alerts ─────────────────────────────────────────────── */}
      <Card>
        <CardHeader
          title="Price Alerts"
          subtitle="Notify when price crosses a threshold (free tier: 3 alerts)"
          action={
            <form onSubmit={handleCreateAlert} className="flex items-center gap-2">
              <Input
                placeholder="Symbol"
                value={alSymbol}
                onChange={e => setAlSymbol(e.target.value.toUpperCase())}
                className="w-24"
              />
              <Input
                placeholder="Price"
                type="number"
                step="0.01"
                value={threshold}
                onChange={e => setThreshold(e.target.value)}
                className="w-24"
              />
              <Select value={direction} onChange={e => setDirection(e.target.value as AlertDir)} className="w-24">
                <option value="above">Above</option>
                <option value="below">Below</option>
              </Select>
              <Button type="submit" size="sm" loading={alertBusy}>Create</Button>
            </form>
          }
        />
        {alError && (
          <div className="px-5 pt-3">
            <Alert message={alError} onClose={() => setAlError('')} />
          </div>
        )}
        <CardContent className="p-0">
          {alLoading ? (
            <div className="flex items-center justify-center py-12"><Spinner /></div>
          ) : alerts.length === 0 ? (
            <EmptyState message="No alerts configured yet" icon="◌" />
          ) : (
            <Table>
              <Thead>
                <Tr>
                  <Th>Symbol</Th>
                  <Th>Threshold</Th>
                  <Th>Direction</Th>
                  <Th>Type</Th>
                  <Th>Status</Th>
                  <Th></Th>
                </Tr>
              </Thead>
              <Tbody>
                {alerts.map(al => (
                  <Tr key={String(al.id)}>
                    <Td><span className="font-semibold text-slate-100">{String(al.symbol)}</span></Td>
                    <Td className="tabular-nums">${Number(al.threshold).toFixed(2)}</Td>
                    <Td>{dirBadge(String(al.direction))}</Td>
                    <Td>
                      <Badge variant="gray">{String(al.alert_type ?? 'price')}</Badge>
                    </Td>
                    <Td>
                      <Badge variant={al.is_active ? 'green' : 'gray'}>
                        {al.is_active ? 'Active' : 'Inactive'}
                      </Badge>
                    </Td>
                    <Td>
                      <Button variant="danger" size="xs" onClick={() => handleDeleteAlert(String(al.id))}>
                        Delete
                      </Button>
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
