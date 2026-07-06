"use client";

import { FormEvent, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  confirmOrder,
  createChat,
  createPosition,
  deletePosition,
  deleteSession,
  getPortfolioSummary,
  listPositions,
  listSessionMessages,
  listSessions,
  proposeTrade,
  streamChat,
  type JsonRecord,
  type PositionResponse,
  type SessionResponse,
} from "@/lib/api";
import { getToken } from "@/lib/tokens";
import { Button, EmptyState, ErrorBanner, Field, JsonBlock, PageShell, Section, TextArea, getErrorMessage } from "@/components/ui";

export default function AgentPage() {
  const router = useRouter();
  const [sessions, setSessions] = useState<SessionResponse[]>([]);
  const [sessionId, setSessionId] = useState("");
  const [messages, setMessages] = useState<unknown>(null);
  const [chatInput, setChatInput] = useState("Summarize my current portfolio risk.");
  const [chatOutput, setChatOutput] = useState("");
  const [positions, setPositions] = useState<PositionResponse[]>([]);
  const [summary, setSummary] = useState<unknown>(null);
  const [positionJson, setPositionJson] = useState('{"symbol":"AAPL","quantity":10,"average_price":190}');
  const [tradeJson, setTradeJson] = useState('{"symbol":"AAPL","side":"buy","quantity":1,"order_type":"market"}');
  const [order, setOrder] = useState<JsonRecord | null>(null);
  const [error, setError] = useState("");
  const [streaming, setStreaming] = useState(false);

  async function load() {
    setError("");
    try {
      const [sessionList, positionList, portfolioSummary] = await Promise.allSettled([listSessions(), listPositions(), getPortfolioSummary()]);
      setSessions(sessionList.status === "fulfilled" ? sessionList.value || [] : []);
      setPositions(positionList.status === "fulfilled" ? positionList.value || [] : []);
      setSummary(portfolioSummary.status === "fulfilled" ? portfolioSummary.value : { error: getErrorMessage(portfolioSummary.reason) });
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  useEffect(() => {
    if (!getToken()) router.replace("/login");
    else void load();
  }, [router]);

  async function sendChat(event: FormEvent) {
    event.preventDefault();
    setError("");
    setChatOutput("");
    setStreaming(true);
    try {
      const response = await streamChat(chatInput, sessionId || undefined);
      if (!response.ok || !response.body) throw new Error(`${response.status} ${await response.text()}`);
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let output = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        output += decoder.decode(value, { stream: true });
        setChatOutput(output);
      }
      await load();
    } catch (err) {
      try {
        const fallback = await createChat(chatInput, sessionId || undefined);
        setChatOutput(JSON.stringify(fallback, null, 2));
        await load();
      } catch (fallbackErr) {
        setError(getErrorMessage(fallbackErr || err));
      }
    } finally {
      setStreaming(false);
    }
  }

  async function loadMessages(id: string) {
    setError("");
    setSessionId(id);
    try {
      setMessages(await listSessionMessages(id));
    } catch (err) {
      setMessages({ error: getErrorMessage(err) });
    }
  }

  async function removeSession(id: string) {
    setError("");
    try {
      await deleteSession(id);
      if (sessionId === id) {
        setSessionId("");
        setMessages(null);
      }
      await load();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function addPosition(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      await createPosition(JSON.parse(positionJson) as JsonRecord);
      await load();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function removePosition(position: PositionResponse) {
    const id = String(position.id || position.position_id || "");
    if (!id) return;
    setError("");
    try {
      await deletePosition(id);
      await load();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function submitTrade(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      const created = await proposeTrade(JSON.parse(tradeJson) as JsonRecord);
      setOrder(created as JsonRecord);
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function confirmTrade() {
    const id = String(order?.id || order?.order_id || "");
    if (!id) return;
    setError("");
    try {
      await confirmOrder(id);
      setOrder({ ...order, status: "confirmed" });
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  return (
    <PageShell title="Agent" description="Chat with the trading agent, inspect sessions, manage positions, and propose trades.">
      <ErrorBanner message={error} />
      <div className="grid gap-4 lg:grid-cols-3">
        <Section title="Chat">
          <form onSubmit={sendChat} className="space-y-3">
            <Field label="Session ID" value={sessionId} onChange={setSessionId} />
            <TextArea label="Message" value={chatInput} onChange={setChatInput} rows={4} />
            <Button type="submit" disabled={streaming || !chatInput.trim()}>
              {streaming ? "Streaming..." : "Send"}
            </Button>
          </form>
          <div className="mt-3">
            <JsonBlock value={chatOutput || "No response yet."} />
          </div>
        </Section>
        <Section title="Sessions">
          {!sessions.length ? <EmptyState>No sessions yet.</EmptyState> : null}
          <div className="space-y-2">
            {sessions.map((session, index) => {
              const id = String(session.id || session.session_id || index);
              return (
                <div key={id} className="rounded border border-gray-800 p-3">
                  <div className="mb-2 truncate text-sm">{session.title || id}</div>
                  <div className="flex gap-2">
                    <Button variant="secondary" onClick={() => loadMessages(id)}>
                      Messages
                    </Button>
                    <Button variant="danger" onClick={() => removeSession(id)}>
                      Delete
                    </Button>
                  </div>
                </div>
              );
            })}
          </div>
        </Section>
        <Section title="Messages">
          <JsonBlock value={messages || "Select a session."} />
        </Section>
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <Section title="Portfolio summary">
          <JsonBlock value={summary || "No summary loaded."} />
        </Section>
        <Section title="Positions">
          <form onSubmit={addPosition} className="mb-3 space-y-3">
            <TextArea label="Position JSON" value={positionJson} onChange={setPositionJson} rows={4} />
            <Button type="submit">Add position</Button>
          </form>
          {!positions.length ? <EmptyState>No positions loaded.</EmptyState> : null}
          <div className="space-y-2">
            {positions.map((position, index) => (
              <div key={String(position.id || position.position_id || index)} className="flex items-center justify-between gap-3 rounded border border-gray-800 p-3">
                <div className="truncate text-sm">{String(position.symbol || position.asset || "Position")}</div>
                <Button variant="danger" onClick={() => removePosition(position)}>
                  Remove
                </Button>
              </div>
            ))}
          </div>
        </Section>
        <Section title="Trading">
          <form onSubmit={submitTrade} className="space-y-3">
            <TextArea label="Trade JSON" value={tradeJson} onChange={setTradeJson} rows={5} />
            <Button type="submit">Propose trade</Button>
          </form>
          <div className="mt-3 space-y-3">
            <JsonBlock value={order || "No order proposal yet."} />
            <Button disabled={!order} onClick={confirmTrade}>
              Confirm order
            </Button>
          </div>
        </Section>
      </div>
    </PageShell>
  );
}
