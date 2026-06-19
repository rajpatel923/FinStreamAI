"use client";

import { useState, useEffect, useRef } from "react";
import { listSessions, streamChat, SessionResponse } from "@/lib/api";

interface Message {
  role: "user" | "assistant";
  content: string;
}

export default function AgentPage() {
  const [sessions, setSessions] = useState<SessionResponse[]>([]);
  const [sessionId, setSessionId] = useState<string | undefined>(undefined);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [error, setError] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    void loadSessions();
  }, []);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  async function loadSessions() {
    try {
      setSessions(await listSessions());
    } catch {
      // sessions may be empty on first load
    }
  }

  function handleNewSession() {
    setSessionId(undefined);
    setMessages([]);
    setError("");
  }

  async function handleSend(e: React.FormEvent) {
    e.preventDefault();
    if (!input.trim() || streaming) return;

    const userMessage = input.trim();
    setInput("");
    setError("");
    setMessages((prev) => [...prev, { role: "user", content: userMessage }]);
    setStreaming(true);

    try {
      const response = await streamChat(userMessage, sessionId);

      if (!response.ok) {
        throw new Error(`${response.status} ${await response.text()}`);
      }

      // Extract thread_id from response headers if present
      const newSessionId = response.headers.get("X-Session-Id") ?? response.headers.get("X-Thread-Id");
      if (newSessionId && !sessionId) {
        setSessionId(newSessionId);
        void loadSessions();
      }

      const reader = response.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let assistantContent = "";

      setMessages((prev) => [...prev, { role: "assistant", content: "" }]);

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value, { stream: true });
        const lines = chunk.split("\n");

        for (const line of lines) {
          if (line.startsWith("data: ")) {
            const data = line.slice(6).trim();
            if (data === "[DONE]") continue;
            try {
              const parsed = JSON.parse(data) as { token?: string; content?: string; session_id?: string; thread_id?: string };
              const token = parsed.token ?? parsed.content ?? "";
              assistantContent += token;
              const sid = parsed.thread_id ?? parsed.session_id;
              if (sid && !sessionId) {
                setSessionId(sid);
              }
              setMessages((prev) => {
                const updated = [...prev];
                updated[updated.length - 1] = { role: "assistant", content: assistantContent };
                return updated;
              });
            } catch {
              // plain text token
              assistantContent += data;
              setMessages((prev) => {
                const updated = [...prev];
                updated[updated.length - 1] = { role: "assistant", content: assistantContent };
                return updated;
              });
            }
          }
        }
      }
    } catch (e) {
      setError(String(e));
      setMessages((prev) => prev.filter((m) => m.content !== ""));
    } finally {
      setStreaming(false);
      void loadSessions();
    }
  }

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)] max-w-3xl">
      <h1 className="text-2xl font-bold mb-4 shrink-0">Agent Chat</h1>

      {/* Session controls */}
      <div className="flex items-center gap-3 mb-4 shrink-0">
        <select
          value={sessionId ?? ""}
          onChange={(e) => {
            setSessionId(e.target.value || undefined);
            setMessages([]);
          }}
          className="flex-1 bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-white focus:outline-none focus:border-indigo-500"
        >
          <option value="">New session</option>
          {sessions.map((s) => (
            <option key={s.id} value={s.thread_id}>
              {s.title ?? s.thread_id.slice(0, 8) + "…"}
            </option>
          ))}
        </select>
        <button
          onClick={handleNewSession}
          className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded text-sm font-medium transition-colors whitespace-nowrap"
        >
          New Session
        </button>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto bg-gray-900 border border-gray-800 rounded-xl p-4 space-y-4 mb-4">
        {messages.length === 0 && (
          <p className="text-sm text-gray-500 text-center mt-8">
            Ask anything about your portfolio, market analysis, or trading...
          </p>
        )}
        {messages.map((m, i) => (
          <div key={i} className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
            <div
              className={`max-w-[80%] rounded-xl px-4 py-2 text-sm whitespace-pre-wrap ${
                m.role === "user"
                  ? "bg-indigo-600 text-white rounded-br-none"
                  : "bg-gray-800 text-gray-100 rounded-bl-none"
              }`}
            >
              {m.content}
              {streaming && i === messages.length - 1 && m.role === "assistant" && (
                <span className="inline-block w-1.5 h-3.5 bg-indigo-400 ml-0.5 animate-pulse align-middle" />
              )}
            </div>
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      {error && (
        <p className="text-xs text-red-400 bg-red-900/20 border border-red-800 rounded px-3 py-2 mb-3 shrink-0">
          {error}
        </p>
      )}

      {/* Input */}
      <form onSubmit={handleSend} className="flex gap-2 shrink-0">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          disabled={streaming}
          placeholder="What is my portfolio risk?"
          className="flex-1 bg-gray-800 border border-gray-700 rounded-xl px-4 py-3 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-indigo-500 disabled:opacity-50"
        />
        <button
          type="submit"
          disabled={streaming || !input.trim()}
          className="px-5 py-3 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded-xl text-sm font-medium transition-colors"
        >
          {streaming ? "…" : "Send"}
        </button>
      </form>
    </div>
  );
}
