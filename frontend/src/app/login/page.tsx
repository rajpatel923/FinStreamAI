"use client";

import { FormEvent, useState } from "react";
import { useRouter } from "next/navigation";
import { ErrorBanner, Field, Button, getErrorMessage } from "@/components/ui";
import { login, register } from "@/lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [mode, setMode] = useState<"login" | "register">("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [fullName, setFullName] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError("");
    setLoading(true);
    try {
      if (mode === "register") await register(email, password, fullName);
      await login(email, password);
      router.replace("/dashboard");
    } catch (err) {
      setError(getErrorMessage(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="flex min-h-screen items-center justify-center px-4">
      <form onSubmit={submit} className="w-full max-w-sm space-y-4 rounded border border-gray-800 bg-black p-5">
        <div>
          <h1 className="text-xl font-semibold">{mode === "login" ? "Login" : "Create account"}</h1>
          <p className="mt-1 text-sm text-gray-400">Access the FinStreamAI workspace.</p>
        </div>
        <ErrorBanner message={error} />
        {mode === "register" ? <Field label="Full name" value={fullName} onChange={setFullName} /> : null}
        <Field label="Email" type="email" value={email} onChange={setEmail} required />
        <Field label="Password" type="password" value={password} onChange={setPassword} required />
        <Button type="submit" disabled={loading}>
          {loading ? "Working..." : mode === "login" ? "Login" : "Register"}
        </Button>
        <button
          type="button"
          className="text-sm text-indigo-300 hover:text-indigo-200"
          onClick={() => {
            setError("");
            setMode(mode === "login" ? "register" : "login");
          }}
        >
          {mode === "login" ? "Need an account? Register" : "Already have an account? Login"}
        </button>
      </form>
    </main>
  );
}
