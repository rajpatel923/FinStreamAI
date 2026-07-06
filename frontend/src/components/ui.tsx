"use client";

import Link from "next/link";
import { ReactNode } from "react";

export function PageShell({ title, description, children }: { title: string; description?: string; children: ReactNode }) {
  return (
    <main className="space-y-6">
      <header>
        <h1 className="text-2xl font-semibold text-gray-100">{title}</h1>
        {description ? <p className="mt-1 text-sm text-gray-400">{description}</p> : null}
      </header>
      {children}
    </main>
  );
}

export function Section({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
  return (
    <section className="rounded border border-gray-800 bg-gray-950 p-4">
      <div className="mb-4 flex items-center justify-between gap-3">
        <h2 className="text-base font-medium text-gray-100">{title}</h2>
        {action}
      </div>
      {children}
    </section>
  );
}

export function Field({
  label,
  value,
  onChange,
  type = "text",
  placeholder,
  required,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  type?: string;
  placeholder?: string;
  required?: boolean;
}) {
  return (
    <label className="block text-sm text-gray-300">
      <span className="mb-1 block">{label}</span>
      <input
        className="w-full rounded border border-gray-700 bg-gray-900 px-3 py-2 text-gray-100 outline-none focus:border-indigo-500"
        type={type}
        value={value}
        placeholder={placeholder}
        required={required}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

export function TextArea({
  label,
  value,
  onChange,
  placeholder,
  rows = 4,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  rows?: number;
}) {
  return (
    <label className="block text-sm text-gray-300">
      <span className="mb-1 block">{label}</span>
      <textarea
        className="w-full rounded border border-gray-700 bg-gray-900 px-3 py-2 text-gray-100 outline-none focus:border-indigo-500"
        value={value}
        placeholder={placeholder}
        rows={rows}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

export function Button({
  children,
  type = "button",
  disabled,
  variant = "primary",
  onClick,
}: {
  children: ReactNode;
  type?: "button" | "submit";
  disabled?: boolean;
  variant?: "primary" | "secondary" | "danger";
  onClick?: () => void;
}) {
  const classes = {
    primary: "border-indigo-500 bg-indigo-600 text-white hover:bg-indigo-500",
    secondary: "border-gray-700 bg-gray-900 text-gray-100 hover:bg-gray-800",
    danger: "border-red-700 bg-red-950 text-red-200 hover:bg-red-900",
  };
  return (
    <button
      type={type}
      disabled={disabled}
      onClick={onClick}
      className={`rounded border px-3 py-2 text-sm disabled:cursor-not-allowed disabled:opacity-50 ${classes[variant]}`}
    >
      {children}
    </button>
  );
}

export function ErrorBanner({ message }: { message?: string | null }) {
  if (!message) return null;
  return <div className="rounded border border-red-900 bg-red-950 px-3 py-2 text-sm text-red-200">{message}</div>;
}

export function EmptyState({ children }: { children: ReactNode }) {
  return <div className="rounded border border-dashed border-gray-800 px-4 py-6 text-center text-sm text-gray-500">{children}</div>;
}

export function JsonBlock({ value }: { value: unknown }) {
  return (
    <pre className="max-h-96 overflow-auto rounded border border-gray-800 bg-gray-900 p-3 text-xs text-gray-200">
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

export function StatusPill({ ok, label }: { ok?: boolean; label: string }) {
  return (
    <span className={`rounded px-2 py-1 text-xs ${ok ? "bg-emerald-950 text-emerald-300" : "bg-red-950 text-red-300"}`}>
      {label}
    </span>
  );
}

export function NavLink({ href, children }: { href: string; children: ReactNode }) {
  return (
    <Link className="rounded px-3 py-2 text-sm text-gray-300 hover:bg-gray-900 hover:text-white" href={href}>
      {children}
    </Link>
  );
}

export function getErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
