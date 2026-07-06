"use client";

import { usePathname, useRouter } from "next/navigation";
import { ReactNode, useEffect, useState } from "react";
import { clearTokens, getToken } from "@/lib/tokens";

const nav = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/watchlist", label: "Watchlist" },
  { href: "/ai", label: "AI" },
  { href: "/agent", label: "Agent" },
  { href: "/data", label: "Data" },
  { href: "/account", label: "Account" },
];

export function AppFrame({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const isLogin = pathname === "/login";
  const [authed, setAuthed] = useState(false);

  useEffect(() => {
    setAuthed(Boolean(getToken()));
  }, [pathname]);

  if (isLogin) return <div className="min-h-screen bg-gray-950 text-gray-100">{children}</div>;

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <aside className="fixed inset-y-0 left-0 hidden w-56 border-r border-gray-800 bg-black px-3 py-5 md:block">
        <div className="mb-6 px-3 text-lg font-semibold">FinStreamAI</div>
        <nav className="space-y-1">
          {nav.map((item) => (
            <button
              key={item.href}
              className={`block w-full rounded px-3 py-2 text-left text-sm ${
                pathname === item.href ? "bg-indigo-600 text-white" : "text-gray-300 hover:bg-gray-900 hover:text-white"
              }`}
              onClick={() => router.push(item.href)}
            >
              {item.label}
            </button>
          ))}
        </nav>
        {authed ? (
          <button
            className="absolute bottom-5 left-3 right-3 rounded border border-gray-800 px-3 py-2 text-sm text-gray-300 hover:bg-gray-900"
            onClick={() => {
              clearTokens();
              router.push("/login");
            }}
          >
            Logout
          </button>
        ) : null}
      </aside>
      <header className="sticky top-0 z-10 border-b border-gray-800 bg-black px-3 py-3 md:hidden">
        <div className="mb-3 flex items-center justify-between">
          <span className="font-semibold">FinStreamAI</span>
          <button
            className="rounded border border-gray-800 px-3 py-1 text-sm"
            onClick={() => {
              clearTokens();
              router.push("/login");
            }}
          >
            Logout
          </button>
        </div>
        <nav className="flex gap-2 overflow-x-auto">
          {nav.map((item) => (
            <button
              key={item.href}
              className={`whitespace-nowrap rounded px-3 py-2 text-sm ${
                pathname === item.href ? "bg-indigo-600 text-white" : "bg-gray-900 text-gray-300"
              }`}
              onClick={() => router.push(item.href)}
            >
              {item.label}
            </button>
          ))}
        </nav>
      </header>
      <div className="p-4 md:ml-56 md:p-6">{children}</div>
    </div>
  );
}
