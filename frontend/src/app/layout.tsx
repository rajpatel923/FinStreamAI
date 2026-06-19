"use client";

import "./globals.css";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { clearTokens } from "@/lib/tokens";

const NAV = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/watchlist", label: "Watchlist & Alerts" },
  { href: "/ai", label: "AI Services" },
  { href: "/agent", label: "Agent Chat" },
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();

  function handleLogout() {
    clearTokens();
    router.push("/login");
  }

  const isAuth = pathname === "/login";

  return (
    <html lang="en">
      <body className="bg-gray-950 text-gray-100 min-h-screen">
        {isAuth ? (
          <main className="min-h-screen flex items-center justify-center">{children}</main>
        ) : (
          <div className="flex min-h-screen">
            <aside className="w-52 bg-gray-900 border-r border-gray-800 flex flex-col py-6 px-3 gap-1 shrink-0">
              <div className="text-sm font-bold text-indigo-400 mb-4 px-2 tracking-widest uppercase">
                FinStreamAI
              </div>
              {NAV.map((n) => (
                <Link
                  key={n.href}
                  href={n.href}
                  className={`px-3 py-2 rounded text-sm transition-colors ${
                    pathname.startsWith(n.href)
                      ? "bg-indigo-600 text-white"
                      : "text-gray-400 hover:bg-gray-800 hover:text-white"
                  }`}
                >
                  {n.label}
                </Link>
              ))}
              <div className="flex-1" />
              <button
                onClick={handleLogout}
                className="px-3 py-2 rounded text-sm text-red-400 hover:bg-gray-800 hover:text-red-300 text-left transition-colors"
              >
                Logout
              </button>
            </aside>
            <main className="flex-1 p-8 overflow-auto">{children}</main>
          </div>
        )}
      </body>
    </html>
  );
}
