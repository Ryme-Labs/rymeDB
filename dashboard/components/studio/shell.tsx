"use client";

import {
  Activity,
  Archive,
  Database,
  GitBranch,
  Home,
  KeyRound,
  Radio,
  Search,
  SquareTerminal,
  Table2,
} from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@radix-ui/react-tooltip";
import { useConnection } from "@/lib/connection";
import { cn } from "@/lib/cn";
import { Button, Card, CardContent, CardHeader, Field, Input, Pill } from "@/components/studio/ui";
import { useState } from "react";

const NAV = [
  { href: "/", label: "Home", icon: Home },
  { href: "/table", label: "Table Editor", icon: Table2 },
  { href: "/sql", label: "SQL Editor", icon: SquareTerminal },
  { href: "/branches", label: "Branches", icon: GitBranch },
  { href: "/backups", label: "Backups", icon: Archive },
  { href: "/realtime", label: "Realtime", icon: Radio },
  { href: "/search", label: "Search", icon: Search },
  { href: "/auth", label: "Auth & Billing", icon: KeyRound },
  { href: "/observe", label: "Observe", icon: Activity },
];

function Logo() {
  return (
    <img
      src="/dashboard/rymedb-logo.png"
      alt="rymeDB"
      className="size-8 rounded-md object-contain"
    />
  );
}

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const { key, connected, latencyMs, initialized, needsSetup, login, bootstrap, logout } = useConnection();
  const [userId, setUserId] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [pending, setPending] = useState(false);

  async function authenticate(action: (id: string, password: string) => Promise<void>) {
    setPending(true);
    setError("");
    try { await action(userId, password); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
    finally { setPending(false); }
  }

  if (!initialized || !key) {
    return (
      <main className="flex min-h-screen items-center justify-center bg-surface-100 p-4">
        <Card className="w-full max-w-md">
          <CardHeader title={needsSetup ? "Set up your rymeDB admin" : "Sign in to rymeDB"}
            description={needsSetup ? "Create the first administrator account. This can only be done once." : "Sign in with your database account."} />
          <CardContent>
            {!initialized ? <p className="text-sm text-foreground-muted">Connecting to this database…</p> : <>
              <Field label="Administrator ID"><Input autoComplete="username" value={userId} onChange={(e) => setUserId(e.target.value)} /></Field>
              <Field label="Password" className="mt-3"><Input type="password" autoComplete={needsSetup ? "new-password" : "current-password"} value={password} onChange={(e) => setPassword(e.target.value)} /></Field>
              {error ? <p role="alert" className="mt-3 text-sm text-red-400">{error}</p> : null}
              <Button variant="primary" className="mt-4 w-full" disabled={pending || !userId || password.length < 8}
                onClick={() => void authenticate(needsSetup ? bootstrap : login)}>
                {pending ? "Please wait…" : needsSetup ? "Create administrator" : "Sign in"}
              </Button>
            </>}
          </CardContent>
        </Card>
      </main>
    );
  }

  return (
    <TooltipProvider delayDuration={200}>
      <div className="flex h-screen overflow-hidden">
        {/* Icon rail — mirrors Studio's outer project rail */}
        <aside className="flex w-14 shrink-0 flex-col items-center gap-1 border-r border-border-muted bg-surface-100 py-3">
          <Logo />
          <div className="my-2 h-px w-8 bg-border-muted" />
          {NAV.map((item) => {
            const active =
              item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
            const Icon = item.icon;
            return (
              <Tooltip key={item.href}>
                <TooltipTrigger asChild>
                  <Link
                    href={item.href}
                    aria-label={item.label}
                    className={cn(
                      "flex size-9 items-center justify-center rounded-md transition-colors",
                      active
                        ? "bg-brand-600/15 text-brand-400"
                        : "text-foreground-muted hover:bg-surface-300 hover:text-foreground-default",
                    )}
                  >
                    <Icon className="size-[18px]" />
                  </Link>
                </TooltipTrigger>
                <TooltipContent
                  side="right"
                  className="rounded-md border border-border-default bg-surface-300 px-2 py-1 text-xs text-foreground-default"
                >
                  {item.label}
                </TooltipContent>
              </Tooltip>
            );
          })}
          <div className="mt-auto flex flex-col items-center gap-1 text-foreground-muted">
            <Database className="size-4" />
          </div>
        </aside>

        <div className="flex min-w-0 flex-1 flex-col">
          {/* Top bar — mirrors Studio's breadcrumb + connection bar */}
          <header className="flex shrink-0 flex-wrap items-center gap-2 border-b border-border-muted bg-surface-100 px-4 py-2">
            <div className="mr-2 hidden items-center gap-2 sm:flex">
              <span className="text-[13px] font-semibold">rymeDB Admin</span>
              <span className="text-xs text-foreground-muted">
                / {NAV.find((n) => (n.href === "/" ? pathname === "/" : pathname.startsWith(n.href)))?.label ?? "Home"}
              </span>
            </div>
            <div className="ml-auto flex flex-wrap items-center gap-2">
              <Button onClick={logout}>Sign out</Button>
              {connected === null ? (
                <Pill tone="muted">idle</Pill>
              ) : connected ? (
                <Pill tone="ok">online{latencyMs !== null ? ` ${latencyMs}ms` : ""}</Pill>
              ) : (
                <Pill tone="bad">offline</Pill>
              )}
            </div>
          </header>

          {/* Main content */}
          <main className="min-h-0 flex-1 overflow-auto bg-surface-100">
            <div className="mx-auto max-w-6xl px-4 py-5 sm:px-6">{children}</div>
          </main>
        </div>
      </div>
    </TooltipProvider>
  );
}
