"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { authToken, normalizeBase, type Connection } from "@/lib/api";

interface ConnectionState extends Connection {
  initialized: boolean;
  needsSetup: boolean;
  connected: boolean | null;
  latencyMs: number | null;
  login: (id: string, password: string) => Promise<void>;
  bootstrap: (id: string, password: string) => Promise<void>;
  logout: () => void;
  refresh: () => Promise<void>;
}

const Ctx = createContext<ConnectionState | null>(null);
const TOKEN_KEY = "rymedb.dashboard.session";

async function requestToken(id: string, password: string): Promise<string> {
  const result = await authToken({ base: window.location.origin, key: "" }, id, password);
  if (!result || typeof result !== "object" || !("key" in result) || typeof result.key !== "string") {
    throw new Error("Login response did not contain a session token");
  }
  return result.key;
}

export function ConnectionProvider({ children }: { children: ReactNode }) {
  const [key, setKey] = useState("");
  const [initialized, setInitialized] = useState(false);
  const [needsSetup, setNeedsSetup] = useState(false);
  const [connected, setConnected] = useState<boolean | null>(null);
  const [latencyMs, setLatencyMs] = useState<number | null>(null);
  const base = typeof window === "undefined" ? "" : window.location.origin;

  const refresh = useCallback(async () => {
    try {
      const response = await fetch("/v1/dashboard/setup", { cache: "no-store" });
      if (!response.ok) throw new Error("Unable to check dashboard setup");
      const data = (await response.json()) as { needs_setup: boolean };
      setNeedsSetup(data.needs_setup);
      setInitialized(true);
    } catch {
      setInitialized(true);
      setConnected(false);
    }
  }, []);

  useEffect(() => {
    setKey(window.sessionStorage.getItem(TOKEN_KEY) ?? "");
    void refresh();
  }, [refresh]);

  const establish = useCallback(async (id: string, password: string) => {
    const token = await requestToken(id, password);
    window.sessionStorage.setItem(TOKEN_KEY, token);
    setKey(token);
    setNeedsSetup(false);
  }, []);
  const login = useCallback((id: string, password: string) => establish(id, password), [establish]);
  const bootstrap = useCallback(async (id: string, password: string) => {
    const response = await fetch("/v1/dashboard/bootstrap", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id, password }),
    });
    if (!response.ok) throw new Error(`Setup failed (${response.status}): ${(await response.text()).slice(0, 240)}`);
    await establish(id, password);
  }, [establish]);
  const logout = useCallback(() => {
    window.sessionStorage.removeItem(TOKEN_KEY);
    setKey("");
  }, []);

  useEffect(() => {
    if (!base || !key) return;
    const start = Date.now();
    void fetch(`${normalizeBase(base)}/health`).then((response) => {
      setConnected(response.ok);
      setLatencyMs(response.ok ? Date.now() - start : null);
    }).catch(() => {
      setConnected(false);
      setLatencyMs(null);
    });
  }, [base, key]);

  const value = useMemo(() => ({ base, key, initialized, needsSetup, connected, latencyMs, login, bootstrap, logout, refresh }),
    [base, key, initialized, needsSetup, connected, latencyMs, login, bootstrap, logout, refresh]);
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useConnection(): ConnectionState {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useConnection must be used inside ConnectionProvider");
  return ctx;
}
