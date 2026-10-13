"use client";

import { useEffect, useRef, useState } from "react";

/** Minimal live-feed hook for the documented WS stream paths. */
export function useFeed(url: string | null) {
  const [lines, setLines] = useState<string[]>([]);
  const [live, setLive] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const ws = useRef<WebSocket | null>(null);

  useEffect(() => {
    return () => {
      ws.current?.close();
      ws.current = null;
    };
  }, []);

  useEffect(() => {
    if (!url) return;
    return () => {
      ws.current?.close();
      ws.current = null;
    };
  }, [url]);

  function start(target: string) {
    stop();
    setLines([]);
    setError(null);
    try {
      const sock = new WebSocket(target);
      ws.current = sock;
      setLive(true);
      sock.onmessage = (ev) => {
        setLines((prev) => [...prev.slice(-499), String(ev.data)]);
      };
      sock.onerror = () => setError("socket error");
      sock.onclose = () => setLive(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setLive(false);
    }
  }

  function stop() {
    try {
      ws.current?.close();
    } catch {
      /* closed */
    }
    ws.current = null;
    setLive(false);
  }

  return { lines, live, error, start, stop };
}
