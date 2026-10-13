export interface HealthState {
  ok: boolean;
  latencyMs: number;
}

export async function fetchHealth(base: string): Promise<HealthState> {
  const start = Date.now();
  const response = await fetch(`${base}/health`);
  return {
    ok: response.ok,
    latencyMs: Date.now() - start
  };
}

export function renderHealth(state: HealthState): string {
  return state.ok ? `online ${state.latencyMs}ms` : `offline`;
}
