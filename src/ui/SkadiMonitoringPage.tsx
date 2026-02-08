import React, { useEffect, useMemo, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer
} from "recharts";

/**
 * Data contracts you can back with your Skadi APIs:
 * - GET /api/metrics/live
 * - GET /api/metrics/timeseries?window=15m
 * - GET /api/queries/history?limit=200
 */

type TimeWindow = "15m" | "1h" | "6h" | "24h";

type LiveMetrics = {
  runningUncached: number;
  runningCached: number;
  clusterNodes: number;
  updatedAtIso: string;
};

type DurationPoint = {
  tsIso: string;              // ISO timestamp
  cachedMs: number | null;    // null if no samples
  uncachedMs: number | null;
};

type QueryRow = {
  startedAtIso: string;
  queryId: string;
  source: "JDBC" | "REST" | "PYTHON" | "OTHER";
  cached: boolean;
  durationMs: number | null;  // null if still running
  rows: number | null;
  status: "RUNNING" | "OK" | "FAILED";
};

function fmtTime(tsIso: string): string {
  const d = new Date(tsIso);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function fmtDateTime(tsIso: string): string {
  const d = new Date(tsIso);
  return d.toLocaleString([], { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function fmtDuration(ms: number | null): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms.toFixed(0)} ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(2)} s`;
  const m = Math.floor(s / 60);
  const rem = (s % 60).toFixed(0).padStart(2, "0");
  return `${m}:${rem} min`;
}

export default function SkadiMonitoringPage() {
  const [window, setWindow] = useState<TimeWindow>("1h");

  const [live, setLive] = useState<LiveMetrics>({
    runningUncached: 0,
    runningCached: 0,
    clusterNodes: 0,
    updatedAtIso: new Date().toISOString(),
  });

  const [series, setSeries] = useState<DurationPoint[]>([]);
  const [history, setHistory] = useState<QueryRow[]>([]);

  // Poll cadence: KPIs faster than the chart, like Databricks
  useEffect(() => {
    const pollLive = async () => {
      try {
        const r = await fetch("/api/metrics/live");
        if (!r.ok) throw new Error(`live ${r.status}`);
        setLive(await r.json());
      } catch {
        // Keep calm; don’t spam errors into UI.
      }
    };

    pollLive();
    const id = window.setInterval(pollLive, 3000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    const pollSeries = async () => {
      try {
        const r = await fetch(`/api/metrics/timeseries?window=${window}`);
        if (!r.ok) throw new Error(`series ${r.status}`);
        setSeries(await r.json());
      } catch {}
    };

    pollSeries();
    const id = window.setInterval(pollSeries, 8000);
    return () => window.clearInterval(id);
  }, [window]);

  useEffect(() => {
    const pollHistory = async () => {
      try {
        const r = await fetch(`/api/queries/history?limit=200`);
        if (!r.ok) throw new Error(`history ${r.status}`);
        setHistory(await r.json());
      } catch {}
    };

    pollHistory();
    const id = window.setInterval(pollHistory, 6000);
    return () => window.clearInterval(id);
  }, []);

  const chartData = useMemo(() => {
    // Convert to a Recharts-friendly shape
    return series.map(p => ({
      t: fmtTime(p.tsIso),
      cachedMs: p.cachedMs,
      uncachedMs: p.uncachedMs,
      tsIso: p.tsIso,
    }));
  }, [series]);

  return (
    <div className="skadi-page">
      {/* KPI STRIP */}
      <div className="kpi-strip">
        <div className="kpi">
          <div className="kpi-label">RUNNING · UNCACHED</div>
          <div className="kpi-value uncached">{live.runningUncached}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">RUNNING · CACHE HIT</div>
          <div className="kpi-value cached">{live.runningCached}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">CLUSTER NODES</div>
          <div className="kpi-value nodes">{live.clusterNodes}</div>
        </div>
      </div>

      {/* TIME SERIES */}
      <div className="panel">
        <div className="panel-header">
          <div>
            <div className="panel-title">Query duration</div>
            <div className="panel-subtitle">
              Cached vs non-cached duration over time · updated {fmtTime(live.updatedAtIso)}
            </div>
          </div>
          <div className="controls">
            <select className="select" value={window} onChange={(e) => setWindow(e.target.value as TimeWindow)}>
              <option value="15m">Last 15m</option>
              <option value="1h">Last 1h</option>
              <option value="6h">Last 6h</option>
              <option value="24h">Last 24h</option>
            </select>
          </div>
        </div>

        <div style={{ width: "100%", height: 280 }}>
          <ResponsiveContainer>
            <LineChart data={chartData} margin={{ top: 10, right: 18, bottom: 0, left: 6 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="t" tick={{ fontSize: 12 }} />
              <YAxis
                tick={{ fontSize: 12 }}
                tickFormatter={(v) => fmtDuration(v)}
              />
              <Tooltip
                formatter={(value: any, name: any, props: any) => [fmtDuration(value), name === "cachedMs" ? "Cached" : "Non-cached"]}
                labelFormatter={(_, payload) => payload?.[0]?.payload?.tsIso ? fmtDateTime(payload[0].payload.tsIso) : ""}
              />

              {/* IMPORTANT: we don’t set chart colors in code if you prefer CSS-driven theming,
                  but Recharts needs stroke values. Keep them token-aligned. */}
              <Line
                type="monotone"
                dataKey="cachedMs"
                name="Cached"
                stroke="var(--accent-cached)"
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
              <Line
                type="monotone"
                dataKey="uncachedMs"
                name="Non-cached"
                stroke="var(--accent-uncached)"
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* HISTORY */}
      <div className="panel">
        <div className="panel-header">
          <div>
            <div className="panel-title">History</div>
            <div className="panel-subtitle">Most recent queries</div>
          </div>

          {/* (Optional) add filters later: cached-only, status, search queryId */}
          <div className="controls">
            <span className="panel-subtitle">{history.length} rows</span>
          </div>
        </div>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th style={{ width: 170 }}>Time</th>
                <th style={{ width: 260 }}>Query ID</th>
                <th style={{ width: 110 }}>Source</th>
                <th style={{ width: 120 }}>Cached</th>
                <th style={{ width: 130 }}>Duration</th>
                <th style={{ width: 110 }}>Rows</th>
                <th style={{ width: 110 }}>Status</th>
              </tr>
            </thead>
            <tbody>
              {history.map((q) => (
                <tr key={q.queryId}>
                  <td>{fmtDateTime(q.startedAtIso)}</td>
                  <td>
                    <a href={`/ui/query/${encodeURIComponent(q.queryId)}`}>{q.queryId}</a>
                  </td>
                  <td>{q.source}</td>
                  <td>
                    {q.cached ? (
                      <span className="badge cached">CACHED</span>
                    ) : (
                      <span className="badge uncached">UNCACHED</span>
                    )}
                  </td>
                  <td>{fmtDuration(q.durationMs)}</td>
                  <td>{q.rows ?? "—"}</td>
                  <td>
                    <span className={`status ${q.status.toLowerCase()}`}>
                      {q.status}
                    </span>
                  </td>
                </tr>
              ))}
              {history.length === 0 && (
                <tr>
                  <td colSpan={7} style={{ padding: "14px 12px", color: "var(--muted)" }}>
                    No history yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
