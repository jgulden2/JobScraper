import { useEffect, useState } from "react";
import { apiGet, API_BASE_URL } from "./api";

function formatDateTime(value) {
  if (!value) return "";
  try {
    const d = new Date(value);
    if (!Number.isNaN(d.getTime())) {
      return d.toLocaleString();
    }
    return String(value);
  } catch {
    return String(value);
  }
}

function summarizeArgs(args) {
  if (!args || typeof args !== "object") return "";
  const parts = [];

  if (Array.isArray(args.scrapers) && args.scrapers.length > 0) {
    parts.push(`scrapers=${args.scrapers.join(",")}`);
  }
  if (args.db_mode) {
    parts.push(`db_mode=${args.db_mode}`);
  }
  if (args.limit != null) {
    parts.push(`limit=${args.limit}`);
  }
  if (args.since) {
    parts.push(`since=${args.since}`);
  }
  if (args.workers != null) {
    parts.push(`workers=${args.workers}`);
  }
  if (args.combine_full != null) {
    parts.push(`combine_full=${String(args.combine_full)}`);
  }

  return parts.join(" • ");
}

function RunLogsModal({ run, onClose }) {
  const [logText, setLogText] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function fetchLogs() {
      setIsLoading(true);
      setError(null);
      try {
        // /runs/<id>/logs returns plain text, not JSON
        const resp = await fetch(`${API_BASE_URL}/runs/${run.id}/logs`);
        if (!resp.ok) {
          const text = await resp.text().catch(() => "");
          const err = new Error(
            `GET /runs/${run.id}/logs failed: ${resp.status}`
          );
          err.body = text;
          throw err;
        }
        const text = await resp.text();
        if (!cancelled) {
          setLogText(text || "(no logs yet)");
        }
      } catch (err) {
        console.error("Error fetching logs:", err);
        if (!cancelled) {
          setError(err.message || "Failed to fetch logs");
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    fetchLogs();

    const handleKey = (e) => {
      if (e.key === "Escape") {
        onClose();
      }
    };
    window.addEventListener("keydown", handleKey);

    return () => {
      cancelled = true;
      window.removeEventListener("keydown", handleKey);
    };
  }, [run.id, onClose]);

  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.45)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          width: "90%",
          maxWidth: 900,
          maxHeight: "80vh",
          background: "white",
          borderRadius: 6,
          boxShadow: "0 10px 30px rgba(0,0,0,0.25)",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <div
          style={{
            padding: "8px 12px",
            borderBottom: "1px solid #e5e7eb",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div>
            <div style={{ fontWeight: 600, fontSize: 14 }}>
              Run logs – {run.id}
            </div>
            <div style={{ fontSize: 12, color: "#6b7280" }}>
              Status: {run.status ?? "unknown"}
            </div>
          </div>
          <button type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <div
          style={{
            padding: 12,
            borderBottom: "1px solid #e5e7eb",
            fontSize: 12,
            color: "#4b5563",
          }}
        >
          <div style={{ marginBottom: 4 }}>
            <strong>Args:</strong>{" "}
            <span>{summarizeArgs(run.args) || "(none)"}</span>
          </div>
          {run.created_at && (
            <div>
              <strong>Created:</strong> {formatDateTime(run.created_at)}
            </div>
          )}
        </div>

        <div
          style={{
            padding: 12,
            flex: 1,
            overflow: "auto",
            background: "#0b1020",
            color: "#e5e7eb",
            fontFamily: "Menlo, Consolas, monospace",
            fontSize: 12,
            whiteSpace: "pre-wrap",
          }}
        >
          {isLoading && !error && <div>Loading logs…</div>}
          {error && (
            <div style={{ color: "#fecaca" }}>
              Error loading logs: <code>{error}</code>
            </div>
          )}
          {!isLoading && !error && <pre>{logText}</pre>}
        </div>
      </div>
    </div>
  );
}

export default function AdminRunsPage() {
  const [runs, setRuns] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedRun, setSelectedRun] = useState(null);

  const fetchRuns = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await apiGet("/runs");
      // Backend returns a list of runs; if it changes to an object, adjust here
      setRuns(Array.isArray(data) ? data : []);
    } catch (err) {
      console.error("Error fetching runs:", err);
      setError(err.message || "Failed to load runs");
      setRuns([]);
    } finally {
      setIsLoading(false);
    }
  };

  // Initial load
  useEffect(() => {
    fetchRuns();
    // We intentionally omit fetchRuns from deps so this runs only once;
    // polling is handled separately.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const hasRunning = runs.some((r) => r.status === "running");

  // Poll every 5s while any run is "running"
  useEffect(() => {
    if (!hasRunning) return;

    const id = setInterval(() => {
      fetchRuns();
    }, 5000);

    return () => clearInterval(id);
  }, [hasRunning]);

  return (
    <section style={{ marginTop: 24 }}>
      <h2>Admin – Runs & Logs</h2>
      <p style={{ fontSize: 14, color: "#4b5563" }}>
        View current and previous runs. Click a row to open the logs.
      </p>

      <div style={{ marginTop: 8, marginBottom: 8, fontSize: 13 }}>
        <button type="button" onClick={fetchRuns} disabled={isLoading}>
          {isLoading ? "Refreshing…" : "Refresh"}
        </button>
        {hasRunning && (
          <span style={{ marginLeft: 12, color: "#059669" }}>
            Auto-refreshing while runs are in progress…
          </span>
        )}
      </div>

      {error && (
        <p style={{ color: "red", fontSize: 13 }}>
          Error: <code>{error}</code>
        </p>
      )}

      {runs.length === 0 && !isLoading && !error && (
        <p style={{ fontSize: 14, color: "#6b7280" }}>
          No runs have been started yet.
        </p>
      )}

      {(runs.length > 0 || isLoading) && (
        <table
          style={{
            marginTop: 8,
            borderCollapse: "collapse",
            width: "100%",
            fontSize: 13,
          }}
        >
          <thead>
            <tr>
              <th
                style={{
                  borderBottom: "1px solid #d1d5db",
                  textAlign: "left",
                  padding: "6px 8px",
                  backgroundColor: "#f3f4f6",
                }}
              >
                Run ID
              </th>
              <th
                style={{
                  borderBottom: "1px solid #d1d5db",
                  textAlign: "left",
                  padding: "6px 8px",
                  backgroundColor: "#f3f4f6",
                }}
              >
                Created At
              </th>
              <th
                style={{
                  borderBottom: "1px solid #d1d5db",
                  textAlign: "left",
                  padding: "6px 8px",
                  backgroundColor: "#f3f4f6",
                }}
              >
                Status
              </th>
              <th
                style={{
                  borderBottom: "1px solid #d1d5db",
                  textAlign: "left",
                  padding: "6px 8px",
                  backgroundColor: "#f3f4f6",
                }}
              >
                Args summary
              </th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => (
              <tr
                key={run.id}
                onClick={() => setSelectedRun(run)}
                style={{
                  cursor: "pointer",
                  backgroundColor:
                    run.status === "running" ? "#ecfdf5" : "transparent",
                }}
              >
                <td
                  style={{
                    borderBottom: "1px solid #e5e7eb",
                    padding: "4px 8px",
                    fontFamily: "Menlo, Consolas, monospace",
                  }}
                >
                  {run.id}
                </td>
                <td
                  style={{
                    borderBottom: "1px solid #e5e7eb",
                    padding: "4px 8px",
                  }}
                >
                  {formatDateTime(run.created_at)}
                </td>
                <td
                  style={{
                    borderBottom: "1px solid #e5e7eb",
                    padding: "4px 8px",
                    textTransform: "capitalize",
                  }}
                >
                  {run.status || "unknown"}
                </td>
                <td
                  style={{
                    borderBottom: "1px solid #e5e7eb",
                    padding: "4px 8px",
                    color: "#4b5563",
                  }}
                >
                  {summarizeArgs(run.args) || "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {selectedRun && (
        <RunLogsModal run={selectedRun} onClose={() => setSelectedRun(null)} />
      )}
    </section>
  );
}
