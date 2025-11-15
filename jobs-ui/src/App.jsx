import { useEffect, useState } from "react";
import {
  Routes,
  Route,
  Link,
  Navigate,
  useNavigate,
} from "react-router-dom";
import { apiGet, apiPost, API_BASE_URL } from "./api";
import JobsPage from "./JobsPage.jsx";

// -----------------------------------------------------------------------------
// Home page (keeps the /health check you already had)
// -----------------------------------------------------------------------------
function HomePage() {
  const [health, setHealth] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    apiGet("/health")
      .then((data) => {
        console.log("Backend /health response:", data);
        setHealth(data);
      })
      .catch((err) => {
        console.error("Backend /health error:", err);
        setError(err.message || "Request failed");
      });
  }, []);

  return (
    <section style={{ marginTop: 24 }}>
      <h2>Backend health check</h2>
      <p>
        API base URL: <code>{API_BASE_URL}</code>
      </p>

      {health && (
        <p style={{ color: "green" }}>
          /health response: <code>{JSON.stringify(health)}</code>
        </p>
      )}

      {error && (
        <p style={{ color: "red" }}>
          Error calling /health: <code>{error}</code>
        </p>
      )}

      {!health && !error && <p>Checking backend…</p>}
    </section>
  );
}

// -----------------------------------------------------------------------------
// AdminScrapePage – real form that POSTs to /runs
// -----------------------------------------------------------------------------

// Scraper options for the multi-select checkboxes
const SCRAPERS = [
  { id: "rtx", label: "RTX" },
  { id: "lockheedmartin", label: "Lockheed Martin" },
  { id: "northropgrumman", label: "Northrop Grumman" },
  { id: "baesystems", label: "BAE Systems" },
  { id: "generaldynamics", label: "General Dynamics" },
];

function AdminScrapePage({ showToast }) {
  // default to "all scrapers selected"
  const [selectedScrapers, setSelectedScrapers] = useState(
    SCRAPERS.map((s) => s.id)
  );
  const [limit, setLimit] = useState("");
  const [since, setSince] = useState("");
  const [workers, setWorkers] = useState("");
  const [dbMode, setDbMode] = useState("min"); // 'min' | 'full'
  const [combineFull, setCombineFull] = useState(false);

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [formError, setFormError] = useState(null);

  const toggleScraper = (id) => {
    setSelectedScrapers((prev) =>
      prev.includes(id)
        ? prev.filter((x) => x !== id)
        : [...prev, id]
    );
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (selectedScrapers.length === 0) {
      setFormError("Select at least one scraper.");
      return;
    }

    setFormError(null);
    setIsSubmitting(true);

    try {
      const payload = {
        scrapers: selectedScrapers,
        db_mode: dbMode,
        combine_full: combineFull,
      };

      if (limit) {
        payload.limit = Number(limit);
      }
      if (since) {
        payload.since = since; // YYYY-MM-DD
      }
      if (workers) {
        payload.workers = Number(workers);
      }

      const data = await apiPost("/runs", payload);

      const runId =
        (data && (data.run_id || data.id)) || "unknown";

      showToast(`Scrape initiated, run ID: ${runId}`);
    } catch (err) {
      console.error("Error starting scrape:", err);
      setFormError(
        err.message || "Failed to start scrape"
      );
      showToast("Failed to start scrape");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <section style={{ marginTop: 24, maxWidth: 700 }}>
      <h2>Admin – Run Scraper</h2>
      <p style={{ color: "#555", fontSize: 14 }}>
        Configure a scrape run and start it via <code>/runs</code>.
      </p>

      <form
        onSubmit={handleSubmit}
        style={{
          marginTop: 16,
          padding: 16,
          borderRadius: 4,
          border: "1px solid #ddd",
          display: "flex",
          flexDirection: "column",
          gap: 16,
        }}
      >
        {/* Scrapers multi-select */}
        <fieldset
          style={{
            border: "1px solid #eee",
            padding: 12,
            borderRadius: 4,
          }}
        >
          <legend>Scrapers</legend>
          <p style={{ marginTop: 0, fontSize: 13, color: "#555" }}>
            Choose one or more scrapers to include in this run.
          </p>
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 8,
            }}
          >
            {SCRAPERS.map((s) => (
              <label
                key={s.id}
                style={{
                  border: "1px solid #ddd",
                  borderRadius: 4,
                  padding: "4px 8px",
                  display: "flex",
                  alignItems: "center",
                  gap: 4,
                  fontSize: 14,
                }}
              >
                <input
                  type="checkbox"
                  checked={selectedScrapers.includes(s.id)}
                  onChange={() => toggleScraper(s.id)}
                />
                {s.label}
              </label>
            ))}
          </div>
        </fieldset>

        {/* Limit */}
        <label
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 4,
            fontSize: 14,
          }}
        >
          <span>
            Limit{" "}
            <span style={{ fontSize: 12, color: "#777" }}>
              (optional, max number of jobs per scraper)
            </span>
          </span>
          <input
            type="number"
            min="1"
            value={limit}
            onChange={(e) => setLimit(e.target.value)}
            placeholder="e.g. 200"
          />
        </label>

        {/* Since date */}
        <label
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 4,
            fontSize: 14,
          }}
        >
          <span>
            Since date{" "}
            <span style={{ fontSize: 12, color: "#777" }}>
              (optional, only scrape jobs posted on/after)
            </span>
          </span>
          <input
            type="date"
            value={since}
            onChange={(e) => setSince(e.target.value)}
          />
        </label>

        {/* Workers number */}
        <label
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 4,
            fontSize: 14,
          }}
        >
          <span>
            Workers{" "}
            <span style={{ fontSize: 12, color: "#777" }}>
              (optional, concurrency)
            </span>
          </span>
          <input
            type="number"
            min="1"
            value={workers}
            onChange={(e) => setWorkers(e.target.value)}
            placeholder="e.g. 4"
          />
        </label>

        {/* DB mode radio buttons */}
        <fieldset
          style={{
            border: "1px solid #eee",
            padding: 12,
            borderRadius: 4,
          }}
        >
          <legend>DB mode</legend>
          <div style={{ display: "flex", gap: 16 }}>
            <label style={{ fontSize: 14 }}>
              <input
                type="radio"
                value="min"
                checked={dbMode === "min"}
                onChange={(e) => setDbMode(e.target.value)}
              />{" "}
              Minimal
            </label>
            <label style={{ fontSize: 14 }}>
              <input
                type="radio"
                value="full"
                checked={dbMode === "full"}
                onChange={(e) => setDbMode(e.target.value)}
              />{" "}
              Full
            </label>
          </div>
        </fieldset>

        {/* Combine full checkbox */}
        <label
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            fontSize: 14,
          }}
        >
          <input
            type="checkbox"
            checked={combineFull}
            onChange={(e) => setCombineFull(e.target.checked)}
          />
          <span>Combine full results into a single dataset</span>
        </label>

        {/* Error + submit */}
        {formError && (
          <p style={{ color: "red", fontSize: 13 }}>
            {formError}
          </p>
        )}

        <div
          style={{
            display: "flex",
            justifyContent: "flex-end",
            gap: 8,
            alignItems: "center",
          }}
        >
          <button
            type="submit"
            disabled={isSubmitting}
          >
            {isSubmitting ? "Starting…" : "Start scrape"}
          </button>
        </div>

        {/* Optional debug: show outgoing JSON payload */}
        <details>
          <summary style={{ fontSize: 13, cursor: "pointer" }}>
            Show example JSON payload
          </summary>
          <pre
            style={{
              fontSize: 12,
              background: "#f9fafb",
              padding: 8,
              borderRadius: 4,
              marginTop: 8,
            }}
          >
{JSON.stringify({
  scrapers: selectedScrapers,
  limit: limit ? Number(limit) : undefined,
  since: since || undefined,
  workers: workers ? Number(workers) : undefined,
  db_mode: dbMode,
  combine_full: combineFull,
}, null, 2)}
          </pre>
        </details>
      </form>
    </section>
  );
}

// -----------------------------------------------------------------------------
// Simple login that just sets a fake role
// -----------------------------------------------------------------------------
function LoginPage({ role, setRole }) {
  const navigate = useNavigate();

  const handleLogin = (newRole) => {
    setRole(newRole);
    // send user somewhere sensible based on role
    navigate(newRole === "admin" ? "/admin/scrape" : "/jobs", {
      replace: true,
    });
  };

  const handleLogout = () => {
    setRole(null);
    navigate("/login", { replace: true });
  };

  return (
    <section style={{ marginTop: 24 }}>
      <h2>Login</h2>
      <p>
        This is a fake login screen. Pick a role – no passwords, no backend
        involved yet.
      </p>

      <div style={{ marginTop: 12, display: "flex", gap: 8 }}>
        <button type="button" onClick={() => handleLogin("user")}>
          Log in as user
        </button>
        <button type="button" onClick={() => handleLogin("admin")}>
          Log in as admin
        </button>
        {role && (
          <button type="button" onClick={handleLogout}>
            Log out
          </button>
        )}
      </div>
    </section>
  );
}

// -----------------------------------------------------------------------------
// ProtectedRoute component
// -----------------------------------------------------------------------------
function ProtectedRoute({ role, requiredRole, children }) {
  if (!role) {
    return <Navigate to="/login" replace />;
  }
  if (requiredRole && role !== requiredRole) {
    return <Navigate to="/login" replace />;
  }
  return children;
}

// -----------------------------------------------------------------------------
// App shell with nav + routes + ToastManager
// -----------------------------------------------------------------------------
export default function App() {
  // Very dumb role state (user | admin | null)
  const [role, setRole] = useState(null);

  // ToastManager state
  const [toasts, setToasts] = useState([]);

  const showToast = (message) => {
    const id = `${Date.now()}-${Math.random()}`;
    setToasts((prev) => [...prev, { id, message }]);
    // Auto-dismiss after 4 seconds
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, 4000);
  };

  return (
    <main style={{ padding: 24, fontFamily: "Arial, sans-serif" }}>
      <h1>JobScraper UI</h1>
      <p>Basic app structure + routing demo.</p>

      {/* Simple navigation */}
      <nav
        style={{
          marginTop: 16,
          marginBottom: 16,
          paddingBottom: 8,
          borderBottom: "1px solid #ccc",
          display: "flex",
          gap: 12,
          alignItems: "center",
        }}
      >
        <Link to="/">Home</Link>
        <Link to="/jobs">Jobs</Link>

        {role === "admin" && <Link to="/admin/scrape">Admin Scrape</Link>}

        {role ? (
          <button
            onClick={() => setRole(null)}
            style={{
              background: "none",
              border: "none",
              padding: 0,
              cursor: "pointer",
              color: "rgb(43, 76, 200)", // react-router link color
              textDecoration: "underline",
              fontSize: "inherit",
            }}
          >
            Log out
          </button>
        ) : (
          <Link to="/login">Log in</Link>
        )}

        <span style={{ marginLeft: "auto", fontStyle: "italic" }}>
          Current role: <strong>{role ?? "none"}</strong>
        </span>
      </nav>

      {/* Route definitions */}
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route
          path="/login"
          element={<LoginPage role={role} setRole={setRole} />}
        />
        <Route
          path="/jobs"
          element={
            <ProtectedRoute role={role}>
              <JobsPage />
            </ProtectedRoute>
          }
        />
        <Route
          path="/admin/scrape"
          element={
            <ProtectedRoute role={role} requiredRole="admin">
              <AdminScrapePage showToast={showToast} />
            </ProtectedRoute>
          }
        />
        {/* Catch-all: redirect unknown routes to home */}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>

      {/* ToastManager */}
      <div
        style={{
          position: "fixed",
          top: 16,
          right: 16,
          display: "flex",
          flexDirection: "column",
          gap: 8,
          zIndex: 2000,
        }}
      >
        {toasts.map((t) => (
          <div
            key={t.id}
            style={{
              background: "#111827",
              color: "#f9fafb",
              padding: "8px 12px",
              borderRadius: 4,
              boxShadow: "0 4px 12px rgba(0,0,0,0.3)",
              fontSize: 14,
              maxWidth: 320,
            }}
          >
            {t.message}
          </div>
        ))}
      </div>
    </main>
  );
}
