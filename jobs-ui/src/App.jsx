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
import AdminRunsPage from "./AdminRunsPage.jsx";
import AdminUsersPage from "./AdminUsersPage.jsx";


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
  { id: "usajobs", label: "USAJOBS" },
  { id: "boeing", label: "Boeing" },
  { id: "l3harris", label: "L3Harris" },
  { id: "hii", label: "HII" },
  { id: "leidos", label: "Leidos" },
  { id: "thales", label: "Thales" },
  { id: "boozallen", label: "Booz Allen" },
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
// Login / Register page – toggle between /auth/login and /auth/register
// -----------------------------------------------------------------------------
function LoginPage({ onLogin }) {
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [mode, setMode] = useState("login"); // "login" | "register"
  const [error, setError] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const isRegister = mode === "register";

  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);

    try {
      const path = isRegister ? "/auth/register" : "/auth/login";
      const user = await apiPost(path, { email, password });

      onLogin(user); // { id, email, role }

      // After login/register, send admin to admin page, others to jobs
      navigate(user.role === "admin" ? "/admin/scrape" : "/jobs", {
        replace: true,
      });
    } catch (err) {
      console.error("Auth failed:", err);

      // Basic friendly messages
      if (isRegister) {
        // backend returns 409 for email already registered
        setError(
          err?.message ||
            "Could not register. This email may already be in use."
        );
      } else {
        setError("Invalid email or password");
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  const toggleMode = () => {
    setMode((prev) => (prev === "login" ? "register" : "login"));
    setError(null);
  };

  return (
    <section style={{ marginTop: 24, maxWidth: 400 }}>
      <h2>{isRegister ? "Create an account" : "Log in"}</h2>
      <form
        onSubmit={handleSubmit}
        style={{
          marginTop: 16,
          padding: 16,
          borderRadius: 4,
          border: "1px solid #ddd",
          display: "flex",
          flexDirection: "column",
          gap: 12,
        }}
      >
        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <span>Email</span>
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
            autoComplete="email"
          />
        </label>

        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <span>Password</span>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            autoComplete={isRegister ? "new-password" : "current-password"}
          />
        </label>

        {error && (
          <p style={{ color: "red", fontSize: 13 }}>
            {error}
          </p>
        )}

        <button type="submit" disabled={isSubmitting}>
          {isSubmitting
            ? isRegister
              ? "Creating account…"
              : "Logging in…"
            : isRegister
            ? "Register"
            : "Log in"}
        </button>

        <div
          style={{
            marginTop: 8,
            fontSize: 13,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <span>
            {isRegister ? "Already have an account?" : "No account yet?"}
          </span>
          <button
            type="button"
            onClick={toggleMode}
            style={{
              background: "none",
              border: "none",
              padding: 0,
              cursor: "pointer",
              color: "rgb(43, 76, 200)",
              textDecoration: "underline",
              fontSize: "inherit",
            }}
          >
            {isRegister ? "Log in" : "Register"}
          </button>
        </div>
      </form>
    </section>
  );
}

// -----------------------------------------------------------------------------
// ProtectedRoute component
// -----------------------------------------------------------------------------
function ProtectedRoute({ currentUser, requiredRole, children }) {
  if (!currentUser) {
    return <Navigate to="/login" replace />;
  }
  if (requiredRole && currentUser.role !== requiredRole) {
    return <Navigate to="/login" replace />;
  }
  return children;
}


// -----------------------------------------------------------------------------
// App shell with nav + routes + ToastManager
// -----------------------------------------------------------------------------
export default function App() {
  const [currentUser, setCurrentUser] = useState(null);
  const [authChecked, setAuthChecked] = useState(false);

  useEffect(() => {
    apiGet("/auth/me")
      .then((data) => {
        if (data.authenticated) {
          setCurrentUser({ id: data.id, email: data.email, role: data.role });
        } else {
          setCurrentUser(null);
        }
      })
      .catch(() => {
        setCurrentUser(null);
      })
      .finally(() => {
        setAuthChecked(true);
      });
  }, []);

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

  const handleLogout = async () => {
    try {
      await apiPost("/auth/logout", {});
    } catch (err) {
      console.error("Logout failed:", err);
    } finally {
      setCurrentUser(null);
    }
  };

  if (!authChecked) return <main style={{ padding: 24 }}>Checking login…</main>;

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

        {currentUser?.role === "admin" && (
          <>
            <Link to="/admin/scrape">Admin Scrape</Link>
            <Link to="/admin/runs">Runs &amp; Logs</Link>
            <Link to="/admin/users">Users</Link>
          </>
        )}


        {currentUser ? (
          <button
            onClick={handleLogout}
            style={{
              background: "none",
              border: "none",
              padding: 0,
              cursor: "pointer",
              color: "rgb(43, 76, 200)",
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
          Current user:{" "}
          <strong>
            {currentUser
              ? `${currentUser.email} (${currentUser.role})`
              : "none"}
          </strong>
        </span>
      </nav>

      {/* Route definitions */}
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route
          path="/login"
          element={<LoginPage onLogin={setCurrentUser} />}
        />
        <Route
          path="/jobs"
          element={
            <ProtectedRoute currentUser={currentUser}>
              <JobsPage />
            </ProtectedRoute>
          }
        />
        <Route
          path="/admin/scrape"
          element={
            <ProtectedRoute currentUser={currentUser} requiredRole="admin">
              <AdminScrapePage showToast={showToast} />
            </ProtectedRoute>
          }
        />
        <Route
          path="/admin/runs"
          element={
            <ProtectedRoute currentUser={currentUser} requiredRole="admin">
              <AdminRunsPage />
            </ProtectedRoute>
          }
        />
        <Route
          path="/admin/users"
          element={
            <ProtectedRoute currentUser={currentUser} requiredRole="admin">
              <AdminUsersPage />
            </ProtectedRoute>
          }
        />
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
