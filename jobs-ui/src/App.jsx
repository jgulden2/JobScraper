import { useEffect, useState } from "react";
import {
  Routes,
  Route,
  Link,
  Navigate,
  useNavigate,
} from "react-router-dom";
import { apiGet, API_BASE_URL } from "./api";

// ------------ Home page (keeps the /health check you already had) ------------
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

// ------------ Placeholder pages ------------
function JobsPage() {
  return (
    <section style={{ marginTop: 24 }}>
      <h2>Jobs</h2>
      <p>JobsPage placeholder – we&apos;ll list jobs here later.</p>
    </section>
  );
}

function AdminScrapePage() {
  return (
    <section style={{ marginTop: 24 }}>
      <h2>Admin – Run Scraper</h2>
      <p>AdminScrapePage placeholder – controls for scrapes will go here.</p>
    </section>
  );
}

// ------------ Simple login that just sets a fake role ------------
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

// ------------ ProtectedRoute component ------------
// - If no role: redirect to /login
// - If requiredRole is given and role !== requiredRole: redirect to /login
function ProtectedRoute({ role, requiredRole, children }) {
  if (!role) {
    return <Navigate to="/login" replace />;
  }
  if (requiredRole && role !== requiredRole) {
    return <Navigate to="/login" replace />;
  }
  return children;
}

// ------------ App shell with nav + routes ------------
export default function App() {
  // Very dumb role state (user | admin | null)
  const [role, setRole] = useState(null);

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
              color: "rgb(43, 76, 200)",  // react-router link color
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
              <AdminScrapePage />
            </ProtectedRoute>
          }
        />
        {/* Catch-all: redirect unknown routes to home */}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </main>
  );
}
