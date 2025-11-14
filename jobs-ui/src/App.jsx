import { useEffect, useState } from "react";
import { apiGet, API_BASE_URL } from "./api";

export default function App() {
  const [health, setHealth] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    // Call backend /health once on mount
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
    <main style={{ padding: 24, fontFamily: "Arial, sans-serif" }}>
      <h1>JobScraper UI boot test</h1>
      <p>
        If you see this, React is rendering <code>src/App.jsx</code> and{" "}
        <code>src/main.jsx</code>.
      </p>

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
    </main>
  );
}
