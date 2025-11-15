import { useEffect, useState } from "react";
import { apiGet } from "./api";

const PAGE_SIZE = 50;

export default function JobsPage() {
  const [jobs, setJobs] = useState([]);
  const [page, setPage] = useState(0);        // 0-based
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function fetchJobs() {
      setIsLoading(true);
      setError(null);
      try {
        const offset = page * PAGE_SIZE;
        const data = await apiGet(`/jobs?limit=${PAGE_SIZE}&offset=${offset}`);

        if (!cancelled) {
          setJobs(Array.isArray(data) ? data : []);
        }
      } catch (err) {
        console.error("Error fetching jobs:", err);
        if (!cancelled) {
          setError(err.message || "Failed to load jobs");
          setJobs([]);
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    fetchJobs();
    return () => {
      cancelled = true;
    };
  }, [page]);

  const canGoPrev = page > 0 && !isLoading;
  const canGoNext = jobs.length === PAGE_SIZE && !isLoading; // assume no more if < 50

  const formatDate = (value) => {
    if (!value) return "";
    try {
      const d = new Date(value);
      if (!Number.isNaN(d.getTime())) {
        return d.toLocaleDateString();
      }
      return String(value);
    } catch {
      return String(value);
    }
  };

  return (
    <section style={{ marginTop: 24 }}>
      <h2>Jobs</h2>
      <p>Showing {jobs.length} jobs – page {page + 1}</p>

      {error && (
        <p style={{ color: "red" }}>
          Error: <code>{error}</code>
        </p>
      )}

      {isLoading && <p>Loading jobs…</p>}

      {!isLoading && jobs.length === 0 && !error && (
        <p>No jobs found.</p>
      )}

      {jobs.length > 0 && (
        <table
          style={{
            marginTop: 12,
            borderCollapse: "collapse",
            width: "100%",
          }}
        >
          <thead>
            <tr>
              <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px 8px" }}>
                Title
              </th>
              <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px 8px" }}>
                Date
              </th>
              <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px 8px" }}>
                Location
              </th>
              <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px 8px" }}>
                Company
              </th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job, idx) => {
              const key =
                job["Posting ID"] ??
                job["Detail URL"] ??
                `${job.Vendor || "job"}-${idx}`;

              return (
                <tr key={key}>
                  <td style={{ borderBottom: "1px solid #eee", padding: "4px 8px" }}>
                    {job["Position Title"] || "(no title)"}
                  </td>
                  <td style={{ borderBottom: "1px solid #eee", padding: "4px 8px" }}>
                    {formatDate(job["Post Date"])}
                  </td>
                  <td style={{ borderBottom: "1px solid #eee", padding: "4px 8px" }}>
                    {job["Raw Location"] || job.City || job.State || ""}
                  </td>
                  <td style={{ borderBottom: "1px solid #eee", padding: "4px 8px" }}>
                    {job.Vendor || "(unknown)"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}

      <div style={{ marginTop: 16, display: "flex", gap: 8, alignItems: "center" }}>
        <button
          type="button"
          disabled={!canGoPrev}
          onClick={() => canGoPrev && setPage((p) => p - 1)}
        >
          Prev
        </button>
        <span>Page {page + 1}</span>
        <button
          type="button"
          disabled={!canGoNext}
          onClick={() => canGoNext && setPage((p) => p + 1)}
        >
          Next
        </button>
      </div>
    </section>
  );
}
