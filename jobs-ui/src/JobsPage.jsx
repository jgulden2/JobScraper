import { useEffect, useState } from "react";
import { apiGet } from "./api";
import FiltersBar from "./FiltersBar";
import JobModal from "./JobModal";

const PAGE_SIZE = 50;
const SKELETON_ROWS = 10;

export default function JobsPage() {
  const [jobs, setJobs] = useState([]);
  const [page, setPage] = useState(0); // 0-based
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedJob, setSelectedJob] = useState(null);

  // Sorting: 'date' | 'title' | 'location' | 'company'
  const [sortField, setSortField] = useState("date");
  const [sortDirection, setSortDirection] = useState("desc"); // 'asc' | 'desc'

  // Filters: vendor, searchTerm (maps to q), since (YYYY-MM-DD)
  const [filters, setFilters] = useState({
    vendor: null,
    searchTerm: null,
    since: null,
  });

  // Whenever filters change, reset to page 0
  const handleFiltersChange = (partial) => {
    setPage(0);
    setFilters((prev) => ({
      ...prev,
      ...partial,
    }));
  };

  useEffect(() => {
    let cancelled = false;

    async function fetchJobs() {
      setIsLoading(true);
      setError(null);
      try {
        const params = new URLSearchParams();
        params.set("limit", PAGE_SIZE.toString());
        params.set("offset", String(page * PAGE_SIZE));

        if (filters.vendor) {
          params.set("vendor", filters.vendor);
        }
        if (filters.searchTerm) {
          params.set("q", filters.searchTerm);
        }
        if (filters.since) {
          params.set("since", filters.since);
        }

        const path = `/jobs?${params.toString()}`;
        const data = await apiGet(path);

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
  }, [page, filters.vendor, filters.searchTerm, filters.since]);

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

  // ---------- Derived helpers ----------

  const hasActiveFilters =
    !!(filters.vendor || filters.searchTerm || filters.since);

  const sortedJobs = (() => {
    const out = [...jobs];
    if (!sortField) return out;

    const compareStrings = (aVal, bVal) => {
      const sa = (aVal || "").toLowerCase();
      const sb = (bVal || "").toLowerCase();
      if (sa < sb) return sortDirection === "asc" ? -1 : 1;
      if (sa > sb) return sortDirection === "asc" ? 1 : -1;
      return 0;
    };

    if (sortField === "title") {
      out.sort((a, b) =>
        compareStrings(a["Position Title"], b["Position Title"])
      );
    } else if (sortField === "date") {
      out.sort((a, b) => {
        const da = a["Post Date"] ? new Date(a["Post Date"]) : null;
        const db = b["Post Date"] ? new Date(b["Post Date"]) : null;
        const ta = da ? da.getTime() : 0;
        const tb = db ? db.getTime() : 0;
        if (ta < tb) return sortDirection === "asc" ? -1 : 1;
        if (ta > tb) return sortDirection === "asc" ? 1 : -1;
        return 0;
      });
    } else if (sortField === "location") {
      out.sort((a, b) => {
        const aLoc = a["Raw Location"] || a.City || a.State || "";
        const bLoc = b["Raw Location"] || b.City || b.State || "";
        return compareStrings(aLoc, bLoc);
      });
    } else if (sortField === "company") {
      out.sort((a, b) => compareStrings(a.Vendor, b.Vendor));
    }

    return out;
  })();

  const handleSort = (field) => {
    setSortField((prevField) => {
      if (prevField === field) {
        // toggle direction if clicking same column
        setSortDirection((prevDir) => (prevDir === "asc" ? "desc" : "asc"));
        return prevField;
      }
      // default directions: newest first for date, A→Z for others
      setSortDirection(field === "date" ? "desc" : "asc");
      return field;
    });
  };

  const renderSortIndicator = (field) => {
    if (sortField !== field) {
      return (
        <span style={{ marginLeft: 4, fontSize: 11, opacity: 0.4 }}>⇅</span>
      );
    }
    return (
      <span style={{ marginLeft: 4, fontSize: 11 }}>
        {sortDirection === "asc" ? "▲" : "▼"}
      </span>
    );
  };

  const skeletonCellStyle = {
    borderBottom: "1px solid #eee",
    padding: "4px 8px",
  };

  const skeletonBarStyle = (width = "80%") => ({
    display: "inline-block",
    height: 12,
    width,
    borderRadius: 4,
    backgroundColor: "#eee",
  });

  return (
    <section style={{ marginTop: 24 }}>
      <h2>Jobs</h2>

      {/* Filters bar */}
      <FiltersBar
        vendor={filters.vendor}
        searchTerm={filters.searchTerm}
        since={filters.since}
        onChange={handleFiltersChange}
      />

      <p style={{ fontSize: 14, color: "#555" }}>
        Showing {jobs.length} jobs – page {page + 1}
      </p>

      {error && (
        <p style={{ color: "red" }}>
          Error: <code>{error}</code>
        </p>
      )}

      {/* Empty state (when not loading, no error, and no jobs) */}
      {!isLoading && jobs.length === 0 && !error && (
        <div
          style={{
            marginTop: 16,
            padding: 16,
            borderRadius: 4,
            background: "#f9fafb",
            border: "1px solid #e5e7eb",
            fontSize: 14,
            color: "#555",
          }}
        >
          <strong style={{ display: "block", marginBottom: 4 }}>
            {hasActiveFilters
              ? "No jobs match your filters"
              : "No jobs found yet"}
          </strong>
          {hasActiveFilters ? (
            <span>
              Try adjusting your vendor, search, or date filters to see more
              results.
            </span>
          ) : (
            <span>
              Once your scrapers have run and written jobs into the database,
              they&apos;ll show up here.
            </span>
          )}
        </div>
      )}

      {/* Table: show it when we have jobs OR we are loading (for skeleton) */}
      {(jobs.length > 0 || (isLoading && !error)) && (
        <table
          style={{
            marginTop: 12,
            borderCollapse: "collapse",
            width: "100%",
            fontSize: 14,
          }}
        >
          <thead>
            <tr>
              <th
                onClick={() => handleSort("title")}
                style={{
                  borderBottom: "1px solid #ccc",
                  textAlign: "left",
                  padding: "6px 8px",
                  backgroundColor: "#f3f4f6",
                  cursor: "pointer",
                  userSelect: "none",
                  whiteSpace: "nowrap",
                }}
              >
                Title
                {renderSortIndicator("title")}
              </th>
              <th
                onClick={() => handleSort("date")}
                style={{
                  borderBottom: "1px solid #ccc",
                  textAlign: "left",
                  padding: "6px 8px",
                  backgroundColor: "#f3f4f6",
                  cursor: "pointer",
                  userSelect: "none",
                  whiteSpace: "nowrap",
                }}
              >
                Date
                {renderSortIndicator("date")}
              </th>
              <th
                onClick={() => handleSort("location")}
                style={{
                  borderBottom: "1px solid #ccc",
                  textAlign: "left",
                  padding: "6px 8px",
                  backgroundColor: "#f3f4f6",
                  cursor: "pointer",
                  userSelect: "none",
                  whiteSpace: "nowrap",
                }}
              >
                Location
                {renderSortIndicator("location")}
              </th>
              <th
                onClick={() => handleSort("company")}
                style={{
                  borderBottom: "1px solid #ccc",
                  textAlign: "left",
                  padding: "6px 8px",
                  backgroundColor: "#f3f4f6",
                  cursor: "pointer",
                  userSelect: "none",
                  whiteSpace: "nowrap",
                }}
              >
                Company
                {renderSortIndicator("company")}
              </th>
            </tr>
          </thead>
          <tbody>
            {isLoading && jobs.length === 0
              ? // ---------- Skeleton rows when first loading ----------
                Array.from({ length: SKELETON_ROWS }).map((_, idx) => (
                  <tr key={`skeleton-${idx}`}>
                    <td style={skeletonCellStyle}>
                      <span style={skeletonBarStyle("90%")} />
                    </td>
                    <td style={skeletonCellStyle}>
                      <span style={skeletonBarStyle("60%")} />
                    </td>
                    <td style={skeletonCellStyle}>
                      <span style={skeletonBarStyle("75%")} />
                    </td>
                    <td style={skeletonCellStyle}>
                      <span style={skeletonBarStyle("50%")} />
                    </td>
                  </tr>
                ))
              : // ---------- Real rows (sorted client-side) ----------
                sortedJobs.map((job, idx) => {
                  const key =
                    job["Posting ID"] ??
                    job["Detail URL"] ??
                    `${job.Vendor || "job"}-${idx}`;

                  const location =
                    job["Raw Location"] || job.City || job.State || "";

                  return (
                    <tr
                      key={key}
                      onClick={() => setSelectedJob(job)}
                      style={{
                        cursor: "pointer",
                      }}
                    >
                      <td
                        style={{
                          borderBottom: "1px solid #eee",
                          padding: "4px 8px",
                        }}
                      >
                        {job["Position Title"] || "(no title)"}
                      </td>
                      <td
                        style={{
                          borderBottom: "1px solid #eee",
                          padding: "4px 8px",
                        }}
                      >
                        {formatDate(job["Post Date"])}
                      </td>
                      <td
                        style={{
                          borderBottom: "1px solid #eee",
                          padding: "4px 8px",
                        }}
                      >
                        {location}
                      </td>
                      <td
                        style={{
                          borderBottom: "1px solid #eee",
                          padding: "4px 8px",
                        }}
                      >
                        {job.Vendor || "(unknown)"}
                      </td>
                    </tr>
                  );
                })}
          </tbody>
        </table>
      )}

      {/* Pagination */}
      <div
        style={{
          marginTop: 16,
          display: "flex",
          gap: 8,
          alignItems: "center",
        }}
      >
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

      {/* Job detail modal */}
      {selectedJob && (
        <JobModal job={selectedJob} onClose={() => setSelectedJob(null)} />
      )}
    </section>
  );
}
