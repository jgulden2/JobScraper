import { useEffect, useRef, useState } from "react";
import { apiGet } from "./api";

export default function FiltersBar({ vendor, searchTerm, since, onChange }) {
  const [vendors, setVendors] = useState([]);
  const [isLoadingVendors, setIsLoadingVendors] = useState(false);
  const [vendorsError, setVendorsError] = useState(null);

  // Local state for the search input so we can debounce before calling onChange
  const [searchInput, setSearchInput] = useState(searchTerm ?? "");
  const searchTimeoutRef = useRef(null);

  // Fetch /vendors once on mount
  useEffect(() => {
    let cancelled = false;

    async function fetchVendors() {
      setIsLoadingVendors(true);
      setVendorsError(null);
      try {
        const data = await apiGet("/vendors");
        if (cancelled) return;

        // backend returns [{ Vendor: "...", n: 123 }, ...]
        const names = Array.isArray(data)
          ? data
              .map((row) => row.Vendor)
              .filter(Boolean)
          : [];

        // sort vendor names alphabetically
        names.sort((a, b) => a.localeCompare(b));
        setVendors(names);
      } catch (err) {
        console.error("Error fetching vendors:", err);
        if (!cancelled) {
          setVendorsError(err.message || "Failed to load vendors");
        }
      } finally {
        if (!cancelled) {
          setIsLoadingVendors(false);
        }
      }
    }

    fetchVendors();
    return () => {
      cancelled = true;
    };
  }, []);

  // If the parent changes searchTerm (e.g., when resetting filters),
  // keep our input in sync.
  useEffect(() => {
    setSearchInput(searchTerm ?? "");
  }, [searchTerm]);

  // Handle vendor change
  const handleVendorChange = (e) => {
    const value = e.target.value || "";
    onChange({ vendor: value || null }); // null / "" means "All"
  };

  // Handle search input with debounce
  const handleSearchChange = (e) => {
    const value = e.target.value;
    setSearchInput(value);

    if (searchTimeoutRef.current) {
      clearTimeout(searchTimeoutRef.current);
    }

    searchTimeoutRef.current = setTimeout(() => {
      onChange({ searchTerm: value || null }); // will map to q=... on JobsPage
    }, 300);
  };

  // Date helpers
  const toYmd = (date) => date.toISOString().slice(0, 10);

  const setSinceDate = (dateValue) => {
    onChange({ since: dateValue || null });
  };

  const handleSinceChange = (e) => {
    setSinceDate(e.target.value || "");
  };

  const handleQuickChip = (type) => {
    const today = new Date();
    let d = null;

    if (type === "all") {
      setSinceDate("");
      return;
    }

    if (type === "last7") {
      d = new Date(today);
      d.setDate(today.getDate() - 7);
    } else if (type === "last30") {
      d = new Date(today);
      d.setDate(today.getDate() - 30);
    } else if (type === "year") {
      d = new Date(today.getFullYear(), 0, 1);
    }

    setSinceDate(d ? toYmd(d) : "");
  };

  return (
    <div
      style={{
        marginTop: 16,
        marginBottom: 16,
        padding: 12,
        border: "1px solid #ddd",
        borderRadius: 4,
        display: "flex",
        flexWrap: "wrap",
        gap: 12,
        alignItems: "center",
      }}
    >
      {/* Vendor dropdown */}
      <label style={{ display: "flex", flexDirection: "column", fontSize: 14 }}>
        <span style={{ marginBottom: 4 }}>Vendor</span>
        <select
          value={vendor ?? ""}
          onChange={handleVendorChange}
          disabled={isLoadingVendors}
        >
          <option value="">All vendors</option>
          {vendors.map((v) => (
            <option key={v} value={v}>
              {v}
            </option>
          ))}
        </select>
        {vendorsError && (
          <span style={{ color: "red", marginTop: 4, fontSize: 12 }}>
            {vendorsError}
          </span>
        )}
      </label>

      {/* Search input */}
      <label style={{ display: "flex", flexDirection: "column", flex: 1, minWidth: 200, fontSize: 14 }}>
        <span style={{ marginBottom: 4 }}>Search</span>
        <input
          type="text"
          placeholder="Search title, description, location..."
          value={searchInput}
          onChange={handleSearchChange}
        />
      </label>

      {/* Date filter + chips */}
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: 4,
          fontSize: 14,
        }}
      >
        <span>Date since</span>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <input
            type="date"
            value={since ?? ""}
            onChange={handleSinceChange}
          />
          <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
            <button type="button" onClick={() => handleQuickChip("all")}>
              All
            </button>
            <button type="button" onClick={() => handleQuickChip("last7")}>
              Last 7 days
            </button>
            <button type="button" onClick={() => handleQuickChip("last30")}>
              Last 30 days
            </button>
            <button type="button" onClick={() => handleQuickChip("year")}>
              This year
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
