import { useEffect } from "react";

export default function JobModal({ job, onClose }) {
  if (!job) return null;

  // Close on ESC key
  useEffect(() => {
    const handleKey = (e) => {
      if (e.key === "Escape") {
        onClose();
      }
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [onClose]);

  const title =
    job["Position Title"] || job.title || "(no title)";
  const company = job.Vendor || job.company || "";
  const location =
    job["Raw Location"] ||
    job.location ||
    [job.City, job.State].filter(Boolean).join(", ");
  const date = job["Post Date"] || job.post_date || job.date;
  const description =
    job["Description"] ||
    job["Full Description"] ||
    job.description ||
    "";

  const detailUrl =
    job["Detail URL"] || job.detail_url || job.url;

  const openOriginal = () => {
    if (detailUrl) {
      window.open(detailUrl, "_blank", "noopener,noreferrer");
    }
  };

  const extraEntries = Object.entries(job).filter(
    ([key]) =>
      ![
        "Position Title",
        "Vendor",
        "Raw Location",
        "Post Date",
        "Description",
        "Full Description",
        "Detail URL",
        "detail_url",
        "url",
        "City",
        "State",
      ].includes(key)
  );

  return (
    <div
      onClick={(e) => {
        // click on dark overlay closes modal
        if (e.target === e.currentTarget) {
          onClose();
        }
      }}
      style={{
        position: "fixed",
        inset: 0,
        backgroundColor: "rgba(0,0,0,0.4)",
        display: "flex",
        justifyContent: "center",
        alignItems: "flex-start",
        paddingTop: 40,
        zIndex: 1000,
      }}
    >
      <div
        style={{
          background: "#fff",
          borderRadius: 4,
          maxWidth: 800,
          width: "90%",
          maxHeight: "calc(100vh - 80px)",
          overflowY: "auto",
          boxShadow: "0 4px 16px rgba(0,0,0,0.25)",
          padding: 24,
        }}
      >
        {/* Header */}
        <div
          style={{
            display: "flex",
            alignItems: "flex-start",
            gap: 12,
          }}
        >
          <div style={{ flex: 1 }}>
            <h2 style={{ marginTop: 0, marginBottom: 4 }}>{title}</h2>
            <div style={{ color: "#555", fontSize: 14 }}>
              {company && <span>{company}</span>}
              {company && location && <span> · </span>}
              {location && <span>{location}</span>}
              {date && (
                <>
                  {" "}
                  ·{" "}
                  <span>
                    {new Date(date).toLocaleDateString()}
                  </span>
                </>
              )}
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            style={{
              border: "none",
              background: "transparent",
              cursor: "pointer",
              fontSize: 18,
              lineHeight: 1,
            }}
            aria-label="Close"
          >
            ×
          </button>
        </div>

        {/* Actions */}
        <div style={{ marginTop: 12, marginBottom: 16 }}>
          <button
            type="button"
            onClick={openOriginal}
            disabled={!detailUrl}
          >
            Open original posting
          </button>
          {!detailUrl && (
            <span
              style={{
                marginLeft: 8,
                fontSize: 12,
                color: "#999",
              }}
            >
              (No detail URL available)
            </span>
          )}
        </div>

        {/* Description */}
        {description && (
          <section style={{ marginBottom: 16 }}>
            <h3>Description</h3>
            <pre
              style={{
                whiteSpace: "pre-wrap",
                fontFamily: "inherit",
                fontSize: 14,
                border: "1px solid #eee",
                padding: 12,
                borderRadius: 4,
                background: "#fafafa",
              }}
            >
              {description}
            </pre>
          </section>
        )}

        {/* Extra fields */}
        {extraEntries.length > 0 && (
          <section style={{ marginBottom: 8 }}>
            <h3>Details</h3>
            <table
              style={{
                borderCollapse: "collapse",
                width: "100%",
                fontSize: 14,
              }}
            >
              <tbody>
                {extraEntries.map(([key, value]) => (
                  <tr key={key}>
                    <th
                      style={{
                        textAlign: "left",
                        padding: "4px 8px",
                        borderBottom: "1px solid #eee",
                        width: "35%",
                        verticalAlign: "top",
                      }}
                    >
                      {key}
                    </th>
                    <td
                      style={{
                        padding: "4px 8px",
                        borderBottom: "1px solid #eee",
                      }}
                    >
                      {String(value)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        )}

        <div style={{ textAlign: "right", marginTop: 12 }}>
          <button type="button" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
