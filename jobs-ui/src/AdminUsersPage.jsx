import { useEffect, useState } from "react";
import { apiGet } from "./api";

export default function AdminUsersPage() {
  const [users, setUsers] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function loadUsers() {
      setIsLoading(true);
      setError(null);
      try {
        const data = await apiGet("/admin/users");
        if (!cancelled) {
          setUsers(Array.isArray(data) ? data : []);
        }
      } catch (err) {
        console.error("Error fetching users:", err);
        if (!cancelled) {
          setError(err.message || "Failed to load users");
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    loadUsers();
    return () => {
      cancelled = true;
    };
  }, []);

  const renderCreatedAt = (createdAt) => {
    if (!createdAt) return "—";
    try {
      const d = new Date(createdAt);
      if (Number.isNaN(d.getTime())) return createdAt;
      return d.toLocaleString();
    } catch {
      return createdAt;
    }
  };

  return (
    <section style={{ marginTop: 24 }}>
      <h2>Users &amp; Admins</h2>
      <p style={{ marginTop: 8, fontSize: 14, color: "#555" }}>
        This page is only visible to admins. It lists all registered accounts.
      </p>

      {isLoading && <p>Loading users…</p>}
      {error && (
        <p style={{ color: "red", fontSize: 13 }}>
          {error}
        </p>
      )}

      {!isLoading && !error && users.length === 0 && (
        <p>No users found.</p>
      )}

      {!isLoading && !error && users.length > 0 && (
        <div style={{ marginTop: 16, overflowX: "auto" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              minWidth: 480,
            }}
          >
            <thead>
              <tr>
                <th
                  style={{
                    textAlign: "left",
                    borderBottom: "1px solid #ccc",
                    padding: "8px 4px",
                  }}
                >
                  Email
                </th>
                <th
                  style={{
                    textAlign: "left",
                    borderBottom: "1px solid #ccc",
                    padding: "8px 4px",
                  }}
                >
                  Role
                </th>
                <th
                  style={{
                    textAlign: "left",
                    borderBottom: "1px solid #ccc",
                    padding: "8px 4px",
                  }}
                >
                  Created At
                </th>
              </tr>
            </thead>
            <tbody>
              {users.map((u) => (
                <tr key={u.id}>
                  <td
                    style={{
                      borderBottom: "1px solid #eee",
                      padding: "6px 4px",
                      fontFamily: "monospace",
                    }}
                  >
                    {u.email}
                  </td>
                  <td
                    style={{
                      borderBottom: "1px solid #eee",
                      padding: "6px 4px",
                      textTransform: "capitalize",
                    }}
                  >
                    {u.role}
                  </td>
                  <td
                    style={{
                      borderBottom: "1px solid #eee",
                      padding: "6px 4px",
                      fontSize: 13,
                      color: "#555",
                    }}
                  >
                    {renderCreatedAt(u.created_at)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
