// src/api.js

// Prefer an env override, but fall back to localhost:8000
export const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

export async function apiGet(path, options = {}) {
  const url = `${API_BASE_URL}${path}`;
  const resp = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    const err = new Error(`API GET ${url} failed: ${resp.status}`);
    err.status = resp.status;
    err.body = text;
    throw err;
  }

  return resp.json();
}
