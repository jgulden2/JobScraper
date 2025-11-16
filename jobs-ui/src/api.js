// src/api.js

// Prefer an env override, but fall back to localhost:8000
export const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

function getCsrfTokenFromCookie() {
  if (typeof document === "undefined") return null;
  const match = document.cookie.match(/(?:^|; )csrf_token=([^;]+)/);
  return match ? decodeURIComponent(match[1]) : null;
}

export async function apiGet(path, options = {}) {
  const url = `${API_BASE_URL}${path}`;
  const csrfToken = getCsrfTokenFromCookie();

  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };
  if (csrfToken) {
    headers["X-CSRFToken"] = csrfToken;
  }

  const resp = await fetch(url, {
    headers,
    credentials: "include",
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

// Generic POST helper
export async function apiPost(path, body, options = {}) {
  const url = `${API_BASE_URL}${path}`;
  const csrfToken = getCsrfTokenFromCookie();

  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };
  if (csrfToken) {
    headers["X-CSRFToken"] = csrfToken;
  }

  const resp = await fetch(url, {
    method: "POST",
    headers,
    credentials: "include",
    body: JSON.stringify(body ?? {}),
    ...options,
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    const err = new Error(`API POST ${url} failed: ${resp.status}`);
    err.status = resp.status;
    err.body = text;
    throw err;
  }

  if (resp.status === 204) {
    return null;
  }

  return resp.json();
}
