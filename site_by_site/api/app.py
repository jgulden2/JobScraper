# app.py  (put at project root, next to main.py)
import os
import threading
import datetime
import subprocess
import sqlalchemy as sa
from sqlalchemy import select, func
from flask import Flask, request, jsonify, Response, session
from flask_cors import CORS
from functools import wraps
from werkzeug.security import check_password_hash, generate_password_hash

# Default to local SQLite cache unless JOBS_DB_URL is set
DATABASE_URL = os.getenv("JOBS_DB_URL", "sqlite:///./.cache/jobs.sqlite")
TABLE_NAME = os.getenv("JOBS_DB_TABLE", "jobs")

engine = sa.create_engine(DATABASE_URL, future=True)
meta = sa.MetaData()
jobs = sa.Table(TABLE_NAME, meta, autoload_with=engine)
users = sa.Table(
    "users",
    meta,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("email", sa.String, unique=True, nullable=False),
    sa.Column("password_hash", sa.String, nullable=False),
    sa.Column("role", sa.String, nullable=False),  # "admin" or "user"
    sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
)
meta.create_all(engine)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "dev-override-this")
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = False  # Set to True when on HTTPS

CORS(app, supports_credentials=True)  # allow the React dev server to call the API


def _int(name, default):
    try:
        return int(request.args.get(name, default))
    except Exception:
        return default


def require_login(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return {"error": "auth_required"}, 401
        return f(*args, **kwargs)

    return wrapper


def require_role(role):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not session.get("user_id"):
                return {"error": "auth_required"}, 401
            if session.get("role") != role:
                return {"error": "forbidden"}, 403
            return f(*args, **kwargs)

        return wrapper

    return decorator


@app.get("/health")
def health():
    return {"ok": True}


# ---------- Vendors (portable for SQLite/Postgres) ----------
@app.get("/vendors")
def vendors():
    stmt = select(jobs.c.Vendor, func.count().label("n")).group_by(jobs.c.Vendor)
    with engine.begin() as conn:
        rows = conn.execute(stmt).mappings().all()
    out = [dict(r) for r in rows]
    out.sort(
        key=lambda r: r["n"], reverse=True
    )  # sort in Python to avoid dialect quirks
    return jsonify(out)


# ---------- Jobs list (case-insensitive search that works on SQLite) ----------
@app.get("/jobs")
@require_login
def list_jobs():
    vendor = request.args.get("vendor")
    q = request.args.get("q")
    since = request.args.get("since")  # YYYY-MM-DD
    limit = _int("limit", 50)
    offset = _int("offset", 0)

    stmt = sa.select(jobs)
    if vendor:
        stmt = stmt.where(jobs.c.Vendor == vendor)
    if since:
        stmt = stmt.where(jobs.c["Post Date"] >= since)

    if q:
        like = f"%{q.lower()}%"
        # Use lower(column).like(lower_query) for SQLite-friendly case-insensitive search
        stmt = stmt.where(
            sa.or_(
                sa.func.lower(jobs.c["Position Title"]).like(like),
                sa.func.lower(jobs.c.Description).like(like),
                sa.func.lower(jobs.c["Raw Location"]).like(like),
            )
        )

    # Simple ordering (SQLite doesn't support NULLS LAST directly)
    stmt = stmt.order_by(sa.desc(jobs.c["Post Date"])).limit(limit).offset(offset)

    with engine.begin() as conn:
        rows = conn.execute(stmt).mappings().all()
    return jsonify([dict(r) for r in rows])


# ---------- Single job ----------
@app.get("/jobs/<vendor>/<key>")
def get_job(vendor, key):
    with engine.begin() as conn:
        row = (
            conn.execute(
                sa.text(
                    f'SELECT * FROM {TABLE_NAME} WHERE "Vendor"=:v AND "Dedupe Key"=:k'
                ),
                {"v": vendor, "k": key},
            )
            .mappings()
            .first()
        )
    if not row:
        return {"error": "not found"}, 404
    return jsonify(dict(row))


# ---------- Run scrapers (simple background thread) ----------
RUNS = {}  # {id: {status, created_at, args, stdout}}


@app.post("/runs")
@require_role("admin")
def start_run():
    payload = request.get_json(silent=True) or {}
    rid = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S%f")

    # ----- Build per-run logfile path in ./logs -----
    logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Use bits of the payload to make the filename a bit descriptive
    name_bits = []
    scrapers = payload.get("scrapers") or []
    if scrapers:
        # e.g. "bae", or "bae-lockheed"
        name_bits.append("-".join(scrapers))
    db_mode = payload.get("db_mode")
    if db_mode:
        name_bits.append(str(db_mode))
    if payload.get("limit") is not None:
        try:
            name_bits.append(f"limit{int(payload['limit'])}")
        except Exception:
            pass

    base = f"run_{rid}"
    if name_bits:
        base += "_" + "_".join(name_bits)

    logfile = os.path.join(logs_dir, base + ".log")

    started_by_user_id = session.get("user_id")
    started_by_email = session.get("email")

    RUNS[rid] = {
        "status": "queued",
        "created_at": datetime.datetime.utcnow().isoformat(),
        "args": payload,
        "stdout": "",
        "logfile": logfile,
        "started_by_user_id": started_by_user_id,
        "started_by_email": started_by_email,
    }

    def _worker(logfile_path=logfile):
        RUNS[rid]["status"] = "running"
        try:
            args = ["python", "main.py"]

            # Tell main.py where to write its structured logs
            args += ["--logfile", logfile_path]

            if payload.get("scrapers"):
                args += ["--scrapers", *payload["scrapers"]]
            if payload.get("limit") is not None:
                args += ["--limit", str(int(payload["limit"]))]
            if payload.get("since"):
                args += ["--since", str(payload["since"])]
            if payload.get("workers"):
                args += ["--workers", str(int(payload["workers"]))]

            args += ["--db-url", os.getenv("JOBS_DB_URL", DATABASE_URL)]
            args += ["--db-table", os.getenv("JOBS_DB_TABLE", TABLE_NAME)]

            if payload.get("db_mode"):
                args += ["--db-mode", payload["db_mode"]]

            cf = payload.get("combine_full")
            if cf is True:
                args += ["--combine-full"]  # bare flag
            elif isinstance(cf, str) and cf.strip():
                args += ["--combine-full", cf.strip()]  # custom path

            proc = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            for line in proc.stdout:
                RUNS[rid]["stdout"] += line
            code = proc.wait()
            RUNS[rid]["status"] = "done" if code == 0 else "error"
        except Exception as e:
            RUNS[rid]["status"] = "error"
            RUNS[rid]["stdout"] += f"\nERROR: {e}\n"

    threading.Thread(target=_worker, daemon=True).start()
    # also return the logfile path to the caller, which is handy for UI
    return {"run_id": rid, "status": "queued", "logfile": logfile}


@app.get("/runs")
@require_role("admin")
def list_runs():
    items = [
        {
            "id": k,
            **v,
            "stdout": None,  # keep stdout out of the list view
        }
        for k, v in sorted(RUNS.items(), key=lambda x: x[0], reverse=True)
    ]
    return jsonify(items)


@app.get("/runs/<rid>/logs")
@require_role("admin")
def run_logs(rid):
    rec = RUNS.get(rid)
    if not rec:
        return {"error": "not found"}, 404
    return Response(rec.get("stdout", ""), mimetype="text/plain")


@app.get("/admin/users")
@require_role("admin")
def admin_list_users():
    """Return all users (admins + regular users) for the admin UI."""
    with engine.begin() as conn:
        rows = (
            conn.execute(
                sa.select(
                    users.c.id,
                    users.c.email,
                    users.c.role,
                    users.c.created_at,
                ).order_by(users.c.created_at.desc())
            )
            .mappings()
            .all()
        )

    return jsonify(
        [
            {
                "id": r["id"],
                "email": r["email"],
                "role": r["role"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    )


# ---------- Optional Prometheus text ----------
@app.get("/metrics")
def metrics():
    path = os.getenv("JOBS_PROM_FILE")
    if not path or not os.path.exists(path):
        return Response("# no metrics yet\n", mimetype="text/plain")
    return Response(open(path, "r", encoding="utf-8").read(), mimetype="text/plain")


@app.post("/auth/login")
def auth_login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return {"error": "email and password required"}, 400

    with engine.begin() as conn:
        row = (
            conn.execute(sa.select(users).where(users.c.email == email))
            .mappings()
            .first()
        )

    if not row or not check_password_hash(row["password_hash"], password):
        # Avoid leaking which of email/password is wrong
        return {"error": "invalid credentials"}, 401

    session["user_id"] = row["id"]
    session["role"] = row["role"]
    session["email"] = row["email"]

    return {
        "id": row["id"],
        "email": row["email"],
        "role": row["role"],
    }


@app.post("/auth/register")
def auth_register():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return {"error": "email and password required"}, 400
    if len(password) < 8:
        return {"error": "password too short"}, 400

    with engine.begin() as conn:
        # Check if email already exists
        existing = conn.execute(
            sa.select(users.c.id).where(users.c.email == email)
        ).first()
        if existing:
            return {"error": "email already registered"}, 409

        password_hash = generate_password_hash(password)
        result = conn.execute(
            users.insert().values(
                email=email,
                password_hash=password_hash,
                role="user",
            )
        )
        user_id = result.inserted_primary_key[0]

    # Optionally log them in immediately
    session["user_id"] = user_id
    session["role"] = "user"
    session["email"] = email

    return {
        "id": user_id,
        "email": email,
        "role": "user",
    }, 201


@app.post("/auth/logout")
def auth_logout():
    session.clear()
    return {"ok": True}


@app.get("/auth/me")
def auth_me():
    uid = session.get("user_id")
    if not uid:
        return {"authenticated": False}, 200

    # Optionally re-fetch role/email in case they changed in DB
    with engine.begin() as conn:
        row = (
            conn.execute(
                sa.select(users.c.id, users.c.email, users.c.role).where(
                    users.c.id == uid
                )
            )
            .mappings()
            .first()
        )

    if not row:
        session.clear()
        return {"authenticated": False}, 200

    return {
        "authenticated": True,
        "id": row["id"],
        "email": row["email"],
        "role": row["role"],
    }


if __name__ == "__main__":
    app.run(debug=True, port=8000)
