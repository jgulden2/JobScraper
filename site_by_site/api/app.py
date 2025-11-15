# app.py  (put at project root, next to main.py)
import os
import threading
import datetime
import subprocess
import sqlalchemy as sa
from sqlalchemy import select, func
from flask import Flask, request, jsonify, Response
from flask_cors import CORS

# Default to your local SQLite cache unless JOBS_DB_URL is set
DATABASE_URL = os.getenv("JOBS_DB_URL", "sqlite:///./.cache/jobs.sqlite")
TABLE_NAME = os.getenv("JOBS_DB_TABLE", "jobs")

engine = sa.create_engine(DATABASE_URL, future=True)
meta = sa.MetaData()
jobs = sa.Table(TABLE_NAME, meta, autoload_with=engine)

app = Flask(__name__)
CORS(app)  # allow the React dev server to call the API


def _int(name, default):
    try:
        return int(request.args.get(name, default))
    except Exception:
        return default


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

RUNS = {}  # {id: {status, created_at, args, stdout, logfile}}


@app.post("/runs")
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

    RUNS[rid] = {
        "status": "queued",
        "created_at": datetime.datetime.utcnow().isoformat(),
        "args": payload,
        "stdout": "",
        "logfile": logfile,
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

            # DB opts default to your current DB (you can override via payload)
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
def list_runs():
    items = [
        {"id": k, **v, "stdout": None}
        for k, v in sorted(RUNS.items(), key=lambda x: x[0], reverse=True)
    ]
    return jsonify(items)


@app.get("/runs/<rid>/logs")
def run_logs(rid):
    rec = RUNS.get(rid)
    if not rec:
        return {"error": "not found"}, 404
    return Response(rec.get("stdout", ""), mimetype="text/plain")


# ---------- Optional Prometheus text ----------
@app.get("/metrics")
def metrics():
    path = os.getenv("JOBS_PROM_FILE")
    if not path or not os.path.exists(path):
        return Response("# no metrics yet\n", mimetype="text/plain")
    return Response(open(path, "r", encoding="utf-8").read(), mimetype="text/plain")


if __name__ == "__main__":
    app.run(debug=True, port=8000)
