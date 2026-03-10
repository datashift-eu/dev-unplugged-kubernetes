import os
import psycopg2
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Mission Control API")


@app.get("/health")
def health():
    return {"status": "ok", "version": os.getenv("APP_VERSION", "unknown")}


@app.get("/secret")
def secret():
    """
    Challenge 2: deze waarde moet via een Kubernetes Secret worden meegegeven.
    Zet de env var MISSION_SECRET in je Deployment via een secretKeyRef.
    """
    value = os.getenv("MISSION_SECRET")
    if not value:
        return JSONResponse(
            status_code=500,
            content={"error": "MISSION_SECRET env var not set — did you wire up the Secret?"},
        )
    return {"secret": value}


@app.get("/db")
def db():
    """
    Challenge 3: verbind met de bestaande PostgreSQL uit platform-storage.
    Configureer DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD via een Secret.
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([host, name, user, password]):
        return JSONResponse(
            status_code=500,
            content={"error": "DB env vars missing — check your Secret and Deployment"},
        )

    try:
        conn = psycopg2.connect(
            host=host, port=int(port), dbname=name, user=user, password=password,
            connect_timeout=5,
        )
        conn.close()
        return {"db": "connected", "host": host, "database": name}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/challenge4")
def challenge4():
    """
    Challenge 4: dit endpoint wordt door de Argo Workflow aangeroepen.
    Geeft de eindcode terug als DB en Secret correct zijn geconfigureerd.
    """
    secret_ok = bool(os.getenv("MISSION_SECRET"))
    db_host_ok = bool(os.getenv("DB_HOST"))
    if secret_ok and db_host_ok:
        return {"status": "MISSION_ACCOMPLISHED", "code": "ARG0-W1NS-K8S-R0CKS"}
    return JSONResponse(
        status_code=400,
        content={"status": "not ready", "secret_ok": secret_ok, "db_ok": db_host_ok},
    )
