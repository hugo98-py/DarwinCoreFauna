# -*- coding: utf-8 -*-
"""
FastAPI robusto para FlutterFlow:
- /export => devuelve job_id + status_url + download_url (respuesta inmediata)
- Genera Excel en background y sirve archivos estáticos en /downloads
- CORS amplio por defecto (puedes restringir con CORS_ORIGINS)
"""

import os, re, base64, uuid, warnings, logging, traceback, threading, time
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

import numpy as np
import pandas as pd

from fastapi import FastAPI, Query, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from openpyxl import load_workbook

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

# ───────────────────────────────────────────────────────────────
# OpenPyXL: silenciar warning (opcional)
warnings.filterwarnings(
    "ignore",
    message="Data Validation extension is not supported and will be removed",
    category=UserWarning,
    module="openpyxl",
)

# ───────────────────────────────────────────────────────────────
# Logging
# ───────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dwc-export")

# ───────────────────────────────────────────────────────────────
# PATHS & CONFIG
# ───────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/downloads")
Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

RUTA_PLANTILLA = os.getenv(
    "RUTA_PLANTILLA",
    (ROOT / "FormatoBiodiversidadMonitoreoYLineaBase_v5.2.xlsx").as_posix()
)

app = FastAPI(title="Exporter DwC-SMA")
app.mount("/downloads", StaticFiles(directory=DOWNLOAD_DIR), name="downloads")

# ───────────────────────────────────────────────────────────────
# 🌐 CORS: amplio por defecto (browser-friendly)
#     - En prod, usa CORS_ORIGINS="https://tu-dominio1,https://tu-dominio2"
# ───────────────────────────────────────────────────────────────
cors_env = os.getenv("CORS_ORIGINS", "").strip()
if cors_env:
    allow_origins = [o.strip() for o in cors_env.split(",") if o.strip()]
else:
    allow_origins = ["*"]  # amplio para evitar “Failed to fetch” por CORS en FF

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=False,         # si pones True, no puedes usar "*"
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# ───────────────────────────────────────────────────────────────
# FIREBASE INIT
# ───────────────────────────────────────────────────────────────
FIREBASE_KEY_B64 = os.getenv("FIREBASE_KEY_B64")
if not FIREBASE_KEY_B64:
    raise RuntimeError("No se encontró la variable de entorno FIREBASE_KEY_B64.")
if not firebase_admin._apps:
    cred_path = ROOT / "firebase_key.json"
    with open(cred_path, "wb") as f:
        f.write(base64.b64decode(FIREBASE_KEY_B64))
    firebase_admin.initialize_app(credentials.Certificate(cred_path.as_posix()))
db = firestore.client()

# ───────────────────────────────────────────────────────────────
# JOB STORE en memoria
# ───────────────────────────────────────────────────────────────
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()
JOB_TTL_SECONDS = 60 * 60  # limpiar trabajos viejos (1 h)

def _job_set(job_id: str, **kwargs):
    with JOBS_LOCK:
        job = JOBS.get(job_id, {})
        job.update(kwargs)
        JOBS[job_id] = job

def _job_get(job_id: str) -> Optional[Dict[str, Any]]:
    with JOBS_LOCK:
        return JOBS.get(job_id)

def _job_cleanup():
    now = time.time()
    to_del = []
    with JOBS_LOCK:
        for jid, job in JOBS.items():
            ts = job.get("ts", now)
            if (now - ts) > JOB_TTL_SECONDS:
                to_del.append(jid)
        for jid in to_del:
            del JOBS[jid]
    if to_del:
        log.info(f"[jobs] limpiados {len(to_del)} jobs expuestos")

# ───────────────────────────────────────────────────────────────
# UTILS
# ───────────────────────────────────────────────────────────────
def _safe_filename(s: str) -> str:
    return re.sub(r"[^\w\-]+", "-", s)

def clean_datetimes(d: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in d.items():
        if isinstance(v, datetime) and v.tzinfo:
            d[k] = v.replace(tzinfo=None)
    return d

def fetch_df_exact(col: str, campana_id: str, filter_by_campana: bool = True) -> pd.DataFrame:
    campana_id = campana_id.strip().strip('"')
    col_ref = db.collection(col)
    first = list(col_ref.limit(1).stream())
    if not first:
        return pd.DataFrame()
    if filter_by_campana and ("campanaID" in first[0].to_dict()):
        q = col_ref.where(filter=FieldFilter("campanaID", "==", campana_id))
        data = [clean_datetimes(doc.to_dict() | {"id": doc.id}) for doc in q.stream()]
        return pd.DataFrame(data)
    else:
        data = [clean_datetimes(doc.to_dict() | {"id": doc.id}) for doc in col_ref.stream()]
        return pd.DataFrame(data)

def fetch_df_any(candidates: Iterable[str], campana_id: str) -> pd.DataFrame:
    for name in candidates:
        try:
            df = fetch_df_exact(name, campana_id)
            if not df.empty:
                log.info(f"[fetch_df_any] '{name}' -> {len(df)} filas")
                return df
            else:
                log.info(f"[fetch_df_any] '{name}' sin filas")
        except Exception as e:
            log.warning(f"[fetch_df_any] Error leyendo '{name}': {e}")
    return pd.DataFrame()

def _ymd(dt: Any):
    try:
        if pd.isna(dt):
            return None, None, None
        return int(dt.year), int(dt.month), int(dt.day)
    except Exception:
        return None, None, None

def _pick_or_fail(df: pd.DataFrame, canonical: str, candidates: list) -> pd.Series:
    for c in candidates:
        if c in df.columns:
            return df[c]
    raise KeyError(f"Falta columna requerida '{canonical}'. Candidatas: {candidates}. Presentes: {list(df.columns)}")

def _col(df: pd.DataFrame, candidates: list, default="") -> pd.Series:
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series([default] * len(df))

def _coerce_time(df: pd.DataFrame) -> pd.Series:
    if "Time" in df.columns:
        return pd.to_datetime(df["Time"], errors="coerce")
    for c in ["registroDate", "date"]:
        if c in df.columns:
            return pd.to_datetime(df[c], errors="coerce")
    parts = ["registroAnoDate", "registrosMesDate", "registrosDiaDate", "registrosHoraDate"]
    if all(p in df.columns for p in parts):
        hhmm = df["registrosHoraDate"].astype(str).str.zfill(5)
        s = (
            df["registroAnoDate"].astype(str) + "-" +
            df["registrosMesDate"].astype(str).str.zfill(2) + "-" +
            df["registrosDiaDate"].astype(str).str.zfill(2) + " " +
            hhmm
        )
        return pd.to_datetime(s, errors="coerce")
    raise KeyError(
        "No se encontró ninguna columna de fecha/hora válida. "
        "Opciones: Time, registroDate, date, o las 4 registroAnoDate/registrosMesDate/registrosDiaDate/registrosHoraDate."
    )

# ───────────────────────────────────────────────────────────────
# LLENADO PLANTILLA
# ───────────────────────────────────────────────────────────────
def llenar_plantilla_dwc(
    df_campana: pd.DataFrame,
    df_metodologia: pd.DataFrame,
    df_registro: pd.DataFrame,
    filename_out: str
) -> str:
    if not Path(RUTA_PLANTILLA).exists():
        raise FileNotFoundError(
            f"No se encontró la plantilla en '{RUTA_PLANTILLA}'. "
            f"cwd={Path.cwd().as_posix()} ROOT={ROOT.as_posix()}."
        )

    log.info(f"[plantilla] usando: {RUTA_PLANTILLA}")
    wb = load_workbook(RUTA_PLANTILLA)

    # Campaña
    df_c = df_campana.copy()
    name = _pick_or_fail(df_c, "Name", ["Name", "nameCamp", "Nombre campaña", "NombreCampaña"])
    ncamp = _pick_or_fail(df_c, "ncampana", ["ncampana", "nCampana", "numeroCampana", "Número de campaña"])
    start = _pick_or_fail(df_c, "startDateCamp", ["startDateCamp", "startDate", "Fecha de inicio de la campaña"])
    end   = _pick_or_fail(df_c, "endDateCamp",   ["endDateCamp", "endDate", "Fecha de término de la campaña"])

    df_c["Name"] = name
    df_c["ncampana"] = ncamp
    df_c["startDateCamp"] = pd.to_datetime(start, errors="coerce")
    df_c["endDateCamp"]   = pd.to_datetime(end,   errors="coerce")

    if len(df_c) > 1:
        df_c = pd.DataFrame(df_c.iloc[0, :]).T

    y_i, m_i, d_i = _ymd(df_c.loc[0, "startDateCamp"])
    y_t, m_t, d_t = _ymd(df_c.loc[0, "endDateCamp"])

    ws_c = wb["Campaña"]
    dic_camp = {
        'ID Campaña': 1, 'Nombre campaña': 2, 'Número de campaña': 3,
        'Año inicio': 4, 'Mes inicio': 5, 'Día inicio': 6,
        'Año término': 7, 'Mes término': 8, 'Día término': 9
    }
    dataCamp = {
        'ID Campaña': 1,
        'Nombre campaña': df_c.loc[0, "Name"],
        'Número de campaña': df_c.loc[0, "ncampana"],
        'Año inicio': y_i, 'Mes inicio': m_i, 'Día inicio': d_i,
        'Año término': y_t, 'Mes término': m_t, 'Día término': d_t
    }
    for col, val in dataCamp.items():
        ws_c.cell(row=3, column=dic_camp[col], value=val)

    # EstacionReplica
    df_m = df_metodologia.copy()
    df_m["Type"] = _col(df_m, ["Type", "type", "Tipo", "Tipo de monitoreo"], default="Transecto")
    df_m["nameest"] = _col(df_m, ["nameest", "nameEst", "Nombre estación", "nombreEst"], default="")
    if df_m["nameest"].isna().all():
        raise KeyError("Faltan columnas 'nameest'/'nameEst' en df_metodologia")

    df_m["Número Réplica"] = df_m.groupby(["nameest", "Type"]).cumcount() + 1
    df_m["ID EstacionReplica"] = np.arange(1, len(df_m) + 1)

    campos_estacion_replica = {
        "ID EstacionReplica": 1, "Nombre estación": 2, "Tipo de monitoreo": 3,
        "Número Réplica": 4, "Descripción EstacionReplica": 5,
        "Ancho (m)": 7, "Radio (m)": 8,
        "Región": 16, "Provincia": 17, "Comuna": 18, "Localidad": 19
    }

    df_m["Tipo de monitoreo"] = df_m["Type"]
    df_m["Nombre estación"] = df_m["nameest"]
    df_m["Descripción EstacionReplica"] = _col(df_m, ["Observaciones", "descripcion", "Descripción"], default="")
    df_m["Ancho (m)"] = _col(df_m, ["Ancho", "ancho"], default="")
    df_m["Radio (m)"] = _col(df_m, ["Radio", "radio"], default="")
    df_m["Región"] = _col(df_m, ["region", "Región"], default="")
    df_m["Provincia"] = _col(df_m, ["provincia", "Provincia"], default="")
    df_m["Comuna"] = _col(df_m, ["comuna", "Comuna"], default="")
    df_m["Localidad"] = _col(df_m, ["localidad", "Localidad"], default="")

    cols_out_est = list(campos_estacion_replica.keys())
    dfMetodologiaTMP = df_m.reindex(columns=(cols_out_est + ["metodologiaID"]))

    ws_e = wb["EstacionReplica"]
    for i in range(len(dfMetodologiaTMP)):
        row_excel = i + 2
        for col_name in cols_out_est:
            ws_e.cell(row=row_excel, column=campos_estacion_replica[col_name], value=dfMetodologiaTMP.loc[i, col_name])

    # Ocurrencia
    df_r = df_registro.copy()
    df_r["Time"] = _coerce_time(df_r)

    # Mapeo metodologíaID → ID EstacionReplica
    id_map = {}
    if "metodologiaID" in df_r.columns and "metodologiaID" in dfMetodologiaTMP.columns:
        for _, r in dfMetodologiaTMP.iterrows():
            id_map[r.get("metodologiaID")] = r.get("ID EstacionReplica")

    def mapValuesId_safe(met_id):
        return id_map.get(met_id, None)

    coords = _col(df_r, ["Coordinates", "coordinates", "coord"], default=None)

    campos_regitro_dwc = {
        'ID Campaña': 1, 'ID EstacionReplica': 3,
        'Año del evento': 5, 'Mes del evento': 6, 'Día del evento': 7,
        'Hora inicio evento (hh:mm)': 8, 'Protocolo de muestreo': 9,
        'Tamaño de la muestra': 10, 'Unidad del tamaño de la muestra': 11,
        'Comentarios del evento': 14,
        'Reino': 15, 'Filo o división': 16, 'Clase': 17, 'Orden': 18,
        'Familia': 19, 'Género': 20, 'Nombre común': 24,
        'Estado del organismo': 26,
        'Parámetro': 28, 'Tipo de cuantificación': 29, 'Valor': 30, 'Unidad de valor': 31,
        'Latitud decimal registro': 32, 'Longitud decimal registro': 33, 'Hora registro': 34,
        'Condición reproductiva': 35, 'Sexo (Fauna)': 36, 'Etapa de vida (Fauna)': 37,
        'Tipo de registro': 41, 'Muestreado por': 44, 'Identificado por': 45,
    }

    dfRegistroTMP = pd.DataFrame({
        'ID Campaña': 1,
        'ID EstacionReplica': _col(df_r, ["metodologiaID"]).map(mapValuesId_safe),
        'Año del evento': df_r["Time"].dt.year,
        'Mes del evento': df_r["Time"].dt.month,
        'Día del evento': df_r["Time"].dt.day,
        'Hora inicio evento (hh:mm)': df_r["Time"].dt.strftime("%H:%M"),
        'Protocolo de muestreo': _col(df_r, ["protocoloMuestreo", "Protocolo de muestreo"], ""),
        'Tamaño de la muestra': _col(df_r, ["tamanoMuestra", "tamañoMuestra", "tamano", "tamaño"], ""),
        'Unidad del tamaño de la muestra': _col(df_r, ["unidadTamanoMuestra", "unidadTamañoMuestra"], ""),
        'Comentarios del evento': _col(df_r, ["comentario", "comentarios", "Comentarios registro"], ""),
        'Reino': _col(df_r, ["reino", "Reino"], ""),
        'Filo o división': _col(df_r, ["division", "Filo o división", "filo"], ""),
        'Clase': _col(df_r, ["clase", "Clase"], ""),
        'Orden': _col(df_r, ["orden", "Orden"], ""),
        'Familia': _col(df_r, ["familia", "Familia"], ""),
        'Género': _col(df_r, ["genero", "Género"], ""),
        'Nombre común': _col(df_r, ["nombreComun", "Nombre común", "nameSp"], ""),
        'Estado del organismo': _col(df_r, ["estadoOrganismo", "Estado del organismo"], ""),
        'Parámetro': _col(df_r, ["parametro", "Parámetro"], ""),
        'Tipo de cuantificación': _col(df_r, ["tipoCuantificación", "tipoCuantificacion"], ""),
        'Valor': _col(df_r, ["valor", "Valor"], ""),
        'Unidad de valor': _col(df_r, ["unidadValor", "Unidad de valor"], ""),
        'Latitud decimal registro': coords.map(lambda c: getattr(c, "latitude", None)) if isinstance(coords, pd.Series) else pd.Series([None]*len(df_r)),
        'Longitud decimal registro': coords.map(lambda c: getattr(c, "longitude", None)) if isinstance(coords, pd.Series) else pd.Series([None]*len(df_r)),
        'Hora registro': df_r["Time"].dt.strftime("%H:%M"),
        'Condición reproductiva': _col(df_r, ["condicionReproductiva", "Condición reproductiva"], ""),
        'Sexo (Fauna)': _col(df_r, ["sexo", "Sexo (Fauna)"], ""),
        'Etapa de vida (Fauna)': _col(df_r, ["etapaVida", "Etapa de vida (Fauna)"], ""),
        'Tipo de registro': _col(df_r, ["tipoRegistro", "Tipo de registro"], ""),
        'Muestreado por': "AMS Consultores",
        'Identificado por': "AMS Consultores",
    })

    out_path = os.path.join(DOWNLOAD_DIR, filename_out)
    log.info(f"[save] guardando Excel en: {out_path}")
    wb_o = wb["Ocurrencia"]
    for i in range(len(dfRegistroTMP)):
        row_excel = i + 3
        for col_name, col_idx in campos_regitro_dwc.items():
            wb_o.cell(row=row_excel, column=col_idx, value=dfRegistroTMP.loc[i, col_name])

    wb.save(out_path)
    log.info(f"[save] Excel guardado OK: {out_path}")
    return out_path

# ───────────────────────────────────────────────────────────────
# WORKER DEL JOB (background)
# ───────────────────────────────────────────────────────────────
def _run_job(job_id: str, campana_id: str, base_url: str):
    try:
        _job_set(job_id, ts=time.time(), ready=False, error=None, download_url=None)

        df_campana = fetch_df_any(["campana", "Campana"], campana_id)
        df_registro = fetch_df_any(["Registro", "registro"], campana_id)
        df_metodologia = fetch_df_any(["Metodologia", "metodologia"], campana_id)

        log.info(f"[job {job_id}] filas -> campana={len(df_campana)}, registro={len(df_registro)}, metodologia={len(df_metodologia)}")

        if df_campana.empty or df_registro.empty or df_metodologia.empty:
            raise HTTPException(
                status_code=404,
                detail={
                    "message": "No hay datos para esta campaña (verifique colecciones/ID exacto).",
                    "campana_id": campana_id,
                    "filas": {
                        "campana": len(df_campana),
                        "registro": len(df_registro),
                        "metodologia": len(df_metodologia),
                    },
                },
            )

        filename = f"DWC_{_safe_filename(campana_id)}_{uuid.uuid4().hex[:6]}.xlsx"
        path = llenar_plantilla_dwc(df_campana, df_metodologia, df_registro, filename)

        base = base_url.rstrip("/")
        download_url = f"{base}/downloads/{os.path.basename(path)}"
        _job_set(job_id, ready=True, download_url=download_url, error=None)
        log.info(f"[job {job_id}] listo: {download_url}")

    except HTTPException as he:
        _job_set(job_id, ready=True, error=he.detail, download_url=None)
        log.warning(f"[job {job_id}] HTTPException: {he.detail}")
    except Exception as e:
        _job_set(job_id, ready=True, error=str(e), download_url=None)
        log.error(f"[job {job_id}] ERROR: {e}\n{traceback.format_exc()}")
    finally:
        _job_cleanup()

# ───────────────────────────────────────────────────────────────
# ENDPOINTS
# ───────────────────────────────────────────────────────────────
@app.get("/export")
def export_excel(
    request: Request,
    campana_id: str = Query(..., description="ID de la campaña (igual al guardado en campanaID)"),
    background_tasks: BackgroundTasks = None
):
    # Responder rápido para que FF no corte por timeout
    campana_id = campana_id.strip().strip('"')
    job_id = uuid.uuid4().hex
    base_url = str(request.base_url)

    _job_set(job_id, ts=time.time(), ready=False, error=None, download_url=None, campana_id=campana_id)
    if background_tasks is not None:
        background_tasks.add_task(_run_job, job_id, campana_id, base_url)
    else:
        # fallback (no debería pasar en FastAPI real)
        threading.Thread(target=_run_job, args=(job_id, campana_id, base_url), daemon=True).start()

    status_url = f"{base_url.rstrip('/')}/status/{job_id}"
    # Nota: el archivo aún no existe; el cliente debe consultar status_url hasta ready=true
    return JSONResponse({"job_id": job_id, "ready": False, "status_url": status_url})

@app.get("/status/{job_id}")
def job_status(job_id: str):
    job = _job_get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no encontrado o expirado.")
    return JSONResponse({
        "job_id": job_id,
        "ready": bool(job.get("ready")),
        "download_url": job.get("download_url"),
        "error": job.get("error"),
    })

@app.get("/")
def root():
    return {"status": "ok", "service": "Exporter DwC-SMA"}
