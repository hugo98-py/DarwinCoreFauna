# -*- coding: utf-8 -*-
"""
FastAPI: Exporta Excel DwC-SMA desde Firestore y entrega URL de descarga.
ÚNICO endpoint: /export -> {"download_url": "..."} (sirve estáticos en /downloads)
Listo para Render: puerto provisto por $PORT (lo toma uvicorn vía start command).
Firebase key vía env var FIREBASE_KEY_B64.
"""

import os, re, base64, uuid, warnings, logging, traceback
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Iterable

import numpy as np
import pandas as pd

from fastapi import FastAPI, Query, Request, HTTPException, Response
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from openpyxl import load_workbook

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

# ───────────────────────────────────────────────────────────────
# (Opcional) Oculta el warning de openpyxl por validaciones extendidas
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
# 📁 PATHS & CONFIG
# ───────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", (ROOT / "downloads").as_posix())
Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

# Si no defines RUTA_PLANTILLA, se asume que el archivo está al lado de main.py
RUTA_PLANTILLA = os.getenv(
    "RUTA_PLANTILLA",
    (ROOT / "FormatoBiodiversidadMonitoreoYLineaBase_v5.2.xlsx").as_posix()
)

app = FastAPI(title="Exporter DwC-SMA")
app.mount("/downloads", StaticFiles(directory=DOWNLOAD_DIR), name="downloads")

# ───────────────────────────────────────────────────────────────
# 🌐 CORS (opcional)
# ───────────────────────────────────────────────────────────────
cors_env = os.getenv("CORS_ORIGINS", "").strip()
if cors_env:
    origins = [o.strip() for o in cors_env.split(",") if o.strip()]
else:
    origins = ["*"]  # relajado para pruebas; ajusta en prod

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ───────────────────────────────────────────────────────────────
# 🔥 FIREBASE INIT (desde env var)
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
# 🔧 UTILS
# ───────────────────────────────────────────────────────────────
def _safe_filename(s: str) -> str:
    return re.sub(r"[^\w\-]+", "-", s)

def clean_datetimes(d: Dict[str, Any]) -> Dict[str, Any]:
    """Quita tzinfo de datetimes (evita problemas al serializar)."""
    for k, v in d.items():
        if isinstance(v, datetime) and v.tzinfo:
            d[k] = v.replace(tzinfo=None)
    return d

def fetch_df_exact(col: str, campana_id: str, filter_by_campana: bool = True) -> pd.DataFrame:
    """Descarga documentos de una colección específica; filtra por campanaID si existe."""
    campana_id = campana_id.strip('"')
    col_ref = db.collection(col)
    first = list(col_ref.limit(1).stream())
    if not first:
        return pd.DataFrame()
    if filter_by_campana and ("campanaID" in first[0].to_dict()):
        query = col_ref.where(filter=FieldFilter("campanaID", "==", campana_id))
    else:
        query = col_ref
    data = [clean_datetimes(doc.to_dict() | {"id": doc.id}) for doc in query.stream()]
    return pd.DataFrame(data)

def fetch_df_any(candidates: Iterable[str], campana_id: str) -> pd.DataFrame:
    """Intenta varias colecciones hasta obtener datos (para tolerar mayúsculas/minúsculas)."""
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
    """Devuelve (año, mes, día) o (None, None, None)."""
    try:
        if pd.isna(dt):
            return None, None, None
        return int(dt.year), int(dt.month), int(dt.day)
    except Exception:
        return None, None, None

def _pick_or_fail(df: pd.DataFrame, canonical: str, candidates: list) -> pd.Series:
    """Devuelve una serie asegurando una de las columnas; lanza error amigable si no existe."""
    for c in candidates:
        if c in df.columns:
            return df[c]
    raise KeyError(f"Falta columna requerida '{canonical}'. Candidatas: {candidates}. Presentes: {list(df.columns)}")

def _col(df: pd.DataFrame, candidates: list, default="") -> pd.Series:
    """Devuelve la primera columna existente; si no hay, devuelve un valor por defecto."""
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series([default] * len(df))

def _coerce_time(df: pd.DataFrame) -> pd.Series:
    """Crea/obtiene columna de tiempo a partir de varios esquemas posibles."""
    # 1) Columna 'Time' directa
    if "Time" in df.columns:
        return pd.to_datetime(df["Time"], errors="coerce")

    # 2) Campo 'registroDate' o 'date'
    for c in ["registroDate", "date"]:
        if c in df.columns:
            return pd.to_datetime(df[c], errors="coerce")

    # 3) Campos descompuestos: año/mes/día/hora (si existen)
    parts = ["registroAnoDate", "registrosMesDate", "registrosDiaDate", "registrosHoraDate"]
    if all(p in df.columns for p in parts):
        # Construye un string YYYY-MM-DD HH:MM
        hhmm = df["registrosHoraDate"].astype(str).str.zfill(5)  # asume "H:MM" o "HH:MM"
        s = (
            df["registroAnoDate"].astype(str) + "-" +
            df["registrosMesDate"].astype(str).str.zfill(2) + "-" +
            df["registrosDiaDate"].astype(str).str.zfill(2) + " " +
            hhmm
        )
        return pd.to_datetime(s, errors="coerce")

    raise KeyError(
        "No se encontró ninguna columna de fecha/hora válida. "
        "Opciones soportadas: Time, registroDate, date, o las 4 registroAnoDate/registrosMesDate/registrosDiaDate/registrosHoraDate."
    )

# ───────────────────────────────────────────────────────────────
# ✍️ LLENADO PLANTILLA DwC-SMA
# ───────────────────────────────────────────────────────────────
def llenar_plantilla_dwc(
    df_campana: pd.DataFrame,
    df_metodologia: pd.DataFrame,
    df_registro: pd.DataFrame,
    filename_out: str
) -> str:
    # Verificación clara con contexto útil
    if not Path(RUTA_PLANTILLA).exists():
        raise FileNotFoundError(
            f"No se encontró la plantilla en '{RUTA_PLANTILLA}'. "
            f"cwd={Path.cwd().as_posix()} ROOT={ROOT.as_posix()}. "
            "Súbela al repo o define RUTA_PLANTILLA como variable de entorno."
        )

    wb = load_workbook(RUTA_PLANTILLA)

    # ── Campaña (mapea nombres alternativos)
    df_c = df_campana.copy()
    name = _pick_or_fail(df_c, "Name", ["Name", "nameCamp", "Nombre campaña", "Nombre campaña", "NombreCampaña"])
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

    # ── EstacionReplica (tolera 'Type'/'type' y 'nameest'/'nameEst')
    df_m = df_metodologia.copy()
    df_m["Type"] = _col(df_m, ["Type", "type", "Tipo", "Tipo de monitoreo"], default="Transecto")
    df_m["nameest"] = _col(df_m, ["nameest", "nameEst", "Nombre estación", "Nombre estación", "nombreEst"], default="")

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

    # ── Ocurrencia (tolera múltiples esquemas de fecha/hora y nombres alternativos)
    df_r = df_registro.copy()
    df_r["Time"] = _coerce_time(df_r)

    # Mapeo metodologíaID → ID EstacionReplica
    id_map = {}
    if "metodologiaID" in df_r.columns and "metodologiaID" in dfMetodologiaTMP.columns:
        for _, r in dfMetodologiaTMP.iterrows():
            id_map[r.get("metodologiaID")] = r.get("ID EstacionReplica")

    def mapValuesId_safe(met_id):
        return id_map.get(met_id, None)

    def get_lat(coord):
        try:
            return coord.latitude
        except Exception:
            return None

    def get_lon(coord):
        try:
            return coord.longitude
        except Exception:
            return None

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
        'Latitud decimal registro': coords.map(get_lat) if isinstance(coords, pd.Series) else pd.Series([None]*len(df_r)),
        'Longitud decimal registro': coords.map(get_lon) if isinstance(coords, pd.Series) else pd.Series([None]*len(df_r)),
        'Hora registro': df_r["Time"].dt.strftime("%H:%M"),
        'Condición reproductiva': _col(df_r, ["condicionReproductiva", "Condición reproductiva"], ""),
        'Sexo (Fauna)': _col(df_r, ["sexo", "Sexo (Fauna)"], ""),
        'Etapa de vida (Fauna)': _col(df_r, ["etapaVida", "Etapa de vida (Fauna)"], ""),
        'Tipo de registro': _col(df_r, ["tipoRegistro", "Tipo de registro"], ""),
        'Muestreado por': "AMS Consultores",
        'Identificado por': "AMS Consultores",
    })

    ws_o = wb["Ocurrencia"]
    for i in range(len(dfRegistroTMP)):
        row_excel = i + 3  # asume 2 filas de meta + encabezado
        for col_name, col_idx in campos_regitro_dwc.items():
            ws_o.cell(row=row_excel, column=col_idx, value=dfRegistroTMP.loc[i, col_name])

    out_path = os.path.join(DOWNLOAD_DIR, filename_out)
    wb.save(out_path)
    return out_path

# ───────────────────────────────────────────────────────────────
# 📤 ÚNICO ENDPOINT
# ───────────────────────────────────────────────────────────────
@app.get("/export")
def export_excel(
    request: Request,
    campana_id: str = Query(..., description="ID de la campaña a filtrar (URL-encode si tiene /)")
):
    try:
        # Intenta con mayúsculas/minúsculas
        df_campana = fetch_df_any(["campana", "Campana"], campana_id)
        df_registro = fetch_df_any(["Registro", "registro"], campana_id)
        df_metodologia = fetch_df_any(["Metodologia", "metodologia"], campana_id)

        log.info(f"[export] filas -> campana={len(df_campana)}, registro={len(df_registro)}, metodologia={len(df_metodologia)}")

        if df_campana.empty or df_registro.empty or df_metodologia.empty:
            raise HTTPException(status_code=404, detail="No hay datos para esta campaña (verifique colecciones e ID).")

        filename = f"DWC_{_safe_filename(campana_id)}_{uuid.uuid4().hex[:6]}.xlsx"
        path = llenar_plantilla_dwc(df_campana, df_metodologia, df_registro, filename)

        base_url = str(request.base_url).rstrip("/")
        download_url = f"{base_url}/downloads/{os.path.basename(path)}"
        return JSONResponse({"download_url": download_url})

    except HTTPException:
        raise
    except Exception as e:
        log.error("[export] ERROR: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Fallo exportando Excel: {e}")

# Healthcheck + HEAD (para limpiar logs en Render)
@app.head("/")
def head_root():
    return Response(status_code=200)

@app.get("/")
def root():
    return {"status": "ok", "service": "Exporter DwC-SMA"}

# ───────────────────────────────────────────────────────────────
# 🔎 Endpoints de diagnóstico (puedes quitarlos luego)
# ───────────────────────────────────────────────────────────────
@app.get("/check")
def check():
    return {
        "template_exists": Path(RUTA_PLANTILLA).exists(),
        "template_path": RUTA_PLANTILLA,
        "download_dir": DOWNLOAD_DIR,
    }

@app.get("/peek")
def peek(campana_id: str):
    return {
        "campana_rows": len(fetch_df_any(["campana", "Campana"], campana_id)),
        "registro_rows": len(fetch_df_any(["Registro", "registro"], campana_id)),
        "metodologia_rows": len(fetch_df_any(["Metodologia", "metodologia"], campana_id)),
    }
