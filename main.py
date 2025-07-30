# -*- coding: utf-8 -*-
"""
FastAPI: Exporta Excel DwC-SMA desde Firestore y entrega URL de descarga.
ÚNICO endpoint: /export -> {"download_url": "..."} (sirve estáticos en /downloads)
Listo para Render: puerto provisto por $PORT (lo toma uvicorn vía start command).
Firebase key vía env var FIREBASE_KEY_B64.
"""

import os, re, base64, uuid
from datetime import datetime
from typing import Any, Dict

import numpy as np
import pandas as pd

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from openpyxl import load_workbook

import firebase_admin
from firebase_admin import credentials, firestore

# ───────────────────────────────────────────────────────────────
# 📁 CONFIG
# ───────────────────────────────────────────────────────────────
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Pon la plantilla en tu repo (p. ej., ./plantillas/archivo.xlsx)
RUTA_PLANTILLA = os.getenv(
    "RUTA_PLANTILLA",
    "plantillas/FormatoBiodiversidadMonitoreoYLineaBase_v5.2.xlsx"
)

app = FastAPI(title="Exporter DwC-SMA")
app.mount("/downloads", StaticFiles(directory=DOWNLOAD_DIR), name="downloads")

# ───────────────────────────────────────────────────────────────
# 🌐 CORS (opcional; útil para FlutterFlow/Web)
# Define CORS_ORIGINS="https://tuapp.web.app,https://app.flutterflow.io" en Render
# o déjalo vacío para permitir todo (solo para pruebas)
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
    cred_path = "firebase_key.json"
    with open(cred_path, "wb") as f:
        f.write(base64.b64decode(FIREBASE_KEY_B64))
    firebase_admin.initialize_app(credentials.Certificate(cred_path))

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

def fetch_df(col: str, campana_id: str, filter_by_campana: bool = True) -> pd.DataFrame:
    """Descarga documentos de una colección; filtra por campanaID si existe."""
    campana_id = campana_id.strip('"')  # por si viene con comillas
    col_ref = db.collection(col)
    first = list(col_ref.limit(1).stream())
    if not first:
        return pd.DataFrame()

    if filter_by_campana and "campanaID" in first[0].to_dict():
        query = col_ref.where("campanaID", "==", campana_id)
    else:
        query = col_ref

    data = [clean_datetimes(doc.to_dict() | {"id": doc.id}) for doc in query.stream()]
    return pd.DataFrame(data)

def _ymd(dt: Any):
    """Devuelve (año, mes, día) o (None, None, None)."""
    try:
        if pd.isna(dt):
            return None, None, None
        return int(dt.year), int(dt.month), int(dt.day)
    except Exception:
        return None, None, None

# ───────────────────────────────────────────────────────────────
# ✍️ LLENADO PLANTILLA DwC-SMA
# ───────────────────────────────────────────────────────────────
def llenar_plantilla_dwc(
    df_campana: pd.DataFrame,
    df_metodologia: pd.DataFrame,
    df_registro: pd.DataFrame,
    filename_out: str
) -> str:
    if not os.path.exists(RUTA_PLANTILLA):
        raise FileNotFoundError(
            f"No se encontró la plantilla en '{RUTA_PLANTILLA}'. "
            "Súbela al repo o define RUTA_PLANTILLA como variable de entorno."
        )

    wb = load_workbook(RUTA_PLANTILLA)

    # ── Campaña
    if len(df_campana) > 1:
        df_campana = pd.DataFrame(df_campana.iloc[0, :]).T
    for required_col in ["startDateCamp", "endDateCamp", "Name", "ncampana"]:
        if required_col not in df_campana.columns:
            raise KeyError(f"Falta columna requerida en df_campana: '{required_col}'")
    df_campana["startDateCamp"] = pd.to_datetime(df_campana["startDateCamp"], errors="coerce")
    df_campana["endDateCamp"]   = pd.to_datetime(df_campana["endDateCamp"],   errors="coerce")
    y_i, m_i, d_i = _ymd(df_campana.loc[0, "startDateCamp"])
    y_t, m_t, d_t = _ymd(df_campana.loc[0, "endDateCamp"])

    ws_c = wb["Campaña"]
    dic_camp = {
        'ID Campaña': 1, 'Nombre campaña': 2, 'Número de campaña': 3,
        'Año inicio': 4, 'Mes inicio': 5, 'Día inicio': 6,
        'Año término': 7, 'Mes término': 8, 'Día término': 9
    }
    dataCamp = {
        'ID Campaña': 1,
        'Nombre campaña': df_campana.loc[0, "Name"],
        'Número de campaña': df_campana.loc[0, "ncampana"],
        'Año inicio': y_i, 'Mes inicio': m_i, 'Día inicio': d_i,
        'Año término': y_t, 'Mes término': m_t, 'Día término': d_t
    }
    for col, val in dataCamp.items():
        ws_c.cell(row=3, column=dic_camp[col], value=val)

    # ── EstacionReplica
    if "nameest" not in df_metodologia.columns or "Type" not in df_metodologia.columns:
        raise KeyError("Faltan columnas 'nameest' o 'Type' en df_metodologia")

    df_metodologia = df_metodologia.copy()
    df_metodologia["Número Réplica"] = df_metodologia.groupby(["nameest", "Type"]).cumcount() + 1
    df_metodologia["ID EstacionReplica"] = np.arange(1, len(df_metodologia) + 1)

    campos_estacion_replica = {
        "ID EstacionReplica": 1, "Nombre estación": 2, "Tipo de monitoreo": 3,
        "Número Réplica": 4, "Descripción EstacionReplica": 5,
        "Ancho (m)": 7, "Radio (m)": 8,
        "Región": 16, "Provincia": 17, "Comuna": 18, "Localidad": 19
    }

    df_metodologia["Tipo de monitoreo"] = "Transecto"  # ajusta si corresponde
    df_metodologia["Nombre estación"] = df_metodologia.get("nameest", "")
    df_metodologia["Descripción EstacionReplica"] = df_metodologia.get("Observaciones", "")
    df_metodologia["Ancho (m)"] = df_metodologia.get("Ancho", "")
    df_metodologia["Radio (m)"] = df_metodologia.get("Radio", "")
    df_metodologia["Región"] = df_metodologia.get("region", "")
    df_metodologia["Provincia"] = df_metodologia.get("provincia", "")
    df_metodologia["Comuna"] = df_metodologia.get("comuna", "")
    df_metodologia["Localidad"] = df_metodologia.get("localidad", "")

    cols_out_est = list(campos_estacion_replica.keys())
    dfMetodologiaTMP = df_metodologia.reindex(columns=(cols_out_est + ["metodologiaID"]))

    ws_e = wb["EstacionReplica"]
    for i in range(len(dfMetodologiaTMP)):
        row_excel = i + 2
        for col_name in cols_out_est:
            ws_e.cell(row=row_excel, column=campos_estacion_replica[col_name], value=dfMetodologiaTMP.loc[i, col_name])

    # ── Ocurrencia
    if "Time" not in df_registro.columns:
        raise KeyError("Falta columna 'Time' en df_registro")

    df_reg = df_registro.copy()
    df_reg["Time"] = pd.to_datetime(df_reg["Time"], errors="coerce")

    # Map metodologíaID → ID EstacionReplica
    id_map = {}
    if "metodologiaID" in df_reg.columns and "metodologiaID" in dfMetodologiaTMP.columns:
        for _, r in dfMetodologiaTMP.iterrows():
            id_map[r.get("metodologiaID")] = r.get("ID EstacionReplica")

    def mapValuesId_safe(met_id):
        return id_map.get(met_id, None)

    # GeoPoint → lat/lon
    def get_lat(coord):
        try: return coord.latitude
        except Exception: return None

    def get_lon(coord):
        try: return coord.longitude
        except Exception: return None

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
        'ID EstacionReplica': df_reg.get("metodologiaID", pd.Series([None]*len(df_reg))).map(mapValuesId_safe),
        'Año del evento': df_reg["Time"].dt.year,
        'Mes del evento': df_reg["Time"].dt.month,
        'Día del evento': df_reg["Time"].dt.day,
        'Hora inicio evento (hh:mm)': df_reg["Time"].dt.strftime("%H:%M"),
        'Protocolo de muestreo': df_reg.get("protocoloMuestreo", ""),
        'Tamaño de la muestra': df_reg.get("tamanoMuestra", ""),
        'Unidad del tamaño de la muestra': df_reg.get("unidadTamanoMuestra", ""),
        'Comentarios del evento': df_reg.get("comentario", ""),
        'Reino': df_reg.get("reino", ""), 'Filo o división': df_reg.get("division", ""),
        'Clase': df_reg.get("clase", ""), 'Orden': df_reg.get("orden", ""),
        'Familia': df_reg.get("familia", ""), 'Género': df_reg.get("genero", ""),
        'Nombre común': df_reg.get("nombreComun", ""),
        'Estado del organismo': df_reg.get("estadoOrganismo", ""),
        'Parámetro': df_reg.get("parametro", ""),
        'Tipo de cuantificación': df_reg.get("tipoCuantificación", ""),
        'Valor': df_reg.get("valor", ""),
        'Unidad de valor': df_reg.get("unidadValor", ""),
        'Latitud decimal registro': df_reg.get("Coordinates", pd.Series([None]*len(df_reg))).map(get_lat),
        'Longitud decimal registro': df_reg.get("Coordinates", pd.Series([None]*len(df_reg))).map(get_lon),
        'Hora registro': df_reg["Time"].dt.strftime("%H:%M"),
        'Condición reproductiva': df_reg.get("condicionReproductiva", ""),
        'Sexo (Fauna)': df_reg.get("sexo", ""),
        'Etapa de vida (Fauna)': df_reg.get("etapaVida", ""),
        'Tipo de registro': df_reg.get("tipoRegistro", ""),
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
def export_excel(request: Request, campana_id: str = Query(..., description="ID de la campaña a filtrar")):
    df_campana = fetch_df("campana", campana_id)
    df_registro = fetch_df("Registro", campana_id)
    df_metodologia = fetch_df("Metodologia", campana_id)

    if df_campana.empty or df_registro.empty or df_metodologia.empty:
        raise HTTPException(status_code=404, detail="No hay datos para esta campaña.")

    filename = f"DWC_{_safe_filename(campana_id)}_{uuid.uuid4().hex[:6]}.xlsx"
    path = llenar_plantilla_dwc(df_campana, df_metodologia, df_registro, filename)

    base_url = str(request.base_url).rstrip("/")
    download_url = f"{base_url}/downloads/{os.path.basename(path)}"
    return JSONResponse({"download_url": download_url})

# Healthcheck simple (útil en Render)
@app.get("/")
def root():
    return {"status": "ok", "service": "Exporter DwC-SMA"}

