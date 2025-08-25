# -*- coding: utf-8 -*-
"""
FastAPI · Exportador Excel DwC-SMA (versión escritura con pandas)
-----------------------------------------------------------------
• Llena la plantilla oficial DwC-SMA (.xlsx) usando pandas.ExcelWriter.
• Hoja 'Campaña' se completa con overlay (respeta la estructura general).
• Hojas 'EstacionReplica' y 'Ocurrencia' se REEMPLAZAN (if_sheet_exists='replace').

Endpoint único:
  /export?campana_id=…   →  {"download_url": "..."}

Requisitos:
  pip install fastapi uvicorn pandas openpyxl google-cloud-firestore firebase-admin numpy
"""

import os, re, base64, json, uuid, warnings, shutil, unicodedata
from pathlib import Path
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

import firebase_admin
from firebase_admin import credentials, firestore

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ────────────────────────────────  Rutas & constantes
ROOT_DIR      = Path(__file__).parent
TEMPLATE_PATH = ROOT_DIR / "FormatoBiodiversidadMonitoreoYLineaBase_v5.2.xlsx"
DOWNLOAD_DIR  = Path("/tmp/downloads")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

LOCAL_TZ = ZoneInfo("America/Santiago")

# ────────────────────────────────  Firebase Init
B64 = os.environ.get("FIREBASE_KEY_B64")
if not B64:
    raise RuntimeError("FIREBASE_KEY_B64 env var is required")

cred_info = json.loads(base64.b64decode(B64))
if not firebase_admin._apps:
    firebase_admin.initialize_app(credentials.Certificate(cred_info))
db = firestore.client()

# ────────────────────────────────  FastAPI + CORS
app = FastAPI(title="Exporter DwC-SMA (pandas)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # ajusta en prod
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.mount("/downloads", StaticFiles(directory=str(DOWNLOAD_DIR)), name="downloads")

# ────────────────────────────────  Utils
def _safe_filename(s: str) -> str:
    return re.sub(r"[^\w\-]+", "-", s)

def clean_dt(x):
    if isinstance(x, datetime):
        if x.tzinfo is not None:
            x = x.astimezone(LOCAL_TZ)
        return x.replace(tzinfo=None)
    return x

def fetch_df(collection: str, campana_id: str) -> pd.DataFrame:
    cid = campana_id.strip('"')
    ref = db.collection(collection)
    docs = list(ref.limit(1).stream())
    if not docs:
        return pd.DataFrame()

    if "campanaID" in docs[0].to_dict():
        ref = ref.where("campanaID", "==", cid)

    data = [{**d.to_dict(), "id": d.id} for d in ref.stream()]
    return pd.DataFrame(data).map(clean_dt)

def ymd(dt: Any):
    if pd.isna(dt):
        return None, None, None
    try:
        return int(dt.year), int(dt.month), int(dt.day)
    except Exception:
        return None, None, None

def _to_none(val):
    if isinstance(val, (int, float)) and val == 999999:
        return None
    if isinstance(val, str) and val.strip().upper() == "NO DATA":
        return None
    return val

def _to_num(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return np.nan
    if isinstance(v, (int, float, np.number)):
        return float(v)
    s = str(v).strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return np.nan
    if s.count(",") >= 1 and s.count(".") == 0:
        s = s.replace(",", ".")
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except Exception:
        return np.nan

def _coerce_numeric_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(_to_num)
    return df

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def get_col(df: pd.DataFrame, *candidatas: str) -> str | None:
    norm_map = {_norm(c): c for c in df.columns}
    for cand in candidatas:
        key = _norm(cand)
        if key in norm_map:
            return norm_map[key]
    return None

# ────────────────────────────────  Generación Excel con pandas
def generar_excel_pandas(df_camp, df_met, df_reg, out_name: str) -> Path:
    # Copiamos la plantilla al destino
    out_path = DOWNLOAD_DIR / out_name
    shutil.copyfile(TEMPLATE_PATH, out_path)

    # ========== 1) CAMPANA (overlay en fila 3, col A) ==========
    camp = df_camp.iloc[0].copy()
    camp["startDateCamp"] = pd.to_datetime(camp.get("startDateCamp"), errors="coerce")
    camp["endDateCamp"]   = pd.to_datetime(camp.get("endDateCamp"),   errors="coerce")
    y_i, m_i, d_i = ymd(camp.get("startDateCamp"))
    y_t, m_t, d_t = ymd(camp.get("endDateCamp"))

    # Construimos una fila tal como llenabas antes
    fila_camp = pd.DataFrame([[
        1,                                      # ID Campaña (si aplica)
        camp.get("Name"),                       # Nombre campaña
        camp.get("ncampana"),                   # Código/num campaña
        y_i, m_i, d_i,                          # fecha inicio (Y M D)
        y_t, m_t, d_t,                          # fecha término (Y M D)
    ]])

    # Escribimos sobre la hoja 'Campaña' en la fila 3 (startrow=2), col A (startcol=0)
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as xw:
        fila_camp.to_excel(
            xw, sheet_name="Campaña", index=False, header=False, startrow=2, startcol=0
        )

    # ========== 2) ESTACIONREPLICA (reemplazo completo) ==========
    dfm = df_met.copy()

    # Asegura columnas mínimas
    for col in ["startCoordTL", "endCoordTL", "centralCoordinate", "Type"]:
        if col not in dfm.columns:
            dfm[col] = None

    # helpers de coordenadas
    def get_lat(p): return getattr(p, "latitude", None) if pd.notna(p) else None
    def get_lon(p): return getattr(p, "longitude", None) if pd.notna(p) else None

    mask_tl = dfm["Type"] == "Transecto Lineal"
    dfm.loc[mask_tl, "Latitud decimal inicio"]   = dfm["startCoordTL"].map(get_lat)
    dfm.loc[mask_tl, "Longitud decimal inicio"]  = dfm["startCoordTL"].map(get_lon)
    dfm.loc[mask_tl, "Latitud decimal término"]  = dfm["endCoordTL"].map(get_lat)
    dfm.loc[mask_tl, "Longitud decimal término"] = dfm["endCoordTL"].map(get_lon)

    # LARGO (m): tolerante a nombres alternativos
    col_largo = get_col(dfm, "largo", "largo (m)", "largo_m", "length", "length (m)")
    if col_largo:
        dfm.loc[mask_tl, "Largo (m)"] = dfm.loc[mask_tl, col_largo]

    # Otras metodologías → coordenada central
    mask_otras = ~mask_tl
    dfm.loc[mask_otras, "Latitud decimal central"]  = dfm["centralCoordinate"].map(get_lat)
    dfm.loc[mask_otras, "Longitud decimal central"] = dfm["centralCoordinate"].map(get_lon)

    # Numerics & área
    dfm = _coerce_numeric_cols(dfm, ["Radio", "Ancho", "Largo", "Ancho M", "Largo M"])
    mask_pm = (dfm["Type"] == "Punto de Muestreo")
    radios  = pd.to_numeric(dfm.get("Radio"), errors="coerce")
    dfm.loc[mask_pm, "Superficie (m2)"] = (np.pi * radios.pow(2)).round(0)

    # Resto
    dfm["Número Réplica"]     = dfm.groupby(["nameest", "Type"]).cumcount() + 1
    dfm["ID EstacionReplica"] = np.arange(1, len(dfm) + 1, dtype=int)
    dfm["Ecosistema nivel 2"] = dfm.get("ambienteest")

    def build_tipo_mon(row):
        if row.get("Type") in ("Transecto Lineal", "Play Back"):
            return f"{row.get('Type')} - {row.get('Clase')}"
        else:
            return row.get("Type")
    dfm["Tipo de monitoreo"] = dfm.apply(build_tipo_mon, axis=1)

    dfm = dfm.rename(columns={
        "nameest":       "Nombre estación",
        "Observaciones": "Descripción EstacionReplica",
        "Ancho":         "Ancho (m)",
        "Radio":         "Radio (m)",
        "region":        "Región",
        "provincia":     "Provincia",
        "comuna":        "Comuna",
        "localidad":     "Localidad",
    })

    cols_e = {
        "ID EstacionReplica": 1,  "Nombre estación": 2,     "Tipo de monitoreo": 3,
        "Número Réplica": 4,      "Descripción EstacionReplica": 5,
        "Largo (m)": 6,           "Ancho (m)": 7,           "Radio (m)": 8,
        "Superficie (m2)": 9,     "Latitud decimal central": 10,
        "Longitud decimal central": 11,
        "Latitud decimal inicio": 12,  "Longitud decimal inicio": 13,
        "Latitud decimal término": 14, "Longitud decimal término": 15,
        "Región": 16, "Provincia": 17, "Comuna": 18, "Localidad": 19,
        "Ecosistema nivel 2": 21,
    }
    # Garantiza columnas
    for c in cols_e:
        if c not in dfm.columns:
            dfm[c] = np.nan

    ordered_cols_e = [c for c, _ in sorted(cols_e.items(), key=lambda kv: kv[1])]
    df_e_write = dfm[ordered_cols_e].reset_index(drop=True)

    # ========== 3) OCURRENCIA (reemplazo completo) ==========
    dfr = df_reg.copy()
    dfr["Time"] = pd.to_datetime(dfr.get("Time"), errors="coerce")

    # map metodologiaID → ID EstacionReplica
    id_map = {}
    if "metodologiaID" in dfm.columns:
        id_map = dict(zip(dfm.get("metodologiaID", pd.Series(index=[])).fillna(""), dfm["ID EstacionReplica"]))
    if "metodologiaID" in dfr.columns:
        dfr["ID EstacionReplica"] = dfr["metodologiaID"].map(id_map)
    else:
        dfr["ID EstacionReplica"] = np.nan

    # Tipos a excluir
    EXCLUDE_TYPES = {
        "Detección de Eco Localizaciones",
        "Trampas Sherman",
        "Trampas Cámara",
    }

    # Tipo por metodologiaID (si aplica) para filtrar exclusiones
    if "metodologiaID" in dfm.columns and "metodologiaID" in dfr.columns:
        tipo_map = dict(zip(dfm["metodologiaID"], dfm["Type"]))
        dfr["Type"] = dfr["metodologiaID"].map(tipo_map)

        # Propiedades dinámicas (Tránsito Aéreo)
        def _build_dyn(row):
            if row.get("Type") != "Tránsito Aéreo":
                return ""
            d = row.get("desdeEl"); h = row.get("haciaEl")
            a = row.get("altura");  t = row.get("tipoVuelo")
            parts = []
            if pd.notna(d) and str(d).strip() != "": parts.append(f"Desde = {d}")
            if pd.notna(h) and str(h).strip() != "": parts.append(f"Hacia = {h}")
            if pd.notna(a) and str(a).strip() != "": parts.append(f"Altura de vuelo (m) = {a}")
            if pd.notna(t) and str(t).strip() != "": parts.append(f"Tipo de vuelo = {t}")
            return "; ".join(parts)

        dfr["Propiedades dinámicas"] = dfr.apply(_build_dyn, axis=1)
        dfr = dfr[~dfr["Type"].isin(EXCLUDE_TYPES)].copy()
        dfr.drop(columns=["Type"], inplace=True, errors="ignore")
        dfr.reset_index(drop=True, inplace=True)

    dfr["Año del evento"]             = dfr["Time"].dt.year
    dfr["Mes del evento"]             = dfr["Time"].dt.month
    dfr["Día del evento"]             = dfr["Time"].dt.day
    dfr["Hora inicio evento (hh:mm)"] = dfr["Time"].dt.strftime("%H:%M")
    dfr["Hora registro"]              = dfr["Hora inicio evento (hh:mm)"]
    dfr["Latitud decimal registro"]   = dfr.get("Coordinates").apply(lambda c: getattr(c, "latitude", None) if pd.notna(c) else None)
    dfr["Longitud decimal registro"]  = dfr.get("Coordinates").apply(lambda c: getattr(c, "longitude", None) if pd.notna(c) else None)

    dfr["ID Campaña"]       = 1
    dfr["Nombre campaña"]   = camp.get("Name")
    dfr["Muestreado por"]   = "AMS Consultores"
    dfr["Identificado por"] = "AMS Consultores"
    dfr["Epíteto específico"] = dfr.get("epiteto")

    campos_o = {
        1:"ID Campaña",2:"Nombre campaña",3:"ID EstacionReplica",
        5:"Año del evento",6:"Mes del evento",7:"Día del evento",
        8:"Hora inicio evento (hh:mm)",
        9:"protocoloMuestreo",
        10:"tamanoMuestra",11:"unidadTamanoMuestra",
        14:"comentario",
        15:"reino",16:"division",17:"clase",18:"orden",
        19:"familia",20:"genero",
        22:"epiteto",24:"nombreComun",
        26:"estadoOrganismo",
        28:"parametro",29:"tipoCuantificacion",
        30:"valor",31:"unidadValor",
        32:"Latitud decimal registro",33:"Longitud decimal registro",
        34:"Hora registro",
        35:"condicionReproductiva",
        36:"sexo",37:"etapaVida",
        40:"Propiedades dinámicas",
        41:"tipoRegistro",
        44:"Muestreado por",45:"Identificado por",
    }
    for col in set(campos_o.values()):
        if col not in dfr.columns:
            dfr[col] = ""

    ordered_cols_o = [col for _, col in sorted(campos_o.items(), key=lambda kv: kv[0])]
    df_o_write = dfr[ordered_cols_o].reset_index(drop=True)

    # ========== Escritura con pandas ==========
    # Reemplazamos por completo EstacionReplica y Ocurrencia (incluye encabezados)
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as xw:
        df_e_write.to_excel(
            xw, sheet_name="EstacionReplica", index=False, header=True
        )
        # En plantilla original 'Ocurrencia' suele tener 2 filas de encabezado;
        # aquí se escribe un header plano (pandas). Ajusta si necesitas dos filas.
        df_o_write.to_excel(
            xw, sheet_name="Ocurrencia", index=False, header=True
        )

    return out_path

# ────────────────────────────────  Endpoint
@app.get("/export")
def export_excel(request: Request, campana_id: str = Query(...)):
    df_camp = fetch_df("campana",     campana_id)
    df_met  = fetch_df("Metodologia", campana_id)
    df_reg  = fetch_df("Registro",    campana_id)

    if df_camp.empty or df_met.empty or df_reg.empty:
        raise HTTPException(status_code=404, detail="No hay datos para la campaña.")

    # Limpia valores “NO DATA”/999999
    df_met = df_met.apply(lambda col: col.map(_to_none))
    df_reg = df_reg.apply(lambda col: col.map(_to_none))

    filename = f"DWC_{_safe_filename(campana_id)}_{uuid.uuid4().hex[:6]}.xlsx"
    path     = generar_excel_pandas(df_camp, df_met, df_reg, filename)

    download_url = f"{str(request.base_url).rstrip('/')}/downloads/{path.name}"
    return JSONResponse({"download_url": download_url})














