import os
import re
import time
import json
from datetime import date
from dateutil.relativedelta import relativedelta

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import openeo


# --- Config ---



"""
Extrae Soil Water Index (SWI) por estación desde OpenEO / CDSE.

CAMBIO ESTRUCTURAL: en vez de pedir todo el período de una (se rompe / timeout),
recorre cada estación en VENTANAS de 4 meses y hace un pedido por ventana.
Pedidos chiquitos = robusto. Junta todo al final.

Pide TODAS las profundidades del SWI (T=2..100). Valor 0-100 (0=seco, 100=saturado).

Robustez:
  - reintentos por ventana ante fallo
  - pausa entre pedidos (no saturar el backend)
  - guardado INCREMENTAL: si se corta, retoma donde quedó sin reprocesar

SEGURIDAD: credenciales por variables de entorno, NUNCA hardcodeadas:
    export OPENEO_CLIENT_ID="tu_id"
    export OPENEO_CLIENT_SECRET="tu_secret"
"""# ID de la colección SWI en CDSE. Si falla, corré listar_ids_swi() y reemplazá.
SWI_COLLECTION = "CLMS_SWI_GLOBAL_12_5KM_10DAILY_V4"

# TODAS las profundidades disponibles del SWI global.
SWI_BANDS = ["swi001", "swi005", "swi010", "swi015",
             "swi020", "swi040", "swi060", "swi100"]

SWI_SCALE = 0.5
SWI_NODATA = 255

FECHA_INICIO = "2021-01-01"
FECHA_FIN = "2026-06-30"
VENTANA_MESES = 4              # tamaño de cada pedido

ESTACIONES_CSV = "km_calculado.csv"
CAMPO_ID = "SHIP TO"
COL_LAT = "LATITUD"
COL_LON = "LONGITUD"
COL_COORD = None              # ej. "coordenadas" si lat/lon están juntas

CSV_SALIDA = "swi_estaciones.csv"
PARCIALES_DIR = "swi_parciales"   # un CSV por (estacion, ventana) — permite retomar

# Robustez
MAX_REINTENTOS = 3
PAUSA_ENTRE_PEDIDOS = 5.0     # segundos
PAUSA_REINTENTO = 20.0


def conectar():
    connection = openeo.connect("https://openeo.dataspace.copernicus.eu", auto_validate=False)
    connection.authenticate_oidc_client_credentials(
        client_id="sh-03305422-8f0c-4234-bc98-cda401383433",
        client_secret="qvPTIrMsWhRQJIRK9lJdWIhJK65VdWyy",
    )
    return connection


def listar_ids_swi(con):
    ids = [c["id"] for c in con.list_collections()
           if "swi" in c["id"].lower() or "soil" in c["id"].lower()]
    print("Colecciones SWI/soil disponibles:", ids or "(ninguna)")
    return ids


def parse_coord(valor):
    if valor is None:
        return None
    nums = re.findall(r"-?\d+[.,]?\d*", str(valor))
    if len(nums) < 2:
        return None
    return float(nums[0].replace(",", ".")), float(nums[1].replace(",", "."))


def cargar_estaciones():
    df = pd.read_csv(ESTACIONES_CSV, sep=";")
    if COL_COORD and COL_COORD in df.columns:
        coords = df[COL_COORD].map(parse_coord)
        df = df[coords.notna()].copy()
        coords = coords[coords.notna()]
        df["_lat"] = [c[0] for c in coords]
        df["_lon"] = [c[1] for c in coords]
    else:
        df["_lat"] = pd.to_numeric(df[COL_LAT], errors="coerce")
        df["_lon"] = pd.to_numeric(df[COL_LON], errors="coerce")
        df = df.dropna(subset=["_lat", "_lon"]).copy()
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(lon, lat) for lat, lon in zip(df["_lat"], df["_lon"])],
        crs="EPSG:4326",
    )
    return gdf


def ventanas_de_4_meses(inicio, fin, meses=VENTANA_MESES):
    """Genera (desde, hasta) en tramos de N meses cubriendo [inicio, fin]."""
    d0 = date.fromisoformat(inicio)
    dfin = date.fromisoformat(fin)
    ventanas = []
    actual = d0
    while actual < dfin:
        siguiente = min(actual + relativedelta(months=meses), dfin)
        ventanas.append((actual.isoformat(), siguiente.isoformat()))
        actual = siguiente
    return ventanas


def ruta_parcial(est_id, desde):
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", str(est_id))
    return os.path.join(PARCIALES_DIR, f"{safe}__{desde}.csv")


def pedir_ventana(con, geom_feature, est_id, desde, hasta):
    """Pide una ventana de 4 meses para UNA estación. Devuelve DataFrame o None."""
    parcial = ruta_parcial(est_id, desde)
    if os.path.exists(parcial):   # ya bajada antes -> retomar sin reprocesar
        return pd.read_csv(parcial)

    fc = {"type": "FeatureCollection", "features": [geom_feature]}
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            cubo = con.load_collection(
                SWI_COLLECTION,
                temporal_extent=[desde, hasta],
                bands=SWI_BANDS,
            )
            serie = cubo.aggregate_spatial(geometries=fc, reducer="mean")
            job = serie.execute_batch(
                out_format="CSV",
                title=f"SWI_{est_id}_{desde}",
            )
            tmp = parcial + ".tmp"
            job.get_results().download_file(tmp)
            df = pd.read_csv(tmp)
            df["estacion"] = est_id
            os.replace(tmp, parcial)   # commit atómico del parcial
            return df
        except Exception as e:
            print(f"    [!] {est_id} {desde}: intento {intento}/{MAX_REINTENTOS} falló: {e}")
            if intento < MAX_REINTENTOS:
                time.sleep(PAUSA_REINTENTO)
    print(f"    [x] {est_id} {desde}: agotados los reintentos, se saltea")
    return None


def extraer_swi():
    os.makedirs(PARCIALES_DIR, exist_ok=True)
    con = conectar()
    estaciones = cargar_estaciones()
    ventanas = ventanas_de_4_meses(FECHA_INICIO, FECHA_FIN)
    print(f"[i] {len(estaciones)} estaciones × {len(ventanas)} ventanas "
          f"= {len(estaciones) * len(ventanas)} pedidos")

    todos = []
    for _, est in estaciones.iterrows():
        est_id = est[CAMPO_ID] if CAMPO_ID in est else est.name
        feature = json.loads(gpd.GeoSeries([est.geometry]).to_json())["features"][0]
        for desde, hasta in ventanas:
            df = pedir_ventana(con, feature, est_id, desde, hasta)
            if df is not None and not df.empty:
                todos.append(df)
            time.sleep(PAUSA_ENTRE_PEDIDOS)

    if not todos:
        print("[x] No se obtuvo ningún dato.")
        return None

    bruto = pd.concat(todos, ignore_index=True)
    limpiar_y_guardar(bruto)
    return CSV_SALIDA


def limpiar_y_guardar(df):
    """Escala 0.5, descarta nodata, formato largo [estacion, fecha, T, swi]."""
    registros = []
    for _, fila in df.iterrows():
        fecha = fila.get("date") or fila.get("t") or fila.get("time")
        est = fila.get("estacion")
        for banda in SWI_BANDS:
            if banda not in df.columns:
                continue
            val = fila[banda]
            if pd.isna(val) or val == SWI_NODATA:
                continue
            registros.append({
                "estacion": est,
                "fecha": fecha,
                "T": int(banda.split("_")[1]),
                "swi": float(val) * SWI_SCALE,
            })
    salida = (pd.DataFrame(registros)
              .drop_duplicates(subset=["estacion", "fecha", "T"])
              .sort_values(["estacion", "fecha", "T"]))
    salida.to_csv(CSV_SALIDA, index=False)
    print(f"[✓] {CSV_SALIDA}: {len(salida)} filas, "
          f"{salida['estacion'].nunique()} estaciones, "
          f"{salida['T'].nunique()} profundidades")


if __name__ == "__main__":
    extraer_swi()
