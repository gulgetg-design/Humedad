
"""
SWI · Script 1 de 2: ENVIAR JOBS (asincrónico, con control de cupo).

Mismo patrón que tu enviar EVI: manda todos los jobs a la API sin esperar
a que terminen, respetando un límite de jobs activos. La descarga va aparte
(swi_2_descargar.py).

Diferencia con EVI: SWI se pide por VENTANAS de 4 meses por estación (período
largo se rompe), y con TODAS las profundidades (T=2..100).

Credenciales por entorno (NO hardcodear):
    set OPENEO_CLIENT_ID=...      (Windows: set / Linux-Mac: export)
    set OPENEO_CLIENT_SECRET=...
"""
import os, json, logging, time, re
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
import geopandas as gpd
import openeo

# ───────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LIMITE_JOBS_ACTIVOS = 30       # ⚠️ ajustá a tu límite real
INTERVALO_ESPERA = 120         # cada cuánto rechequear cupo (seg)

CSV_KM = r"C:\Users\xaxa41\OneDrive - PAE\Documentos\Trabajos\Volumen - Prediccion\km_calculado.csv"

# Colección y bandas SWI
SWI_COLLECTION = "CLMS_SWI_GLOBAL_12_5KM_10DAILY_V4"

# TODAS las profundidades disponibles del SWI global.
SWI_BANDS = ["swi001", "swi005", "swi010", "swi015",
             "swi020", "swi040", "swi060", "swi100"]

# Período y tamaño de ventana
FECHA_INICIO = "2021-01-01"
FECHA_FIN    = date.today().strftime("%Y-%m-%d")
VENTANA_MESES = 4

ESTADOS_ACTIVOS = {"queued", "running", "created", "queued_for_start"}

# ───────────────────────────────────────────────────────────────────
# LOGGING
# ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=os.path.join(BASE_DIR, "log_swi_envio.txt"),
    level=logging.INFO, format="%(asctime)s - %(message)s", encoding="utf-8"
)
def log(msg):
    print(msg); logging.info(msg)

log("=== INICIO ENVÍO SWI ===")
log(f"BASE_DIR: {BASE_DIR}")

pepe = pd.read_csv("km_calculado.csv", sep=";")
log(f"CSV km_calculado leído: {len(pepe)} filas")

cid = os.environ.get("OPENEO_CLIENT_ID")
csec = os.environ.get("OPENEO_CLIENT_SECRET")
if not cid or not csec:
    raise SystemExit("Faltan OPENEO_CLIENT_ID / OPENEO_CLIENT_SECRET en el entorno.")

connection = openeo.connect("https://openeo.dataspace.copernicus.eu", auto_validate=False)
log("Autenticando...")
connection.authenticate_oidc_client_credentials(client_id=cid, client_secret=csec)
log("Autenticado OK.")

# ───────────────────────────────────────────────────────────────────
# Ventanas de 4 meses
# ───────────────────────────────────────────────────────────────────
def ventanas_4m(inicio, fin, meses=VENTANA_MESES):
    d0, dfin = date.fromisoformat(inicio), date.fromisoformat(fin)
    out, a = [], d0
    while a < dfin:
        sig = min(a + relativedelta(months=meses), dfin)
        out.append((a.isoformat(), sig.isoformat())); a = sig
    return out

VENTANAS = ventanas_4m(FECHA_INICIO, FECHA_FIN)
log(f"Ventanas de {VENTANA_MESES} meses: {len(VENTANAS)} "
    f"({VENTANAS[0][0]} → {VENTANAS[-1][1]})")

# ───────────────────────────────────────────────────────────────────
# Espera de cupo (igual que tu EVI)
# ───────────────────────────────────────────────────────────────────
def esperar_cupo():
    while True:
        try:
            jobs = connection.list_jobs()
            activos = sum(1 for j in jobs if j.get("status") in ESTADOS_ACTIVOS)
        except Exception as e:
            log(f"  ⚠️ No se pudo consultar jobs ({e}). Reintento en {INTERVALO_ESPERA}s.")
            time.sleep(INTERVALO_ESPERA); continue
        if activos < LIMITE_JOBS_ACTIVOS:
            log(f"  ✅ Hay lugar ({activos}/{LIMITE_JOBS_ACTIVOS} activos).")
            return
        log(f"  ⏳ Sin lugar ({activos}/{LIMITE_JOBS_ACTIVOS}). Espero {INTERVALO_ESPERA}s...")
        time.sleep(INTERVALO_ESPERA)

# ───────────────────────────────────────────────────────────────────
# QUÉ VENTANAS YA ESTÁN CUBIERTAS (para NO reenviar al pedo)
# Una ventana se SALTEA si:
#   - ya está descargada en la carpeta (parcial crudo o consolidado), o
#   - tiene un job ACTIVO (queued / running / created), o
#   - tiene un job TERMINADO esperando descarga (finished)
# Una ventana se ENVÍA si:
#   - no existe en ningún lado, o
#   - su último job quedó en ERROR (hay que reintentar)
# ───────────────────────────────────────────────────────────────────
output_folder = os.path.join(BASE_DIR, "SWI_estaciones")
os.makedirs(output_folder, exist_ok=True)

# 1) Títulos ya DESCARGADOS: archivos crudos SWI_{est}_{AAAAMMDD}.csv en la carpeta
descargados = set()
patron_crudo = re.compile(r"^(SWI_\d+_\d{8})\.csv$")
for f in os.listdir(output_folder):
    m = patron_crudo.match(f)
    if m:
        descargados.add(m.group(1))   # ej. "SWI_231457_20210101"

# 2) Estado de los jobs en la API, por título
ESTADOS_VIVOS = {"queued", "running", "created", "queued_for_start", "finished"}
jobs_vivos = set()      # títulos que NO hay que reenviar (activos o listos)
jobs_con_error = set()  # títulos que SÍ hay que reenviar
try:
    for j in connection.list_jobs():
        t = j.get("title", "")
        if not t.startswith("SWI_"):
            continue
        estado = j.get("status", "")
        if estado == "error":
            jobs_con_error.add(t)
        elif estado in ESTADOS_VIVOS:
            jobs_vivos.add(t)
except Exception as e:
    log(f"⚠️ No se pudo listar jobs ({e}). Se usa solo la carpeta como referencia.")

# Un título se considera "cubierto" si está descargado o tiene job vivo,
# PERO si está en error igual se reenvía (gana el error).
def ya_cubierto(title):
    if title in jobs_con_error:
        return False   # tuvo error -> reenviar
    return title in descargados or title in jobs_vivos

log(f"Ya descargados: {len(descargados)} | jobs vivos: {len(jobs_vivos)} | "
    f"con error (a reintentar): {len(jobs_con_error)}")

# ───────────────────────────────────────────────────────────────────
# RECORRER ESTACIONES × VENTANAS Y ENVIAR
# ───────────────────────────────────────────────────────────────────
estaciones = pepe.drop_duplicates(subset=["SHIP TO"])
log(f"Estaciones: {len(estaciones)} → {len(estaciones) * len(VENTANAS)} jobs totales")

for _, fila in estaciones.iterrows():
    id_estacion = int(fila["SHIP TO"])

    # Geometría puntual de la estación (SWI 12.5km: punto cae en 1 píxel)
    gdf = gpd.GeoDataFrame(
        fila.to_frame().T,
        geometry=gpd.points_from_xy([fila["LONGITUD"]], [fila["LATITUD"]]),
        crs="EPSG:4326",
    )
    geojson = json.loads(gdf.to_json())

    for desde, hasta in VENTANAS:
        # título único por estación+ventana (el _ del rango se reemplaza por -)
        v = desde.replace("-", "")
        title = f"SWI_{id_estacion}_{v}"
        if ya_cubierto(title):
            continue   # ya descargado / activo / listo -> no reenviar
        if title in jobs_con_error:
            log(f"  ♻️ Reintentando (tuvo error): {title}")

        esperar_cupo()   # esperar lugar ANTES de enviar
        try:
            cubo = connection.load_collection(
                SWI_COLLECTION,
                temporal_extent=[desde, hasta],
                bands=SWI_BANDS,
            )
            serie = cubo.aggregate_spatial(geometries=geojson, reducer="mean")
            job = serie.save_result(format="CSV").create_job(title=title)
            job.start_job()
            log(f"  📤 Enviado: {title} → {job.job_id}")
            time.sleep(10)
        except Exception as e:
            log(f"  ❌ Error enviando {title}: {e}")

log("=== FIN ENVÍO SWI ===")
     
