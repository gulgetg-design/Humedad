"""
SWI · Script 2 de 2: DESCARGAR JOBS terminados y consolidar.

Mismo patrón que tu descargar EVI: toma los jobs SWI, verifica que estén
terminados, baja cada CSV y hace merge incremental por estación.

Diferencia con EVI: como SWI se mandó por ventanas de 4 meses, cada estación
tiene VARIOS jobs (uno por ventana). El merge junta todas las ventanas de una
estación en un único CSV, aplica la escala 0.5 y descarta nodata (255), y deja
formato largo [estacion, fecha, T, swi].

Credenciales por entorno (NO hardcodear).
"""
import os, logging, re
from datetime import date
import pandas as pd
import openeo

# ───────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_KM = r"C:\Users\xaxa41\OneDrive - PAE\Documentos\Trabajos\Volumen - Prediccion\km_calculado.csv"
output_folder = os.path.join(BASE_DIR, "SWI_estaciones")
os.makedirs(output_folder, exist_ok=True)

SWI_BANDS = ["SWI_002", "SWI_005", "SWI_010", "SWI_015",
             "SWI_020", "SWI_040", "SWI_060", "SWI_100"]
SWI_SCALE = 0.5
SWI_NODATA = 255

# Exigir que TODOS los jobs SWI estén terminados antes de bajar (como tu EVI).
EXIGIR_TODOS_TERMINADOS = True

# ───────────────────────────────────────────────────────────────────
# LOGGING
# ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=os.path.join(BASE_DIR, "log_swi_descarga.txt"),
    level=logging.INFO, format="%(asctime)s - %(message)s", encoding="utf-8"
)
def log(msg):
    print(msg); logging.info(msg)

log("=== INICIO DESCARGA SWI ===")

cid = os.environ.get("OPENEO_CLIENT_ID")
csec = os.environ.get("OPENEO_CLIENT_SECRET")
if not cid or not csec:
    raise SystemExit("Faltan OPENEO_CLIENT_ID / OPENEO_CLIENT_SECRET en el entorno.")

connection = openeo.connect("https://openeo.dataspace.copernicus.eu", auto_validate=False)
connection.authenticate_oidc_client_credentials(client_id=cid, client_secret=csec)
log("Autenticado OK.")

# ───────────────────────────────────────────────────────────────────
# 1. TOMAR TODOS LOS JOBS SWI Y VERIFICAR ESTADOS
# ───────────────────────────────────────────────────────────────────
todos = connection.list_jobs()
jobs_swi = [j for j in todos if j.get("title", "").startswith("SWI_")]
log(f"Jobs SWI encontrados: {len(jobs_swi)}")

con_error     = [j for j in jobs_swi if j.get("status") == "error"]
no_terminados = [j for j in jobs_swi if j.get("status") != "finished"
                 and j.get("status") != "error"]

if con_error:
    log(f"❌ {len(con_error)} jobs con ERROR: {[j.get('title') for j in con_error][:10]}...")

if EXIGIR_TODOS_TERMINADOS and no_terminados:
    log(f"⏳ Faltan terminar {len(no_terminados)} jobs. No se descarga aún.")
    for j in no_terminados[:10]:
        log(f"   - {j.get('title')}: {j.get('status')}")
    log("=== FIN (esperando que terminen) ===")
    raise SystemExit(0)

# Bajamos solo los terminados (si no exigís todos, baja lo que haya listo)
terminados = [j for j in jobs_swi if j.get("status") == "finished"]
log(f"✅ Descargando {len(terminados)} jobs terminados...")

# ───────────────────────────────────────────────────────────────────
# 2. DESCARGAR CADA JOB (un CSV crudo por estacion+ventana)
# ───────────────────────────────────────────────────────────────────
patron = re.compile(r"^SWI_(\d+)_(\d{8})$")   # SWI_{estacion}_{AAAAMMDD}

for jinfo in terminados:
    title = jinfo.get("title", "")
    m = patron.match(title)
    if not m:
        log(f"⚠️ Título inesperado: {title}")
        continue
    id_estacion, vent = m.group(1), m.group(2)
    crudo = os.path.join(output_folder, f"SWI_{id_estacion}_{vent}.csv")

    if os.path.exists(crudo):
        log(f"⏩ Ya existe: {os.path.basename(crudo)}")
        continue
    try:
        job = connection.job(jinfo["id"])
        bajado = False
        for asset in job.get_results().get_assets():
            if asset.name.lower().endswith(".csv"):
                asset.download(crudo)
                log(f"✅ Descargado: {title}")
                bajado = True
                break
        if not bajado:
            log(f"⚠️ Sin CSV en {title}")
    except Exception as e:
        log(f"❌ Error descargando {title}: {e}")

# ───────────────────────────────────────────────────────────────────
# 3. MERGE INCREMENTAL POR ESTACIÓN (junta todas las ventanas)
# ───────────────────────────────────────────────────────────────────
pepe = pd.read_csv("km_calculado.csv", sep=";")
estaciones = (pepe.groupby("SHIP TO")["km_calculado"].min()
              .sort_values().index.tolist())

def a_formato_largo(df, est):
    """Escala 0.5, descarta nodata, pasa a [estacion, fecha, T, swi]."""
    regs = []
    for _, fila in df.iterrows():
        fecha = fila.get("date") or fila.get("t") or fila.get("time")
        for banda in SWI_BANDS:
            if banda not in df.columns:
                continue
            val = fila[banda]
            if pd.isna(val) or val == SWI_NODATA:
                continue
            regs.append({"estacion": est, "fecha": fecha,
                         "T": int(banda.split("_")[1]), "swi": float(val) * SWI_SCALE})
    return pd.DataFrame(regs)

for id_estacion in estaciones:
    parciales = [
        os.path.join(output_folder, f)
        for f in os.listdir(output_folder)
        if re.match(rf"^SWI_{id_estacion}_\d{{8}}\.csv$", f)
    ]
    if not parciales:
        continue
    archivo_final = os.path.join(output_folder, f"SWI_Estacion_{id_estacion}.csv")
    try:
        largos = [a_formato_largo(pd.read_csv(p), id_estacion) for p in parciales]
        nuevos = pd.concat(largos, ignore_index=True)

        if os.path.exists(archivo_final):
            existente = pd.read_csv(archivo_final)
            combinado = pd.concat([existente, nuevos], ignore_index=True)
        else:
            combinado = nuevos

        combinado = (combinado
                     .drop_duplicates(subset=["estacion", "fecha", "T"])
                     .sort_values(["fecha", "T"])
                     .reset_index(drop=True))
        combinado.to_csv(archivo_final, index=False)
        log(f"✅ {id_estacion}: {len(combinado)} filas → {os.path.basename(archivo_final)}")
    except Exception as e:
        log(f"❌ Error mergeando {id_estacion}: {e}")

log("=== FIN DESCARGA SWI ===")
