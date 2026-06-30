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
from dateutil.relativedelta import relativedelta
import pandas as pd
import openeo

# ───────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_KM = r"C:\Users\xaxa41\OneDrive - PAE\Documentos\Trabajos\Volumen - Prediccion\km_calculado.csv"
output_folder = os.path.join(BASE_DIR, "SWI_estaciones")
os.makedirs(output_folder, exist_ok=True)

SWI_BANDS = ["swi001", "swi005", "swi010", "swi015",
             "swi020", "swi040", "swi060", "swi100"]
SWI_SCALE = 0.5
SWI_NODATA = 255

# Mismo período y ventana que el envío (para saber qué se espera y qué falta)
FECHA_INICIO = "2026-01-01"
VENTANA_MESES = 4
ARCHIVO_FALTANTES = os.path.join(BASE_DIR, "swi_faltantes.txt")

# Bajar lo que esté listo en CADA corrida (no esperar a que terminen todos).
# Para un pipeline que corre cada 45 min, esto va descargando incrementalmente.
EXIGIR_TODOS_TERMINADOS = False

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
pepe = pd.read_csv(CSV_KM, sep=";")
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

# ───────────────────────────────────────────────────────────────────
# 4. REPORTE DE FALTANTES → swi_faltantes.txt
# Calcula el universo esperado (estaciones × ventanas desde FECHA_INICIO) y
# resta lo que YA está descargado en la carpeta. Lo que queda, falta.
# ───────────────────────────────────────────────────────────────────
def ventanas_4m(inicio, fin, meses=VENTANA_MESES):
    d0, dfin = date.fromisoformat(inicio), date.fromisoformat(fin)
    out, a = [], d0
    while a < dfin:
        sig = min(a + relativedelta(months=meses), dfin)
        out.append(a.isoformat()); a = sig
    return out

hoy = date.today().isoformat()
ventanas_inicio = ventanas_4m(FECHA_INICIO, hoy)   # solo la fecha "desde" de cada una
ids_estaciones = pepe.drop_duplicates(subset=["SHIP TO"])["SHIP TO"].astype(int).tolist()

# Crudos ya descargados en la carpeta: SWI_{est}_{AAAAMMDD}.csv
ya_desc = set()
pat_crudo = re.compile(r"^SWI_(\d+)_(\d{8})\.csv$")
for f in os.listdir(output_folder):
    m = pat_crudo.match(f)
    if m:
        ya_desc.add((m.group(1), m.group(2)))

faltantes = []
for est in ids_estaciones:
    for desde in ventanas_inicio:
        v = desde.replace("-", "")             # AAAAMMDD
        if (str(est), v) not in ya_desc:
            faltantes.append(f"SWI_{est}_{v}")

with open(ARCHIVO_FALTANTES, "w", encoding="utf-8") as fh:
    fh.write(f"# Reporte de faltantes SWI — {hoy}\n")
    fh.write(f"# Esperadas: {len(ids_estaciones)} estaciones × "
             f"{len(ventanas_inicio)} ventanas = "
             f"{len(ids_estaciones) * len(ventanas_inicio)}\n")
    fh.write(f"# Descargadas: {len(ya_desc)} | Faltan: {len(faltantes)}\n\n")
    for t in faltantes:
        fh.write(t + "\n")

log(f"📝 Faltantes escritos en {os.path.basename(ARCHIVO_FALTANTES)}: "
    f"{len(faltantes)} de {len(ids_estaciones) * len(ventanas_inicio)}")
