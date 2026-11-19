"""FleetLogix Avance 3: comprobaciones y programacion del ETL nocturno.

Comandos (desde la carpeta del proyecto):
    py 06_etl_automation.py --check  # conexiones, permisos y filas; no carga
    py 06_etl_automation.py --once   # ejecuta una carga integrada AHORA
    py 06_etl_automation.py          # proceso vivo, agenda las 02:00

Una tarea schedule solo corre si Python sigue abierto y la PC esta despierta.
Para trabajo nocturno real se requiere Windows Task Scheduler o un servidor.
"""
import argparse
import importlib
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import schedule

ROOT = Path(__file__).resolve().parent
ETL = ROOT / "05_etl_pipeline.py"
SCD = ROOT / "07_driver_scd2.py"
HORA_EJECUCION = "02:00"
LOG = logging.getLogger("fleetlogix_daily")


def conectar_etl():
    return importlib.import_module("05_etl_pipeline")


def comprobar():
    """Prueba no destructiva. No carga ni modifica tablas."""
    for f in (ETL, SCD):
        if not f.is_file():
            raise FileNotFoundError(f"Falta {f.name} en {ROOT}")
    pipeline = conectar_etl()
    with pipeline.pg_connect() as pg:
        with pg.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM drivers")
            print("PostgreSQL conductores:", cur.fetchone()[0])
    with pipeline.sf_connect() as sf:
        with sf.cursor() as cur:
            cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
            print("Snowflake servicio:", cur.fetchone())
            for table in ("DIM_DRIVER", "FACT_DELIVERIES", "STG_DRIVER_SNAPSHOT"):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                print(table, cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM DIM_DRIVER WHERE IS_CURRENT=TRUE")
            print("Conductores actuales:", cur.fetchone()[0])
    print("CHECK OK - no se modifico ninguna tabla")


def ejecutar_etl():
    """Ejecuta 4 etapas; si una falla, detiene el proceso y devuelve error."""
    print("Inicio ETL:", datetime.now().isoformat(timespec="seconds"), flush=True)
    tasks = [(ETL, "--dimensions"), (SCD, "--apply"), (ETL, "--facts")]
    try:
        for script, flag in tasks:
            if not script.is_file():
                raise FileNotFoundError(str(script))
            LOG.info("Iniciando %s %s", script.name, flag)
            subprocess.run([sys.executable, str(script), flag], cwd=ROOT, check=True)
        with conectar_etl().sf_connect() as sf:
            with sf.cursor() as cur:
                cur.execute("CALL FLEETLOGIX_DW.ANALYTICS.SP_REFRESH_AGGREGATES()")
                print("Agregados:", cur.fetchone()[0])
        print("ETL completo OK:", datetime.now().isoformat(timespec="seconds"))
        return True
    except Exception:
        LOG.exception("ETL FALLIDO: revisar errores; no asumir carga completa")
        return False


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="Verificar sin escritura")
    mode.add_argument("--once", action="store_true", help="Ejecutar ETL integrado ahora")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.check:
        comprobar()
        return
    if args.once:
        if not ejecutar_etl():
            sys.exit(1)
        return
    schedule.every().day.at(HORA_EJECUCION).do(ejecutar_etl)
    print(f"FleetLogix programado a las {HORA_EJECUCION} (hora local de Windows).")
    print("Mantener este proceso activo o configurar Windows Task Scheduler.")
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
