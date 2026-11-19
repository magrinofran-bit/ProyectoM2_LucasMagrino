"""FleetLogix Avance 3: detectar cambios reales de conductores (SCD Tipo 2).

Uso desde la carpeta del proyecto:
    py 07_driver_scd2.py                 # previsualizar, NO modifica Snowflake
    py 07_driver_scd2.py --apply         # cargar snapshot + actualizar DIM_DRIVER

Reutiliza pg_connect/sf_connect de 05_etl_pipeline.py. En la configuracion
actual usa pgpass.conf y credenciales RSA locales, fuera de GitHub.
No cambia PostgreSQL ni FACT_DELIVERIES. No inventa cambios historicos.
VALID_TO se interpreta como limite EXCLUSIVO, valido para consultas de rango.
"""
import argparse
import importlib
import logging

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

LOG = logging.getLogger("fleetlogix_scd2")

SOURCE_SQL = """
SELECT driver_id, employee_code,
       CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')) AS full_name,
       license_expiry, hire_date, status
FROM drivers
ORDER BY driver_id
"""

DIFFERENT = """(
    d.EMPLOYEE_CODE IS DISTINCT FROM s.EMPLOYEE_CODE OR
    d.FULL_NAME IS DISTINCT FROM s.FULL_NAME OR
    d.LICENSE_EXPIRY IS DISTINCT FROM s.LICENSE_EXPIRY OR
    d.HIRE_DATE IS DISTINCT FROM s.HIRE_DATE OR
    d.STATUS IS DISTINCT FROM s.STATUS
)"""


def run(apply=False):
    pipeline = importlib.import_module("05_etl_pipeline")
    # Leemos los 400 conductores reales, sin datos ficticios.
    with pipeline.pg_connect() as pg:
        df = pipeline.read_pg(pg, SOURCE_SQL)
    if df.empty or df["driver_id"].isna().any() or df["driver_id"].duplicated().any():
        raise ValueError("Snapshot vacio, IDs nulos o IDs duplicados: operacion cancelada")

    df.columns = [col.upper() for col in df.columns]
    # Mantener fechas como datetime/date para tipos DATE en Snowflake.
    for col in ("LICENSE_EXPIRY", "HIRE_DATE"):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    df = df.astype(object).where(pd.notna(df), None)

    with pipeline.sf_connect() as sf:
        with sf.cursor() as cur:
            cur.execute("CREATE OR REPLACE TEMPORARY TABLE STG_DRIVER_LOAD LIKE STG_DRIVER_SNAPSHOT")
        try:
            ok, _, n, _ = write_pandas(
                sf, df, "STG_DRIVER_LOAD",
                database=pipeline.DATABASE, schema=pipeline.SCHEMA,
                quote_identifiers=False, auto_create_table=False,
            )
            if not ok or n != len(df):
                raise RuntimeError(f"Carga temporal incompleta: {n}/{len(df)}")

            with sf.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM STG_DRIVER_LOAD s JOIN DIM_DRIVER d
                      ON s.DRIVER_ID = d.DRIVER_ID AND d.IS_CURRENT = TRUE
                    WHERE {DIFFERENT}
                """)
                modified = cur.fetchone()[0]
                cur.execute("""
                    SELECT COUNT(*) FROM STG_DRIVER_LOAD s
                    LEFT JOIN DIM_DRIVER d ON s.DRIVER_ID=d.DRIVER_ID
                     AND d.IS_CURRENT=TRUE
                    WHERE d.DRIVER_ID IS NULL
                """)
                new = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM STG_DRIVER_LOAD")
                total = cur.fetchone()[0]

            # La dimension utiliza DATE, no TIMESTAMP. No cerrar y abrir
            # dos versiones del mismo conductor con igual VALID_FROM.
            if modified:
                with sf.cursor() as cur:
                    cur.execute(f"""
                        SELECT COUNT(*)
                        FROM STG_DRIVER_LOAD s JOIN DIM_DRIVER d
                          ON s.DRIVER_ID=d.DRIVER_ID AND d.IS_CURRENT=TRUE
                        WHERE {DIFFERENT} AND d.VALID_FROM >= CURRENT_DATE()
                    """)
                    same_day = cur.fetchone()[0]
                if same_day:
                    raise ValueError(
                        f"{same_day} cambios ocurren en la misma fecha de inicio "
                        "de la version vigente. Con columnas DATE, esto necesita "
                        "resolucion manual para evitar rangos historicos vacios."
                    )

            print(f"Conductores extraidos de PostgreSQL: {total}")
            print(f"Conductores modificados: {modified}; nuevos/sin version activa: {new}")
            if not apply:
                print("MODO PREVISUALIZACION: no se modifico ninguna tabla permanente.")
                return

            # Una transaccion: cerrar versiones, insertar nuevas y actualizar snapshot.
            # Evitar confirmar cambios parciales si alguna sentencia falla.
            with sf.cursor() as cur:
                try:
                    cur.execute("BEGIN")
                    cur.execute(f"""
                        UPDATE DIM_DRIVER d
                        SET VALID_TO = CURRENT_DATE(), IS_CURRENT = FALSE
                        FROM STG_DRIVER_LOAD s
                        WHERE d.IS_CURRENT = TRUE
                          AND s.DRIVER_ID = d.DRIVER_ID
                          AND {DIFFERENT}  
                    """)
                    cur.execute("""
                        INSERT INTO DIM_DRIVER
                        (DRIVER_KEY, DRIVER_ID, EMPLOYEE_CODE, FULL_NAME,
                         LICENSE_EXPIRY, HIRE_DATE, STATUS,
                         VALID_FROM, VALID_TO, IS_CURRENT)
                        SELECT SEQ_DRIVER_VERSION.NEXTVAL,
                               s.DRIVER_ID, s.EMPLOYEE_CODE, s.FULL_NAME,
                               s.LICENSE_EXPIRY, s.HIRE_DATE, s.STATUS,
                               CURRENT_DATE(), NULL, TRUE
                        FROM STG_DRIVER_LOAD s
                        LEFT JOIN DIM_DRIVER d ON s.DRIVER_ID=d.DRIVER_ID
                         AND d.IS_CURRENT=TRUE
                        WHERE d.DRIVER_ID IS NULL
                    """)
                    cur.execute("DELETE FROM STG_DRIVER_SNAPSHOT")
                    cur.execute("""
                        INSERT INTO STG_DRIVER_SNAPSHOT
                        (DRIVER_ID, EMPLOYEE_CODE, FULL_NAME,
                         LICENSE_EXPIRY, HIRE_DATE, STATUS, EXTRACTED_AT)
                        SELECT DRIVER_ID, EMPLOYEE_CODE, FULL_NAME,
                               LICENSE_EXPIRY, HIRE_DATE, STATUS, CURRENT_TIMESTAMP()
                        FROM STG_DRIVER_LOAD
                    """)
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise
            print("SCD2 aplicado y snapshot actualizado correctamente.")
            print("NOTA: si cambian conductores, futuras entregas requieren asignar DRIVER_KEY por vigencia.")
        finally:
            with sf.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS STG_DRIVER_LOAD")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FleetLogix SCD2 conductores")
    parser.add_argument("--apply", action="store_true", help="Aplicar cambios (default: solo previsualizar)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    run(args.apply)
