"""FleetLogix | Avance 3 — ETL PostgreSQL -> Snowflake.

Uso:
    py 05_etl_pipeline.py --dimensions
    py 05_etl_pipeline.py --facts
    py 05_etl_pipeline.py --verify

No reconfigura ni modifica PostgreSQL; no crea columnas ficticias.
La fact solo incluye delivery_status='delivered'.
Los ingresos permanecen NULL: no existen en el esquema transaccional.
Requiere las 7 tablas ya creadas en FLEETLOGIX_DW.ANALYTICS.
"""
import argparse
import hashlib
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT', 'NRZJFXQ-JO55501')
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
DATABASE = 'FLEETLOGIX_DW'
SCHEMA = 'ANALYTICS'
BATCH = 10000
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
LOG = logging.getLogger('fleetlogix_etl')


def pg_connect():
    return psycopg2.connect(
        host=os.getenv('PGHOST', 'localhost'), port=int(os.getenv('PGPORT', '5432')),
        dbname=os.getenv('PGDATABASE', 'fleetlogix'), user=os.getenv('PGUSER', 'postgres'),
        password=os.getenv('PGPASSWORD') or None,
        connect_timeout=20,
    )



def sf_connect():
    # Conexion automatica con usuario de servicio y clave RSA
    clave = os.path.join(
        os.environ["LOCALAPPDATA"],
        "FleetLogix",
        "keys",
        "snowflake_rsa_key.p8"
    )

    return snowflake.connector.connect(
        account=ACCOUNT,
        user="FLEETLOGIX_ETL_SVC",
        authenticator="SNOWFLAKE_JWT",
        private_key_file=clave,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role="FLEETLOGIX_ETL_ROLE",
        session_parameters={
            "QUERY_TAG": "FleetLogix_Avance3_ETL"
        }
    )



def read_pg(pg, sql):
    with pg.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall())


def snowflake_stage(sf, table_name, df):
    """Publica un batch en una tabla temporal de la sesión (sin modificar el destino)."""
    if df.empty:
        return None
    stage = 'STG_' + table_name
    df = df.copy()
    df.columns = [str(c).upper() for c in df.columns]
    df = df.astype(object).where(pd.notnull(df), None)
    with sf.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS {stage}')
        cur.execute(f'CREATE TEMP TABLE {stage} LIKE {table_name}')
        # Fact y dimensiones tienen campos calculados/extra: staging igual al destino,
        # usar solo columnas existentes del dataframe.
    ok, chunks, count, _ = write_pandas(
        sf, df, stage, database=DATABASE, schema=SCHEMA,
        quote_identifiers=False, auto_create_table=False,
        use_logical_type=True, chunk_size=10000,
    )
    if not ok or count != len(df):
        raise RuntimeError(f'Error cargando staging {stage}: {count}/{len(df)}')
    return stage


def merge_table(sf, target, df, key, update_cols=None):
    """MERGE reproducible. Evita duplicados al ejecutar el pipeline nuevamente."""
    if df.empty:
        return 0
    cols = [str(c).upper() for c in df.columns]
    stage = snowflake_stage(sf, target, df)
    ins_cols = ', '.join(cols)
    ins_vals = ', '.join('s.' + c for c in cols)
    updates = update_cols if update_cols is not None else [c for c in cols if c != key]
    sql = (f'MERGE INTO {target} t USING {stage} s ON t.{key}=s.{key} '
           + (('WHEN MATCHED THEN UPDATE SET ' + ', '.join(f't.{c}=s.{c}' for c in updates) + ' ') if updates else '')
           + f'WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})')
    try:
        with sf.cursor() as cur:
            cur.execute(sql)
        sf.commit()
        LOG.info('%s: %s registros procesados', target, len(df))
    finally:
        with sf.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {stage}')
    return len(df)


def customer_key(name, address):
    # Misma combinación cliente+dirección en cargas incrementales.
    plain = f'{str(name).strip().lower()}|{str(address).strip().lower()}'
    return hashlib.sha256(plain.encode('utf-8')).hexdigest()


def dimension_basicas(pg, sf):
    vehicles = read_pg(pg, '''SELECT vehicle_id AS vehicle_key, vehicle_id, license_plate,
      vehicle_type, capacity_kg, fuel_type, acquisition_date, status FROM vehicles''')
    merge_table(sf, 'DIM_VEHICLE', vehicles, 'VEHICLE_KEY')

    routes = read_pg(pg, '''SELECT route_id AS route_key, route_id, route_code,
      origin_city, destination_city, distance_km, estimated_duration_hours,
      toll_cost FROM routes''')
    merge_table(sf, 'DIM_ROUTE', routes, 'ROUTE_KEY')

    # Snapshot inicial: el origen no conserva el historial de cambios de conductores.
    # Valid_from de 1900 permite asociar viajes históricos a la versión disponible.
    drivers = read_pg(pg, '''SELECT driver_id AS driver_key, driver_id, employee_code,
      (first_name || ' ' || last_name) AS full_name, license_expiry, hire_date,
      status FROM drivers''')
    drivers['valid_from'] = datetime(1900, 1, 1)
    drivers['valid_to'] = None
    drivers['is_current'] = True
    # No sobrescribir versiones históricas: la SCD2 completa requiere un proceso
    # de detección de cambios; la primera carga incorpora la versión actual.
    merge_table(sf, 'DIM_DRIVER', drivers, 'DRIVER_KEY', update_cols=[])


def clientes_entregados(pg, sf):
    """Una fila por cliente+dirección, solo de entregas completadas."""
    sql = '''SELECT DISTINCT customer_name, delivery_address FROM deliveries
             WHERE delivery_status='delivered' '''
    with pg.cursor(name='clientes_fleetlogix') as cur:
        cur.itersize = BATCH
        cur.execute(sql)
        batch = []
        for name, address in cur:
            batch.append({'customer_key': customer_key(name, address),
                          'customer_name': name, 'delivery_address': address})
            if len(batch) == BATCH:
                merge_table(sf, 'DIM_CUSTOMER', pd.DataFrame(batch), 'CUSTOMER_KEY')
                batch.clear()
        if batch:
            merge_table(sf, 'DIM_CUSTOMER', pd.DataFrame(batch), 'CUSTOMER_KEY')


def load_dimensions(pg, sf):
    LOG.info('Cargando dimensiones desde PostgreSQL (solo lectura)')
    dimension_basicas(pg, sf)
    clientes_entregados(pg, sf)
    LOG.info('DIM_DATE y DIM_TIME ya fueron cargadas en Snowflake; no se modifican')


def driver_versions_by_id(sf):
    """Lee versiones vigentes e historicas para resolver DRIVER_KEY por fecha."""
    from collections import defaultdict
    from datetime import date
    versions = defaultdict(list)
    with sf.cursor() as cur:
        cur.execute("SELECT DRIVER_ID, DRIVER_KEY, VALID_FROM, VALID_TO "
                    "FROM DIM_DRIVER ORDER BY DRIVER_ID, VALID_FROM, DRIVER_KEY")
        for driver_id, driver_key, valid_from, valid_to in cur:
            versions[int(driver_id)].append((int(driver_key), valid_from, valid_to))
    if not versions:
        raise RuntimeError("DIM_DRIVER no tiene versiones; cancelar carga de hechos")
    return versions


def driver_key_at(versions, driver_id, delivered_datetime):
    """VALID_FROM inclusivo y VALID_TO exclusivo (segun modelo SCD2)."""
    delivery_date = delivered_datetime.date()
    candidates = [key for key, start, end in versions.get(int(driver_id), [])
                  if start is not None and start <= delivery_date
                  and (end is None or delivery_date < end)]
    if len(candidates) != 1:
        raise ValueError(
            f"DRIVER_ID={driver_id}, fecha={delivery_date}: "
            f"se esperaban 1 version vigente, se encontraron {len(candidates)}"
        )
    return candidates[0]


def load_facts(pg, sf):
    """Carga incremental e idempotente de entregas completadas."""
    # Métricas a nivel viaje distribuidas proporcionalmente al peso del paquete;
    # si todos los pesos son cero o nulos, se reparten por cantidad de entregas.
    # Esto impide contar el combustible y los km completos por cada entrega.
    sql = '''
        SELECT d.delivery_id, d.trip_id, t.vehicle_id, t.driver_id, t.route_id,
            d.customer_name, d.delivery_address, d.scheduled_datetime,
            d.delivered_datetime, d.package_weight_kg,
            t.departure_datetime, r.distance_km, t.fuel_consumed_liters,
            SUM(COALESCE(d.package_weight_kg, 0)) OVER (PARTITION BY d.trip_id)
                AS total_peso_entregado,
            COUNT(*) OVER (PARTITION BY d.trip_id) AS cantidad_entregada
        FROM deliveries d
        JOIN trips t ON t.trip_id = d.trip_id
        JOIN routes r ON r.route_id = t.route_id
        WHERE d.delivery_status='delivered'
          AND d.delivered_datetime IS NOT NULL
          AND t.departure_datetime IS NOT NULL
        ORDER BY d.delivery_id
    '''
    versions = driver_versions_by_id(sf)
    total = 0
    rechazados = 0
    with pg.cursor(name='facts_fleetlogix') as cur:
        cur.itersize = BATCH
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(BATCH)
            if not rows:
                break
            processed = []
            for row in rows:
                (delivery_id, trip_id, vehicle_id, driver_id, route_id,
                 name, address, scheduled, delivered, weight, departure,
                 route_km, fuel, total_weight, count) = row
                # Calidad: no permitir duración negativa (entrega anterior a salida).
                duration = (delivered - departure).total_seconds() / 60
                if duration < 0 or duration > 60 * 24 * 30:
                    rechazados += 1
                    continue
                share = (float(weight or 0) / float(total_weight)) if total_weight and float(total_weight)>0 else 1 / count
                processed.append({
                    'delivery_id': delivery_id, 'trip_id': trip_id,
                    'date_key': int(delivered.strftime('%Y%m%d')),
                    'time_key': delivered.hour,
                    'vehicle_key': vehicle_id,
                    'driver_key': driver_key_at(versions, driver_id, delivered),
                    'route_key': route_id, 'customer_key': customer_key(name, address),
                    'scheduled_datetime': scheduled, 'delivered_datetime': delivered,
                    'delivery_duration_minutes': round(duration, 2),
                    'distance_km': round(float(route_km or 0) * share, 2),
                    'fuel_consumed_liters': round(float(fuel or 0) * share, 3),
                    'package_weight_kg': weight,
                    'revenue_estimated': None,  # El origen no contiene ingresos.
                    'is_on_time': (delivered <= scheduled) if scheduled else None,
                    # LOAD_TIMESTAMP queda como DEFAULT CURRENT_TIMESTAMP()
                })
            if processed:
                merge_table(sf, 'FACT_DELIVERIES', pd.DataFrame(processed),
                            'DELIVERY_ID', update_cols=[])
                total += len(processed)
                LOG.info('FACT_DELIVERIES: acumulado %s; excluidas %s', total, rechazados)
    LOG.info('Finalizado: %s entregas procesadas, %s excluidas por tiempo inválido', total, rechazados)


def verify(sf):
    sql = '''SELECT 'DIM_VEHICLE' nombre, COUNT(*) total FROM DIM_VEHICLE
        UNION ALL SELECT 'DIM_DRIVER', COUNT(*) FROM DIM_DRIVER
        UNION ALL SELECT 'DIM_ROUTE', COUNT(*) FROM DIM_ROUTE
        UNION ALL SELECT 'DIM_CUSTOMER', COUNT(*) FROM DIM_CUSTOMER
        UNION ALL SELECT 'DIM_DATE', COUNT(*) FROM DIM_DATE
        UNION ALL SELECT 'DIM_TIME', COUNT(*) FROM DIM_TIME
        UNION ALL SELECT 'FACT_DELIVERIES', COUNT(*) FROM FACT_DELIVERIES'''
    with sf.cursor() as cur:
        cur.execute(sql)
        for name, count in cur:
            print(f'{name}: {count:,}')


def main():
    parser = argparse.ArgumentParser(description='FleetLogix Avance 3 ETL')
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument('--dimensions', action='store_true', help='Cargar dimensiones desde PostgreSQL')
    g.add_argument('--facts', action='store_true', help='Cargar entregas completadas')
    g.add_argument('--verify', action='store_true', help='Contar tablas en Snowflake')
    args = parser.parse_args()
    pg = None
    sf = None
    try:
        if not args.verify:
            pg = pg_connect()
        sf = sf_connect()
        if args.dimensions:
            load_dimensions(pg, sf)
        elif args.facts:
            load_facts(pg, sf)
        verify(sf)
    finally:
        if sf:
            sf.close()
        if pg:
            pg.close()


if __name__ == '__main__':
    main()
