from src.clickhouse_utils import get_clickhouse_client, get_vehicle_clickhouse_client
import os


def _first_column(columns: set[str], candidates: list[str]) -> str | None:
    for column in candidates:
        if column in columns:
            return column
    return None


def fetch_vehicle_master_rows():
    remote = get_vehicle_clickhouse_client()
    table_name = (os.getenv('VEHICLE_PROFILE_TABLE') or 'vehicle_profile_ss').strip()

    try:
        describe_rows = remote.execute(f'DESCRIBE TABLE {table_name}')
        columns = {str(row[0]) for row in describe_rows}
    except Exception:
        columns = set()

    uniqueid_col = _first_column(columns, ['uniqueid', 'unique_id'])
    vehicle_number_col = _first_column(columns, ['vehicle_number', 'registration_number', 'vehicle_no'])
    model_col = _first_column(columns, ['model', 'vehicle_model'])
    manufacturing_year_col = _first_column(columns, ['manufacturing_year', 'mfg_year', 'model_year'])
    customer_name_col = _first_column(columns, ['customer_name', 'customer', 'customername'])
    tank_capacity_col = _first_column(columns, ['tank_capacity', 'fuel_tank_capacity', 'tankcapacity'])

    if not uniqueid_col:
        raise ValueError(f'Missing uniqueid column in source table: {table_name}')

    return remote.execute(
        f"""
        SELECT
            toString({uniqueid_col}) AS uniqueid,
            {f'toString({vehicle_number_col})' if vehicle_number_col else "''"} AS vehicle_number,
            {f'toString({model_col})' if model_col else "''"} AS model,
            {f'toUInt16OrZero({manufacturing_year_col})' if manufacturing_year_col else '0'} AS manufacturing_year,
            {f'toString({customer_name_col})' if customer_name_col else "''"} AS customer_name,
            {f'toFloat32OrZero({tank_capacity_col})' if tank_capacity_col else '0'} AS tank_capacity
        FROM {table_name}
        """
    )


def upsert_vehicle_master_local(rows):
    if not rows:
        return
    client = get_clickhouse_client()
    rows_with_ts = [
        (
            uniqueid,
            vehicle_number,
            model,
            int(manufacturing_year) if manufacturing_year is not None else 0,
            customer_name,
            float(tank_capacity) if tank_capacity is not None else 0.0,
        )
        for uniqueid, vehicle_number, model, manufacturing_year, customer_name, tank_capacity in rows
    ]
    client.execute(
        "INSERT INTO vehicle_master (uniqueid, vehicle_number, model, manufacturing_year, customer_name, tank_capacity) VALUES",
        rows_with_ts,
    )


def sync_vehicle_master():
    rows = fetch_vehicle_master_rows()
    upsert_vehicle_master_local(rows)
