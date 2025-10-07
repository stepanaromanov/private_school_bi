from src.utils.utils_dataframe import *
from src.utils.utils_general import *
import pandas as pd
import psycopg
from psycopg.rows import dict_row
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("omonschool_etl")

def load_to_postgres(
        df: pd.DataFrame,
        table_base_name: str,source .venv/bin/activate
        postfix: str,
        primary_key: str = 'id',
        truncate: bool = False,
        creds_file: str = 'credentials.json',
        batch_size: int = 1000
) -> None:
    """
    Load a pandas DataFrame to PostgreSQL with upsert functionality (psycopg 3.x).
    """
    # Load credentials
    with open(creds_file, 'r') as f:
        creds = json.load(f)

    conn_params = {
        'host': creds['host'],
        'port': creds['port'],
        'dbname': creds['database'],
        'user': creds['user'],
        'password': creds['password']
    }

    # Check data types
    logger.info(f"[{table_base_name}{postfix}] Data types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"[{table_base_name}{postfix}] {col}: {dtype}")

    # Map pandas dtypes to PostgreSQL types
    def map_dtype_to_pg(dtype, col_name):
        if pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            return 'DOUBLE PRECISION'
        elif pd.api.types.is_datetime64_any_dtype(dtype) or 'timestamp' in col_name.lower():
            return 'TIMESTAMP'
        else:
            return 'TEXT'

    # Table name with postfix
    table_name = f"{table_base_name}{postfix}"

    # Connect using psycopg3
    with psycopg.connect(**conn_params, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                # Create table IF NOT EXISTS
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                """
                columns_def = []
                for col in df.columns:
                    pg_type = map_dtype_to_pg(df[col].dtype, col)
                    columns_def.append(f'    {col} {pg_type}')
                    if col == primary_key:
                        columns_def[-1] += ' PRIMARY KEY'

                create_table_sql += ',\n'.join(columns_def) + '\n);'
                logger.info(f"[{table_name}] Create table SQL:\n{create_table_sql}")

                cur.execute(create_table_sql)
                conn.commit()
                logger.info(f"[{table_name}] Table created or verified.")

                # Truncate table if requested
                if truncate:
                    truncate_sql = f"TRUNCATE TABLE {table_name};"
                    cur.execute(truncate_sql)
                    conn.commit()
                    logger.info(f"[{table_name}] Table truncated.")

                # Prepare upsert SQL
                update_cols = [col for col in df.columns if col != primary_key]
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])

                upsert_sql = f"""
                INSERT INTO {table_name} ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(df.columns))})
                ON CONFLICT ({primary_key}) DO UPDATE SET {update_set}
                RETURNING (xmax = 0) AS inserted;
                """

                # Function to execute a batch and count inserts/updates
                def execute_batch(batch_data):
                    try:
                        with psycopg.connect(**conn_params) as batch_conn:
                            with batch_conn.cursor() as batch_cur:
                                batch_cur.executemany(upsert_sql, batch_data, returning=True)
                                results = batch_cur.fetchall()
                                batch_conn.commit()
                                inserts = sum(1 for (inserted,) in results if inserted)
                                updates = len(results) - inserts
                                return inserts, updates
                    except Exception as e:
                        logger.error(f"[{table_name}] Batch execution error: {e}")
                        return 0, 0

                # Parallel insertion using ThreadPoolExecutor
                total_inserts = 0
                total_updates = 0
                futures = []
                with ThreadPoolExecutor(max_workers=4) as executor:
                    for i in range(0, len(df), batch_size):
                        batch_df = df.iloc[i:i + batch_size]
                        batch_data = [tuple(row) for row in batch_df.itertuples(index=False)]
                        futures.append(executor.submit(execute_batch, batch_data))

                    for future in as_completed(futures):
                        inserts, updates = future.result()
                        total_inserts += inserts
                        total_updates += updates
                        logger.info(f"[{table_name}] Processed batch: {inserts} inserted, {updates} updated.")

                logger.info(f"[{table_name}] Total: {total_inserts} rows inserted, {total_updates} rows updated.")

            except Exception as e:
                logger.error(f"[{table_name}] Error during load: {e}")
                conn.rollback()
                raise
            finally:
                logger.info(f"[{table_name}] PostgreSQL connection closed.")
