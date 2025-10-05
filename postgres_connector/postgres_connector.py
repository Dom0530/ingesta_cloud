import os
import psycopg2
import pandas as pd
from datetime import datetime
from psycopg2 import OperationalError
from merge_s3 import merge_with_s3

def get_last_pull():
    last_pull = os.getenv("LAST_PULL")
    return datetime.fromisoformat(last_pull) if last_pull else datetime.min

def connect_postgres():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            database=os.getenv("PG_DB")
        )
        return conn
    except Exception as e:
        print("Error de conexión PostgreSQL:", e)
        return None

def list_tables(conn):
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE';
    """
    return pd.read_sql(query, conn)['table_name'].tolist()

def export_all_tables():
    conn = connect_postgres()
    if not conn:
        return

    last_pull = get_last_pull()
    tables = list_tables(conn)

    for table in tables:
        try:
            query = f"SELECT * FROM {table} WHERE updated_at > %s;"
            df = pd.read_sql(query, conn, params=[last_pull])
            if not df.empty:
                filename = f"/data/postgres_{table}_{datetime.now().isoformat()}.csv"
                df.to_csv(filename, index=False)
                print(f"Tabla {table} exportada a {filename} ({len(df)} registros)")
            else:
                print(f"Tabla {table} no tiene registros nuevos.")
        except Exception as e:
            print(f"Error exportando tabla {table}: {e}")

    conn.close()
    print("Conexión PostgreSQL cerrada")


if __name__ == "__main__":
    csv_path = export_all_tables()  # modifica export_new_rows para que devuelva el path del CSV generado
    if csv_path:
        merge_with_s3(csv_path)
