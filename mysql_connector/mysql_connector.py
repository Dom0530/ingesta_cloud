import os
import mysql.connector
import pandas as pd
from datetime import datetime
from mysql.connector import Error
from merge_s3 import merge_with_s3

def get_last_pull():
    last_pull = os.getenv("LAST_PULL")
    return datetime.fromisoformat(last_pull) if last_pull else datetime.min

def connect_mysql():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB")
        )
        print("Conectado a MySQL")
        return connection
    except Error as e:
        print("Error de conexión MySQL:", e)
        return None

def export_new_rows():
    conn = connect_mysql()
    if not conn:
        return

    last_pull = get_last_pull()
    query = "SELECT * FROM employees WHERE updated_at > %s;"
    
    try:
        df = pd.read_sql(query, conn, params=[last_pull])
        filename = f"/data/mysql_pull_{datetime.now().isoformat()}.csv"
        df.to_csv(filename, index=False)
        print(f"Exportado {len(df)} registros a {filename}")
    except Exception as e:
        print("Error exportando datos MySQL:", e)
    finally:
        conn.close()
        print("Conexión MySQL cerrada")

if __name__ == "__main__":
    csv_path = export_new_rows()  # modifica export_new_rows para que devuelva el path del CSV generado
    if csv_path:
        merge_with_s3(csv_path)
