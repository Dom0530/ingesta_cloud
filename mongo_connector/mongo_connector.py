import os
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
from merge_s3 import merge_with_s3

def get_last_pull():
    last_pull = os.getenv("LAST_PULL")
    return datetime.fromisoformat(last_pull) if last_pull else datetime.min

def connect_mongo():
    try:
        uri = os.getenv("MONGO_URI")
        client = MongoClient(uri)
        print("Conectado a MongoDB")
        return client
    except ConnectionFailure as e:
        print("Error de conexión MongoDB:", e)
        return None

def export_new_docs():
    client = connect_mongo()
    if not client:
        return

    db = client[os.getenv("MONGO_DB")]
    collection = db["employees"]
    last_pull = get_last_pull()
    
    try:
        cursor = collection.find({"updated_at": {"$gt": last_pull}})
        df = pd.DataFrame(list(cursor))
        filename = f"/data/mongo_pull_{datetime.now().isoformat()}.csv"
        df.to_csv(filename, index=False)
        print(f"Exportado {len(df)} documentos a {filename}")
    except Exception as e:
        print("Error exportando datos MongoDB:", e)
    finally:
        client.close()
        print("Conexión MongoDB cerrada")

if __name__ == "__main__":
    csv_path = export_new_docs()  # modifica export_new_rows para que devuelva el path del CSV generado
    if csv_path:
        merge_with_s3(csv_path)
