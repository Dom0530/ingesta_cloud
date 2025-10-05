import os
import boto3
import pandas as pd
from io import StringIO

def merge_with_s3(new_data_path: str):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    bucket = os.getenv('S3_BUCKET')
    key = os.getenv('S3_KEY')

    # Cargar los nuevos datos
    new_df = pd.read_csv(new_data_path)

    try:
        # Descargar el archivo actual desde S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        old_df = pd.read_csv(obj['Body'])
        print(f"Archivo existente en S3 encontrado con {len(old_df)} filas.")
    except s3.exceptions.NoSuchKey:
        print("No existe archivo previo en S3. Se creará uno nuevo.")
        old_df = pd.DataFrame()

    # Si el archivo no existía antes
    if old_df.empty:
        merged_df = new_df
    else:
        # Asumimos que hay una columna 'id' como clave primaria
        merged_df = pd.concat([old_df, new_df]).drop_duplicates(subset='id', keep='last')

    # Guardar el merge en memoria
    csv_buffer = StringIO()
    merged_df.to_csv(csv_buffer, index=False)

    # Subir el nuevo archivo a S3
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

    print(f"Archivo consolidado subido a s3://{bucket}/{key} con {len(merged_df)} registros totales.")
