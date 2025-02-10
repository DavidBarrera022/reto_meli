from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator  # ✅ Para la tarea final
from airflow.utils.dates import days_ago

from datetime import timedelta
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import datetime

# 🔹 Parámetros generales
BQ_PROJECT = "scenic-era-450314-f4"
BQ_DATASET = "reto_meli"
BQ_TABLE = "billing_data_meli"

BUCKET_NAME = "billing_data_meli"
DATA_FOLDER = "billing_data/"
PROCESSED_FOLDER = "billing_data/processed/"
OCI_CONVERSION_RATE = 500
CHUNK_SIZE = 500_000

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def determine_reprocess_months():
    """Determina qué meses deben ser reprocesados según la fecha actual."""
    today = datetime.datetime.utcnow().date()
    first_day_of_month = today.replace(day=1)
    last_month = first_day_of_month - timedelta(days=1)
    
    if today.day <= 15:
        return [last_month.strftime("%Y-%m"), today.strftime("%Y-%m")]  # Reprocesar mes anterior y actual
    else:
        return [today.strftime("%Y-%m")]  # Solo reprocesar mes actual


def process_large_csv(file_name):
    """Procesa archivos CSV grandes por partes y los carga a BigQuery."""
    
    storage_client = storage.Client(project=BQ_PROJECT)
    bucket = storage_client.bucket(BUCKET_NAME)
    bq_client = bigquery.Client(project=BQ_PROJECT)
    
    blob = bucket.blob(file_name)
    
    print(f"Procesando archivo: {file_name}")

    # 🔹 Determinar proveedor y columnas a leer
    if "aws" in file_name.lower():
        provider = "AWS"
        column_mapping = {
            "start_date": "billing_date",
            "product_name": "service",
            "usage_type": "service_description",
            "net_cost": "cost",
        }
    elif "oracle" in file_name.lower():
        provider = "OCI"
        column_mapping = {
            "intervalUsageStart": "billing_date",
            "product_service": "service",
            "product_description": "service_description",
            "total_cost": "cost",
        }
    elif "gcp" in file_name.lower():
        provider = "GCP"
        column_mapping = {
            "usage_start_date": "billing_date",
            "service_description": "service",
            "sku_description": "service_description",
            "cost": "cost",
        }
    else:
        print(f"Archivo {file_name} omitido (no coincide con AWS, OCI o GCP).")
        return

    selected_columns = list(column_mapping.keys())

    # 🔹 Leer y procesar en chunks
    blob_reader = blob.open("r")

    reprocess_months = determine_reprocess_months()
    print(f"Reprocesando datos para los meses: {reprocess_months}")

    # Convertir las fechas de reprocesamiento a datetime para comparación
    reprocess_dates = [datetime.datetime.strptime(m, "%Y-%m").date() for m in reprocess_months]

    for chunk in pd.read_csv(blob_reader, dtype=str, usecols=selected_columns, chunksize=CHUNK_SIZE):  
        df = chunk.rename(columns=column_mapping)

        df["billing_date"] = pd.to_datetime(df["billing_date"]).dt.date
        df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0)

        if provider == "OCI": 
            df["cost"] = df["cost"] / OCI_CONVERSION_RATE  # Convertir ARS → USD

        df["provider"] = provider
        df["updated_at"] = datetime.datetime.utcnow()

        df = df[df["billing_date"].apply(lambda x: x.replace(day=1) in reprocess_dates)]
        
        # 🔹 Cargar chunk a BigQuery
        if df.empty:
            raise ValueError(f"No hay datos para cargar en BigQuery desde el archivo {file_name}.")
            
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=[
                bigquery.SchemaField("billing_date", "DATE"),
                bigquery.SchemaField("provider", "STRING"),
                bigquery.SchemaField("service", "STRING"),
                bigquery.SchemaField("service_description", "STRING"),
                bigquery.SchemaField("cost", "FLOAT"),
                bigquery.SchemaField("updated_at", "TIMESTAMP"),
            ],
        )
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
        load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()
        print(f"Chunk de {file_name} cargado en BigQuery.")

    # 🔹 Mover archivo procesado a carpeta `/processed/`
    today_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    new_path = f"{PROCESSED_FOLDER}{today_str}/{file_name.split('/')[-1]}"
    new_blob = bucket.blob(new_path)
    new_blob.rewrite(blob)
    blob.delete()
    
    print(f"Archivo {file_name} movido a {new_path}.")

def list_files_to_process():
    """Lista los archivos CSV disponibles y los divide en tareas separadas."""
    storage_client = storage.Client(project=BQ_PROJECT)
    bucket = storage_client.bucket(BUCKET_NAME)

    blobs = bucket.list_blobs(prefix=DATA_FOLDER)
    files = [
        blob.name for blob in blobs
        if blob.name.endswith(".csv") and not "/" in blob.name[len(DATA_FOLDER):]  # Solo archivos CSV sin subcarpetas
    ]

    if not files:
        print("No se encontraron archivos CSV para procesar.")
        return []

    print(f"Se encontraron {len(files)} archivos para procesar.")
    return files

# 🔹 Definir el DAG en Airflow
with DAG(
    dag_id="billing_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    max_active_runs=1,
    schedule_interval="0 8 * * *",
    catchup=False,
) as dag:

    files_to_process = list_files_to_process()

    # 🔹 Crear tarea final
    end_task = EmptyOperator(
        task_id="all_tasks_completed"
    )

    if files_to_process:
        tasks = []
        for file in files_to_process:
            task = PythonOperator(
                task_id=f"process_{file.replace('/', '_')}",
                python_callable=process_large_csv,
                op_args=[file],
                execution_timeout=timedelta(hours=2),
            )
            tasks.append(task)
        
        # 🔹 Todas las tareas deben terminar antes de `end_task`
        for task in tasks:
            task >> end_task  # Cada tarea de procesamiento se enlaza a `end_task`

