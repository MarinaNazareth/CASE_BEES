import requests
import json
import pandas as pd
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def fetch_breweries(page=1, per_page=200, breweries=[]):
    url = f"https://api.openbrewerydb.org/v1/breweries?per_page={per_page}&page={page}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        if not data:
            save_data_as_json(breweries)
            return
        
        breweries.extend(data)
        print(f"Página {page} carregada...")
        fetch_breweries(page + 1, per_page, breweries)
    else:
        print(f"Erro: {response.status_code}")

def save_data_as_json(breweries):
    os.makedirs("/opt/airflow/data_lake", exist_ok=True)
    file_path = os.path.join("/opt/airflow/data_lake", "bronze_layer.json")
    with open(file_path, "w") as file:
        json.dump(breweries, file, indent=4)
    print(f"Dados salvos em {file_path}")

def load_json_to_dataframe():
    file_path = os.path.join("/opt/airflow/data_lake", "bronze_layer.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            breweries = json.load(file)
        df = pd.DataFrame(breweries)
        save_silver_layer(df)
    else:
        print("Arquivo JSON não encontrado.")

def save_silver_layer(df):
    silver_path = os.path.join("/opt/airflow/data_lake", "silver_layer")
    os.makedirs(silver_path, exist_ok=True)
    
    for state in df["state_province"].dropna().unique():
        state_df = df[df["state_province"] == state]
        state_parquet_path = os.path.join(silver_path, f"state={state}.parquet")
        state_df.to_parquet(state_parquet_path, index=False)
    
    print(f"Camada Silver salva em {silver_path}, particionada por estado_província")

def aggregate_breweries():
    silver_path = os.path.join("/opt/airflow/data_lake", "silver_layer")
    aggregated_data = []
    
    for file in os.listdir(silver_path):
        if file.endswith(".parquet"):
            df = pd.read_parquet(os.path.join(silver_path, file))
            aggregated_df = df.groupby(["brewery_type", "state_province"]).size().reset_index(name="quantity")
            aggregated_data.append(aggregated_df)
    
    final_aggregated_df = pd.concat(aggregated_data)
    gold_path = os.path.join("/opt/airflow/data_lake", "gold_layer.parquet")
    final_aggregated_df.to_parquet(gold_path, index=False)
    print(f"Camada Gold salva em {gold_path}")

def orchestrate():
    fetch_breweries()
    load_json_to_dataframe()
    aggregate_breweries()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 26),
    'retries': 1,
}

dag = DAG(
    'brewery_data_pipeline',
    default_args=default_args,
    description='Pipeline para extração, transformação e carga de dados de cervejarias',
    schedule_interval='@daily',
    catchup=False,
)

task_orchestrate = PythonOperator(
    task_id='orchestrate_pipeline',
    python_callable=orchestrate,
    dag=dag,
)

task_orchestrate
