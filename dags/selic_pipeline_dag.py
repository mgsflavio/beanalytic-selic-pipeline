"""
DAG: selic_pipeline_dag
Descrição: Pipeline de ingestão, transformação e agregação da Taxa SELIC
           consumindo dados da API do Banco Central do Brasil.
Autor: beAnalytic
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Garante que os módulos do projeto sejam encontrados
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bronze.ingest_selic import run_bronze
from silver.transform_selic import run_silver
from gold.aggregate_selic import run_gold

# ---------------------------------------------------------------------------
# Configurações padrão da DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "beanalytic",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Definição da DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="selic_pipeline",
    default_args=default_args,
    description="Pipeline Bronze → Silver → Gold para a Taxa SELIC (BCB)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["beanalytic", "bcb", "selic", "production"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1 — Ingestão (Bronze)
    # ------------------------------------------------------------------
    task_bronze = PythonOperator(
        task_id="ingestao_bronze",
        python_callable=run_bronze,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    # ------------------------------------------------------------------
    # Task 2 — Transformação (Silver)
    # ------------------------------------------------------------------
    task_silver = PythonOperator(
        task_id="transformacao_silver",
        python_callable=run_silver,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    # ------------------------------------------------------------------
    # Task 3 — Agregação (Gold)
    # ------------------------------------------------------------------
    task_gold = PythonOperator(
        task_id="agregacao_gold",
        python_callable=run_gold,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    # Dependência sequencial: Bronze → Silver → Gold
    task_bronze >> task_silver >> task_gold
