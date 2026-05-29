"""
Camada Silver — Transformação e Limpeza
Responsável por padronizar tipos, tratar nulos e enriquecer os dados
provenientes da camada Bronze.
"""

import logging
import os
from datetime import datetime

import pandas as pd

from utils.data_quality import (
    check_not_empty,
    check_no_nulls,
    check_schema,
    check_value_range,
    DataQualityError,
)

# ---------------------------------------------------------------------------
# Configuração de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
BRONZE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "bronze")
SILVER_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "silver")
REQUIRED_BRONZE_COLS = ["data", "valor"]
REQUIRED_SILVER_COLS = ["data", "valor", "ano", "mes"]


# ---------------------------------------------------------------------------
# Funções principais
# ---------------------------------------------------------------------------
def load_bronze(path: str) -> pd.DataFrame:
    """
    Lê e valida o arquivo Parquet da camada Bronze.

    Args:
        path: Caminho para o arquivo Parquet Bronze.

    Returns:
        DataFrame com os dados brutos.

    Raises:
        FileNotFoundError: Se o arquivo não for encontrado.
        DataQualityError: Se o schema estiver incorreto.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo Bronze não encontrado: {path}")

    df = pd.read_parquet(path, engine="pyarrow")
    logger.info("Bronze carregado: %d registros de '%s'.", len(df), path)

    check_schema(df, REQUIRED_BRONZE_COLS)
    return df


def transform_selic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas as transformações e validações nos dados brutos.

    Transformações aplicadas:
    - Converte 'data' de string 'dd/MM/yyyy' para datetime
    - Converte 'valor' de string para float
    - Remove duplicatas e registros nulos
    - Adiciona colunas auxiliares 'ano' e 'mes'

    Args:
        df: DataFrame bruto da camada Bronze.

    Returns:
        DataFrame limpo e padronizado.

    Raises:
        DataQualityError: Se alguma validação falhar após a transformação.
    """
    logger.info("Iniciando transformações na camada Silver.")
    df = df.copy()

    # --- Conversão de tipos ---
    logger.info("Convertendo coluna 'data' para datetime.")
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")

    logger.info("Convertendo coluna 'valor' para float.")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    # --- Remoção de nulos e duplicatas ---
    before = len(df)
    df = df.dropna(subset=["data", "valor"])
    df = df.drop_duplicates(subset=["data"])
    removed = before - len(df)
    if removed > 0:
        logger.warning("Removidos %d registros nulos/duplicados.", removed)

    # --- Colunas auxiliares ---
    df["ano"] = df["data"].dt.year.astype(int)
    df["mes"] = df["data"].dt.month.astype(int)

    # --- Ordenação cronológica ---
    df = df.sort_values("data").reset_index(drop=True)

    # --- Validações pós-transformação ---
    logger.info("Executando validações de qualidade pós-transformação.")
    check_not_empty(df)
    check_no_nulls(df, ["data", "valor", "ano", "mes"])
    check_value_range(df, "valor", min_val=0.0, max_val=10.0)

    logger.info("Transformação concluída: %d registros válidos.", len(df))
    return df


def save_silver(df: pd.DataFrame, output_path: str) -> None:
    """
    Persiste o DataFrame transformado em formato Parquet na camada Silver.

    Args:
        df: DataFrame transformado.
        output_path: Caminho de saída do arquivo Parquet.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    logger.info("Silver salvo em: %s (%d linhas)", output_path, len(df))


def run_silver(execution_date: str, **context) -> str:
    """
    Função orquestradora da camada Silver, chamada pela DAG do Airflow.

    Args:
        execution_date: Data de execução no formato YYYY-MM-DD.
        **context: Contexto do Airflow (XCom, etc.).

    Returns:
        Caminho do arquivo Parquet gerado.
    """
    logger.info("=== TASK SILVER INICIADA | execution_date=%s ===", execution_date)

    date_str = execution_date.replace("-", "")

    # Recupera o caminho Bronze via XCom (se disponível no Airflow)
    if context.get("ti"):
        bronze_path = context["ti"].xcom_pull(
            task_ids="ingestao_bronze", key="bronze_path"
        )
    else:
        bronze_path = os.path.join(BRONZE_DIR, f"selic_{date_str}.parquet")

    output_path = os.path.join(SILVER_DIR, f"selic_clean_{date_str}.parquet")

    df_raw = load_bronze(bronze_path)
    df_clean = transform_selic(df_raw)
    save_silver(df_clean, output_path)

    if context.get("ti"):
        context["ti"].xcom_push(key="silver_path", value=output_path)

    logger.info("=== TASK SILVER CONCLUÍDA ===")
    return output_path


# ---------------------------------------------------------------------------
# Execução standalone para testes locais
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    result = run_silver(execution_date=datetime.today().strftime("%Y-%m-%d"))
    print(f"Arquivo gerado: {result}")
