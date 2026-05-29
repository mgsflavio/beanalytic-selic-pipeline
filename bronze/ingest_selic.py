"""
Camada Bronze — Ingestão de Dados
Responsável por consumir a API do Banco Central do Brasil e salvar
os dados brutos no formato Parquet, sem nenhuma transformação.
"""

import logging
import os
from datetime import datetime

import pandas as pd
import requests

from utils.data_quality import check_not_empty, check_schema, DataQualityError

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
BCB_URL = (
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
    "?formato=json&dataInicial=01/01/2020&dataFinal=31/12/2024"
)
BRONZE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "bronze")
EXPECTED_SCHEMA = ["data", "valor"]


# ---------------------------------------------------------------------------
# Funções principais
# ---------------------------------------------------------------------------
def fetch_selic_data(url: str, timeout: int = 30) -> pd.DataFrame:
    """
    Consome a API do Banco Central e retorna os dados como DataFrame.

    Args:
        url: Endpoint da API do BCB.
        timeout: Tempo máximo de espera pela resposta em segundos.

    Returns:
        DataFrame com colunas 'data' e 'valor' em formato bruto (strings).

    Raises:
        requests.HTTPError: Se a API retornar status HTTP de erro.
        requests.Timeout: Se a requisição exceder o tempo limite.
        ValueError: Se a resposta não contiver os campos esperados.
    """
    logger.info("Iniciando requisição à API do BCB: %s", url)

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.Timeout:
        logger.error("Timeout ao acessar a API do BCB após %ds.", timeout)
        raise
    except requests.HTTPError as exc:
        logger.error("Erro HTTP na API do BCB: %s", exc)
        raise

    payload = response.json()

    if not isinstance(payload, list) or len(payload) == 0:
        raise ValueError("Resposta da API vazia ou em formato inesperado.")

    df = pd.DataFrame(payload)
    logger.info("Dados recebidos: %d registros.", len(df))
    return df


def save_bronze(df: pd.DataFrame, output_path: str) -> None:
    """
    Valida e persiste o DataFrame bruto em formato Parquet.

    Args:
        df: DataFrame com os dados brutos da API.
        output_path: Caminho completo do arquivo Parquet de saída.

    Raises:
        DataQualityError: Se as validações de qualidade falharem.
    """
    logger.info("Iniciando validações de qualidade na camada Bronze.")
    check_not_empty(df)
    check_schema(df, EXPECTED_SCHEMA)

    # Adiciona metadados de controle como colunas extras
    df = df.copy()
    df["_ingestao_ts"] = datetime.utcnow().isoformat()
    df["_fonte_url"] = BCB_URL
    df["_total_registros"] = len(df)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    logger.info("Bronze salvo em: %s (%d linhas)", output_path, len(df))


def run_bronze(execution_date: str, **context) -> str:
    """
    Função orquestradora da camada Bronze, chamada pela DAG do Airflow.

    Args:
        execution_date: Data de execução no formato YYYY-MM-DD.
        **context: Contexto do Airflow (XCom, etc.).

    Returns:
        Caminho do arquivo Parquet gerado.
    """
    logger.info("=== TASK BRONZE INICIADA | execution_date=%s ===", execution_date)

    date_str = execution_date.replace("-", "")
    output_path = os.path.join(BRONZE_DIR, f"selic_{date_str}.parquet")

    df = fetch_selic_data(BCB_URL)
    save_bronze(df, output_path)

    # Publica o caminho via XCom para a próxima task
    if context.get("ti"):
        context["ti"].xcom_push(key="bronze_path", value=output_path)

    logger.info("=== TASK BRONZE CONCLUÍDA ===")
    return output_path


# ---------------------------------------------------------------------------
# Execução standalone para testes locais
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    result = run_bronze(execution_date=datetime.today().strftime("%Y-%m-%d"))
    print(f"Arquivo gerado: {result}")
