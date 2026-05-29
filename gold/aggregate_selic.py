"""
Camada Gold — Agregação e Métricas
Responsável por gerar as métricas consolidadas da Taxa SELIC:
média mensal, variação mês a mês e taxa acumulada anual.
"""

import logging
import os
from datetime import datetime

import pandas as pd

from utils.data_quality import (
    check_not_empty,
    check_schema,
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
SILVER_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "silver")
GOLD_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "gold")
REQUIRED_SILVER_COLS = ["data", "valor", "ano", "mes"]


# ---------------------------------------------------------------------------
# Funções de cálculo
# ---------------------------------------------------------------------------
def calc_monthly_avg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a média mensal da taxa SELIC agrupando por ano e mês.

    Args:
        df: DataFrame Silver com dados diários.

    Returns:
        DataFrame com colunas [ano, mes, media_mensal].
    """
    logger.info("Calculando média mensal.")
    monthly = (
        df.groupby(["ano", "mes"], as_index=False)
        .agg(media_mensal=("valor", "mean"))
    )
    monthly["media_mensal"] = monthly["media_mensal"].round(6)
    return monthly


def calc_mom_variation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a variação percentual mês a mês (Month-over-Month) da média mensal.

    A variação é calculada como:
        variacao_mom_pct = ((media_atual / media_anterior) - 1) * 100

    Args:
        df: DataFrame com coluna 'media_mensal' ordenado por ano/mes.

    Returns:
        DataFrame com coluna adicional 'variacao_mom_pct'.
    """
    logger.info("Calculando variação mês a mês (MoM).")
    df = df.sort_values(["ano", "mes"]).reset_index(drop=True)
    df["variacao_mom_pct"] = (
        df["media_mensal"].pct_change() * 100
    ).round(4)
    return df


def calc_annual_accumulated(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a taxa acumulada anual pelo produto dos fatores diários.

    Metodologia oficial do Banco Central do Brasil:
    A API do BCB (série SGS 11) retorna a taxa SELIC Over diária em %/dia.
    O acumulado anual é calculado pelo produto encadeado dos fatores diários
    (capitalização composta):

        taxa_acumulada = PRODUTO[(1 + taxa_i/100) para cada dia útil] - 1

    IMPORTANTE: a taxa diária da API já é a taxa efetiva overnight
    capitalizada. NAO use divisão simples por 252 — isso gera resultados
    incorretos por ignorar os efeitos da capitalização composta.

    Valores de referência (Fonte: Banco Central do Brasil):
        2020: ~2.76%  2021: ~4.42%  2022: ~12.39%
        2023: ~13.04% 2024: ~10.88%

    Args:
        df: DataFrame Silver com dados diários.
            Coluna 'valor' em %/dia conforme API BCB (ex: 0.0420 = 0.0420%/dia).

    Returns:
        DataFrame com colunas [ano, taxa_acumulada_anual] em decimal
        (ex: 0.1088 = 10.88%).
    """
    logger.info("Calculando taxa acumulada anual.")

    def accumulate(group: pd.DataFrame) -> float:
        # Produto encadeado dos fatores diários (capitalização composta)
        fatores = 1 + group["valor"] / 100
        return round(fatores.prod() - 1, 6)

    annual = (
        df.groupby("ano")
        .apply(accumulate)
        .reset_index()
    )
    annual.columns = ["ano", "taxa_acumulada_anual"]
    return annual


def save_gold(df: pd.DataFrame, output_path: str) -> None:
    """
    Salva o DataFrame consolidado em Parquet e CSV.

    Args:
        df: DataFrame com as métricas Gold.
        output_path: Caminho base (sem extensão) para os arquivos de saída.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    parquet_path = output_path if output_path.endswith(".parquet") else output_path + ".parquet"
    csv_path = parquet_path.replace(".parquet", ".csv")

    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    df.to_csv(csv_path, index=False, encoding="utf-8")

    logger.info("Gold (Parquet) salvo em: %s", parquet_path)
    logger.info("Gold (CSV)     salvo em: %s", csv_path)


def run_gold(execution_date: str, **context) -> str:
    """
    Função orquestradora da camada Gold, chamada pela DAG do Airflow.

    Args:
        execution_date: Data de execução no formato YYYY-MM-DD.
        **context: Contexto do Airflow (XCom, etc.).

    Returns:
        Caminho do arquivo Parquet gerado.
    """
    logger.info("=== TASK GOLD INICIADA | execution_date=%s ===", execution_date)

    date_str = execution_date.replace("-", "")

    # Recupera o caminho Silver via XCom (se disponível no Airflow)
    if context.get("ti"):
        silver_path = context["ti"].xcom_pull(
            task_ids="transformacao_silver", key="silver_path"
        )
    else:
        silver_path = os.path.join(SILVER_DIR, f"selic_clean_{date_str}.parquet")

    output_path = os.path.join(GOLD_DIR, f"selic_metrics_{date_str}.parquet")

    # Carrega dados Silver
    if not os.path.exists(silver_path):
        raise FileNotFoundError(f"Arquivo Silver não encontrado: {silver_path}")

    df_silver = pd.read_parquet(silver_path, engine="pyarrow")
    logger.info("Silver carregado: %d registros.", len(df_silver))

    check_not_empty(df_silver)
    check_schema(df_silver, REQUIRED_SILVER_COLS)

    # Cálculo das métricas
    monthly_avg = calc_monthly_avg(df_silver)
    monthly_mom = calc_mom_variation(monthly_avg)
    annual_acc = calc_annual_accumulated(df_silver)

    # Merge final
    df_gold = monthly_mom.merge(annual_acc, on="ano", how="left")
    df_gold = df_gold[["ano", "mes", "media_mensal", "variacao_mom_pct", "taxa_acumulada_anual"]]

    save_gold(df_gold, output_path)

    if context.get("ti"):
        context["ti"].xcom_push(key="gold_path", value=output_path)

    logger.info("=== TASK GOLD CONCLUÍDA | %d métricas geradas ===", len(df_gold))
    return output_path


# ---------------------------------------------------------------------------
# Execução standalone para testes locais
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    result = run_gold(execution_date=datetime.today().strftime("%Y-%m-%d"))
    print(f"Arquivo gerado: {result}")
