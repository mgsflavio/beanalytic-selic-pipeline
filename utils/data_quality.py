"""
Utilitários de Qualidade de Dados
Funções reutilizáveis para validação entre as camadas do pipeline.
"""

import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Exceção customizada para falhas de qualidade de dados."""
    pass


def check_not_empty(df: pd.DataFrame) -> None:
    """
    Verifica que o DataFrame não está vazio.

    Args:
        df: DataFrame a ser verificado.

    Raises:
        DataQualityError: Se o DataFrame não contiver linhas.
    """
    if df is None or len(df) == 0:
        raise DataQualityError(
            "Verificação falhou: DataFrame está vazio (0 linhas). "
            "Verifique a origem dos dados."
        )
    logger.info("[QA] check_not_empty: OK (%d linhas)", len(df))


def check_no_nulls(df: pd.DataFrame, columns: List[str]) -> None:
    """
    Verifica que as colunas críticas não contêm valores nulos.

    Args:
        df: DataFrame a ser verificado.
        columns: Lista de nomes de colunas que não devem conter nulos.

    Raises:
        DataQualityError: Se qualquer coluna listada contiver nulos.
    """
    for col in columns:
        if col not in df.columns:
            raise DataQualityError(
                f"Verificação falhou: coluna '{col}' não existe no DataFrame."
            )
        null_count = df[col].isnull().sum()
        if null_count > 0:
            raise DataQualityError(
                f"Verificação falhou: coluna '{col}' contém {null_count} valor(es) nulo(s)."
            )
    logger.info("[QA] check_no_nulls: OK para colunas %s", columns)


def check_schema(df: pd.DataFrame, expected_columns: List[str]) -> None:
    """
    Valida que todas as colunas esperadas estão presentes no DataFrame.

    Args:
        df: DataFrame a ser verificado.
        expected_columns: Lista de colunas que devem existir.

    Raises:
        DataQualityError: Se alguma coluna esperada estiver ausente.
    """
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise DataQualityError(
            f"Verificação de schema falhou: colunas ausentes: {missing}. "
            f"Colunas presentes: {list(df.columns)}"
        )
    logger.info("[QA] check_schema: OK para colunas %s", expected_columns)


def check_value_range(
    df: pd.DataFrame,
    column: str,
    min_val: float,
    max_val: float,
) -> None:
    """
    Valida que os valores de uma coluna numérica estão dentro de um intervalo.

    Args:
        df: DataFrame a ser verificado.
        column: Nome da coluna numérica.
        min_val: Valor mínimo permitido (inclusive).
        max_val: Valor máximo permitido (inclusive).

    Raises:
        DataQualityError: Se algum valor estiver fora do intervalo.
    """
    if column not in df.columns:
        raise DataQualityError(
            f"Verificação falhou: coluna '{column}' não existe no DataFrame."
        )

    out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
    if len(out_of_range) > 0:
        raise DataQualityError(
            f"Verificação falhou: {len(out_of_range)} valor(es) em '{column}' "
            f"fora do intervalo [{min_val}, {max_val}]. "
            f"Min encontrado: {df[column].min():.6f}, "
            f"Max encontrado: {df[column].max():.6f}"
        )
    logger.info(
        "[QA] check_value_range: OK para '%s' em [%s, %s]",
        column, min_val, max_val,
    )
