"""
Testes unitários — Camada Silver
"""

import os
import sys
import pytest
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from silver.transform_selic import transform_selic, save_silver
from utils.data_quality import DataQualityError


def make_bronze_df(rows=None):
    if rows is None:
        rows = [
            {"data": "01/01/2020", "valor": "0.0169"},
            {"data": "02/01/2020", "valor": "0.0169"},
            {"data": "03/01/2020", "valor": "0.0158"},
        ]
    return pd.DataFrame(rows)


class TestTransformSelic:
    def test_converte_data_para_datetime(self):
        df = transform_selic(make_bronze_df())
        assert pd.api.types.is_datetime64_any_dtype(df["data"])

    def test_converte_valor_para_float(self):
        df = transform_selic(make_bronze_df())
        assert pd.api.types.is_float_dtype(df["valor"])

    def test_adiciona_colunas_ano_e_mes(self):
        df = transform_selic(make_bronze_df())
        assert "ano" in df.columns
        assert "mes" in df.columns
        assert df["ano"].iloc[0] == 2020
        assert df["mes"].iloc[0] == 1

    def test_remove_duplicatas(self):
        rows = [
            {"data": "01/01/2020", "valor": "0.0169"},
            {"data": "01/01/2020", "valor": "0.0169"},  # duplicata
            {"data": "02/01/2020", "valor": "0.0158"},
        ]
        df = transform_selic(pd.DataFrame(rows))
        assert len(df) == 2

    def test_remove_nulos(self):
        rows = [
            {"data": "01/01/2020", "valor": "invalido"},  # vira NaN
            {"data": "02/01/2020", "valor": "0.0158"},
        ]
        df = transform_selic(pd.DataFrame(rows))
        assert len(df) == 1

    def test_ordenado_cronologicamente(self):
        rows = [
            {"data": "03/01/2020", "valor": "0.0158"},
            {"data": "01/01/2020", "valor": "0.0169"},
            {"data": "02/01/2020", "valor": "0.0169"},
        ]
        df = transform_selic(pd.DataFrame(rows))
        datas = df["data"].tolist()
        assert datas == sorted(datas)

    def test_lanca_erro_valor_negativo(self):
        rows = [{"data": "01/01/2020", "valor": "-1.5"}]
        with pytest.raises(DataQualityError):
            transform_selic(pd.DataFrame(rows))


class TestSaveSilver:
    def test_salva_parquet_corretamente(self, tmp_path):
        df_raw = make_bronze_df()
        df_clean = transform_selic(df_raw)
        output = str(tmp_path / "silver.parquet")
        save_silver(df_clean, output)

        assert os.path.exists(output)
        df_loaded = pd.read_parquet(output)
        assert len(df_loaded) == len(df_clean)
