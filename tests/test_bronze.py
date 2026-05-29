"""
Testes unitários — Camada Bronze
"""

import os
import sys
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bronze.ingest_selic import fetch_selic_data, save_bronze
from utils.data_quality import DataQualityError


MOCK_PAYLOAD = [
    {"data": "01/01/2020", "valor": "0.0169"},
    {"data": "02/01/2020", "valor": "0.0169"},
    {"data": "03/01/2020", "valor": "0.0169"},
]


class TestFetchSelicData:
    def test_retorna_dataframe_com_colunas_corretas(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = MOCK_PAYLOAD
        mock_resp.raise_for_status = MagicMock()

        with patch("bronze.ingest_selic.requests.get", return_value=mock_resp):
            df = fetch_selic_data("http://mock-url")

        assert isinstance(df, pd.DataFrame)
        assert "data" in df.columns
        assert "valor" in df.columns
        assert len(df) == 3

    def test_lanca_erro_em_resposta_vazia(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()

        with patch("bronze.ingest_selic.requests.get", return_value=mock_resp):
            with pytest.raises(ValueError):
                fetch_selic_data("http://mock-url")

    def test_propaga_http_error(self):
        import requests as req
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req.HTTPError("404")

        with patch("bronze.ingest_selic.requests.get", return_value=mock_resp):
            with pytest.raises(req.HTTPError):
                fetch_selic_data("http://mock-url")


class TestSaveBronze:
    def test_salva_parquet_com_metadados(self, tmp_path):
        df = pd.DataFrame(MOCK_PAYLOAD)
        output = str(tmp_path / "test_bronze.parquet")
        save_bronze(df, output)

        assert os.path.exists(output)
        df_saved = pd.read_parquet(output)
        assert "_ingestao_ts" in df_saved.columns
        assert "_fonte_url" in df_saved.columns
        assert "_total_registros" in df_saved.columns

    def test_lanca_erro_dataframe_vazio(self, tmp_path):
        df = pd.DataFrame()
        output = str(tmp_path / "empty.parquet")
        with pytest.raises(DataQualityError):
            save_bronze(df, output)

    def test_lanca_erro_schema_invalido(self, tmp_path):
        df = pd.DataFrame([{"coluna_errada": "x"}])
        output = str(tmp_path / "bad_schema.parquet")
        with pytest.raises(DataQualityError):
            save_bronze(df, output)
