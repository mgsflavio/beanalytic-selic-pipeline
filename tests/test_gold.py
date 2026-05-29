"""
Testes unitários — Camada Gold
"""

import os
import sys
import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from gold.aggregate_selic import (
    calc_monthly_avg,
    calc_mom_variation,
    calc_annual_accumulated,
    save_gold,
)


def make_silver_df():
    """Cria DataFrame Silver sintético para testes."""
    records = []
    start = date(2020, 1, 1)
    for i in range(60):  # 60 dias úteis
        d = start + timedelta(days=i)
        if d.weekday() < 5:
            ano = d.year
            mes = d.month
            records.append({
                "data": pd.Timestamp(d),
                "valor": 0.0169 if mes <= 3 else 0.0100,
                "ano": ano,
                "mes": mes,
            })
    return pd.DataFrame(records)


class TestCalcMonthlyAvg:
    def test_retorna_media_por_ano_mes(self):
        df = make_silver_df()
        result = calc_monthly_avg(df)
        assert "media_mensal" in result.columns
        assert "ano" in result.columns
        assert "mes" in result.columns
        assert len(result) > 0

    def test_media_mensal_nao_negativa(self):
        df = make_silver_df()
        result = calc_monthly_avg(df)
        assert (result["media_mensal"] >= 0).all()


class TestCalcMomVariation:
    def test_primeiro_mes_nan(self):
        df = make_silver_df()
        monthly = calc_monthly_avg(df)
        result = calc_mom_variation(monthly)
        assert pd.isna(result["variacao_mom_pct"].iloc[0])

    def test_variacao_calculada_para_meses_seguintes(self):
        df = make_silver_df()
        monthly = calc_monthly_avg(df)
        result = calc_mom_variation(monthly)
        # Todos exceto o primeiro devem ter valor
        assert result["variacao_mom_pct"].iloc[1:].notna().all()


class TestCalcAnnualAccumulated:
    def test_retorna_taxa_por_ano(self):
        df = make_silver_df()
        result = calc_annual_accumulated(df)
        assert "ano" in result.columns
        assert "taxa_acumulada_anual" in result.columns

    def test_taxa_positiva(self):
        df = make_silver_df()
        result = calc_annual_accumulated(df)
        assert (result["taxa_acumulada_anual"] > 0).all()


class TestSaveGold:
    def test_salva_parquet_e_csv(self, tmp_path):
        df = make_silver_df()
        monthly = calc_monthly_avg(df)
        monthly_mom = calc_mom_variation(monthly)
        annual = calc_annual_accumulated(df)
        df_gold = monthly_mom.merge(annual, on="ano", how="left")

        output = str(tmp_path / "gold.parquet")
        save_gold(df_gold, output)

        assert os.path.exists(output)
        assert os.path.exists(output.replace(".parquet", ".csv"))
