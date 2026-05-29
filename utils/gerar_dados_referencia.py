"""
Gerador de Dados de Referência — Taxa SELIC
============================================
Gera um arquivo Parquet na camada Bronze com taxas diárias sintéticas
derivadas dos valores mensais REAIS da SELIC Over publicados pelo BCB.

Use este script para testes locais quando a API do BCB não estiver acessível.
Os dados gerados produzem acumulados anuais que batem com os valores oficiais:

    2020: ~2,76%   2021: ~4,42%   2022: ~12,39%
    2023: ~13,04%  2024: ~10,88%

Fonte dos valores mensais: Banco Central do Brasil
https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados

Metodologia:
    A taxa diária de cada mês é calculada por capitalização composta:
    taxa_diaria = (1 + taxa_mensal/100)^(1/dias_uteis_mes) - 1

    Isso garante que o produto encadeado de todos os dias úteis do mês
    resulte exatamente na taxa mensal oficial.
"""

import os
import sys
from datetime import date, timedelta

import pandas as pd

# ---------------------------------------------------------------------------
# Taxas mensais reais da SELIC Over (% ao mês) — Fonte: BCB
# ---------------------------------------------------------------------------
SELIC_OVER_MENSAL = {
    2020: [0.37, 0.29, 0.34, 0.28, 0.24, 0.21, 0.19, 0.16, 0.16, 0.16, 0.15, 0.16],
    2021: [0.15, 0.13, 0.20, 0.21, 0.27, 0.29, 0.34, 0.43, 0.44, 0.48, 0.59, 0.77],
    2022: [0.73, 0.76, 0.93, 0.83, 1.03, 1.02, 1.03, 1.17, 1.07, 1.02, 1.02, 1.12],
    2023: [1.12, 0.92, 1.17, 0.92, 1.12, 1.07, 1.07, 1.14, 0.97, 1.00, 0.92, 0.89],
    2024: [0.97, 0.80, 0.83, 0.89, 0.83, 0.79, 0.91, 0.87, 0.84, 0.93, 0.79, 0.93],
}

# Acumulados esperados para validação
ACUMULADOS_REFERENCIA = {
    2020: 2.76, 2021: 4.42, 2022: 12.39, 2023: 13.04, 2024: 10.88
}


def dias_uteis_mes(ano: int, mes: int) -> int:
    """Conta os dias úteis (seg–sex) de um mês/ano."""
    count = 0
    d = date(ano, mes, 1)
    while d.month == mes:
        if d.weekday() < 5:
            count += 1
        d += timedelta(days=1)
    return count


def gerar_dados_referencia(output_path: str) -> pd.DataFrame:
    """
    Gera DataFrame com taxa SELIC diária sintética baseada nos valores
    mensais reais do BCB, usando capitalização composta.

    Args:
        output_path: Caminho do arquivo Parquet de saída.

    Returns:
        DataFrame com colunas ['data', 'valor'] no mesmo formato
        retornado pela API do BCB.
    """
    records = []

    for ano, meses in SELIC_OVER_MENSAL.items():
        for mes_idx, taxa_mensal in enumerate(meses, start=1):
            du = dias_uteis_mes(ano, mes_idx)

            # Taxa diária por capitalização composta
            # (1 + taxa_mensal/100)^(1/dias_uteis) - 1
            taxa_diaria = ((1 + taxa_mensal / 100) ** (1 / du) - 1) * 100

            d = date(ano, mes_idx, 1)
            while d.month == mes_idx:
                if d.weekday() < 5:  # apenas dias úteis
                    records.append({
                        "data": d.strftime("%d/%m/%Y"),
                        "valor": f"{taxa_diaria:.4f}",
                    })
                d += timedelta(days=1)

    df = pd.DataFrame(records)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    print(f"Dados de referência gerados: {len(df)} registros → {output_path}")
    return df


def validar_acumulados(output_path: str) -> None:
    """
    Valida que os dados gerados produzem os acumulados anuais corretos.

    Args:
        output_path: Caminho do arquivo Parquet gerado.
    """
    df = pd.read_parquet(output_path)
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = df["valor"].astype(float)
    df["ano"] = df["data"].dt.year

    print("\n=== Validação dos Acumulados Anuais ===")
    print(f"{'Ano':<6} {'Calculado':>12} {'Referência':>12} {'Diferença':>12} {'Status':>8}")
    print("-" * 55)

    all_ok = True
    for ano in sorted(SELIC_OVER_MENSAL.keys()):
        grupo = df[df["ano"] == ano]
        acum = (1 + grupo["valor"] / 100).prod() - 1
        calculado_pct = acum * 100
        referencia_pct = ACUMULADOS_REFERENCIA[ano]
        diff = abs(calculado_pct - referencia_pct)
        status = "✓ OK" if diff < 0.05 else "✗ ERRO"
        if diff >= 0.05:
            all_ok = False
        print(f"{ano:<6} {calculado_pct:>11.2f}% {referencia_pct:>11.2f}% {diff:>11.4f}pp {status:>8}")

    print("-" * 55)
    if all_ok:
        print("Todos os valores dentro da margem de 0,05pp ✓")
    else:
        print("ATENÇÃO: Divergência detectada nos acumulados!")
        sys.exit(1)


if __name__ == "__main__":
    from datetime import datetime
    date_str = datetime.today().strftime("%Y%m%d")
    output = os.path.join(
        os.path.dirname(__file__), "..", "data", "bronze",
        f"selic_{date_str}.parquet"
    )
    gerar_dados_referencia(output)
    validar_acumulados(output)
