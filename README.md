# beAnalytic — Pipeline SELIC (BCB)

Pipeline de engenharia de dados on-premise para ingestão, transformação e
agregação da Taxa SELIC diária, consumindo a API pública do Banco Central
do Brasil e orquestrado pelo Apache Airflow.

O projeto adota a **Arquitetura Medallion** (Bronze → Silver → Gold),
padrão amplamente utilizado em engenharia de dados moderna para organizar
o dado em zonas de qualidade crescente, garantindo rastreabilidade,
reprodutibilidade e separação clara de responsabilidades entre as etapas
do pipeline. Cada camada tem uma responsabilidade única: a Bronze preserva
os dados brutos exatamente como recebidos da fonte, a Silver os transforma
e valida, e a Gold os agrega em métricas prontas para consumo analítico.

---

## Arquitetura Medallion

```
                    API BANCO CENTRAL DO BRASIL
                    https://api.bcb.gov.br (SELIC)
                                 │
                                 ▼
╔══════════════════════════════════════════════════════════════════╗
║             Apache Airflow — DAG: selic_pipeline                 ║
║                                                                  ║
║  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   ║
║  │    ◆ BRONZE ◆   │  │    ◆ SILVER ◆   │  │     ◆ GOLD ◆    │   ║
║  │                 │  │                 │  │                 │   ║
║  │   Ingestão      │→ │  Transformação  │→ │    Métricas     │   ║
║  │                 │  │                 │  │                 │   ║
║  │ • Consome API   │  │ • Converte tipos│  │ • Média mensal  │   ║
║  │ • Valida schema │  │ • Remove nulos  │  │ • Variação MoM  │   ║
║  │ • Salva bruto   │  │ • Valida dados  │  │ • Acumulado ano │   ║
║  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘   ║
║           │                    │                    │            ║
╚═══════════╪════════════════════╪════════════════════╪════════════╝
            │                    │                    │
            ▼                    ▼                    ▼
     data/bronze/          data/silver/          data/gold/
     selic_                selic_clean_          selic_metrics_
     YYYYMMDD.parquet      YYYYMMDD.parquet      YYYYMMDD.parquet
     (dados brutos)        (dados limpos)        + .csv (métricas)
```

### Camadas

| Camada | Pasta     | Responsabilidade                                        |
|--------|-----------|---------------------------------------------------------|
| Bronze | `/bronze` | Ingestão bruta da API, sem nenhuma transformação        |
| Silver | `/silver` | Transformação, tipagem, validação e padronização        |
| Gold   | `/gold`   | Agregações e métricas analíticas prontas para consumo   |

---

## Estrutura do Repositório

```
beanalytic-selic-pipeline/
├── dags/
│   └── selic_pipeline_dag.py     # DAG principal do Airflow
├── bronze/
│   ├── __init__.py
│   └── ingest_selic.py           # Task 1 — ingestão da API BCB
├── silver/
│   ├── __init__.py
│   └── transform_selic.py        # Task 2 — transformação e validação
├── gold/
│   ├── __init__.py
│   └── aggregate_selic.py        # Task 3 — métricas consolidadas
├── utils/
│   ├── __init__.py
│   └── data_quality.py           # Funções reutilizáveis de qualidade de dados
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── data/                         # Gerado pelo pipeline — ignorado pelo Git
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Decisões Técnicas

**Por que Arquitetura Medallion?**
Garante separação clara de responsabilidades entre as etapas. Os dados de
cada fase ficam preservados, permitindo reprocessar qualquer camada de
forma independente sem perder os dados originais.

**Por que Parquet?**
Formato colunar com compressão nativa. Preserva tipos de dados entre
etapas sem conversão manual e é o padrão do mercado para pipelines analíticos.

**Por que XCom?**
Permite que as tasks do Airflow troquem metadados (caminhos de arquivos)
sem acoplamento direto, mantendo cada task independente e testável.

**Por que camadas separadas por pasta?**
Facilita substituição isolada de cada etapa e rastreabilidade de erros.
Cada pasta é um pacote Python independente, importável e testável.

**Por que `retry=3` com `retry_delay=5min`?**
A API do BCB pode ter instabilidades pontuais. Três tentativas com
intervalo de 5 minutos cobrem a maioria dos cenários de falha transitória.

---

## Pré-requisitos

### Com Docker (recomendado)
- Docker >= 20.x
- Docker Compose >= 2.x
- Git

### Sem Docker
- Python >= 3.11
- Git

---

## Execução Local com Docker (Recomendado)

> O Docker isola todas as dependências e garante que o ambiente seja
> idêntico ao de produção. Certifique-se de que o **Docker Desktop
> está aberto** antes de executar os comandos.
> No Windows, use o **Git Bash** ou **WSL2**.

```bash
# 1. Clone o repositório
git clone https://github.com/SEU-USUARIO/beanalytic-selic-pipeline.git
cd beanalytic-selic-pipeline

# 2. Crie o arquivo de variáveis de ambiente
#    O padrão já funciona para uso local — não é necessário alterar nada
cp .env.example .env

# 3. Crie os diretórios de dados e logs
mkdir -p data/bronze data/silver data/gold logs

# 4. Inicialize o banco de dados do Airflow
#    Aguarde a mensagem: "Admin user admin created" antes de continuar
docker compose up airflow-init

# 5. Suba todos os serviços em segundo plano
docker compose up -d

# 6. Verifique se os serviços estão saudáveis (todos devem mostrar "healthy")
docker compose ps

# 7. Acesse a interface do Airflow no navegador
#    URL:   http://localhost:8080
#    Login: admin
#    Senha: admin

# 8. Ative e execute a DAG
#    - Clique no toggle à esquerda de "selic_pipeline" para ativar (fica azul)
#    - Clique no botão ▶ "Trigger DAG" para executar manualmente
#    - Acompanhe Bronze → Silver → Gold ficando verdes

# 9. Verifique os arquivos gerados
ls data/bronze/    # selic_YYYYMMDD.parquet
ls data/silver/    # selic_clean_YYYYMMDD.parquet
ls data/gold/      # selic_metrics_YYYYMMDD.parquet e .csv

# 10. Encerrar o ambiente quando terminar
docker compose down
```

---

## Execução Local sem Docker

> Use esta opção para testar scripts isolados ou quando o Docker
> não estiver disponível. No Windows, use o **Git Bash** ou **WSL2**.

```bash
# 1. Clone o repositório
git clone https://github.com/SEU-USUARIO/beanalytic-selic-pipeline.git
cd beanalytic-selic-pipeline

# 2. Crie e ative o ambiente virtual Python
python3 -m venv .venv
source .venv/bin/activate          # Linux e macOS
source .venv/Scripts/activate      # Windows (Git Bash)
# O prompt muda para: (.venv) $

# 3. Instale as dependências
pip install --upgrade pip
pip install -r requirements.txt

# 4. Crie os diretórios de dados
mkdir -p data/bronze data/silver data/gold

# 5. Execute cada camada na ordem correta
#    IMPORTANTE: a ordem Bronze → Silver → Gold é obrigatória
python bronze/ingest_selic.py      # Camada Bronze — consome API e salva Parquet
python silver/transform_selic.py   # Camada Silver — transforma e valida os dados
python gold/aggregate_selic.py     # Camada Gold   — gera métricas consolidadas

# 6. Verifique os arquivos gerados
ls data/bronze/    # selic_YYYYMMDD.parquet
ls data/silver/    # selic_clean_YYYYMMDD.parquet
ls data/gold/      # selic_metrics_YYYYMMDD.parquet e .csv

# 7. Execute os testes unitários
python -m pytest tests/ -v
# Resultado esperado: 21 passed

# ── Opcional: gerar dados de referência com valores reais do BCB ──────────
# Use este script quando a API do BCB não estiver acessível.
# Gera taxas diárias derivadas dos valores mensais reais (capitalização composta)
# e valida que os acumulados anuais batem com os valores oficiais:
#   2020: ~2,76%  2021: ~4,42%  2022: ~12,39%  2023: ~13,04%  2024: ~10,88%
python utils/gerar_dados_referencia.py

# 8. Desative o ambiente virtual quando terminar
deactivate
```

---

## Saída da Camada Gold

O arquivo `data/gold/selic_metrics_YYYYMMDD.csv` contém:

| Coluna               | Tipo  | Descrição                                                              |
|----------------------|-------|------------------------------------------------------------------------|
| ano                  | int   | Ano de referência                                                      |
| mes                  | int   | Mês de referência (1 a 12)                                             |
| media_mensal         | float | Média da taxa SELIC diária no mês                                      |
| variacao_mom_pct     | float | Variação % em relação ao mês anterior                                  |
| taxa_acumulada_anual | float | Capitalização composta dos fatores diários: ∏(1 + taxa_diaria/100) - 1 |
