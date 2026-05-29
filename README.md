# beAnalytic — Pipeline SELIC (BCB)

Pipeline de engenharia de dados on-premise para ingestão, transformação e
agregação da Taxa SELIC, consumindo a API pública do Banco Central do Brasil
e orquestrado pelo Apache Airflow.

---

## Arquitetura

```
API BCB
  │
  ▼
┌─────────────────────────────────────────────────────┐
│  Apache Airflow — DAG: selic_pipeline               │
│                                                     │
│  [Bronze]  →  [Silver]  →  [Gold]                   │
│  Ingestão     Limpeza      Métricas                 │
└─────────────────────────────────────────────────────┘
  │              │              │
  ▼              ▼              ▼
Parquet        Parquet      Parquet + CSV
(bruto)       (limpo)      (consolidado)
```

### Camadas

| Camada | Pasta | Responsabilidade |
|--------|-------|-----------------|
| Bronze | `/bronze` | Ingestão bruta da API, sem transformações |
| Silver | `/silver` | Limpeza, tipagem e padronização |
| Gold   | `/gold`   | Agregações e métricas analíticas |

---

## Estrutura do Repositório

```
beanalytic-selic-pipeline/
├── dags/
│   └── selic_pipeline_dag.py   # DAG principal do Airflow
├── bronze/
│   └── ingest_selic.py         # Task de ingestão
├── silver/
│   └── transform_selic.py      # Task de transformação
├── gold/
│   └── aggregate_selic.py      # Task de agregação
├── utils/
│   └── data_quality.py         # Funções de qualidade de dados
├── tests/                      # Testes unitários
├── data/                       # Dados gerados (ignorado pelo Git)
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

**Por que Parquet?**
Formato colunar com compressão nativa, ideal para DataFrames analíticos.
Preserva tipos de dados entre etapas sem conversão manual.

**Por que XCom?**
Permite que tasks do Airflow troquem metadados (como caminhos de arquivos)
sem acoplamento direto, mantendo cada task independente e testável.

**Por que camadas separadas por pasta?**
Facilita substituição isolada de cada etapa e rastreabilidade de erros.
Cada pasta é um pacote Python independente, importável e testável.

**Por que `retry=3` com `retry_delay=5min`?**
A API do BCB pode ter instabilidades pontuais. Três tentativas com intervalo
de 5 minutos cobrem a maioria dos cenários de falha transitória.

---

## Pré-requisitos

- Docker >= 20.x
- Docker Compose >= 2.x

---

## Como Executar Localmente

```bash
# 1. Clone o repositório
git clone https://github.com/beanalytic/selic-pipeline.git
cd beanalytic-selic-pipeline

# 2. Configure variáveis de ambiente
cp .env.example .env

# 3. Crie os diretórios de dados
mkdir -p data/bronze data/silver data/gold logs

# 4. Suba o ambiente
docker compose up airflow-init
docker compose up -d

# 5. Acesse a UI do Airflow
# URL:   http://localhost:8080
# Login: admin
# Senha: admin

# 6. Ative a DAG 'selic_pipeline' e dispare manualmente
```

---

## Saída da Camada Gold

O arquivo `data/gold/selic_metrics_YYYYMMDD.csv` contém:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| ano | int | Ano de referência |
| mes | int | Mês de referência |
| media_mensal | float | Média da taxa SELIC diária no mês |
| variacao_mom_pct | float | Variação % em relação ao mês anterior |
| taxa_acumulada_anual | float | Produto dos fatores diários no ano - 1 |
