# Gerado a partir de notebooks/NexusAI_techfase2.ipynb (mesmo código, separado por camada).
# A execução é feita pelo notebook, na ordem das etapas: as células compartilham
# variáveis (client, PROJECT_ID e as funções das Etapas 1.1 e 1.2).

# CONFIGURAÇÃO · ID do projeto no Google Cloud
# Troque o valor abaixo pelo ID do seu projeto (veja o passo a passo no README).
# É o ID, não o nome: por exemplo "meu-projeto-123456".
ID_DO_PROJETO = "fresh-lens-512607-c2"


# ETAPA 1 · Ambiente
# Faz o login no Google, conecta ao BigQuery e cria os quatro conjuntos de tabelas
# (bronze, silver, gold e quality_reports).
# Também define o código desta execução, o endereço de cada conjunto de tabelas e um
# limite de 5 GiB por consulta, para que nenhuma consulta gere custo inesperado.
# No fim, mostra as versões das bibliotecas usadas (base do requirements.txt).
import os
from datetime import datetime, timezone

from google.colab import auth
from google.api_core.exceptions import BadRequest, Forbidden, NotFound
from google.cloud import bigquery

auth.authenticate_user()

# ID do projeto: vem da célula Configuração. Se a variável de ambiente GCP_PROJECT_ID
# existir, ela vale mais.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", ID_DO_PROJETO)

# A Base dos Dados fica na região US, então os conjuntos de tabelas também ficam em US.
LOCATION = "US"

# Limite de dados lidos por consulta. Se uma consulta passar disso, ela falha antes de
# rodar, sem gerar custo. A maior consulta do trabalho lê cerca de 0,35 GiB.
LIMITE_GIB_POR_CONSULTA = 5
client = bigquery.Client(
    project=PROJECT_ID,
    location=LOCATION,
    default_query_job_config=bigquery.QueryJobConfig(
        maximum_bytes_billed=LIMITE_GIB_POR_CONSULTA * 1024**3),
)

# Confere se o projeto existe e se esta conta tem acesso, antes de criar qualquer coisa.
# Assim, um ID errado aparece já aqui, e não como erro de permissão mais adiante.
try:
    client.query("SELECT 1").result()
except (BadRequest, Forbidden, NotFound) as erro:
    raise RuntimeError(
        f"Não consegui usar o projeto '{PROJECT_ID}'. Confira o ID na célula "
        "Configuração e se você entrou com a conta Google dona do projeto. "
        f"Detalhe: {erro}") from erro

# Cria um conjunto de tabelas para cada camada e o quality_reports.
CAMADAS = ["bronze", "silver", "gold", "quality_reports"]

for camada in CAMADAS:
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{camada}")
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)

# Código desta execução (data e hora). Todas as etapas usam o mesmo.
INGESTION_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
EXECUCAO_ID = INGESTION_TIMESTAMP
INGESTION_DATE = (
    f"{INGESTION_TIMESTAMP[:4]}-{INGESTION_TIMESTAMP[4:6]}-{INGESTION_TIMESTAMP[6:8]}"
)

# Versão do pipeline, gravada em cada linha da Silver e da Gold.
PIPELINE_VERSION = "v1.0"

B = f"{PROJECT_ID}.bronze"
S = f"{PROJECT_ID}.silver"
G = f"{PROJECT_ID}.gold"
Q = f"{PROJECT_ID}.quality_reports"

print("Ambiente configurado!")
print(f"Projeto: {PROJECT_ID} | Local: {LOCATION} | Teto por consulta: {LIMITE_GIB_POR_CONSULTA} GiB")
print("Datasets:")
print("   ├── bronze/          → Dados brutos (raw)")
print("   ├── silver/          → Dados limpos e normalizados")
print("   ├── gold/            → Dados agregados para consumo")
print("   └── quality_reports/ → Relatórios de qualidade")
print(f"\nTimestamp de ingestão: {INGESTION_TIMESTAMP}")

# Versões das bibliotecas desta execução, usadas para montar o requirements.txt.
from importlib.metadata import version, PackageNotFoundError


def versao(pacote: str) -> str:
    try:
        return version(pacote)
    except PackageNotFoundError:
        return "não instalado"


print("Versões:", ", ".join(f"{p} {versao(p)}" for p in
      ["google-cloud-bigquery", "pandas", "pyarrow", "db-dtypes", "requests"]))
