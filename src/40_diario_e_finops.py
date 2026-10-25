# Gerado a partir de notebooks/NexusAI_techfase2.ipynb (mesmo código, separado por camada).
# A execução é feita pelo notebook, na ordem das etapas: as células compartilham
# variáveis (client, PROJECT_ID e as funções das Etapas 1.1 e 1.2).

# ETAPA 5.1 · Diário de bordo
# Grava esta execução como "sucesso" no diário de bordo e mostra, por tabela, as linhas
# de entrada e de saída, o consumo por camada e o histórico.
# Falhas são gravadas pelo vigia de erros (Etapa 1.2).
df_diario, df_resumo = registrar_execucao("sucesso")

if df_diario.empty:
    raise ValueError(
        "Não há medições nesta sessão. Rode a Bronze, a Silver e a Gold "
        "na mesma sessão antes do diário de bordo."
    )

camadas = [c for c in ["bronze", "silver", "gold"] if c in set(df_diario["camada"])]

print(f"\n=== EXECUÇÃO {EXECUCAO_ID}: ENTRADA x SAÍDA POR TABELA ===")
print(df_diario[["tabela", "linhas_entrada", "linhas", "mb_estimados", "mb_lidos", "duracao_s", "status"]]
      .rename(columns={"linhas": "linhas_saida"}).to_string(index=False))

print("\n=== CONSUMO POR CAMADA ===")
print(df_diario.groupby("camada").agg(
    tabelas=("tabela", "count"),
    linhas_saida=("linhas", "sum"),
    mb_lidos=("mb_lidos", "sum"),
    duracao_s=("duracao_s", "sum"),
).reindex(camadas).round(2).reset_index().to_string(index=False))

print("\n=== RESUMO DESTA EXECUÇÃO ===")
print(df_resumo.drop(columns=["registrado_em", "etapa_da_falha", "erro"]).T.to_string(header=False))

print("\n=== HISTÓRICO: EXECUÇÕES GRAVADAS NO DIÁRIO ===")
print(consulta(f"""
    SELECT
        execucao_id,
        FORMAT_TIMESTAMP('%d/%m/%Y %H:%M', registrado_em) AS registrado_em_utc,
        situacao,
        tabelas_gravadas,
        linhas_gravadas,
        mb_lidos,
        duracao_s,
        regras_que_passaram,
        regras_medidas,
        SUBSTR(etapa_da_falha, 1, 40) AS etapa_da_falha
    FROM `{TABELA_EXECUCOES}`
    ORDER BY registrado_em
""").to_string(index=False))


# ETAPA 5.2 · FinOps: consumo medido e custo projetado
# Mede o consumo real desta execução e projeta o custo mensal do mesmo desenho em produção
# (cobrança por consulta, região US). O Sandbox não cobra; a projeção mostra o custo fora dele.
# - Consultas: bytes faturados de todos os jobs da execução (inclui conferências e
#   validações), lidos de INFORMATION_SCHEMA.JOBS_BY_USER.
# - Armazenamento: tamanho atual de cada conjunto de tabelas (metadados, sem custo) e de
#   uma cópia da Bronze, que cresce a cada dia guardado (retenção de 59 dias no Sandbox).
# - Cargas de arquivo (load jobs) e streaming por load job: gratuitos.
# Preços: cloud.google.com/bigquery/pricing, região US (várias regiões), a mesma dos
# datasets. A página abre em Iowa (us-central1), onde o armazenamento é mais caro.
# Consultados em 22/10/2026.
from google.api_core.exceptions import BadRequest, Forbidden, NotFound

PRECO_TIB_CONSULTA = 6.25                  # US$ por TiB processado (on-demand)
PRECO_GIB_ATIVO_MES = 0.000027397 * 730    # US$ por GiB-mês, armazenamento lógico ativo (US)
PRECO_GIB_LONGO_MES = 0.000013699 * 730    # US$ por GiB-mês, após 90 dias sem alteração (US)
GRATIS_TIB_CONSULTA = 1.0                  # por mês
GRATIS_GIB_ARMAZENAMENTO = 10.0            # por mês

# Frequência de execução pretendida: mensal. O INEP publica uma vez por ano, então rodar
# todo mês já pega qualquer atualização. A semanal e a diária entram só para comparar.
CADENCIAS = {"mensal (pretendida)": 1, "semanal": 4, "diária": 30}

MESES_DE_HISTORICO = 12                    # horizonte para o crescimento da Bronze

GIB, TIB = 1024**3, 1024**4
inicio_execucao = datetime.strptime(EXECUCAO_ID, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)

# Consultas: tudo o que esta execução rodou no BigQuery.
try:
    df_jobs = consulta(f"""
        SELECT
            job_type,
            COUNT(*)                                   AS jobs,
            SUM(IFNULL(total_bytes_processed, 0))      AS bytes_processados,
            SUM(IFNULL(total_bytes_billed, 0))         AS bytes_faturados,
            ROUND(SUM(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) / 1000, 1) AS duracao_s
        FROM `region-{LOCATION.lower()}`.INFORMATION_SCHEMA.JOBS_BY_USER
        WHERE project_id = '{PROJECT_ID}'
          AND creation_time >= TIMESTAMP('{inicio_execucao.isoformat()}')
          AND state = 'DONE'
        GROUP BY job_type
        ORDER BY bytes_faturados DESC
    """)
    bytes_faturados = float(df_jobs["bytes_faturados"].sum())
    fonte_consumo = "INFORMATION_SCHEMA.JOBS_BY_USER"
except (BadRequest, Forbidden, NotFound) as erro:
    # Sem acesso à visão de jobs: usa o diário de bordo (só as gravações).
    print(f"INFORMATION_SCHEMA indisponível ({type(erro).__name__}); usando o diário de bordo.")
    df_jobs = None
    bytes_faturados = float(df_diario["mb_lidos"].sum()) * 1024**2
    fonte_consumo = "diário de bordo (gravações)"

# Armazenamento: tamanho atual de cada dataset.
linhas_armazenamento = []
for camada in ["bronze", "silver", "gold", "quality_reports"]:
    for item in client.list_tables(f"{PROJECT_ID}.{camada}"):
        t = client.get_table(item.reference)
        linhas_armazenamento.append({"dataset": camada, "tabela": t.table_id,
                                     "linhas": t.num_rows, "bytes": t.num_bytes or 0})
df_armazenamento = pd.DataFrame(linhas_armazenamento)

# Tamanho de uma cópia da Bronze: proporção das linhas da cópia usada em cada tabela.
copia_bronze_bytes = 0.0
for r in df_armazenamento[df_armazenamento.dataset == "bronze"].itertuples():
    linhas_copia = LINHAS_DA_COPIA.get(r.tabela)
    if linhas_copia and r.linhas:
        copia_bronze_bytes += r.bytes * linhas_copia / r.linhas
fixo_bytes = float(df_armazenamento[df_armazenamento.dataset != "bronze"]["bytes"].sum())

print(f"=== CONSUMO MEDIDO NA EXECUÇÃO {EXECUCAO_ID} (fonte: {fonte_consumo}) ===")
if df_jobs is not None:
    print(df_jobs.assign(gib_faturados=(df_jobs.bytes_faturados / GIB).round(3))
          [["job_type", "jobs", "gib_faturados", "duracao_s"]].to_string(index=False))
print(f"Total faturável por execução: {bytes_faturados / GIB:.3f} GiB")

print("\n=== ARMAZENAMENTO ATUAL POR DATASET ===")
print(df_armazenamento.groupby("dataset").agg(tabelas=("tabela", "count"), linhas=("linhas", "sum"),
      gib=("bytes", lambda b: round(b.sum() / GIB, 3))).reset_index().to_string(index=False))
print(f"Uma cópia da Bronze: {copia_bronze_bytes / GIB:.3f} GiB | Silver + Gold + controle: {fixo_bytes / GIB:.3f} GiB")

# Tabela de custos.
custos = []
for nome, execucoes in CADENCIAS.items():
    tib_mes = bytes_faturados * execucoes / TIB
    gib_armazenado = (fixo_bytes + copia_bronze_bytes * execucoes * MESES_DE_HISTORICO) / GIB
    custos += [
        {"cadencia": nome, "servico": "BigQuery · consultas on-demand", "unidade": "TiB processado",
         "quantidade_medida": f"{bytes_faturados / GIB:.3f} GiB/execução",
         "preco_unitario_usd": PRECO_TIB_CONSULTA,
         "custo_mensal_usd": round(tib_mes * PRECO_TIB_CONSULTA, 4),
         "custo_mensal_com_gratuidade_usd": round(max(tib_mes - GRATIS_TIB_CONSULTA, 0) * PRECO_TIB_CONSULTA, 4)},
        {"cadencia": nome, "servico": "BigQuery · armazenamento ativo", "unidade": "GiB-mês",
         "quantidade_medida": f"{gib_armazenado:.2f} GiB após {MESES_DE_HISTORICO} meses de histórico",
         "preco_unitario_usd": round(PRECO_GIB_ATIVO_MES, 4),
         "custo_mensal_usd": round(gib_armazenado * PRECO_GIB_ATIVO_MES, 4),
         "custo_mensal_com_gratuidade_usd": round(max(gib_armazenado - GRATIS_GIB_ARMAZENAMENTO, 0) * PRECO_GIB_ATIVO_MES, 4)},
        {"cadencia": nome, "servico": "BigQuery · carga (load jobs, batch e streaming)", "unidade": "GiB carregado",
         "quantidade_medida": "load jobs da execução", "preco_unitario_usd": 0.0,
         "custo_mensal_usd": 0.0, "custo_mensal_com_gratuidade_usd": 0.0},
    ]
df_custos = pd.DataFrame(custos)
print("\n=== TABELA DE CUSTOS PROJETADA (produção, on-demand, US) ===")
print(df_custos.to_string(index=False))
print("\nTotal mensal por cadência (US$):")
print(df_custos.groupby("cadencia", sort=False)[["custo_mensal_usd", "custo_mensal_com_gratuidade_usd"]]
      .sum().round(4).to_string())
print(f"\nPreço de armazenamento após 90 dias sem alteração (long-term): US$ {PRECO_GIB_LONGO_MES:.4f}/GiB-mês.")


# ETAPA 5.3 · Evidências do ambiente
# Mostra os conjuntos de tabelas, o particionamento da Bronze e as partições guardadas
# (uma por dia de execução).

# Lista os conjuntos de tabelas do projeto: mostra que as quatro camadas existem.
print("=== DATASETS DO PROJETO ===")
for ds in sorted(client.list_datasets(), key=lambda d: d.dataset_id):
    print(f"   {ds.dataset_id}")

# Busca as partições de bronze.alunos (uma por dia de execução). Isso é feito antes de
# imprimir o bloco abaixo, porque a quantidade de dias vem desta lista.
df_particoes = consulta(f"""
    SELECT
        partition_id                                  AS particao,
        total_rows                                    AS linhas,
        ROUND(total_logical_bytes / POW(1024, 3), 3)  AS gib
    FROM `{B}`.INFORMATION_SCHEMA.PARTITIONS
    WHERE table_name = 'alunos'
    ORDER BY partition_id
""")

# Lê as informações de configuração da tabela de alunos da Bronze.
tabela_alunos = client.get_table(f"{B}.alunos")
particao = tabela_alunos.time_partitioning
print("\n=== bronze.alunos ===")
print(f"Particionada por: {particao.field} ({particao.type_})")
if particao.expiration_ms is None:
    print("Expiração das partições: nenhuma")
else:
    dias = particao.expiration_ms / (24 * 60 * 60 * 1000)
    print(f"Expiração das partições: {dias:.0f} dias (limite do Sandbox)")
print(f"Expiração da tabela (Sandbox): {tabela_alunos.expires}")
# Linhas e tamanho são o total de todas as partições. Por isso o número de dias
# guardados aparece junto: com 1 dia, o total é o de uma cópia; com 2 dias, é o dobro.
print(f"Dias guardados: {len(df_particoes)} | "
      f"Linhas (total dos dias): {tabela_alunos.num_rows:,} | "
      f"tamanho: {tabela_alunos.num_bytes / GIB:.3f} GiB")

# Mostra uma linha por dia de execução, ou seja, as cópias diárias que ainda estão na Bronze.
print("\n=== PARTIÇÕES DE bronze.alunos (uma por dia de execução) ===")
print(df_particoes.to_string(index=False))
