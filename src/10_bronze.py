# Gerado a partir de notebooks/NexusAI_techfase2.ipynb (mesmo código, separado por camada).
# A execução é feita pelo notebook, na ordem das etapas: as células compartilham
# variáveis (client, PROJECT_ID e as funções das Etapas 1.1 e 1.2).

# ETAPA 2 · Bronze: ingestão batch
# Copia as tabelas da fonte (Base dos Dados) para a camada Bronze sem alterar os dados.
# Cada linha ganha colunas de rastreio (data e hora da cópia, tabela de origem, hash da
# linha etc.).
# Cada dia tem a sua cópia: rodar de novo no mesmo dia substitui só a cópia de hoje, e
# os outros dias não são mexidos, durante a retenção configurada. No Sandbox o BigQuery
# exige prazo menor que 60 dias; nesse caso usamos 59 dias, o máximo aceito, e o notebook
# avisa. Com cobrança ativada, a expiração é removida.
import pandas as pd
from google.api_core.exceptions import Forbidden, NotFound

# Fonte: Base dos Dados no BigQuery.
DATASET_ORIGEM = "basedosdados.br_inep_avaliacao_alfabetizacao"

# As 6 entidades do trabalho + o dicionário de códigos.
TABELAS_ORIGEM = [
    "uf",
    "municipio",
    "alunos",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
]


# Limite do Sandbox: o prazo de expiração da partição tem de ser menor que 60 dias.
RETENCAO_SANDBOX_MS = 59 * 24 * 60 * 60 * 1000
ambiente = {"sandbox": False}   # vira True quando o BigQuery recusa partição sem prazo (Sandbox)


def _atualizar_expiracao(tabela, expiracao_ms) -> None:
    """Define o prazo de expiração das partições da tabela."""
    particao = tabela.time_partitioning
    particao.expiration_ms = expiracao_ms
    tabela.time_partitioning = particao
    client.update_table(tabela, ["time_partitioning"])


def sem_expiracao(destino: str) -> None:
    """Remove o prazo de expiração das partições, para manter as cópias diárias pelo maior tempo permitido.

    Em projeto com cobrança, o prazo é removido. No Sandbox o BigQuery recusa (erro 403
    Forbidden); nesse caso usamos 59 dias, o máximo aceito, e avisamos.
    """
    tabela = client.get_table(destino)
    particao = tabela.time_partitioning
    if particao is None or particao.expiration_ms is None:
        return                                    # já está sem prazo

    if not ambiente["sandbox"]:
        try:
            _atualizar_expiracao(tabela, None)
            return
        except Forbidden as erro:
            # Só a recusa do Sandbox é tratada aqui; qualquer outro erro 403 continua
            # sendo erro.
            if "sandbox" not in str(erro).lower():
                raise
            ambiente["sandbox"] = True
            print("[sandbox] O BigQuery Sandbox não aceita partição sem expiração. "
                  "As partições da Bronze ficam 59 dias (o máximo permitido). "
                  "Com cobrança ativada, a expiração é removida.")
            tabela = client.get_table(destino)    # estado atual, antes de tentar de novo

    if tabela.time_partitioning.expiration_ms != RETENCAO_SANDBOX_MS:
        _atualizar_expiracao(tabela, RETENCAO_SANDBOX_MS)


def gravar_copia_do_dia(sql: str, destino: str):
    """Grava o resultado de uma consulta na partição de hoje da tabela e devolve o job.

    - Tabela nova: é criada particionada por _ingestion_date, sem prazo de expiração (no
      Sandbox, 59 dias).
    - Tabela existente: troca só a cópia de hoje (WRITE_TRUNCATE na partição
      tabela$AAAAMMDD) e preserva os outros dias. Para isso, todas as linhas precisam
      ter _ingestion_date igual à data da partição, o que vale aqui porque a data vem de
      INGESTION_DATE.
    """
    try:
        client.get_table(destino)
        tabela_existe = True
    except NotFound:
        tabela_existe = False

    if tabela_existe:
        config = bigquery.QueryJobConfig(
            destination=f"{destino}${INGESTION_DATE.replace('-', '')}",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
    else:
        config = bigquery.QueryJobConfig(
            destination=destino,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(field="_ingestion_date"),
        )

    # Sem try/except de propósito: se a cópia falhar, a execução para aqui.
    job = client.query(sql, job_config=config)
    job.result()
    sem_expiracao(destino)
    return job


def execucao_ja_gravada(tabela_destino: str) -> bool:
    """Diz se esta execução já foi gravada na tabela (evita duplicar)."""
    try:
        client.get_table(tabela_destino)
    except NotFound:
        # A tabela ainda não existe: é a primeira cópia.
        return False

    sql = f"""
        SELECT COUNT(*) AS total
        FROM `{tabela_destino}`
        WHERE _ingestion_date = DATE '{INGESTION_DATE}'
          AND _ingestion_timestamp = '{INGESTION_TIMESTAMP}'
    """
    total = list(client.query(sql).result())[0].total
    return total > 0


def ingerir_para_bronze(tabela: str) -> dict:
    """Copia uma tabela da fonte para a Bronze, sem transformar os dados."""
    origem = f"{DATASET_ORIGEM}.{tabela}"
    destino = f"{B}.{tabela}"

    if execucao_ja_gravada(destino):
        print(f"[{tabela}] esta execução já estava gravada. Nada foi duplicado.")
        return {"tabela": tabela, "status": "JA_GRAVADA"}

    # Só na Bronze usamos t.* (cópia fiel da fonte); Silver e Gold listam as colunas.
    sql = f"""
        SELECT
            t.*,
            '{INGESTION_TIMESTAMP}'        AS _ingestion_timestamp,
            DATE '{INGESTION_DATE}'        AS _ingestion_date,
            '{origem}'                     AS _source_table,
            'basedosdados_bigquery'        AS _source_system,
            TO_HEX(MD5(TO_JSON_STRING(t))) AS _record_hash
        FROM `{origem}` AS t
    """

    # Dry run: estima quantos MB a cópia vai ler, antes de executar.
    mb_estimados = simular_custo(sql)

    job = gravar_copia_do_dia(sql, destino)

    sql_contagem = f"""
        SELECT COUNT(*) AS total
        FROM `{destino}`
        WHERE _ingestion_date = DATE '{INGESTION_DATE}'
          AND _ingestion_timestamp = '{INGESTION_TIMESTAMP}'
    """
    linhas = list(client.query(sql_contagem).result())[0].total

    # Linhas de entrada pelos metadados da tabela; a Etapa 2.3 troca pela contagem
    # exata.
    entrada, lidas = linhas_de_entrada(job)

    print(f"[{tabela}] copiada: {linhas:,} linhas | simulação: {mb_estimados} MB")
    return {
        "tabela": tabela,
        "status": "OK",
        "linhas_gravadas": linhas,
        "linhas_entrada": entrada,
        "tabelas_lidas": lidas,
        "mb_estimados": mb_estimados,
        "mb_lidos": round(job.total_bytes_processed / 1024**2, 2),
        "duracao_s": round((job.ended - job.started).total_seconds(), 2),
        "job_id": job.job_id,
    }


print("=" * 60)
print(f"INGESTÃO BRONZE | execução {INGESTION_TIMESTAMP}")
print(f"Partição de hoje: {INGESTION_DATE}")
print("=" * 60)

# Métricas de cada tabela, usadas no diário de bordo e no cálculo de custo.
metricas_bronze = [ingerir_para_bronze(tabela) for tabela in TABELAS_ORIGEM]

print("\nINGESTÃO CONCLUÍDA")
# Tira as colunas tabelas_lidas e mb_estimados só da tela (continuam no diário de bordo).
display(pd.DataFrame(metricas_bronze).drop(columns=["tabelas_lidas", "mb_estimados"]))


# ETAPA 2.3 · Conferência da cópia (fonte x Bronze)
# Compara o número de linhas de cada tabela da fonte com o da cópia feita hoje na
# Bronze, numa única consulta.
# Qualquer diferença bloqueia a execução. O resultado é gravado em
# quality_reports.conferencias.
contagem_fonte = "\nUNION ALL\n".join(
    f"SELECT '{t}' AS tabela, COUNT(*) AS linhas_origem FROM `{DATASET_ORIGEM}.{t}`"
    for t in TABELAS_ORIGEM
)
# O filtro por _ingestion_date lê só a cópia de hoje.
contagem_copia = "\nUNION ALL\n".join(
    f"SELECT '{t}' AS tabela, COUNT(*) AS linhas_bronze FROM `{B}.{t}` "
    f"WHERE _ingestion_date = DATE '{INGESTION_DATE}' "
    f"AND _ingestion_timestamp = '{INGESTION_TIMESTAMP}'"
    for t in TABELAS_ORIGEM
)
df_conferencia = consulta(f"""
    WITH fonte AS (
        {contagem_fonte}
    ),
    copia AS (
        {contagem_copia}
    )
    SELECT f.tabela, f.linhas_origem, c.linhas_bronze
    FROM fonte AS f
    INNER JOIN copia AS c
        ON c.tabela = f.tabela
    ORDER BY f.tabela
""")
display(df_conferencia)

# A contagem exata da fonte entra no diário de bordo como linhas de entrada.
origem_por_tabela = dict(zip(df_conferencia.tabela, df_conferencia.linhas_origem))
for m in metricas_bronze:
    if m["tabela"] in origem_por_tabela:
        m["linhas_entrada"] = int(origem_por_tabela[m["tabela"]])

# Uma regra por tabela: diferença entre fonte e cópia (o esperado é zero).
resultados_bronze = [
    conferencia_direta("bronze", r.tabela, "linhas diferentes da fonte", "Completude",
                       abs(int(r.linhas_origem) - int(r.linhas_bronze)))
    for r in df_conferencia.itertuples()
]
df_conferencia_bronze = gravar_conferencias(resultados_bronze)
print(f"{len(df_conferencia_bronze)} resultados gravados em {TABELA_CONFERENCIAS}")
validacao_de_qualidade(df_conferencia_bronze, "2.3 conferência da cópia")


# ETAPA 2.3.1 · Investigação 1: metas por UF (só leitura)
# Só leitura. Mostra quantas linhas e quantos estados a tabela meta_alfabetizacao_uf tem
# por ano na cópia de hoje. O resultado é explicado no texto logo abaixo.
print("\nMetas por UF — linhas por ano:")
display(consulta(f"""
    SELECT
        ano,
        COUNT(*)                 AS linhas,
        COUNT(DISTINCT sigla_uf) AS estados
    FROM `{B}.meta_alfabetizacao_uf`
    WHERE _ingestion_date = DATE '{INGESTION_DATE}'
      AND _ingestion_timestamp = '{INGESTION_TIMESTAMP}'
    GROUP BY ano
    ORDER BY ano
"""))


# ETAPA 2.4 · Bronze: tabela de apoio territorial
# Copia também a tabela de municípios da Base dos Dados. As tabelas de alfabetização só
# trazem o código IBGE da cidade; esta tabela dá o nome, a UF e a região.
# A coluna centroide, que não é usada, fica de fora.
from google.api_core.exceptions import NotFound

ORIGEM_APOIO = "basedosdados.br_bd_diretorios_brasil.municipio"
DESTINO_APOIO = f"{B}.diretorio_municipio"


def apoio_ja_copiado() -> bool:
    """Não grava de novo se esta execução já gravou a tabela (a célula pode ser rodada duas
    vezes).
    """
    try:
        client.get_table(DESTINO_APOIO)
    except NotFound:
        return False
    sql = f"""
        SELECT COUNT(*) AS total
        FROM `{DESTINO_APOIO}`
        WHERE _ingestion_date = DATE '{INGESTION_DATE}'
          AND _ingestion_timestamp = '{INGESTION_TIMESTAMP}'
    """
    return list(client.query(sql).result())[0].total > 0


job_apoio = None
if apoio_ja_copiado():
    print("[diretorio_municipio] esta execução já estava gravada. Nada foi duplicado.")
else:
    sql = f"""
        SELECT
            t.* EXCEPT (centroide),
            '{INGESTION_TIMESTAMP}'  AS _ingestion_timestamp,
            DATE '{INGESTION_DATE}'  AS _ingestion_date,
            '{ORIGEM_APOIO}'         AS _source_table,
            'basedosdados_bigquery'  AS _source_system,
            -- Hash (impressão digital) das colunas usadas.
            TO_HEX(MD5(CONCAT(t.id_municipio, '|', t.nome, '|', t.sigla_uf))) AS _record_hash
        FROM `{ORIGEM_APOIO}` AS t
    """
    mb_estimados_apoio = simular_custo(sql)
    # Mesma regra da Etapa 2: cópia na partição do dia, com a expiração tratada por
    # sem_expiracao().
    job_apoio = gravar_copia_do_dia(sql, DESTINO_APOIO)
    print(f"[diretorio_municipio] copiada | simulação: {mb_estimados_apoio} MB | job_id: {job_apoio.job_id}")

# Conta as linhas da origem e as da cópia desta execução na Bronze.
r = consulta(f"""
    WITH fonte AS (
        SELECT COUNT(*) AS linhas_origem
        FROM `{ORIGEM_APOIO}`
    ),
    copia AS (
        SELECT COUNT(*) AS linhas_bronze
        FROM `{DESTINO_APOIO}`
        WHERE _ingestion_date = DATE '{INGESTION_DATE}'
          AND _ingestion_timestamp = '{INGESTION_TIMESTAMP}'
    )
    SELECT f.linhas_origem, c.linhas_bronze
    FROM fonte AS f
    CROSS JOIN copia AS c
""").iloc[0]
print(f"Origem: {int(r.linhas_origem)} linhas | Bronze: {int(r.linhas_bronze)} linhas")

# Métricas para o diário de bordo.
if job_apoio is not None:
    metricas_bronze.append({
        "tabela": "diretorio_municipio",
        "status": "OK",
        "linhas_gravadas": int(r.linhas_bronze),
        "linhas_entrada": int(r.linhas_origem),
        "tabelas_lidas": "br_bd_diretorios_brasil.municipio",
        "mb_estimados": mb_estimados_apoio,
        "mb_lidos": round(job_apoio.total_bytes_processed / 1024**2, 2),
        "duracao_s": round((job_apoio.ended - job_apoio.started).total_seconds(), 2),
        "job_id": job_apoio.job_id,
    })

# Bloqueante: a cópia precisa ter o mesmo número de linhas da origem.
df_conferencia_apoio = gravar_conferencias([
    conferencia_direta("bronze", "diretorio_municipio", "linhas diferentes da fonte", "Completude",
                       abs(int(r.linhas_origem) - int(r.linhas_bronze)))
])
validacao_de_qualidade(df_conferencia_apoio, "2.4 tabela de apoio")
