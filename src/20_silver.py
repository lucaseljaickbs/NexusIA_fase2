# Gerado a partir de notebooks/NexusAI_techfase2.ipynb (mesmo código, separado por camada).
# A execução é feita pelo notebook, na ordem das etapas: as células compartilham
# variáveis (client, PROJECT_ID e as funções das Etapas 1.1 e 1.2).

# ETAPA 3.0.1 · Como achar a cópia mais recente da Bronze
# Define as 7 tabelas do conjunto de alfabetização e as funções que descobrem a cópia
# mais recente de cada uma, sem ler todo o histórico (só os metadados das partições).
# Não mostra nada na tela.
import pandas as pd

TABELAS_DADOS = [
    "uf",
    "municipio",
    "alunos",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
]

# As tabelas do conjunto de alfabetização na Bronze (as 6 entidades + dicionário).
TABELAS_BRONZE = TABELAS_DADOS + ["dicionario"]


def copias_por_data(tabelas: list) -> pd.DataFrame:
    """Linhas de cada cópia da Bronze, por tabela e por data.

    Cada partição é uma cópia diária. As linhas vêm de INFORMATION_SCHEMA.PARTITIONS
    (metadados: não lê os dados e não é cobrado), então o custo não cresce com o
    histórico. Como esse metadado pode demorar a refletir uma gravação recém-feita, a
    cópia de hoje é contada direto na tabela (o filtro por partição conta as linhas sem
    ler dados).
    """
    lista = ", ".join(f"'{t}'" for t in tabelas)
    historico = consulta(f"""
        SELECT table_name AS tabela,
               FORMAT_DATE('%F', PARSE_DATE('%Y%m%d', partition_id)) AS data_copia,
               total_rows AS linhas
        FROM `{B}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name IN ({lista})
          AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
    """)
    hoje = consulta("\nUNION ALL\n".join(
        f"SELECT '{t}' AS tabela, '{INGESTION_DATE}' AS data_copia, COUNT(*) AS linhas "
        f"FROM `{B}.{t}` WHERE _ingestion_date = DATE '{INGESTION_DATE}'"
        for t in tabelas
    ))
    # A contagem direta de hoje vale mais que o metadado; cópia vazia não conta.
    hoje = hoje[hoje["linhas"] > 0]
    historico = historico[historico["data_copia"] != INGESTION_DATE]
    return pd.concat([historico, hoje], ignore_index=True)


def carimbos_da_data(data: str, tabelas: list) -> dict:
    """Data e hora (_ingestion_timestamp) da cópia de cada tabela numa data.

    O filtro por _ingestion_date lê só a partição desse dia.
    """
    df = consulta("\nUNION ALL\n".join(
        f"SELECT '{t}' AS tabela, MAX(_ingestion_timestamp) AS carimbo "
        f"FROM `{B}.{t}` WHERE _ingestion_date = DATE '{data}'"
        for t in tabelas
    ))
    return dict(zip(df["tabela"], df["carimbo"]))


# Data da cópia mais recente de cada tabela (uma partição por dia).
_copias = copias_por_data(TABELAS_BRONZE)
DATA_DA_COPIA = _copias.groupby("tabela")["data_copia"].max().to_dict()


def da_ultima_copia(tabela: str) -> str:
    """Devolve o trecho de SQL que lê só a cópia mais recente de uma tabela da Bronze.

    O filtro por _ingestion_date faz o BigQuery ler uma partição só, e não todo o
    histórico.
    """
    return (
        f"FROM `{B}.{tabela}` "
        f"WHERE _ingestion_date = DATE '{DATA_DA_COPIA[tabela]}'"
    )


# ETAPA 3.0.2 · Reconhecimento do dicionário (só leitura)
# Só leitura. Mostra o dicionário de códigos e como ano, série e rede estão escritos em
# cada tabela.
# Também mostra, tabela por tabela, em quais colunas o dicionário atua e se ele traduz
# todos os códigos que aparecem nos dados.
# Dicionário completo: a tradução oficial dos códigos.
print("=== 1) DICIONARIO ===")
sql = (
    "SELECT id_tabela, nome_coluna, chave, valor, cobertura_temporal "
    f"{da_ultima_copia('dicionario')} ORDER BY 1, 2, 3"
)
print(client.query(sql).to_dataframe().to_string(index=False))

# Colunas de cada tabela. Serve só para saber quais colunas existem; não é impresso.
sql = f"""
    SELECT table_name, column_name, data_type
    FROM `{B}.INFORMATION_SCHEMA.COLUMNS`
    WHERE NOT STARTS_WITH(column_name, '_')
    ORDER BY table_name, ordinal_position
"""
df_formatos = client.query(sql).to_dataframe()

# Como ano, série e rede estão escritos em cada tabela.
print("\n=== 2) VALORES DE ANO, SERIE E REDE ===")
for tabela in TABELAS_DADOS:
    colunas_da_tabela = set(
        df_formatos.loc[df_formatos["table_name"] == tabela, "column_name"]
    )
    # Usa só as colunas que existem naquela tabela.
    colunas = [c for c in ("ano", "serie", "rede") if c in colunas_da_tabela]
    selecao = ", ".join(f"CAST({c} AS STRING) AS {c}" for c in colunas)
    posicoes = ", ".join(str(i + 1) for i in range(len(colunas)))
    sql = f"""
        SELECT {selecao}, COUNT(*) AS linhas
        {da_ultima_copia(tabela)}
        GROUP BY {posicoes}
        ORDER BY {posicoes}
    """
    print(f"\n--- {tabela} ---")
    print(client.query(sql).to_dataframe().to_string(index=False))

# Onde o dicionário atua: o próprio dicionário diz quais colunas de cada tabela ele
# traduz (id_tabela + nome_coluna). Para cada uma, conta quantos códigos diferentes
# existem nos dados e quantos deles o dicionário não conhece (esses virariam
# "desconhecido" na Silver).
print("\n=== 3) ONDE O DICIONARIO ATUA (colunas que ele traduz, tabela por tabela) ===")
sql = f"""
    SELECT DISTINCT TRIM(id_tabela) AS id_tabela, TRIM(nome_coluna) AS nome_coluna
    {da_ultima_copia('dicionario')}
    ORDER BY 1, 2
"""
df_colunas_do_dicionario = client.query(sql).to_dataframe()
linhas_dic = []
for tabela in TABELAS_DADOS:
    colunas_da_tabela = set(
        df_formatos.loc[df_formatos["table_name"] == tabela, "column_name"]
    )
    colunas_traduzidas_pelo_dic = df_colunas_do_dicionario.loc[
        df_colunas_do_dicionario["id_tabela"] == tabela, "nome_coluna"
    ]
    for coluna in colunas_traduzidas_pelo_dic:
        # Se o dicionário citar uma coluna que a tabela não tem, aparece como aviso.
        if coluna not in colunas_da_tabela:
            linhas_dic.append({"tabela": tabela, "coluna": coluna,
                               "codigos_distintos": None,
                               "codigos_no_dicionario": None,
                               "codigos_sem_traducao": None})
            continue
        # Os dois lados são comparados como texto e sem espaços, que é como a Silver faz o
        # cruzamento (TRIM do código da tabela = código do dicionário).
        df_dic = consulta(f"""
            WITH dic AS (
                SELECT DISTINCT TRIM(CAST(chave AS STRING)) AS cod
                {da_ultima_copia('dicionario')}
                  AND TRIM(id_tabela) = '{tabela}'
                  AND TRIM(nome_coluna) = '{coluna}'
            ),
            codigos AS (
                SELECT DISTINCT TRIM(CAST({coluna} AS STRING)) AS cod
                {da_ultima_copia(tabela)}
                  AND {coluna} IS NOT NULL
            )
            SELECT
                COUNT(*)                 AS codigos_distintos,
                COUNTIF(dic.cod IS NULL) AS codigos_sem_traducao,
                (SELECT COUNT(*) FROM dic) AS codigos_no_dicionario
            FROM codigos
            LEFT JOIN dic ON dic.cod = codigos.cod
        """)
        linhas_dic.append({
            "tabela": tabela,
            "coluna": coluna,
            "codigos_distintos": int(df_dic.loc[0, "codigos_distintos"]),
            "codigos_no_dicionario": int(df_dic.loc[0, "codigos_no_dicionario"]),
            "codigos_sem_traducao": int(df_dic.loc[0, "codigos_sem_traducao"]),
        })
print(pd.DataFrame(linhas_dic).to_string(index=False))


# ETAPA 3.0.3 · Preparação da Silver
# Prepara as funções que a Silver usa: bronze() lê a cópia escolhida da Bronze e
# gravar_silver() grava a tabela.
# Mostra só qual cópia da Bronze ficou escolhida neste ponto.
import pandas as pd

# Cópia mais recente de cada tabela. A leitura filtra por _ingestion_date, então só uma
# partição por tabela é lida.
ULTIMA_COPIA = {}
for data in sorted(set(DATA_DA_COPIA.values())):
    tabelas_da_data = [t for t in TABELAS_BRONZE if DATA_DA_COPIA[t] == data]
    ULTIMA_COPIA.update(carimbos_da_data(data, tabelas_da_data))
print("Cópia da Bronze usada:", sorted(set(ULTIMA_COPIA.values())))


def bronze(tabela: str, colunas: list) -> str:
    """Devolve o SQL que lê a cópia escolhida de uma tabela da Bronze, só com as colunas
    pedidas.

    O filtro por _ingestion_date faz o BigQuery ler só a partição do dia. Nomear as
    colunas evita o SELECT *, que leria todas as colunas (no BigQuery o custo vem dos
    dados lidos).
    """
    carimbo = ULTIMA_COPIA[tabela]
    data = f"{carimbo[:4]}-{carimbo[4:6]}-{carimbo[6:8]}"
    return (
        f"SELECT {', '.join(colunas)} FROM `{B}.{tabela}` "
        f"WHERE _ingestion_date = DATE '{data}' "
        f"AND _ingestion_timestamp = '{carimbo}'"
    )


# Números de cada tabela gravada na Silver (linhas, MB, duração).
metricas_silver = []


def gravar_silver(nome: str, sql: str) -> int:
    """Estima o custo (dry run), grava em silver.<nome> (WRITE_TRUNCATE) e devolve as
    linhas.

    A Silver é refeita a cada execução, a partir da cópia mais recente da Bronze.
    """
    mb_estimados = simular_custo(sql)
    destino = f"{S}.{nome}"
    config = bigquery.QueryJobConfig(
        destination=destino,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.query(sql, job_config=config)
    job.result()
    linhas = client.get_table(destino).num_rows
    # Entrada: as tabelas que o job leu e quantas linhas elas têm.
    entrada, lidas = linhas_de_entrada(job)
    metricas_silver.append({
        "tabela": f"silver.{nome}",
        "linhas": linhas,
        "linhas_entrada": entrada,
        "tabelas_lidas": lidas,
        "status": "OK",
        "mb_estimados": mb_estimados,
        "mb_lidos": round(job.total_bytes_processed / 1024**2, 2),
        "duracao_s": round((job.ended - job.started).total_seconds(), 2),
        "job_id": job.job_id,
    })
    print(f"[silver.{nome}] gravada: {linhas:,} linhas (entrada: {entrada:,}) | simulação: {mb_estimados} MB")
    return linhas


# ETAPA 3.0.4 · Reconhecimento da tabela de apoio (só leitura)
# Só leitura. Lê a tabela de apoio das cidades (cópia da Bronze) e mostra como o dado
# está escrito, se há defeitos e onde ela atua em cada tabela.
# Usa bronze(), definida na Etapa 3.0.3.
# Leitura A: tabela de apoio territorial (nome, UF e região das cidades), lida da cópia
# que está na Bronze, a mesma que a Silver vai usar. Ela funciona como um dicionário de
# cidades: entra o código, sai o nome. Aqui só se olha; o tratamento fica para a Etapa 3.1.
print("\n=== A) TABELA DE APOIO TERRITORIAL (cópia da Bronze) ===")

# Descobre a cópia mais recente da tabela de apoio. Ela tem partição própria (Etapa 2.4),
# e a Etapa 3.0.6 repete esta mesma escolha para a Silver.
copia_apoio_atual = copias_por_data(["diretorio_municipio"]).sort_values("data_copia").iloc[-1]
ULTIMA_COPIA.update(carimbos_da_data(copia_apoio_atual["data_copia"], ["diretorio_municipio"]))

# Só as colunas que a Silver usa (sem SELECT *, que leria colunas desnecessárias).
COLUNAS_LEITURA_APOIO = ["id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao", "capital_uf"]
sql_apoio = bronze("diretorio_municipio", COLUNAS_LEITURA_APOIO)

# A1) Cinco linhas, para ver como o dado está escrito (em ordem de código, para mostrar sempre
# as mesmas).
print("A1) Amostra:")
print(consulta(f"SELECT * FROM ({sql_apoio}) ORDER BY id_municipio LIMIT 5").to_string(index=False))

# A2) Contagens que mostram o que pode precisar de tratamento. Aqui só se conta; as
# regras de qualidade equivalentes são medidas na Etapa 3.9.
print("\nA2) O que pode precisar de tratamento:")
print(consulta(f"""
    WITH apoio AS ({sql_apoio})
    SELECT
        COUNT(*)                                   AS linhas,
        COUNT(DISTINCT id_municipio)               AS codigos_distintos,
        COUNTIF(id_municipio IS NULL
                OR LENGTH(CAST(id_municipio AS STRING)) != 7) AS codigo_sem_7_digitos,
        COUNTIF(nome IS NULL)                      AS nome_vazio,
        COUNTIF(nome != TRIM(nome))                AS nome_com_espaco_sobrando,
        COUNTIF(sigla_uf != UPPER(TRIM(sigla_uf))) AS sigla_fora_do_padrao
    FROM apoio
""").to_string(index=False))

# A3) Como a coluna capital_uf vem escrita. A Etapa 3.1 a transforma em verdadeiro/falso.
print("\nA3) Como capital_uf está escrita:")
print(consulta(f"""
    WITH apoio AS ({sql_apoio})
    SELECT capital_uf, COUNT(*) AS linhas
    FROM apoio
    GROUP BY capital_uf
    ORDER BY capital_uf
""").to_string(index=False))

# A4) Onde a tabela de apoio atua. A Etapa 3.0.2 mostra, tabela por tabela, onde o
# dicionário vai traduzir códigos. Aqui a tabela de apoio faz o mesmo para o código da
# cidade (id_municipio) e para a sigla do estado (sigla_uf): para cada tabela que tem uma
# dessas colunas, conta quantos códigos diferentes existem e quantos não aparecem na
# tabela de apoio (esses ficariam sem nome quando a Silver ligar as tabelas).
print("\nA4) Onde a tabela de apoio atua (tabelas com id_municipio ou sigla_uf):")
CHAVES_DE_APOIO = ["id_municipio", "sigla_uf"]
linhas_a4 = []
for tabela in TABELAS_DADOS:
    colunas_da_tabela = set(
        df_formatos.loc[df_formatos["table_name"] == tabela, "column_name"]
    )
    for coluna in CHAVES_DE_APOIO:
        # Só as tabelas que realmente têm essa coluna.
        if coluna not in colunas_da_tabela:
            continue
        # Os dois lados são comparados como texto, sem espaços e em maiúsculas, que é o
        # formato que a Silver vai usar nas chaves.
        df_a4 = consulta(f"""
            WITH apoio AS (
                SELECT DISTINCT UPPER(TRIM(CAST({coluna} AS STRING))) AS cod
                FROM ({sql_apoio})
            ),
            codigos AS (
                SELECT DISTINCT UPPER(TRIM(CAST({coluna} AS STRING))) AS cod
                {da_ultima_copia(tabela)}
                  AND {coluna} IS NOT NULL
            )
            SELECT
                COUNT(*)                   AS codigos_distintos,
                COUNTIF(apoio.cod IS NULL) AS codigos_sem_par_na_apoio
            FROM codigos
            LEFT JOIN apoio ON apoio.cod = codigos.cod
        """)
        linhas_a4.append({
            "tabela": tabela,
            "chave": coluna,
            "codigos_distintos": int(df_a4.loc[0, "codigos_distintos"]),
            "codigos_sem_par_na_apoio": int(df_a4.loc[0, "codigos_sem_par_na_apoio"]),
        })
print(pd.DataFrame(linhas_a4).to_string(index=False))


# ETAPA 3.0.5 · Reconhecimento dos alunos (só leitura)
# Só leitura. Lê a cópia de alunos na Bronze e mostra, para cada combinação de presença,
# preenchimento do caderno e alfabetizado, quantos alunos têm nota e peso. Serve para
# decidir o tratamento dos alunos na Etapa 3.4: quem foi avaliado e o que fica vazio.
# Usa bronze(), definida na Etapa 3.0.3.
print("=== ALUNOS: PRESENCA x PREENCHIMENTO x ALFABETIZADO ===")
COLUNAS_LEITURA_ALUNOS = [
    "ano", "presenca", "preenchimento_caderno", "alfabetizado", "proficiencia", "peso_aluno",
]
print(consulta(f"""
    WITH alunos AS ({bronze('alunos', COLUNAS_LEITURA_ALUNOS)})
    SELECT
        ano,
        presenca,
        preenchimento_caderno,
        alfabetizado,
        COUNT(*)                         AS linhas,
        COUNTIF(proficiencia IS NULL)    AS sem_nota,
        COUNTIF(peso_aluno IS NULL)      AS sem_peso,
        COUNTIF(peso_aluno <= 0)         AS peso_zero_ou_negativo,
        ROUND(MIN(proficiencia), 1)      AS menor_nota,
        ROUND(MAX(proficiencia), 1)      AS maior_nota,
        COUNTIF(proficiencia >= 743)     AS nota_743_ou_mais
    FROM alunos
    GROUP BY ano, presenca, preenchimento_caderno, alfabetizado
    ORDER BY ano, presenca, preenchimento_caderno, alfabetizado
""").to_string(index=False))


# ETAPA 3.0.6 · Cópia da Bronze usada pela Silver
# Escolhe qual cópia da Bronze a Silver vai usar: a data mais recente em que as 7
# tabelas existem. Assim, toda a Silver vem do mesmo momento da fonte.
# As datas vêm dos metadados das partições (INFORMATION_SCHEMA.PARTITIONS), sem ler os
# dados.
historico_bronze = (
    copias_por_data(TABELAS_BRONZE)
    .pivot(index="data_copia", columns="tabela", values="linhas")
    .reindex(columns=TABELAS_BRONZE)
    .fillna(0).astype(int).sort_index()
)
print("=== HISTÓRICO DA BRONZE (linhas por data de cópia e por tabela) ===")
print(historico_bronze.to_string())

completas = historico_bronze[(historico_bronze > 0).all(axis=1)]
if completas.empty:
    raise ValueError("Nenhuma cópia completa na Bronze. Pipeline interrompido.")

COPIA_COMPLETA = completas.index.max()          # data (AAAA-MM-DD) da cópia usada
carimbos = carimbos_da_data(COPIA_COMPLETA, TABELAS_BRONZE)
if len(set(carimbos.values())) != 1:
    raise ValueError(f"As tabelas da cópia de {COPIA_COMPLETA} vêm de execuções "
                     f"diferentes: {carimbos}. Rode a Etapa 2 de novo.")
ULTIMA_COPIA = carimbos
# Linhas da cópia usada (entrada da Silver no diário).
LINHAS_DA_COPIA = {t: int(historico_bronze.loc[COPIA_COMPLETA, t]) for t in TABELAS_BRONZE}

# A tabela de apoio tem partição própria (Etapa 2.4).
copia_apoio = copias_por_data(["diretorio_municipio"]).sort_values("data_copia").iloc[-1]
ULTIMA_COPIA.update(carimbos_da_data(copia_apoio["data_copia"], ["diretorio_municipio"]))
LINHAS_DA_COPIA["diretorio_municipio"] = int(copia_apoio["linhas"])

print(f"\nCópias guardadas: {len(historico_bronze)} | completas: {len(completas)}")
print(f"Cópia usada pela Silver: {COPIA_COMPLETA} (execução {ULTIMA_COPIA['alunos']})")


# ETAPA 3.1 · Silver: dicionário e território
# Cria na Silver o dicionário de códigos (codigo/descricao, sem espaços sobrando) e as
# tabelas de território: territorio_municipio (nome, UF e região de cada cidade; só 6
# das 27 colunas da fonte são lidas) e territorio_uf.
# As colunas de controle abaixo são gravadas em toda tabela da Silver.
METADADOS = (
    "CURRENT_TIMESTAMP() AS _silver_processed_at,\n"
    f"        '{PIPELINE_VERSION}' AS _pipeline_version"
)


def juncao_dicionario(apelido: str, tabela_dic: str, coluna: str) -> str:
    """Faz LEFT JOIN com o dicionário para traduzir o código de uma coluna."""
    return (
        f"LEFT JOIN `{S}.dicionario` AS {apelido}\n"
        f"        ON {apelido}.id_tabela = '{tabela_dic}'\n"
        f"       AND {apelido}.nome_coluna = '{coluna}'\n"
        f"       AND {apelido}.codigo = TRIM(o.{coluna})"
    )


def nome_traduzido(apelido: str, coluna: str) -> str:
    """Código vazio continua vazio; código sem tradução vira 'desconhecido'."""
    return (
        f"CASE WHEN o.{coluna} IS NULL THEN NULL "
        f"ELSE COALESCE({apelido}.descricao, 'desconhecido') END"
    )


COLUNAS_DICIONARIO = [
    "id_tabela", "nome_coluna", "chave", "valor", "cobertura_temporal",
    "_ingestion_timestamp",
]
COLUNAS_DIRETORIO = [
    "id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao", "capital_uf",
    "_ingestion_timestamp",
]

gravar_silver("dicionario", f"""
    WITH o AS ({bronze('dicionario', COLUNAS_DICIONARIO)})
    SELECT
        TRIM(o.id_tabela)   AS id_tabela,
        TRIM(o.nome_coluna) AS nome_coluna,
        TRIM(o.chave)       AS codigo,
        TRIM(o.valor)       AS descricao,
        o.cobertura_temporal,
        o._ingestion_timestamp AS _bronze_ingestion_timestamp,
        {METADADOS}
    FROM o
""")

gravar_silver("territorio_municipio", f"""
    WITH o AS ({bronze('diretorio_municipio', COLUNAS_DIRETORIO)})
    SELECT
        TRIM(o.id_municipio)       AS id_municipio,
        TRIM(o.nome)               AS nome_municipio,
        UPPER(TRIM(o.sigla_uf))    AS sigla_uf,
        TRIM(o.nome_uf)            AS nome_uf,
        TRIM(o.nome_regiao)        AS nome_regiao,
        -- Na fonte, capital vem como 0, 1 ou vazio. Só o 1 é capital; 0 e vazio viram "não é capital".
        IFNULL(o.capital_uf = 1, FALSE) AS eh_capital,
        o._ingestion_timestamp AS _bronze_ingestion_timestamp,
        {METADADOS}
    FROM o
""")

gravar_silver("territorio_uf", f"""
    SELECT DISTINCT
        sigla_uf,
        nome_uf,
        nome_regiao
    FROM `{S}.territorio_municipio`
""")

print("\nExemplo do dicionário tratado:")
print(consulta(f"""
    SELECT id_tabela, nome_coluna, codigo, descricao
    FROM `{S}.dicionario`
    WHERE id_tabela = 'uf'
    ORDER BY nome_coluna, codigo
""").to_string(index=False))

print("\nEstados por região:")
print(consulta(f"""
    SELECT nome_regiao, COUNT(*) AS estados, STRING_AGG(sigla_uf, ', ' ORDER BY sigla_uf) AS siglas
    FROM `{S}.territorio_uf`
    GROUP BY nome_regiao
    ORDER BY nome_regiao
""").to_string(index=False))


# ETAPA 3.2 · Silver: resultado_uf
# Traduz os códigos com o dicionário, padroniza a sigla da UF e acrescenta o nome da UF
# e a região.
# A coluna comparavel_com_meta marca a rede que tem meta no nível estadual (Pública, 5).
# Valores vazios são mantidos (nunca viram zero).
COLUNAS_NIVEL = [f"proporcao_aluno_nivel_{n}" for n in range(9)]

# Rede que tem meta em cada nível (vem da leitura das tabelas de meta).
REDE_COM_META = {"uf": "5", "municipio": "3"}


def sql_resultado(tabela: str, coluna_chave: str, chave_sql: str,
                  colunas_territorio: str, juncao_territorio: str) -> str:
    """Receita comum às duas tabelas de resultado (UF e município)."""
    # Só as colunas usadas são lidas da Bronze (sem SELECT *).
    colunas_bronze = (
        ["ano", coluna_chave, "serie", "rede", "taxa_alfabetizacao", "media_portugues"]
        + COLUNAS_NIVEL + ["_ingestion_timestamp"]
    )
    niveis = ",\n        ".join(f"o.{c}" for c in COLUNAS_NIVEL)
    todos_niveis = " AND ".join(f"o.{c} IS NOT NULL" for c in COLUNAS_NIVEL)
    return f"""
    WITH origem AS ({bronze(tabela, colunas_bronze)})
    SELECT
        o.ano,
        {chave_sql},
        {colunas_territorio},
        TRIM(o.serie) AS serie_codigo,
        {nome_traduzido('d_serie', 'serie')} AS serie_nome,
        TRIM(o.rede) AS rede_codigo,
        {nome_traduzido('d_rede', 'rede')} AS rede_nome,
        -- Só esta rede tem meta correspondente neste nível.
        IFNULL(TRIM(o.rede) = '{REDE_COM_META[tabela]}', FALSE) AS comparavel_com_meta,
        o.taxa_alfabetizacao,
        o.media_portugues,
        {niveis},
        ({todos_niveis}) AS tem_distribuicao_niveis,
        o._ingestion_timestamp AS _bronze_ingestion_timestamp,
        {METADADOS}
    FROM origem AS o
    {juncao_dicionario('d_serie', tabela, 'serie')}
    {juncao_dicionario('d_rede', tabela, 'rede')}
    {juncao_territorio}
    """


gravar_silver("resultado_uf", sql_resultado(
    tabela="uf",
    coluna_chave="sigla_uf",
    chave_sql="UPPER(TRIM(o.sigla_uf)) AS sigla_uf",
    colunas_territorio="t.nome_uf,\n        t.nome_regiao",
    juncao_territorio=(
        f"LEFT JOIN `{S}.territorio_uf` AS t\n"
        "        ON t.sigla_uf = UPPER(TRIM(o.sigla_uf))"
    ),
))

print("\nExemplo: Amazonas")
print(consulta(f"""
    SELECT ano, sigla_uf, nome_regiao, rede_codigo, rede_nome, comparavel_com_meta,
           taxa_alfabetizacao, tem_distribuicao_niveis
    FROM `{S}.resultado_uf`
    WHERE sigla_uf = 'AM'
    ORDER BY ano, rede_codigo
""").to_string(index=False))


# ETAPA 3.3 · Silver: resultado_municipio
# Mesma transformação da Etapa 3.2. Nome, UF e região vêm da tabela de território.
# A rede que pode ser comparada com a meta municipal é a Municipal (3).
gravar_silver("resultado_municipio", sql_resultado(
    tabela="municipio",
    coluna_chave="id_municipio",
    chave_sql="TRIM(o.id_municipio) AS id_municipio",
    colunas_territorio=(
        "t.nome_municipio,\n        t.sigla_uf,\n        t.nome_uf,\n        t.nome_regiao"
    ),
    juncao_territorio=(
        f"LEFT JOIN `{S}.territorio_municipio` AS t\n"
        "        ON t.id_municipio = TRIM(o.id_municipio)"
    ),
))

print("\nExemplo: Rio de Janeiro (3304557) e Costa Marques (1100080)")
print(consulta(f"""
    SELECT ano, id_municipio, nome_municipio, sigla_uf, rede_nome, comparavel_com_meta,
           taxa_alfabetizacao
    FROM `{S}.resultado_municipio`
    WHERE id_municipio IN ('3304557', '1100080')
    ORDER BY id_municipio, ano, rede_codigo
""").to_string(index=False))


# ETAPA 3.4 · Silver: alunos
# Alunos ausentes vêm da fonte com alfabetizado = 0. Para isso não distorcer a taxa:
# - foi_avaliado: o aluno esteve presente, preencheu e tem nota (é o denominador da
#   taxa);
# - esta_alfabetizado: fica vazio para quem não foi avaliado;
# - alfabetizado_pelo_corte (nota >= 743): serve para conferir a regra do INEP.
NOTA_DE_CORTE = 743          # corte definido pelo INEP
REDES_PUBLICAS = ("1", "2", "3")  # Federal, Estadual e Municipal (dicionário)

# Colunas lidas da Bronze (todas as que a Silver usa; sem SELECT *).
COLUNAS_ALUNOS = [
    "ano", "id_municipio", "id_escola", "id_aluno", "caderno", "serie", "rede",
    "presenca", "preenchimento_caderno", "alfabetizado", "proficiencia",
    "peso_aluno", "_ingestion_timestamp",
]
colunas_base = ",\n            ".join(f"o.{c}" for c in COLUNAS_ALUNOS)

codificadas = ["serie", "rede", "presenca", "preenchimento_caderno", "alfabetizado"]
colunas_traduzidas = ",\n        ".join(
    f"TRIM(o.{c}) AS {c}_codigo,\n        {nome_traduzido('d_' + c, c)} AS {c}_nome"
    for c in codificadas
)
juncoes_dicionario = "\n    ".join(
    juncao_dicionario("d_" + c, "alunos", c) for c in codificadas
)
redes_publicas_sql = ", ".join(f"'{r}'" for r in REDES_PUBLICAS)

gravar_silver("alunos", f"""
    WITH origem AS ({bronze('alunos', COLUNAS_ALUNOS)}),
    base AS (
        SELECT
            {colunas_base},
            -- Quem entra no denominador da taxa de alfabetização.
            IFNULL(
                o.presenca = '1'
                AND o.preenchimento_caderno = '1'
                AND o.proficiencia IS NOT NULL,
                FALSE
            ) AS foi_avaliado
        FROM origem AS o
    )
    SELECT
        o.ano,
        TRIM(o.id_municipio) AS id_municipio,
        t.nome_municipio,
        t.sigla_uf,
        t.nome_regiao,
        o.id_escola,
        o.id_aluno,
        o.caderno,
        {colunas_traduzidas},
        CASE o.presenca WHEN '1' THEN TRUE WHEN '0' THEN FALSE END AS esta_presente,
        CASE o.preenchimento_caderno WHEN '1' THEN TRUE WHEN '0' THEN FALSE END AS preencheu_caderno,
        o.foi_avaliado,
        -- Vazio para quem não foi avaliado (não vira "falso").
        CASE
            WHEN NOT o.foi_avaliado THEN NULL
            WHEN o.alfabetizado = '1' THEN TRUE
            WHEN o.alfabetizado = '0' THEN FALSE
        END AS esta_alfabetizado,
        -- Mesma informação, calculada pela nota. Serve para conferir a regra.
        CASE
            WHEN o.proficiencia IS NULL THEN NULL
            ELSE o.proficiencia >= {NOTA_DE_CORTE}
        END AS alfabetizado_pelo_corte,
        IFNULL(TRIM(o.rede) IN ({redes_publicas_sql}), FALSE) AS rede_publica,
        o.proficiencia,
        o.peso_aluno,
        o._ingestion_timestamp AS _bronze_ingestion_timestamp,
        {METADADOS}
    FROM base AS o
    {juncoes_dicionario}
    LEFT JOIN `{S}.territorio_municipio` AS t
        ON t.id_municipio = TRIM(o.id_municipio)
""")

print("\nResumo por ano:")
print(consulta(f"""
    SELECT
        ano,
        COUNT(*) AS alunos,
        COUNTIF(foi_avaliado) AS avaliados,
        COUNTIF(NOT foi_avaliado) AS nao_avaliados,
        COUNTIF(esta_alfabetizado) AS alfabetizados,
        COUNTIF(NOT rede_publica) AS rede_privada
    FROM `{S}.alunos`
    GROUP BY ano
    ORDER BY ano
""").to_string(index=False))


# ETAPA 3.5 · Silver: meta_brasil
# Trata a tabela de meta do Brasil. A rede vem escrita por extenso ("Pública") e é
# convertida para o código usado nas tabelas de resultado (5).
# Colunas renomeadas: ano -> ano_referencia e taxa -> taxa_observada. Cria meta_do_ano e
# as marcas tem_meta_do_ano e tem_meta_2030.
ANOS_META = list(range(2024, 2031))
COLUNAS_META = [f"meta_alfabetizacao_{ano}" for ano in ANOS_META]

# O dicionário não cobre as tabelas de meta. Esta conversão foi conferida na Etapa 3.9:
# as taxas coincidem com as das tabelas de resultado.
MAPA_REDE_META = {"Pública": "5", "Municipal": "3"}


def sql_meta(tabela: str, colunas_chave: list = (), chave_sql: str = "",
             colunas_territorio: str = "", juncao_territorio: str = "",
             extras: str = "") -> str:
    """Receita comum às três tabelas de meta."""
    # Só as colunas usadas são lidas da Bronze (sem SELECT *).
    colunas_bronze = (
        ["ano", *colunas_chave, "rede", "taxa_alfabetizacao"] + COLUNAS_META
        + ["percentual_participacao", "_ingestion_timestamp"]
    )
    casos_rede = " ".join(
        f"WHEN TRIM(o.rede) = '{nome}' THEN '{codigo}'"
        for nome, codigo in MAPA_REDE_META.items()
    )
    metas = ",\n        ".join(f"o.{c}" for c in COLUNAS_META)
    meta_do_ano = "CASE o.ano " + " ".join(
        f"WHEN {ano} THEN o.meta_alfabetizacao_{ano}" for ano in ANOS_META
    ) + " END"
    return f"""
    WITH origem AS ({bronze(tabela, colunas_bronze)})
    SELECT
        o.ano AS ano_referencia,
        {chave_sql}{colunas_territorio}CASE WHEN o.rede IS NULL THEN NULL {casos_rede}
             ELSE 'desconhecido' END AS rede_codigo,
        TRIM(o.rede) AS rede_nome_fonte,
        o.taxa_alfabetizacao AS taxa_observada,
        {metas},
        -- Meta do próprio ano da linha. Para 2023 não existe meta: fica vazio.
        {meta_do_ano} AS meta_do_ano,
        ({meta_do_ano}) IS NOT NULL AS tem_meta_do_ano,
        o.meta_alfabetizacao_2030 IS NOT NULL AS tem_meta_2030,
        {extras}o.percentual_participacao,
        o._ingestion_timestamp AS _bronze_ingestion_timestamp,
        {METADADOS}
    FROM origem AS o
    {juncao_territorio}
    """


gravar_silver("meta_brasil", sql_meta("meta_alfabetizacao_brasil"))

print("\nMetas do país, tratadas:")
print(consulta(f"""
    SELECT ano_referencia, rede_codigo, rede_nome_fonte, taxa_observada,
           meta_do_ano, tem_meta_do_ano, meta_alfabetizacao_2030
    FROM `{S}.meta_brasil`
    ORDER BY ano_referencia
""").to_string(index=False))


# ETAPA 3.6 · Silver: meta_uf
# Mesma regra da Etapa 3.5, com sigla padronizada, nome da UF e região.
# Roraima tem todas as metas em branco: vira "sem meta", e não zero.
gravar_silver("meta_uf", sql_meta(
    tabela="meta_alfabetizacao_uf",
    colunas_chave=["sigla_uf"],
    chave_sql="UPPER(TRIM(o.sigla_uf)) AS sigla_uf,\n        ",
    colunas_territorio="t.nome_uf,\n        t.nome_regiao,\n        ",
    juncao_territorio=(
        f"LEFT JOIN `{S}.territorio_uf` AS t\n"
        "        ON t.sigla_uf = UPPER(TRIM(o.sigla_uf))"
    ),
))

print("\nExemplo: Amazonas, Ceará e Roraima")
print(consulta(f"""
    SELECT ano_referencia, sigla_uf, rede_codigo, taxa_observada, meta_do_ano,
           tem_meta_do_ano, tem_meta_2030
    FROM `{S}.meta_uf`
    WHERE sigla_uf IN ('AM', 'CE', 'RR')
    ORDER BY sigla_uf, ano_referencia
""").to_string(index=False))


# ETAPA 3.7 · Silver: meta_municipio
# Mesma regra da Etapa 3.5 (Municipal -> 3), com nome da cidade, UF e região.
# A coluna nivel_alfabetizacao não tem tradução no dicionário e fica como veio.
gravar_silver("meta_municipio", sql_meta(
    tabela="meta_alfabetizacao_municipio",
    colunas_chave=["id_municipio", "nivel_alfabetizacao"],
    chave_sql="TRIM(o.id_municipio) AS id_municipio,\n        ",
    colunas_territorio=(
        "t.nome_municipio,\n        t.sigla_uf,\n        t.nome_uf,\n        t.nome_regiao,\n        "
    ),
    juncao_territorio=(
        f"LEFT JOIN `{S}.territorio_municipio` AS t\n"
        "        ON t.id_municipio = TRIM(o.id_municipio)"
    ),
    extras="o.nivel_alfabetizacao,\n        ",
))

print("\nExemplo: Rio de Janeiro (3304557) e Costa Marques (1100080)")
print(consulta(f"""
    SELECT ano_referencia, id_municipio, nome_municipio, sigla_uf, rede_codigo,
           taxa_observada, meta_do_ano, tem_meta_do_ano, nivel_alfabetizacao
    FROM `{S}.meta_municipio`
    WHERE id_municipio IN ('3304557', '1100080')
    ORDER BY id_municipio, ano_referencia
""").to_string(index=False))


# ETAPA 3.8 · Silver: trajetória das metas
# As metas de 2024 a 2030 vêm em 7 colunas. Aqui cada ano vira uma linha (UNPIVOT), em
# três tabelas novas: meta_brasil_trajetoria, meta_uf_trajetoria e
# meta_municipio_trajetoria.
# Usadas na Etapa 4.3.
def sql_trajetoria(tabela_silver: str, chaves: str = "") -> str:
    """Metas 'em pé': uma linha por ano de meta (UNPIVOT)."""
    colunas = ", ".join(COLUNAS_META)
    return f"""
    WITH larga AS (
        SELECT ano_referencia, {chaves}rede_codigo, {colunas}
        FROM `{S}.{tabela_silver}`
    )
    SELECT
        ano_referencia,
        {chaves}rede_codigo,
        CAST(REGEXP_EXTRACT(coluna_meta, r'[0-9]+$') AS INT64) AS ano_meta,
        meta,
        {METADADOS}
    FROM larga
    UNPIVOT INCLUDE NULLS (meta FOR coluna_meta IN ({colunas}))
    """


gravar_silver("meta_brasil_trajetoria", sql_trajetoria("meta_brasil"))
gravar_silver("meta_uf_trajetoria", sql_trajetoria("meta_uf", "sigla_uf, "))
gravar_silver("meta_municipio_trajetoria", sql_trajetoria("meta_municipio", "id_municipio, "))

print("\nANTES · metas 'deitadas' (silver.meta_brasil, ano de referência 2024):")
print(consulta(f"""
    SELECT ano_referencia, meta_alfabetizacao_2024, meta_alfabetizacao_2025,
           meta_alfabetizacao_2026, meta_alfabetizacao_2027, meta_alfabetizacao_2028,
           meta_alfabetizacao_2029, meta_alfabetizacao_2030
    FROM `{S}.meta_brasil`
    WHERE ano_referencia = 2024
""").to_string(index=False))

print("\nDEPOIS · metas 'em pé' (silver.meta_brasil_trajetoria, ano de referência 2024):")
print(consulta(f"""
    SELECT ano_referencia, rede_codigo, ano_meta, meta
    FROM `{S}.meta_brasil_trajetoria`
    WHERE ano_referencia = 2024
    ORDER BY ano_meta
""").to_string(index=False))

print("\nTodas as tabelas da Silver gravadas nesta sessão:")
# Tira as colunas tabelas_lidas e mb_estimados só da tela (continuam no diário de bordo).
print(pd.DataFrame(metricas_silver).drop(columns=["tabelas_lidas", "mb_estimados"]).to_string(index=False))


# ETAPA 3.9.1 · Medições de qualidade da Silver: regras por tabela
# Mede as regras de qualidade da Silver: uma consulta por tabela (cada regra vira uma
# coluna da consulta). Aqui só se mede e se mostra; a Etapa 3.11 grava os resultados e
# decide se a Gold pode rodar.
import pandas as pd

soma_niveis = " + ".join(f"proporcao_aluno_nivel_{n}" for n in range(9))
queda_de_meta = " OR ".join(
    f"meta_alfabetizacao_{ano + 1} < meta_alfabetizacao_{ano}" for ano in range(2024, 2030)
)
CODIGO_CIDADE_INVALIDO = "COUNTIF(id_municipio IS NULL OR LENGTH(id_municipio) != 7)"

regras_silver = {}      # tabela -> lista de regras

# Tabelas de resultado: chave (ano, lugar, série, rede) e coluna com o nome do lugar.
for tabela, chave, nome_do_lugar in [
    ("resultado_uf", ["ano", "sigla_uf", "serie_codigo", "rede_codigo"], "nome_uf"),
    ("resultado_municipio", ["ano", "id_municipio", "serie_codigo", "rede_codigo"], "nome_municipio"),
]:
    regras_silver[tabela] = [
        regra("linhas repetidas na chave", "Unicidade", repetidas(*chave)),
        regra("codigo sem traducao", "Validade",
              "COUNTIF(rede_nome = 'desconhecido' OR serie_nome = 'desconhecido')"),
        regra("serie diferente de 2 (2o ano)", "Validade",
              "COUNTIF(serie_codigo IS NULL OR serie_codigo != '2')"),
        regra("taxa fora de 0 a 100", "Validade",
              "COUNTIF(taxa_alfabetizacao < 0 OR taxa_alfabetizacao > 100)"),
        regra("lugar sem nome", "Completude", f"COUNTIF({nome_do_lugar} IS NULL)"),
        # As nove proporções de cada nível somam 100 (tolerância de 0,5 por
        # arredondamento).
        regra("proporcoes por nivel nao somam 100", "Consistência",
              f"COUNTIF(tem_distribuicao_niveis AND ABS(({soma_niveis}) - 100) > 0.5)", tipo=I_),
    ]
regras_silver["resultado_municipio"].append(
    regra("codigo da cidade sem 7 digitos", "Validade", CODIGO_CIDADE_INVALIDO, tipo=I_))

regras_silver["alunos"] = [
    regra("linhas repetidas na chave", "Unicidade", repetidas("ano", "id_aluno")),
    regra("codigo sem traducao", "Validade", (
        "COUNTIF(rede_nome = 'desconhecido' OR serie_nome = 'desconhecido' "
        "OR presenca_nome = 'desconhecido' OR preenchimento_caderno_nome = 'desconhecido' "
        "OR alfabetizado_nome = 'desconhecido')")),
    regra("serie diferente de 2 (2o ano)", "Validade",
          "COUNTIF(serie_codigo IS NULL OR serie_codigo != '2')"),
    regra("aluno avaliado sem peso", "Completude",
          "COUNTIF(foi_avaliado AND (peso_aluno IS NULL OR peso_aluno <= 0))"),
    regra("aluno nao avaliado com nota", "Consistência",
          "COUNTIF(NOT foi_avaliado AND proficiencia IS NOT NULL)"),
    regra("alfabetizado diferente da nota de corte", "Consistência",
          "COUNTIF(foi_avaliado AND esta_alfabetizado != alfabetizado_pelo_corte)"),
    regra("nota fora de 0 a 1000", "Validade",
          "COUNTIF(proficiencia < 0 OR proficiencia > 1000)", tipo=I_),
    regra("cidade sem nome", "Completude", "COUNTIF(nome_municipio IS NULL)", tipo=I_),
    regra("codigo da cidade sem 7 digitos", "Validade", CODIGO_CIDADE_INVALIDO, tipo=I_),
]

# Tabelas de meta: chave, intervalo de valores, trajetória e território.
for tabela, chave, nome_do_lugar in [
    ("meta_brasil", ["ano_referencia", "rede_codigo"], None),
    ("meta_uf", ["ano_referencia", "sigla_uf", "rede_codigo"], "nome_uf"),
    ("meta_municipio", ["ano_referencia", "id_municipio", "rede_codigo"], "nome_municipio"),
]:
    regras = [
        regra("linhas repetidas na chave", "Unicidade", repetidas(*chave)),
        regra("rede sem codigo", "Validade",
              "COUNTIF(rede_codigo IS NULL OR rede_codigo = 'desconhecido')"),
        regra("taxa ou meta do ano fora de 0 a 100", "Validade", (
            "COUNTIF(taxa_observada < 0 OR taxa_observada > 100 "
            "OR meta_do_ano < 0 OR meta_do_ano > 100)")),
        regra("meta de 2030 diferente de 80", "Consistência",
              "COUNTIF(ABS(meta_alfabetizacao_2030 - 80) > 0.001)", tipo=I_),
        regra("meta que diminui de um ano para o seguinte", "Consistência",
              f"COUNTIF({queda_de_meta})", tipo=I_),
    ]
    if nome_do_lugar:
        regras.append(regra("lugar sem nome", "Completude",
                            f"COUNTIF({nome_do_lugar} IS NULL)", tipo=I_))
    if tabela == "meta_municipio":
        regras.append(regra("codigo da cidade sem 7 digitos", "Validade",
                            CODIGO_CIDADE_INVALIDO, tipo=I_))
    regras_silver[tabela] = regras

regras_silver["dicionario"] = [
    regra("traducao repetida", "Unicidade", repetidas("id_tabela", "nome_coluna", "codigo")),
    # A tradução da Silver não olha a coluna cobertura_temporal. Isso só está certo
    # enquanto ela for sempre "(1)"; outro valor exigiria considerar o ano na tradução.
    regra("cobertura temporal diferente de (1)", "Validade",
          "COUNTIF(cobertura_temporal IS NULL OR cobertura_temporal != '(1)')"),
]
regras_silver["territorio_municipio"] = [
    regra("codigo da cidade sem 7 digitos", "Validade", CODIGO_CIDADE_INVALIDO, tipo=I_),
]

# Uma consulta por tabela.
medicoes_silver = []
for tabela, regras in regras_silver.items():
    medicoes_silver += medir("silver", tabela, f"`{S}.{tabela}`", regras)

df_medicoes_silver = pd.DataFrame(medicoes_silver)
# A lista com o resultado de cada regra é impressa na Etapa 3.11 (validação da Silver).
print(f"=== REGRAS DA SILVER: {len(df_medicoes_silver)} medidas em "
      f"{df_medicoes_silver['tabela'].nunique()} tabelas (o esperado é zero linhas violando) ===")

reprovadas = df_medicoes_silver[(df_medicoes_silver["tipo"] == B_) & (~df_medicoes_silver["passou"])]
print(f"\nRegras bloqueantes reprovadas nesta medição: {len(reprovadas)}. "
      "A decisão é da validação da Etapa 3.11.")


# ETAPA 3.9.2 · Medições de qualidade da Silver: tipos das chaves
# Mostra o tipo da coluna usada como chave em cada tabela que se cruza.
# Uma mesma coluna pode ter tipos diferentes em tabelas do mesmo dataset, e o cruzamento
# com tipos diferentes falha ou perde linhas. Por isso a chave precisa ter o mesmo tipo
# em todas as tabelas em que aparece.
CHAVES_DE_JOIN = {
    "ano": [("resultado_uf", "ano"), ("resultado_municipio", "ano"), ("alunos", "ano"),
            ("meta_brasil", "ano_referencia"), ("meta_uf", "ano_referencia"),
            ("meta_municipio", "ano_referencia")],
    "id_municipio": [(t, "id_municipio") for t in
                     ["territorio_municipio", "resultado_municipio", "meta_municipio", "alunos"]],
    "sigla_uf": [(t, "sigla_uf") for t in ["territorio_uf", "resultado_uf", "meta_uf"]],
    "rede_codigo": [(t, "rede_codigo") for t in
                    ["resultado_uf", "resultado_municipio", "meta_brasil", "meta_uf",
                     "meta_municipio", "alunos"]],
}
tipos_das_colunas = consulta(f"""
    SELECT table_name, column_name, data_type
    FROM `{S}.INFORMATION_SCHEMA.COLUMNS`
    WHERE column_name IN ('ano', 'ano_referencia', 'id_municipio', 'sigla_uf', 'rede_codigo')
""")
tipos_por_chave = []
for chave, colunas in CHAVES_DE_JOIN.items():
    achadas = tipos_das_colunas.merge(
        pd.DataFrame(colunas, columns=["table_name", "column_name"]),
        on=["table_name", "column_name"])
    tipos_por_chave.append({
        "chave": chave,
        "colunas_conferidas": len(achadas),
        "tipos": ", ".join(sorted(achadas["data_type"].unique())),
        # violações = tipos diferentes a mais + colunas não encontradas
        "violacoes": max(achadas["data_type"].nunique() - 1, 0) + (len(colunas) - len(achadas)),
    })
print("\n=== TIPO DAS CHAVES NAS TABELAS QUE SE CRUZAM ===")
print(pd.DataFrame(tipos_por_chave).to_string(index=False))


# ETAPA 3.9.3 · Medições de qualidade da Silver: correspondência dos cruzamentos
# Mede, em cada cruzamento entre tabelas, a porcentagem de linhas que encontra par.
def taxa_de_correspondencia(descricao, esquerda, direita, pares, filtro="TRUE"):
    """Conta quantas linhas da tabela da esquerda acham par na da direita.

    pares: lista de (coluna_da_esquerda, coluna_da_direita).
    """
    colunas_direita = ", ".join(d for _, d in pares)
    condicao = " AND ".join(f"e.{a} = d.{b}" for a, b in pares)
    df = consulta(f"""
        WITH direita AS (SELECT DISTINCT {colunas_direita} FROM `{S}.{direita}`)
        SELECT COUNT(*) AS linhas,
               COUNTIF(d.{pares[0][1]} IS NOT NULL) AS com_par
        FROM `{S}.{esquerda}` AS e
        LEFT JOIN direita AS d ON {condicao}
        WHERE {filtro}
    """)
    linhas, com_par = int(df.linhas[0]), int(df.com_par[0])
    return {
        "cruzamento": descricao,
        "linhas": linhas,
        "com_par": com_par,
        "sem_par": linhas - com_par,
        "taxa_%": round(100 * com_par / linhas, 2) if linhas else None,
    }


print("\n=== TAXA DE CORRESPONDÊNCIA DOS CRUZAMENTOS ===")
df_cruzamentos = pd.DataFrame([
    taxa_de_correspondencia(
        "resultado_uf (com meta) -> meta_uf", "resultado_uf", "meta_uf",
        [("ano", "ano_referencia"), ("sigla_uf", "sigla_uf"), ("rede_codigo", "rede_codigo")],
        "e.comparavel_com_meta"),
    taxa_de_correspondencia(
        "meta_uf (anos da tabela uf) -> resultado_uf", "meta_uf", "resultado_uf",
        [("ano_referencia", "ano"), ("sigla_uf", "sigla_uf"), ("rede_codigo", "rede_codigo")],
        # EXISTS no lugar de IN: para na primeira linha que satisfaz a condição.
        f"EXISTS (SELECT 1 FROM `{S}.resultado_uf` AS anos WHERE anos.ano = e.ano_referencia)"),
    taxa_de_correspondencia(
        "resultado_municipio (com meta) -> meta_municipio", "resultado_municipio",
        "meta_municipio",
        [("ano", "ano_referencia"), ("id_municipio", "id_municipio"),
         ("rede_codigo", "rede_codigo")],
        "e.comparavel_com_meta"),
    taxa_de_correspondencia(
        "meta_municipio -> resultado_municipio", "meta_municipio", "resultado_municipio",
        [("ano_referencia", "ano"), ("id_municipio", "id_municipio"),
         ("rede_codigo", "rede_codigo")]),
    taxa_de_correspondencia(
        "alunos -> resultado_municipio", "alunos", "resultado_municipio",
        [("ano", "ano"), ("id_municipio", "id_municipio"), ("rede_codigo", "rede_codigo")]),
])
print(df_cruzamentos.to_string(index=False))


# ETAPA 3.9.4 · Medições de qualidade da Silver: coerência das taxas
# Confere se a taxa da tabela de meta é igual à da tabela de resultado.
# Tolerância de 0,06 porque, em 2023, as tabelas de meta trazem a taxa com uma casa
# decimal (56,2) e as de resultado com duas (56,19).
print("\n=== COERÊNCIA DAS TAXAS ENTRE TABELAS ===")
coerencia = []
for descricao, resultado, meta, chave in [
    ("resultado_uf x meta_uf", "resultado_uf", "meta_uf", "sigla_uf"),
    ("resultado_municipio x meta_municipio", "resultado_municipio", "meta_municipio",
     "id_municipio"),
]:
    df = consulta(f"""
        SELECT
            r.ano,
            COUNT(*) AS pares,
            COUNTIF(r.taxa_alfabetizacao IS NOT NULL
                    AND m.taxa_observada IS NOT NULL) AS com_as_duas_taxas,
            COUNTIF(ABS(r.taxa_alfabetizacao - m.taxa_observada) <= 0.06) AS taxas_iguais,
            ROUND(MAX(ABS(r.taxa_alfabetizacao - m.taxa_observada)), 2) AS maior_diferenca
        FROM `{S}.{resultado}` AS r
        INNER JOIN `{S}.{meta}` AS m
            ON r.ano = m.ano_referencia
           AND r.{chave} = m.{chave}
           AND r.rede_codigo = m.rede_codigo
        WHERE r.comparavel_com_meta
        GROUP BY r.ano
        ORDER BY r.ano
    """)
    df.insert(0, "comparacao", descricao)
    coerencia.append(df)
df_coerencia = pd.concat(coerencia, ignore_index=True)
print(df_coerencia.to_string(index=False))


# ETAPA 3.9.5 · Medições de qualidade da Silver: cobertura de estados
# Lista os estados sem resultado da rede pública, por ano.
print("\n=== ESTADOS SEM RESULTADO DA REDE PÚBLICA, POR ANO ===")
print(consulta(f"""
    WITH anos AS (SELECT DISTINCT ano FROM `{S}.resultado_uf`)
    SELECT
        a.ano,
        COUNT(*) AS estados_ausentes,
        STRING_AGG(t.sigla_uf, ', ' ORDER BY t.sigla_uf) AS siglas
    FROM anos AS a
    CROSS JOIN `{S}.territorio_uf` AS t
    LEFT JOIN `{S}.resultado_uf` AS r
        ON r.ano = a.ano
       AND r.sigla_uf = t.sigla_uf
       AND r.comparavel_com_meta
    WHERE r.sigla_uf IS NULL
    GROUP BY a.ano
    ORDER BY a.ano
""").to_string(index=False))


# ETAPA 3.10 · Investigações da Silver (só leitura)
# Só leitura. Investiga quatro pontos que apareceram nas verificações:
# A) 3 cidades têm taxa de 2023 diferente entre a tabela de resultado e a de meta;
# B) pares em que uma das duas taxas está vazia;
# C) 4.199 alunos não aparecem em resultado_municipio;
# D) cidades que têm resultado mas não têm meta, e cidades que têm meta mas não têm
# resultado.
CRUZA_MUNICIPIO = """
        ON r.ano = m.ano_referencia
       AND r.id_municipio = m.id_municipio
       AND r.rede_codigo = m.rede_codigo
"""

print("=== A) CIDADES COM TAXA DIFERENTE ENTRE AS DUAS TABELAS ===")
print(consulta(f"""
    SELECT
        r.ano,
        r.id_municipio,
        r.nome_municipio,
        r.sigla_uf,
        r.taxa_alfabetizacao AS taxa_na_tabela_de_resultado,
        m.taxa_observada     AS taxa_na_tabela_de_meta,
        ROUND(ABS(r.taxa_alfabetizacao - m.taxa_observada), 2) AS diferenca
    FROM `{S}.resultado_municipio` AS r
    INNER JOIN `{S}.meta_municipio` AS m
    {CRUZA_MUNICIPIO}
    WHERE r.comparavel_com_meta
      AND ABS(r.taxa_alfabetizacao - m.taxa_observada) > 0.06
    ORDER BY diferenca DESC
""").to_string(index=False))

print("\n=== B) PARES COM UMA DAS TAXAS VAZIA ===")
print(consulta(f"""
    SELECT
        r.ano,
        COUNT(*) AS pares,
        COUNTIF(r.taxa_alfabetizacao IS NULL) AS vazia_na_tabela_de_resultado,
        COUNTIF(m.taxa_observada IS NULL)     AS vazia_na_tabela_de_meta
    FROM `{S}.resultado_municipio` AS r
    INNER JOIN `{S}.meta_municipio` AS m
    {CRUZA_MUNICIPIO}
    WHERE r.comparavel_com_meta
    GROUP BY r.ano
    ORDER BY r.ano
""").to_string(index=False))

print("\n=== C) ALUNOS SEM LINHA EM resultado_municipio ===")
print(consulta(f"""
    WITH chaves AS (
        SELECT DISTINCT ano, id_municipio, rede_codigo
        FROM `{S}.resultado_municipio`
    ),
    sem_par AS (
        SELECT a.ano, a.rede_nome, a.id_municipio, a.foi_avaliado
        FROM `{S}.alunos` AS a
        LEFT JOIN chaves AS c
            ON a.ano = c.ano
           AND a.id_municipio = c.id_municipio
           AND a.rede_codigo = c.rede_codigo
        WHERE c.ano IS NULL
    ),
    por_cidade AS (
        SELECT ano, rede_nome, id_municipio,
               COUNT(*) AS alunos, COUNTIF(foi_avaliado) AS avaliados
        FROM sem_par
        GROUP BY ano, rede_nome, id_municipio
    )
    SELECT
        ano,
        rede_nome,
        COUNT(*)       AS cidades,
        SUM(alunos)    AS alunos,
        SUM(avaliados) AS avaliados,
        MAX(avaliados) AS maior_numero_de_avaliados_em_uma_cidade
    FROM por_cidade
    GROUP BY ano, rede_nome
    ORDER BY ano, rede_nome
""").to_string(index=False))

print("\n=== D1) CIDADES COM RESULTADO (rede municipal) E SEM LINHA NA TABELA DE META ===")
print(consulta(f"""
    SELECT
        r.ano,
        COUNT(*) AS cidades,
        COUNTIF(r.taxa_alfabetizacao IS NULL) AS sem_taxa
    FROM `{S}.resultado_municipio` AS r
    LEFT JOIN `{S}.meta_municipio` AS m
    {CRUZA_MUNICIPIO}
    WHERE r.comparavel_com_meta
      AND m.id_municipio IS NULL
    GROUP BY r.ano
    ORDER BY r.ano
""").to_string(index=False))

print("\n=== D2) CIDADES COM LINHA NA TABELA DE META E SEM RESULTADO ===")
print(consulta(f"""
    SELECT
        m.ano_referencia,
        COUNT(*) AS cidades,
        COUNTIF(m.taxa_observada IS NULL) AS sem_taxa_observada,
        COUNTIF(NOT m.tem_meta_2030)      AS sem_meta_2030
    FROM `{S}.meta_municipio` AS m
    LEFT JOIN `{S}.resultado_municipio` AS r
    {CRUZA_MUNICIPIO}
       AND r.comparavel_com_meta
    WHERE r.id_municipio IS NULL
    GROUP BY m.ano_referencia
    ORDER BY m.ano_referencia
""").to_string(index=False))


# ETAPA 3.11 · Validação de qualidade da Silver
# Junta as medições da Etapa 3.9 com as regras que não leem tabela (contagem de linhas e
# tipos das chaves) e com os limites de correspondência dos cruzamentos.
# Grava tudo em quality_reports.conferencias e só depois decide: regra bloqueante
# reprovada = a Gold não roda.
# Limites de correspondência (valor medido na 3.9 -> limite): UF 100% -> 100%,
# municípios 97,78% -> 95%, alunos 99,89% -> 99%.
resultados_silver = list(medicoes_silver)

# Limpar os dados não pode criar nem perder linhas: compara a Silver com a cópia da
# Bronze usada.
# A contagem da cópia vem da Etapa 3.0.6 e a da Silver, dos metadados da tabela.
PARES_BRONZE_SILVER = {
    "dicionario": "dicionario",
    "territorio_municipio": "diretorio_municipio",
    "resultado_uf": "uf",
    "resultado_municipio": "municipio",
    "alunos": "alunos",
    "meta_brasil": "meta_alfabetizacao_brasil",
    "meta_uf": "meta_alfabetizacao_uf",
    "meta_municipio": "meta_alfabetizacao_municipio",
}
for silver, tabela_bronze in PARES_BRONZE_SILVER.items():
    linhas_silver = client.get_table(f"{S}.{silver}").num_rows
    resultados_silver.append(conferencia_direta(
        "silver", silver, "linhas diferentes da Bronze", "Completude",
        abs(linhas_silver - LINHAS_DA_COPIA[tabela_bronze])))

# Tabelas de metas em formato longo: cada meta vira uma linha por ano (2024 a 2030).
for nivel in ["brasil", "uf", "municipio"]:
    linhas_longas = client.get_table(f"{S}.meta_{nivel}_trajetoria").num_rows
    linhas_largas = client.get_table(f"{S}.meta_{nivel}").num_rows
    resultados_silver.append(conferencia_direta(
        "silver", f"meta_{nivel}_trajetoria", "linhas diferentes de uma por ano de meta",
        "Consistência", abs(linhas_longas - len(ANOS_META) * linhas_largas)))

# Tipos das colunas usadas nos cruzamentos (medidos na 3.9).
for linha in tipos_por_chave:
    resultados_silver.append(conferencia_direta(
        "silver", "chaves dos joins", f"tipo diferente na chave {linha['chave']}",
        "Validade", linha["violacoes"]))

# Taxa de correspondência dos cruzamentos, com limite mínimo.
LIMITES_DOS_JOINS = [
    ("resultado_uf (com meta) -> meta_uf", "resultado_uf com meta_uf", 100),
    ("resultado_municipio (com meta) -> meta_municipio",
     "resultado_municipio com meta_municipio", 95),
    ("alunos -> resultado_municipio", "alunos com resultado_municipio", 99),
]
for cruzamento, nome, limite in LIMITES_DOS_JOINS:
    taxa = df_cruzamentos.loc[df_cruzamentos["cruzamento"] == cruzamento, "taxa_%"].iloc[0]
    resultados_silver.append(conferencia_direta(
        "cruzamento", nome, "linhas que acharam par", "Consistência", taxa,
        limite=limite, sentido="no minimo", unidade="%"))

# Grava antes de decidir, para a reprovação também ficar registrada.
df_validacao_silver = gravar_conferencias(resultados_silver)
mostrar_conferencias(df_validacao_silver)
print(f"\n{len(df_validacao_silver)} resultados gravados em {TABELA_CONFERENCIAS}")
validacao_de_qualidade(df_validacao_silver, "3.11 validação da Silver")
