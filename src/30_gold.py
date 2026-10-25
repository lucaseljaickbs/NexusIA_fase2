# Gerado a partir de notebooks/NexusAI_techfase2.ipynb (mesmo código, separado por camada).
# A execução é feita pelo notebook, na ordem das etapas: as células compartilham
# variáveis (client, PROJECT_ID e as funções das Etapas 1.1 e 1.2).

# ETAPA 4.0 · Preparação da Gold
# Prepara a Gold. gravar_gold() estima o custo (dry run) antes de cada gravação e
# registra linhas, MB lidos, duração e job_id.
# A célula também confere se as tabelas da Silver existem (get_table lê só metadados,
# sem custo) e guarda a composição das redes, usada nas Etapas 4.1 e 4.4.
import pandas as pd

# Colunas de controle gravadas em todas as tabelas da Gold.
# A coluna origem separa o dado oficial do simulado (Etapa 6).
METADADOS_GOLD = (
    "'oficial' AS origem,\n"
    "        CURRENT_TIMESTAMP() AS _gold_processed_at,\n"
    f"        '{PIPELINE_VERSION}' AS _pipeline_version"
)


# Quais redes da tabela de alunos formam cada rede da tabela de resultado.
# Vem das descrições do dicionário: 5 = "Pública (Estadual e Municipal)", 0 = "Total
# (Federal, Estadual, Municipal e Privada)", e assim por diante.
# Usada nas Etapas 4.1 (indicador_municipio) e 4.4 (validacao_taxa).
COMPOSICAO_DA_REDE = {
    "2": ["2"],
    "3": ["3"],
    "5": ["2", "3"],
    "6": ["1", "2", "3"],
    "0": ["1", "2", "3", "4"],
}
pares_rede = [
    (rede_resultado, rede_aluno)
    for rede_resultado, redes in COMPOSICAO_DA_REDE.items()
    for rede_aluno in redes
]
# Monta a lista de pares no formato que o SQL entende.
mapa_rede_sql = ",\n            ".join(
    (f"STRUCT('{r}' AS rede_resultado, '{a}' AS rede_aluno)" if i == 0
     else f"STRUCT('{r}', '{a}')")
    for i, (r, a) in enumerate(pares_rede)
)

# Números de cada tabela gravada na Gold.
metricas_gold = []


def gravar_gold(nome: str, sql: str) -> int:
    """Dry run + gravação em gold.<nome> (WRITE_TRUNCATE). Retorna as linhas."""
    mb_estimados = simular_custo(sql)
    destino = f"{G}.{nome}"
    config = bigquery.QueryJobConfig(
        destination=destino,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.query(sql, job_config=config)
    job.result()
    linhas = client.get_table(destino).num_rows
    # Entrada: as tabelas da Silver que o job leu e quantas linhas têm.
    entrada, lidas = linhas_de_entrada(job)
    metricas_gold.append({
        "tabela": f"gold.{nome}",
        "linhas": linhas,
        "linhas_entrada": entrada,
        "tabelas_lidas": lidas,
        "status": "OK",
        "mb_estimados": mb_estimados,
        "mb_lidos": round(job.total_bytes_processed / 1024**2, 2),
        "duracao_s": round((job.ended - job.started).total_seconds(), 2),
        "job_id": job.job_id,
    })
    print(f"[gold.{nome}] gravada: {linhas:,} linhas | simulação: {mb_estimados} MB")
    return linhas


# Pré-condição: as tabelas da Silver existem (get_table lê só metadados).
TABELAS_SILVER = [
    "territorio_municipio", "territorio_uf",
    "resultado_uf", "resultado_municipio", "alunos",
    "meta_brasil", "meta_uf", "meta_municipio",
    "meta_brasil_trajetoria", "meta_uf_trajetoria", "meta_municipio_trajetoria",
]
situacao_silver = []
for tabela in TABELAS_SILVER:
    info = client.get_table(f"{S}.{tabela}")
    situacao_silver.append({
        "tabela": f"silver.{tabela}",
        "linhas": info.num_rows,
        "tamanho_mb": round(info.num_bytes / 1024**2, 2),
    })

print("Ferramentas da Gold prontas.")
print("\nTabelas da Silver disponíveis:")
print(pd.DataFrame(situacao_silver).to_string(index=False))


# ETAPA 4.1 · Gold: indicador_municipio
# Taxa oficial por cidade, ano e rede, mais quantos alunos sustentam cada taxa. As redes
# 5 (Pública) e 0 (Total) são somadas a partir das redes dos alunos, conforme o
# dicionário.
# Também mede quantas linhas de resultado acharam os alunos correspondentes.
niveis_sql = ",\n        ".join(f"r.{c}" for c in COLUNAS_NIVEL)

sql_indicador_municipio = f"""
    WITH alunos_base AS (
        -- Uma única leitura da tabela de alunos (3,8 milhões de linhas), já resumida
        -- por ano, cidade e rede. Só três colunas são lidas.
        SELECT
            ano,
            id_municipio,
            rede_codigo,
            COUNT(*)              AS alunos,
            COUNTIF(foi_avaliado) AS avaliados
        FROM `{S}.alunos`
        GROUP BY ano, id_municipio, rede_codigo
    ),
    mapa_rede AS (
        SELECT rede_resultado, rede_aluno
        FROM UNNEST([
            {mapa_rede_sql}
        ])
    ),
    alunos_por_rede AS (
        -- Soma as redes dos alunos para formar Pública (5) e Total (0).
        SELECT
            a.ano,
            a.id_municipio,
            m.rede_resultado  AS rede_codigo,
            SUM(a.alunos)     AS alunos_total,
            SUM(a.avaliados)  AS alunos_avaliados
        FROM alunos_base AS a
        INNER JOIN mapa_rede AS m
            ON m.rede_aluno = a.rede_codigo
        GROUP BY a.ano, a.id_municipio, m.rede_resultado
    )
    SELECT
        r.ano,
        r.id_municipio,
        r.nome_municipio,
        r.sigla_uf,
        r.nome_uf,
        r.nome_regiao,
        r.rede_codigo,
        r.rede_nome,
        r.comparavel_com_meta,
        -- Taxa usada nas análises. Nas 3 cidades de 2023 em que a tabela de resultado e a de
        -- meta divergem (Investigação 2), vale a da meta, confirmada pelos alunos (Etapa 4.4).
        -- A taxa publicada na tabela de resultado fica em taxa_original_resultado.
        IF(ABS(r.taxa_alfabetizacao - m.taxa_observada) > 0.06,
           m.taxa_observada, r.taxa_alfabetizacao) AS taxa_alfabetizacao,
        r.taxa_alfabetizacao AS taxa_original_resultado,
        IF(ABS(r.taxa_alfabetizacao - m.taxa_observada) > 0.06,
           'tabela de meta (confirmada pelos alunos)', 'tabela de resultado') AS fonte_da_taxa,
        r.media_portugues,
        {niveis_sql},
        r.tem_distribuicao_niveis,
        p.alunos_total,
        p.alunos_avaliados,
        -- SAFE_DIVIDE devolve vazio (e não erro) se o total for zero.
        ROUND(100 * SAFE_DIVIDE(p.alunos_avaliados, p.alunos_total), 2) AS percentual_avaliados,
        p.id_municipio IS NOT NULL AS tem_dados_de_alunos,
        {METADADOS_GOLD}
    FROM `{S}.resultado_municipio` AS r
    LEFT JOIN `{S}.meta_municipio` AS m
        ON m.ano_referencia = r.ano
       AND m.id_municipio = r.id_municipio
       AND m.rede_codigo = r.rede_codigo
    LEFT JOIN alunos_por_rede AS p
        ON p.ano = r.ano
       AND p.id_municipio = r.id_municipio
       AND p.rede_codigo = r.rede_codigo
"""

linhas_gold = gravar_gold("indicador_municipio", sql_indicador_municipio)

# O cruzamento com os alunos não pode criar nem perder linhas: a Gold tem de ter as
# mesmas linhas da Silver.
# A diferença é guardada e vira regra bloqueante na validação da Etapa 4.7 (registrada
# em quality_reports.conferencias).
linhas_silver = client.get_table(f"{S}.resultado_municipio").num_rows
DIFERENCA_LINHAS_INDICADOR = abs(linhas_gold - linhas_silver)
print(f"Linhas: Gold {linhas_gold:,} | Silver {linhas_silver:,} | diferença {DIFERENCA_LINHAS_INDICADOR}")

# Taxa de correspondência do cruzamento:
# quantas linhas de resultado acharam os alunos correspondentes.
print("\nCruzamento resultado -> alunos, por ano e rede:")
print(consulta(f"""
    SELECT
        ano,
        rede_nome,
        COUNT(*)                     AS linhas,
        COUNTIF(tem_dados_de_alunos) AS com_alunos,
        ROUND(100 * COUNTIF(tem_dados_de_alunos) / COUNT(*), 2) AS taxa_pct
    FROM `{G}.indicador_municipio`
    GROUP BY ano, rede_codigo, rede_nome
    ORDER BY ano, rede_codigo
""").to_string(index=False))

print("\nExemplo: Rio de Janeiro e Costa Marques")
print(consulta(f"""
    SELECT ano, nome_municipio, sigla_uf, rede_nome, taxa_alfabetizacao,
           alunos_total, alunos_avaliados, percentual_avaliados
    FROM `{G}.indicador_municipio`
    WHERE id_municipio IN ('3304557', '1100080')
    ORDER BY id_municipio, ano, rede_codigo
""").to_string(index=False))


# ETAPA 4.2 · Gold: meta_vs_resultado
# Reúne município, UF e Brasil numa tabela só (a coluna nivel diz de qual nível é a linha).
# O FULL OUTER JOIN junta resultado e meta sem perder quem aparece só em um dos lados.
# Taxa usada: a da tabela de resultado; se não houver, a da tabela de meta. Nas 3 cidades
# de 2023 em que as duas divergem (Investigação 2, Etapa 3.10), vale a da meta, confirmada
# pelo recálculo a partir dos alunos (Etapa 4.4).
# situacao_meta tem 5 valores; "sem meta" é diferente de "não atingiu".
# meta_do_ano e meta_2030 vêm da trajetória de metas de 2024 a 2030, que termina em 80%.
# Colunas comuns aos três níveis (a ordem precisa ser a mesma no UNION ALL):
COLUNAS_COMPARACAO = (
    "nivel, ano, codigo_local, nome_local, sigla_uf, nome_regiao, rede_codigo, "
    "taxa_resultado, taxa_na_tabela_de_meta, meta_do_ano, meta_2030"
)

sql_meta_vs_resultado = f"""
    WITH resultado_municipio AS (
        -- Só as linhas que podem ser comparadas com a meta (rede Municipal).
        SELECT ano, id_municipio, nome_municipio, sigla_uf, nome_regiao,
               rede_codigo, taxa_alfabetizacao
        FROM `{S}.resultado_municipio`
        WHERE comparavel_com_meta
    ),
    resultado_uf AS (
        -- Só as linhas que podem ser comparadas com a meta (rede Pública).
        SELECT ano, sigla_uf, nome_uf, nome_regiao, rede_codigo, taxa_alfabetizacao
        FROM `{S}.resultado_uf`
        WHERE comparavel_com_meta
    ),
    nivel_municipio AS (
        SELECT
            'municipio' AS nivel,
            COALESCE(r.ano, m.ano_referencia)           AS ano,
            COALESCE(r.id_municipio, m.id_municipio)     AS codigo_local,
            COALESCE(r.nome_municipio, m.nome_municipio) AS nome_local,
            COALESCE(r.sigla_uf, m.sigla_uf)             AS sigla_uf,
            COALESCE(r.nome_regiao, m.nome_regiao)       AS nome_regiao,
            COALESCE(r.rede_codigo, m.rede_codigo)       AS rede_codigo,
            r.taxa_alfabetizacao      AS taxa_resultado,
            m.taxa_observada          AS taxa_na_tabela_de_meta,
            m.meta_do_ano,
            m.meta_alfabetizacao_2030 AS meta_2030
        FROM resultado_municipio AS r
        FULL OUTER JOIN `{S}.meta_municipio` AS m
            ON r.ano = m.ano_referencia
           AND r.id_municipio = m.id_municipio
           AND r.rede_codigo = m.rede_codigo
    ),
    nivel_uf AS (
        SELECT
            'uf' AS nivel,
            COALESCE(r.ano, m.ano_referencia)      AS ano,
            COALESCE(r.sigla_uf, m.sigla_uf)       AS codigo_local,
            COALESCE(r.nome_uf, m.nome_uf)         AS nome_local,
            COALESCE(r.sigla_uf, m.sigla_uf)       AS sigla_uf,
            COALESCE(r.nome_regiao, m.nome_regiao) AS nome_regiao,
            COALESCE(r.rede_codigo, m.rede_codigo) AS rede_codigo,
            r.taxa_alfabetizacao      AS taxa_resultado,
            m.taxa_observada          AS taxa_na_tabela_de_meta,
            m.meta_do_ano,
            m.meta_alfabetizacao_2030 AS meta_2030
        FROM resultado_uf AS r
        FULL OUTER JOIN `{S}.meta_uf` AS m
            ON r.ano = m.ano_referencia
           AND r.sigla_uf = m.sigla_uf
           AND r.rede_codigo = m.rede_codigo
    ),
    nivel_brasil AS (
        -- Não existe tabela de resultado para o país: a taxa vem da tabela de meta.
        SELECT
            'brasil' AS nivel,
            m.ano_referencia AS ano,
            'BR'             AS codigo_local,
            'Brasil'         AS nome_local,
            CAST(NULL AS STRING) AS sigla_uf,
            CAST(NULL AS STRING) AS nome_regiao,
            m.rede_codigo,
            CAST(NULL AS FLOAT64)     AS taxa_resultado,
            m.taxa_observada          AS taxa_na_tabela_de_meta,
            m.meta_do_ano,
            m.meta_alfabetizacao_2030 AS meta_2030
        FROM `{S}.meta_brasil` AS m
    ),
    unidos AS (
        SELECT {COLUNAS_COMPARACAO} FROM nivel_municipio
        UNION ALL
        SELECT {COLUNAS_COMPARACAO} FROM nivel_uf
        UNION ALL
        SELECT {COLUNAS_COMPARACAO} FROM nivel_brasil
    ),
    comparado AS (
        SELECT
            {COLUNAS_COMPARACAO},
            -- Tolerância de 0,06: em 2023 a tabela de meta traz só uma casa decimal.
            IFNULL(ABS(taxa_resultado - taxa_na_tabela_de_meta) > 0.06, FALSE) AS taxas_divergem
        FROM unidos
    ),
    calculado AS (
        SELECT
            nivel, ano, codigo_local, nome_local, sigla_uf, nome_regiao, rede_codigo,
            -- Escolha da taxa oficial:
            -- 1) se as duas tabelas divergem (3 cidades em 2023), usa a da tabela de
            --    meta (ver a conclusão da Investigação 2, depois da Etapa 4.4);
            -- 2) nos demais casos, usa a da tabela de resultado;
            -- 3) se não há resultado, usa a da tabela de meta.
            CASE
                WHEN taxas_divergem THEN taxa_na_tabela_de_meta
                ELSE COALESCE(taxa_resultado, taxa_na_tabela_de_meta)
            END AS taxa_alfabetizacao,
            CASE
                WHEN taxas_divergem THEN 'tabela de meta (confirmada pelos alunos)'
                WHEN taxa_resultado IS NOT NULL THEN 'tabela de resultado'
                WHEN taxa_na_tabela_de_meta IS NOT NULL THEN 'tabela de meta'
            END AS fonte_da_taxa,
            taxa_na_tabela_de_meta,
            taxas_divergem,
            meta_do_ano,
            meta_2030
        FROM comparado
    )
    SELECT
        nivel,
        ano,
        codigo_local,
        nome_local,
        sigla_uf,
        nome_regiao,
        rede_codigo,
        taxa_alfabetizacao,
        fonte_da_taxa,
        taxa_na_tabela_de_meta,
        taxas_divergem,
        meta_do_ano,
        meta_2030,
        -- "Sem meta" (não existe meta) é diferente de "não atingiu" (existe meta e a
        -- taxa ficou abaixo).
        CASE
            WHEN taxa_alfabetizacao IS NULL AND meta_do_ano IS NULL THEN 'sem resultado e sem meta'
            WHEN taxa_alfabetizacao IS NULL THEN 'sem resultado'
            WHEN meta_do_ano IS NULL THEN 'sem meta'
            WHEN taxa_alfabetizacao >= meta_do_ano THEN 'atingiu'
            ELSE 'nao atingiu'
        END AS situacao_meta,
        -- Positivo = acima da meta. Vazio quando falta a taxa ou a meta.
        ROUND(taxa_alfabetizacao - meta_do_ano, 2) AS distancia_meta_do_ano,
        ROUND(taxa_alfabetizacao - meta_2030, 2)   AS distancia_meta_2030,
        {METADADOS_GOLD}
    FROM calculado
"""

gravar_gold("meta_vs_resultado", sql_meta_vs_resultado)

# Conta as linhas por nível. A regra de unicidade (cada lugar uma vez por ano) é
# conferida na validação da Etapa 4.7.
print(consulta(f"""
    SELECT
        COUNT(*)                     AS linhas,
        COUNTIF(nivel = 'municipio') AS linhas_municipio,
        COUNTIF(nivel = 'uf')        AS linhas_uf,
        COUNTIF(nivel = 'brasil')    AS linhas_brasil
    FROM `{G}.meta_vs_resultado`
""").to_string(index=False))

print("\nSituação por nível e ano:")
print(consulta(f"""
    SELECT nivel, ano, situacao_meta, COUNT(*) AS lugares
    FROM `{G}.meta_vs_resultado`
    GROUP BY nivel, ano, situacao_meta
    ORDER BY nivel, ano, situacao_meta
""").to_string(index=False))

print("\nExemplos:")
print(consulta(f"""
    SELECT nivel, ano, nome_local, taxa_alfabetizacao, fonte_da_taxa, meta_do_ano,
           situacao_meta, distancia_meta_do_ano, distancia_meta_2030
    FROM `{G}.meta_vs_resultado`
    WHERE codigo_local IN ('3304557', '1100080', 'AM', 'CE', 'RR', 'BR')
    ORDER BY nivel, nome_local, ano
""").to_string(index=False))

print("\nLugares em que as duas tabelas oficiais divergem (taxa usada = a confirmada):")
print(consulta(f"""
    SELECT nivel, ano, nome_local, sigla_uf, taxa_alfabetizacao, fonte_da_taxa
    FROM `{G}.meta_vs_resultado`
    WHERE taxas_divergem
    ORDER BY nivel, nome_local
""").to_string(index=False))


# ETAPA 4.3 · Gold: evolucao_temporal
# Uma linha por lugar e ano (2023 a 2030), com: taxa observada, meta do ano, variação em
# relação ao ano anterior (função LAG) e distância até a meta de 2030.
# Meta do ano: a publicada no próprio ano; se não houver, a mais recente.
# cenario_ritmo_necessario_por_ano é um cenário (quanto precisaria crescer por ano), não
# uma previsão.
sql_evolucao_temporal = f"""
    WITH trajetorias AS (
        SELECT 'municipio' AS nivel, id_municipio AS codigo_local,
               ano_referencia, ano_meta, meta
        FROM `{S}.meta_municipio_trajetoria`
        UNION ALL
        SELECT 'uf', sigla_uf, ano_referencia, ano_meta, meta
        FROM `{S}.meta_uf_trajetoria`
        UNION ALL
        SELECT 'brasil', 'BR', ano_referencia, ano_meta, meta
        FROM `{S}.meta_brasil_trajetoria`
    ),
    metas AS (
        -- Uma meta por lugar e ano: a publicada no próprio ano; se não houver, a
        -- publicação mais recente.
        SELECT
            nivel,
            codigo_local,
            ano_meta       AS ano,
            meta           AS meta_do_ano,
            ano_referencia AS ano_de_publicacao_da_meta
        FROM trajetorias
        WHERE meta IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY nivel, codigo_local, ano_meta
            ORDER BY IF(ano_referencia = ano_meta, 0, 1), ano_referencia DESC
        ) = 1
    ),
    taxas AS (
        -- A taxa já unificada na Etapa 4.2 (resultado ou, na falta dele, meta).
        SELECT nivel, codigo_local, ano, taxa_alfabetizacao, fonte_da_taxa
        FROM `{G}.meta_vs_resultado`
        WHERE taxa_alfabetizacao IS NOT NULL
    ),
    lugares AS (
        -- Nome, estado e região de cada lugar, uma linha por lugar.
        SELECT
            nivel,
            codigo_local,
            ANY_VALUE(nome_local)  AS nome_local,
            ANY_VALUE(sigla_uf)    AS sigla_uf,
            ANY_VALUE(nome_regiao) AS nome_regiao,
            ANY_VALUE(rede_codigo) AS rede_codigo
        FROM `{G}.meta_vs_resultado`
        GROUP BY nivel, codigo_local
    ),
    linha_do_tempo AS (
        -- FULL OUTER JOIN: mantém os anos que só têm taxa e os que só têm meta.
        SELECT
            COALESCE(t.nivel, m.nivel)               AS nivel,
            COALESCE(t.codigo_local, m.codigo_local) AS codigo_local,
            COALESCE(t.ano, m.ano)                   AS ano,
            t.taxa_alfabetizacao,
            t.fonte_da_taxa,
            m.meta_do_ano,
            m.ano_de_publicacao_da_meta
        FROM taxas AS t
        FULL OUTER JOIN metas AS m
            ON t.nivel = m.nivel
           AND t.codigo_local = m.codigo_local
           AND t.ano = m.ano
    ),
    com_janelas AS (
        SELECT
            nivel,
            codigo_local,
            ano,
            taxa_alfabetizacao,
            fonte_da_taxa,
            meta_do_ano,
            ano_de_publicacao_da_meta,
            -- Taxa do ano anterior. Só vale se a linha anterior for do ano
            -- imediatamente anterior (sem pular anos).
            IF(LAG(ano) OVER historico = ano - 1,
               LAG(taxa_alfabetizacao) OVER historico, NULL) AS taxa_ano_anterior,
            -- Meta de 2030 repetida em todas as linhas do lugar.
            MAX(IF(ano = 2030, meta_do_ano, NULL))
                OVER (PARTITION BY nivel, codigo_local) AS meta_2030
        FROM linha_do_tempo
        WINDOW historico AS (PARTITION BY nivel, codigo_local ORDER BY ano)
    )
    SELECT
        c.nivel,
        c.codigo_local,
        g.nome_local,
        g.sigla_uf,
        g.nome_regiao,
        g.rede_codigo,
        c.ano,
        IF(c.taxa_alfabetizacao IS NOT NULL, 'observado', 'somente meta') AS tipo_de_linha,
        c.taxa_alfabetizacao,
        c.fonte_da_taxa,
        c.meta_do_ano,
        c.ano_de_publicacao_da_meta,
        c.taxa_ano_anterior,
        ROUND(c.taxa_alfabetizacao - c.taxa_ano_anterior, 2) AS variacao_anual,
        c.meta_2030,
        -- Positivo = ainda falta. Negativo = já passou da meta de 2030.
        ROUND(c.meta_2030 - c.taxa_alfabetizacao, 2) AS falta_para_meta_2030,
        2030 - c.ano AS anos_ate_2030,
        -- Cenário, não previsão: quantos pontos por ano seriam necessários para chegar
        -- à meta de 2030, partindo da taxa deste ano.
        CASE
            WHEN c.taxa_alfabetizacao IS NULL OR c.meta_2030 IS NULL OR c.ano >= 2030 THEN NULL
            WHEN c.taxa_alfabetizacao >= c.meta_2030 THEN 0
            ELSE ROUND((c.meta_2030 - c.taxa_alfabetizacao) / (2030 - c.ano), 2)
        END AS cenario_ritmo_necessario_por_ano,
        {METADADOS_GOLD}
    FROM com_janelas AS c
    LEFT JOIN lugares AS g
        ON g.nivel = c.nivel
       AND g.codigo_local = c.codigo_local
"""

gravar_gold("evolucao_temporal", sql_evolucao_temporal)

# As regras de unicidade da chave e de nome preenchido são conferidas na validação da
# Etapa 4.7. Aqui só aparece o resumo da tabela.
print(consulta(f"""
    SELECT COUNT(*) AS linhas, MIN(ano) AS primeiro_ano, MAX(ano) AS ultimo_ano
    FROM `{G}.evolucao_temporal`
""").to_string(index=False))

print("\nLinhas por nível e tipo:")
print(consulta(f"""
    SELECT nivel, tipo_de_linha, COUNT(*) AS linhas, COUNT(DISTINCT codigo_local) AS lugares
    FROM `{G}.evolucao_temporal`
    GROUP BY nivel, tipo_de_linha
    ORDER BY nivel, tipo_de_linha
""").to_string(index=False))

print("\nQuem melhorou e quem piorou de um ano para o outro:")
print(consulta(f"""
    SELECT
        nivel,
        ano,
        COUNTIF(variacao_anual > 0) AS melhoraram,
        COUNTIF(variacao_anual < 0) AS pioraram,
        COUNTIF(variacao_anual = 0) AS iguais,
        ROUND({percentil_sql('variacao_anual', 0.5)}, 2) AS variacao_do_meio
    FROM `{G}.evolucao_temporal`
    WHERE variacao_anual IS NOT NULL
    GROUP BY nivel, ano
    ORDER BY nivel, ano
""").to_string(index=False))

print("\nExemplo: a linha do tempo do Brasil")
print(consulta(f"""
    SELECT ano, tipo_de_linha, taxa_alfabetizacao, meta_do_ano, variacao_anual,
           falta_para_meta_2030, cenario_ritmo_necessario_por_ano
    FROM `{G}.evolucao_temporal`
    WHERE nivel = 'brasil'
    ORDER BY ano
""").to_string(index=False))

print("\nExemplo: a linha do tempo do Rio de Janeiro")
print(consulta(f"""
    SELECT ano, tipo_de_linha, taxa_alfabetizacao, meta_do_ano, variacao_anual,
           falta_para_meta_2030, cenario_ritmo_necessario_por_ano
    FROM `{G}.evolucao_temporal`
    WHERE nivel = 'municipio' AND codigo_local = '3304557'
    ORDER BY ano
""").to_string(index=False))


# ETAPA 4.4 · Gold: validacao_taxa
# Recalcula a taxa a partir dos alunos avaliados, usando o peso de cada aluno
# (peso_aluno), para os três níveis, e compara com a taxa publicada.
# Também calcula a taxa sem peso, só para mostrar o efeito do peso. Diferença aceita:
# até 0,05 ponto percentual (arredondamento).

# Diferença máxima, em pontos percentuais, para considerar que as taxas conferem. A taxa
# oficial é publicada com duas casas decimais (uma, em algumas tabelas), então
# diferenças até 0,05 são só arredondamento.
TOLERANCIA_VALIDACAO = 0.05

# A composição das redes (COMPOSICAO_DA_REDE, mapa_rede_sql) vem da Etapa 4.0.

sql_validacao_taxa = f"""
    WITH alunos_base AS (
        -- Uma única leitura da tabela de alunos, só com os avaliados, já resumida por
        -- ano, cidade, estado e rede.
        SELECT
            ano,
            id_municipio,
            sigla_uf,
            rede_codigo,
            COUNT(*)                                  AS avaliados,
            COUNTIF(esta_alfabetizado)                AS alfabetizados,
            SUM(peso_aluno)                           AS soma_pesos,
            SUM(IF(esta_alfabetizado, peso_aluno, 0)) AS soma_pesos_alfabetizados
        FROM `{S}.alunos`
        WHERE foi_avaliado
        GROUP BY ano, id_municipio, sigla_uf, rede_codigo
    ),
    mapa_rede AS (
        SELECT rede_resultado, rede_aluno
        FROM UNNEST([
            {mapa_rede_sql}
        ])
    ),
    alunos_por_rede AS (
        -- Soma as redes dos alunos para formar Pública (5) e Total (0).
        SELECT
            a.ano,
            a.id_municipio,
            a.sigla_uf,
            m.rede_resultado AS rede_codigo,
            a.avaliados,
            a.alfabetizados,
            a.soma_pesos,
            a.soma_pesos_alfabetizados
        FROM alunos_base AS a
        INNER JOIN mapa_rede AS m
            ON m.rede_aluno = a.rede_codigo
    ),
    recalculo AS (
        SELECT 'municipio' AS nivel, ano, id_municipio AS codigo_local, rede_codigo,
               SUM(avaliados) AS avaliados, SUM(alfabetizados) AS alfabetizados,
               SUM(soma_pesos) AS soma_pesos,
               SUM(soma_pesos_alfabetizados) AS soma_pesos_alfabetizados
        FROM alunos_por_rede
        GROUP BY ano, id_municipio, rede_codigo
        UNION ALL
        SELECT 'uf', ano, sigla_uf, rede_codigo,
               SUM(avaliados), SUM(alfabetizados), SUM(soma_pesos), SUM(soma_pesos_alfabetizados)
        FROM alunos_por_rede
        GROUP BY ano, sigla_uf, rede_codigo
        UNION ALL
        SELECT 'brasil', ano, 'BR', rede_codigo,
               SUM(avaliados), SUM(alfabetizados), SUM(soma_pesos), SUM(soma_pesos_alfabetizados)
        FROM alunos_por_rede
        GROUP BY ano, rede_codigo
    ),
    oficial AS (
        SELECT 'municipio' AS nivel, ano, id_municipio AS codigo_local,
               nome_municipio AS nome_local, sigla_uf, rede_codigo,
               taxa_alfabetizacao AS taxa_oficial
        FROM `{S}.resultado_municipio`
        UNION ALL
        SELECT 'uf', ano, sigla_uf, nome_uf, sigla_uf, rede_codigo, taxa_alfabetizacao
        FROM `{S}.resultado_uf`
        UNION ALL
        SELECT 'brasil', ano_referencia, 'BR', 'Brasil', CAST(NULL AS STRING),
               rede_codigo, taxa_observada
        FROM `{S}.meta_brasil`
    ),
    comparado AS (
        SELECT
            o.nivel,
            o.ano,
            o.codigo_local,
            o.nome_local,
            o.sigla_uf,
            o.rede_codigo,
            o.taxa_oficial,
            -- Com peso: é a conta que deve reproduzir a taxa oficial.
            ROUND(100 * SAFE_DIVIDE(r.soma_pesos_alfabetizados, r.soma_pesos), 2) AS taxa_recalculada,
            -- Sem peso: contagem simples, só para comparação.
            ROUND(100 * SAFE_DIVIDE(r.alfabetizados, r.avaliados), 2) AS taxa_recalculada_sem_peso,
            r.avaliados AS alunos_avaliados,
            r.codigo_local IS NOT NULL AS tem_alunos_na_base
        FROM oficial AS o
        LEFT JOIN recalculo AS r
            ON r.nivel = o.nivel
           AND r.ano = o.ano
           AND r.codigo_local = o.codigo_local
           AND r.rede_codigo = o.rede_codigo
    )
    SELECT
        nivel,
        ano,
        codigo_local,
        nome_local,
        sigla_uf,
        rede_codigo,
        taxa_oficial,
        taxa_recalculada,
        taxa_recalculada_sem_peso,
        ROUND(taxa_recalculada - taxa_oficial, 2) AS diferenca,
        alunos_avaliados,
        CASE
            WHEN taxa_oficial IS NULL THEN 'sem taxa oficial'
            WHEN NOT tem_alunos_na_base THEN 'sem alunos na base'
            WHEN ABS(taxa_recalculada - taxa_oficial) <= {TOLERANCIA_VALIDACAO} THEN 'confere'
            ELSE 'diverge'
        END AS situacao_validacao,
        {METADADOS_GOLD}
    FROM comparado
"""

gravar_gold("validacao_taxa", sql_validacao_taxa)

print("\n=== A) RESUMO POR NIVEL, ANO E REDE ===")
print(consulta(f"""
    SELECT
        nivel,
        ano,
        rede_codigo AS rede,
        COUNT(*) AS linhas,
        COUNTIF(situacao_validacao = 'confere')            AS confere,
        COUNTIF(situacao_validacao = 'diverge')            AS diverge,
        COUNTIF(situacao_validacao = 'sem alunos na base') AS sem_alunos,
        ROUND({percentil_sql('ABS(diferenca)', 0.5)}, 2) AS diferenca_do_meio,
        ROUND(MAX(ABS(diferenca)), 2) AS maior_diferenca
    FROM `{G}.validacao_taxa`
    GROUP BY nivel, ano, rede_codigo
    ORDER BY nivel, ano, rede_codigo
""").to_string(index=False))

print("\n=== B) TAMANHO DAS DIFERENCAS (linhas com as duas taxas) ===")
print(consulta(f"""
    SELECT
        nivel,
        COUNTIF(ABS(diferenca) <= 0.01)                         AS ate_0_01,
        COUNTIF(ABS(diferenca) > 0.01 AND ABS(diferenca) <= 0.05) AS ate_0_05,
        COUNTIF(ABS(diferenca) > 0.05 AND ABS(diferenca) <= 0.5)  AS ate_0_5,
        COUNTIF(ABS(diferenca) > 0.5 AND ABS(diferenca) <= 1)     AS ate_1,
        COUNTIF(ABS(diferenca) > 1)                             AS acima_de_1
    FROM `{G}.validacao_taxa`
    WHERE diferenca IS NOT NULL
    GROUP BY nivel
    ORDER BY nivel
""").to_string(index=False))

print("\n=== C) BRASIL E ESTADOS DE EXEMPLO (rede Publica) ===")
print(consulta(f"""
    SELECT nivel, ano, nome_local, taxa_oficial, taxa_recalculada,
           taxa_recalculada_sem_peso, diferenca, alunos_avaliados, situacao_validacao
    FROM `{G}.validacao_taxa`
    WHERE rede_codigo = '5' AND codigo_local IN ('BR', 'AM', 'CE', 'RJ', 'SP')
    ORDER BY nivel, nome_local, ano
""").to_string(index=False))

print("\n=== D) CIDADES DE EXEMPLO (rede Municipal) ===")
print(consulta(f"""
    SELECT ano, nome_local, taxa_oficial, taxa_recalculada,
           taxa_recalculada_sem_peso, diferenca, alunos_avaliados, situacao_validacao
    FROM `{G}.validacao_taxa`
    WHERE nivel = 'municipio' AND rede_codigo = '3'
      AND codigo_local IN ('3304557', '1100080')
    ORDER BY nome_local, ano
""").to_string(index=False))

print("\n=== E) AS TRES CIDADES COM TAXAS DIVERGENTES ENTRE TABELAS OFICIAIS ===")
# Cada taxa vem da sua tabela de origem na Silver. gold.meta_vs_resultado nao serve aqui:
# nessas 3 cidades ela ja traz o valor da tabela de meta no lugar do de resultado.
print(consulta(f"""
    SELECT
        v.ano,
        v.nome_local,
        v.sigla_uf,
        r.taxa_alfabetizacao   AS na_tabela_de_resultado,
        m.taxa_observada       AS na_tabela_de_meta,
        v.taxa_recalculada     AS recalculada_dos_alunos,
        v.alunos_avaliados
    FROM `{G}.validacao_taxa` AS v
    INNER JOIN `{S}.resultado_municipio` AS r
        ON r.ano = v.ano
       AND r.id_municipio = v.codigo_local
       AND r.rede_codigo = v.rede_codigo
    INNER JOIN `{S}.meta_municipio` AS m
        ON m.ano_referencia = v.ano
       AND m.id_municipio = v.codigo_local
       AND m.rede_codigo = v.rede_codigo
    WHERE v.nivel = 'municipio'
      AND ABS(r.taxa_alfabetizacao - m.taxa_observada) > 0.06
    ORDER BY v.nome_local
""").to_string(index=False))

print("\n=== F) 2023, REDE MUNICIPAL: CIDADES COM RESULTADO E SEM ALUNOS NA BASE, POR ESTADO ===")
print(consulta(f"""
    SELECT
        sigla_uf,
        COUNT(*) AS cidades_com_resultado,
        COUNTIF(situacao_validacao = 'sem alunos na base') AS sem_alunos_na_base
    FROM `{G}.validacao_taxa`
    WHERE nivel = 'municipio' AND ano = 2023 AND rede_codigo = '3'
    GROUP BY sigla_uf
    HAVING COUNTIF(situacao_validacao = 'sem alunos na base') > 0
    ORDER BY sem_alunos_na_base DESC
""").to_string(index=False))


# ETAPA 4.5 · Gold: desigualdade_regional
# Compara as cidades (rede Municipal) dentro de cada região, de cada UF e do Brasil:
# taxa mínima, quartis, máxima, amplitude e porcentagem de cidades que atingiram a meta.
# A média entre cidades não é o indicador oficial do lugar (por isso a coluna se chama
# media_simples_entre_cidades).
def resumo_por(agrupamento: str, grupo_sql: str, regiao_sql: str, agrupar_por: str) -> str:
    """Monta o resumo das cidades para um tipo de agrupamento (região, UF ou Brasil)."""
    return f"""
        SELECT
            '{agrupamento}' AS agrupamento,
            {grupo_sql}     AS grupo,
            {regiao_sql}    AS nome_regiao,
            ano,
            COUNT(*)                        AS cidades,
            COUNTIF(taxa IS NOT NULL)       AS cidades_com_taxa,
            SUM(alunos_avaliados)           AS alunos_avaliados,
            MIN(taxa)                       AS taxa_minima,
            -- Quartis exatos (25%, 50% e 75%) das taxas das cidades, sem contar vazios.
            {percentil_sql('taxa', 0.25)} AS taxa_quarto_de_baixo,
            {percentil_sql('taxa', 0.50)} AS taxa_do_meio,
            {percentil_sql('taxa', 0.75)} AS taxa_quarto_de_cima,
            MAX(taxa)                       AS taxa_maxima,
            ROUND(AVG(taxa), 2)             AS media_simples_entre_cidades,
            COUNTIF(situacao_meta = 'atingiu')     AS cidades_atingiram,
            COUNTIF(situacao_meta = 'nao atingiu') AS cidades_nao_atingiram,
            COUNTIF(situacao_meta = 'sem meta')    AS cidades_sem_meta
        FROM cidades
        GROUP BY {agrupar_por}
    """


sql_desigualdade_regional = f"""
    WITH cidades AS (
        SELECT
            m.ano,
            m.codigo_local AS id_municipio,
            m.sigla_uf,
            m.nome_regiao,
            -- Nas 3 cidades em que as tabelas oficiais divergem, vale o valor
            -- confirmado pelo recálculo a partir dos alunos (Etapa 4.4).
            IF(m.taxas_divergem, m.taxa_na_tabela_de_meta, m.taxa_alfabetizacao) AS taxa,
            m.situacao_meta,
            i.alunos_avaliados
        FROM `{G}.meta_vs_resultado` AS m
        LEFT JOIN `{G}.indicador_municipio` AS i
            ON i.ano = m.ano
           AND i.id_municipio = m.codigo_local
           AND i.rede_codigo = m.rede_codigo
        WHERE m.nivel = 'municipio'
    ),
    resumos AS (
        {resumo_por('regiao', 'nome_regiao', 'nome_regiao', 'nome_regiao, ano')}
        UNION ALL
        {resumo_por('uf', 'sigla_uf', 'ANY_VALUE(nome_regiao)', 'sigla_uf, ano')}
        UNION ALL
        {resumo_por('brasil', "'Brasil'", 'CAST(NULL AS STRING)', 'ano')}
    )
    SELECT
        agrupamento,
        grupo,
        nome_regiao,
        ano,
        cidades,
        cidades_com_taxa,
        alunos_avaliados,
        taxa_minima,
        taxa_quarto_de_baixo,
        taxa_do_meio,
        taxa_quarto_de_cima,
        taxa_maxima,
        -- Distância entre a cidade com a maior taxa e a com a menor.
        ROUND(taxa_maxima - taxa_minima, 2) AS distancia_entre_extremos,
        media_simples_entre_cidades,
        cidades_atingiram,
        cidades_nao_atingiram,
        cidades_sem_meta,
        -- Só entre as cidades que têm meta e resultado para comparar.
        ROUND(100 * SAFE_DIVIDE(cidades_atingiram,
                                cidades_atingiram + cidades_nao_atingiram), 2) AS percentual_que_atingiu,
        -- 1 = o grupo com a maior mediana naquele ano.
        RANK() OVER (PARTITION BY agrupamento, ano ORDER BY taxa_do_meio DESC) AS posicao_pela_taxa_do_meio,
        {METADADOS_GOLD}
    FROM resumos
"""

gravar_gold("desigualdade_regional", sql_desigualdade_regional)

# Em cada ano, os três agrupamentos têm de somar o mesmo número de cidades (nenhuma
# perdida ou repetida). A regra bloqueante está na validação da Etapa 4.7; aqui só se
# mostra a soma por ano.
conferencia = consulta(f"""
    SELECT
        ano,
        SUM(IF(agrupamento = 'regiao', cidades, 0)) AS cidades_por_regiao,
        SUM(IF(agrupamento = 'uf', cidades, 0))     AS cidades_por_uf,
        SUM(IF(agrupamento = 'brasil', cidades, 0)) AS cidades_brasil
    FROM `{G}.desigualdade_regional`
    GROUP BY ano
    ORDER BY ano
""")
print(conferencia.to_string(index=False))

print("\n=== REGIOES EM 2024 (rede municipal) ===")
print(consulta(f"""
    SELECT grupo AS regiao, cidades_com_taxa, taxa_minima, taxa_quarto_de_baixo,
           taxa_do_meio, taxa_quarto_de_cima, taxa_maxima, percentual_que_atingiu
    FROM `{G}.desigualdade_regional`
    WHERE agrupamento = 'regiao' AND ano = 2024
    ORDER BY taxa_do_meio DESC
""").to_string(index=False))

print("\n=== ESTADOS EM 2024, DO MAIOR PARA O MENOR PERCENTUAL DE CIDADES QUE ATINGIRAM A META ===")
print(consulta(f"""
    SELECT grupo AS uf, nome_regiao, cidades_com_taxa, taxa_do_meio,
           distancia_entre_extremos, cidades_atingiram, cidades_nao_atingiram,
           percentual_que_atingiu
    FROM `{G}.desigualdade_regional`
    WHERE agrupamento = 'uf' AND ano = 2024
    ORDER BY percentual_que_atingiu DESC
""").to_string(index=False))

print("\n=== BRASIL: RESUMO DAS CIDADES POR ANO ===")
print(consulta(f"""
    SELECT ano, cidades_com_taxa, taxa_minima, taxa_do_meio, taxa_maxima,
           media_simples_entre_cidades, percentual_que_atingiu
    FROM `{G}.desigualdade_regional`
    WHERE agrupamento = 'brasil'
    ORDER BY ano
""").to_string(index=False))


# ETAPA 4.6 · Gold: base_modelagem (parte 1)
# Cria a base para a Fase 3 (modelagem): uma linha por cidade, rede Municipal.
# As colunas de entrada usam só o que já se sabia antes de 2024. As colunas-alvo
# dependem do resultado de 2024 e têm o prefixo "alvo_".
# PAPEL_DAS_COLUNAS declara o papel de cada coluna para orientar a seleção das
# entradas e dos alvos na Fase 3.

# Papel de cada coluna da tabela (identificação, entrada, alvo ou controle). A seleção
# das entradas e sua disponibilidade temporal precisam ser validadas antes do treino.
PAPEL_DAS_COLUNAS = {
    "id_municipio": "identificacao",
    "nome_municipio": "identificacao",
    "sigla_uf": "entrada",
    "nome_regiao": "entrada",
    "eh_capital": "entrada",
    "taxa_2023": "entrada",
    "media_portugues_2023": "entrada",
    "alunos_total_2023": "entrada",
    "alunos_avaliados_2023": "entrada",
    "percentual_avaliados_2023": "entrada",
    "meta_2024": "entrada",
    "meta_2030": "entrada",
    "distancia_taxa_2023_para_meta_2024": "entrada",
    "alvo_taxa_2024": "alvo",
    "alvo_variacao_2024": "alvo",
    "alvo_atingiu_meta_2024": "alvo",
    "pronta_para_treino": "controle",
    "origem": "controle",
    "_gold_processed_at": "controle",
    "_pipeline_version": "controle",
}

sql_base_modelagem = f"""
    WITH indicador AS (
        -- Só a rede Municipal, que é a que tem meta nas cidades.
        SELECT ano, id_municipio, nome_municipio, sigla_uf, nome_regiao,
               taxa_alfabetizacao, media_portugues, alunos_total,
               alunos_avaliados, percentual_avaliados
        FROM `{G}.indicador_municipio`
        WHERE comparavel_com_meta
    ),
    comparacao AS (
        SELECT ano, codigo_local AS id_municipio, taxa_na_tabela_de_meta,
               taxas_divergem, meta_do_ano, meta_2030, situacao_meta
        FROM `{G}.meta_vs_resultado`
        WHERE nivel = 'municipio'
    ),
    ano_2023 AS (
        SELECT
            i.id_municipio,
            i.nome_municipio,
            i.sigla_uf,
            i.nome_regiao,
            -- Nas 3 cidades divergentes, vale o valor confirmado pelo recálculo a
            -- partir dos alunos (Etapa 4.4).
            IF(c.taxas_divergem, c.taxa_na_tabela_de_meta, i.taxa_alfabetizacao) AS taxa_2023,
            i.media_portugues      AS media_portugues_2023,
            i.alunos_total         AS alunos_total_2023,
            i.alunos_avaliados     AS alunos_avaliados_2023,
            i.percentual_avaliados AS percentual_avaliados_2023
        FROM indicador AS i
        LEFT JOIN comparacao AS c
            ON c.ano = i.ano
           AND c.id_municipio = i.id_municipio
        WHERE i.ano = 2023
    ),
    ano_2024 AS (
        SELECT
            i.id_municipio,
            i.nome_municipio,
            i.sigla_uf,
            i.nome_regiao,
            i.taxa_alfabetizacao AS taxa_2024,
            c.meta_do_ano        AS meta_2024,
            c.meta_2030,
            c.situacao_meta      AS situacao_meta_2024
        FROM indicador AS i
        LEFT JOIN comparacao AS c
            ON c.ano = i.ano
           AND c.id_municipio = i.id_municipio
        WHERE i.ano = 2024
    )
    SELECT
        -- Identificação
        COALESCE(a.id_municipio, b.id_municipio)     AS id_municipio,
        COALESCE(a.nome_municipio, b.nome_municipio) AS nome_municipio,

        -- Variáveis de entrada: tudo o que já se sabia antes de 2024
        COALESCE(a.sigla_uf, b.sigla_uf)       AS sigla_uf,
        COALESCE(a.nome_regiao, b.nome_regiao) AS nome_regiao,
        t.eh_capital,
        a.taxa_2023,
        a.media_portugues_2023,
        a.alunos_total_2023,
        a.alunos_avaliados_2023,
        a.percentual_avaliados_2023,
        b.meta_2024,
        b.meta_2030,
        ROUND(b.meta_2024 - a.taxa_2023, 2) AS distancia_taxa_2023_para_meta_2024,

        -- Alvos: dependem do resultado de 2024. Não usar como entrada.
        b.taxa_2024                         AS alvo_taxa_2024,
        ROUND(b.taxa_2024 - a.taxa_2023, 2) AS alvo_variacao_2024,
        CASE b.situacao_meta_2024
            WHEN 'atingiu' THEN TRUE
            WHEN 'nao atingiu' THEN FALSE
        END AS alvo_atingiu_meta_2024,

        -- Controle: a linha serve para treino se tiver a entrada principal e o alvo.
        (a.taxa_2023 IS NOT NULL AND b.taxa_2024 IS NOT NULL) AS pronta_para_treino,
        {METADADOS_GOLD}
    FROM ano_2023 AS a
    FULL OUTER JOIN ano_2024 AS b
        ON a.id_municipio = b.id_municipio
    LEFT JOIN `{S}.territorio_municipio` AS t
        ON t.id_municipio = COALESCE(a.id_municipio, b.id_municipio)
"""

_ = gravar_gold("base_modelagem", sql_base_modelagem)


# ETAPA 4.6 · Gold: base_modelagem (parte 2): conferências

# Duas verificações de estrutura da tabela. Os resultados viram regras bloqueantes na
# validação da Etapa 4.7 (get_table lê só a estrutura, sem custo).
# 1) Toda coluna da tabela precisa ter um papel declarado.
colunas_da_tabela = [campo.name for campo in client.get_table(f"{G}.base_modelagem").schema]
COLUNAS_SEM_PAPEL = [c for c in colunas_da_tabela if c not in PAPEL_DAS_COLUNAS]

# 2) Todo alvo começa com "alvo_", e nenhuma entrada começa.
COLUNAS_MAL_NOMEADAS = [
    c for c, papel in PAPEL_DAS_COLUNAS.items()
    if (papel == "alvo") != c.startswith("alvo_")
]
print(f"Colunas sem papel declarado: {COLUNAS_SEM_PAPEL or 'nenhuma'}")
print(f"Colunas com nome incompatível com o papel: {COLUNAS_MAL_NOMEADAS or 'nenhuma'}")

print("=== PAPEL DE CADA COLUNA ===")
print(pd.DataFrame(
    [{"coluna": c, "papel": PAPEL_DAS_COLUNAS[c]} for c in colunas_da_tabela]
).to_string(index=False))

# Resumo da base, numa única leitura (a unicidade é conferida na validação da Etapa
# 4.7).
resumo = consulta(f"""
    SELECT
        COUNT(*) AS cidades,
        COUNTIF(pronta_para_treino)             AS prontas_para_treino,
        COUNTIF(taxa_2023 IS NULL)              AS sem_taxa_2023,
        COUNTIF(alvo_taxa_2024 IS NULL)         AS sem_alvo_2024,
        COUNTIF(percentual_avaliados_2023 IS NULL) AS sem_dados_de_alunos_2023,
        COUNTIF(alvo_atingiu_meta_2024)         AS alvo_atingiu,
        COUNTIF(NOT alvo_atingiu_meta_2024)     AS alvo_nao_atingiu,
        COUNTIF(alvo_atingiu_meta_2024 IS NULL AND alvo_taxa_2024 IS NOT NULL) AS alvo_sem_meta,
        COUNTIF(alvo_taxa_2024 IS NULL)         AS alvo_sem_resultado,
        ROUND(CORR(taxa_2023, alvo_taxa_2024), 3) AS correlacao_2023_com_2024
    FROM `{G}.base_modelagem`
""")
print("\n=== RESUMO DA BASE ===")
print(resumo.T.to_string(header=False))

print("\n=== EXEMPLO: Rio de Janeiro e Costa Marques ===")
print(consulta(f"""
    SELECT nome_municipio, sigla_uf, eh_capital, taxa_2023, percentual_avaliados_2023,
           meta_2024, distancia_taxa_2023_para_meta_2024,
           alvo_taxa_2024, alvo_variacao_2024, alvo_atingiu_meta_2024, pronta_para_treino
    FROM `{G}.base_modelagem`
    WHERE id_municipio IN ('3304557', '1100080')
    ORDER BY nome_municipio
""").T.to_string(header=False))

print("\n=== TODAS AS TABELAS DA GOLD ===")
# Tira as colunas tabelas_lidas e mb_estimados só da tela (continuam no diário de bordo).
print(pd.DataFrame(metricas_gold).drop(columns=["tabelas_lidas", "mb_estimados"]).to_string(index=False))


# VERIFICAÇÃO · Vazamento de alvo nas metas de base_modelagem (só leitura)
# meta_2024 e meta_2030 entram como entrada, mas vêm da publicação com ano_referencia =
# 2024, a mesma que traz o resultado de 2024. Compara essas metas com as da publicação
# de 2023 (rede Municipal). Uma linha por cidade em cada publicação (MAX agrupa
# eventuais repetições causadas por nivel_alfabetizacao).
# Iguais: a meta já era conhecida antes do alvo, então ela não revela o resultado de 2024.
# Diferentes ou ausentes na publicação de 2023: usar a publicação de 2023 ou tirar a
# coluna das entradas.
comparacao_publicacoes = consulta(f"""
    WITH publicacao AS (
        SELECT
            ano_referencia,
            id_municipio,
            COUNT(*)                         AS linhas,
            MAX(meta_alfabetizacao_2024)     AS meta_2024,
            MAX(meta_alfabetizacao_2030)     AS meta_2030
        FROM `{S}.meta_municipio`
        WHERE rede_codigo = '3'
          AND ano_referencia IN (2023, 2024)
        GROUP BY ano_referencia, id_municipio
    )
    SELECT
        COUNT(*)                                                    AS cidades_nas_duas,
        COUNTIF(a.linhas > 1 OR b.linhas > 1)                       AS cidades_com_linha_repetida,
        COUNTIF(a.meta_2024 IS NULL AND b.meta_2024 IS NOT NULL)    AS meta_2024_so_na_pub_2024,
        COUNTIF(a.meta_2024 = b.meta_2024)                          AS meta_2024_igual,
        COUNTIF(a.meta_2024 != b.meta_2024)                         AS meta_2024_diferente,
        COUNTIF(a.meta_2030 IS NULL AND b.meta_2030 IS NOT NULL)    AS meta_2030_so_na_pub_2024,
        COUNTIF(a.meta_2030 = b.meta_2030)                          AS meta_2030_igual,
        COUNTIF(a.meta_2030 != b.meta_2030)                         AS meta_2030_diferente
    FROM publicacao AS a
    INNER JOIN publicacao AS b
        ON b.id_municipio = a.id_municipio
       AND a.ano_referencia = 2023
       AND b.ano_referencia = 2024
""")
print(comparacao_publicacoes.T.to_string(header=False))


# ETAPA 4.7 · Validação de qualidade da Gold
# Aplica as regras de qualidade às 6 tabelas da Gold. Inclui as verificações da Etapa
# 4.1 (linhas da Gold x Silver) e da 4.6 (papel das colunas), a taxa de correspondência
# do cruzamento resultado x alunos e a validação da taxa contra os microdados.
# Regra bloqueante reprovada = a exportação (4.8) não roda.
# Validação da taxa: Brasil até 0,05 ponto; pelo menos 98% das cidades até 0,5 ponto.
# Cruzamento resultado x alunos (medido na 4.1): 2024 em 100% -> limite 99%; 2023 em
# 88,49% (São Paulo não tem alunos na base) -> limite informativo de 85%.


def taxa_com_alunos(ano: int) -> str:
    """Porcentagem das linhas da rede municipal, num ano, que acharam alunos correspondentes."""
    return (
        f"ROUND(100 * SAFE_DIVIDE("
        f"COUNTIF(ano = {ano} AND comparavel_com_meta AND tem_dados_de_alunos), "
        f"COUNTIF(ano = {ano} AND comparavel_com_meta)), 2)"
    )


# A exceção vale só para a chave completa dos 3 casos investigados: nível município, ano 2023,
# rede Municipal (código 3) e os três códigos de cidade. COALESCE evita que uma chave
# incompleta (NULL) escape da contagem.
DIVERGENCIA_NAO_AUTORIZADA = """taxas_divergem
        AND NOT COALESCE(
            nivel = 'municipio'
            AND ano = 2023
            AND CAST(rede_codigo AS STRING) = '3'
            AND codigo_local IN ('2304905', '2305100', '5106752'),
            FALSE)"""

# Teste da regra com linhas de exemplo (não toca nas tabelas oficiais): só os 3 casos de
# 2023 podem divergir; qualquer outra divergência tem de contar como violação.
teste_da_excecao = consulta(f"""
    WITH exemplos AS (
        SELECT 'caso investigado (2023)' AS caso, TRUE AS taxas_divergem, 'municipio' AS nivel,
               2023 AS ano, '3' AS rede_codigo, '2304905' AS codigo_local
        UNION ALL SELECT 'mesma cidade em 2024', TRUE, 'municipio', 2024, '3', '2304905'
        UNION ALL SELECT 'mesma cidade em 2025', TRUE, 'municipio', 2025, '3', '2304905'
        UNION ALL SELECT 'outra cidade em 2023', TRUE, 'municipio', 2023, '3', '1100015'
        UNION ALL SELECT 'outra rede', TRUE, 'municipio', 2023, '2', '2304905'
        UNION ALL SELECT 'outro nivel', TRUE, 'uf', 2023, '3', '2304905'
        UNION ALL SELECT 'chave incompleta', TRUE, 'municipio', CAST(NULL AS INT64), '3', '2304905'
        UNION ALL SELECT 'sem divergencia', FALSE, 'municipio', 2024, '3', '1100015'
    )
    SELECT caso, COUNTIF({DIVERGENCIA_NAO_AUTORIZADA}) AS violacoes
    FROM exemplos
    GROUP BY caso
""").set_index("caso")["violacoes"].to_dict()
esperado = {c: (0 if c in ("caso investigado (2023)", "sem divergencia") else 1)
            for c in teste_da_excecao}
assert teste_da_excecao == esperado, f"A regra de exceção não se comportou como o previsto: {teste_da_excecao}"
print(f"Teste da regra de exceção: {len(esperado)} casos de exemplo, todos como previsto "
      f"(só o caso investigado de 2023 e a linha sem divergência não violam).")

resultados_gold = []

resultados_gold += medir("gold", "indicador_municipio", f"`{G}.indicador_municipio`", [
    regra("linhas repetidas na chave", "Unicidade", repetidas("ano", "id_municipio", "rede_codigo")),
    regra("resultado municipal que achou alunos (2024)", "Consistência", taxa_com_alunos(2024),
          limite=99, sentido="no minimo", unidade="%"),
    regra("resultado municipal que achou alunos (2023)", "Consistência", taxa_com_alunos(2023),
          tipo=I_, limite=85, sentido="no minimo", unidade="%"),
])
# Etapa 4.1: o cruzamento com os alunos não pode criar nem perder linhas.
resultados_gold.append(conferencia_direta(
    "gold", "indicador_municipio", "linhas diferentes da Silver (resultado_municipio)",
    "Consistência", DIFERENCA_LINHAS_INDICADOR))
# A mesma cidade, no mesmo ano e rede, tem de ter a mesma taxa em indicador_municipio e em
# meta_vs_resultado.
taxas_diferentes_entre_tabelas = int(consulta(f"""
    SELECT COUNT(*)
    FROM `{G}.indicador_municipio` AS i
    INNER JOIN `{G}.meta_vs_resultado` AS m
        ON m.nivel = 'municipio'
       AND m.ano = i.ano
       AND m.codigo_local = i.id_municipio
       AND m.rede_codigo = i.rede_codigo
    WHERE i.taxa_alfabetizacao IS NOT NULL
      AND m.taxa_alfabetizacao IS NOT NULL
      AND ABS(i.taxa_alfabetizacao - m.taxa_alfabetizacao) > 0.001
""").iloc[0, 0])
resultados_gold.append(conferencia_direta(
    "gold", "indicador_municipio", "taxa diferente da de meta_vs_resultado",
    "Consistência", taxas_diferentes_entre_tabelas))

resultados_gold += medir("gold", "meta_vs_resultado", f"`{G}.meta_vs_resultado`", [
    regra("linhas repetidas na chave", "Unicidade", repetidas("nivel", "ano", "codigo_local")),
    regra("situacao fora da lista prevista", "Validade", (
        "COUNTIF(situacao_meta NOT IN ('atingiu', 'nao atingiu', 'sem meta', "
        "'sem resultado', 'sem resultado e sem meta'))")),
    regra("atingiu ou nao atingiu sem taxa ou sem meta", "Consistência", (
        "COUNTIF(situacao_meta IN ('atingiu', 'nao atingiu') "
        "AND (taxa_alfabetizacao IS NULL OR meta_do_ano IS NULL))")),
    # A taxa da meta só substitui a do resultado nos 3 casos investigados (Investigação 2).
    # Uma divergência nova bloqueia a execução até ser investigada.
    regra("divergencia entre tabelas oficiais fora dos 3 casos investigados", "Consistência",
          f"COUNTIF({DIVERGENCIA_NAO_AUTORIZADA})"),
])

resultados_gold += medir("gold", "evolucao_temporal", f"`{G}.evolucao_temporal`", [
    regra("linhas repetidas na chave", "Unicidade", repetidas("nivel", "codigo_local", "ano")),
    # Cruzamento com a tabela de lugares: nome vazio = lugar sem par.
    regra("lugar sem nome", "Completude", "COUNTIF(nome_local IS NULL)"),
])

resultados_gold += medir("gold", "validacao_taxa", f"`{G}.validacao_taxa`", [
    regra("diferenca do Brasil no ano mais recente com alunos", "Precisão", (
        "ARRAY_AGG(IF(nivel = 'brasil', ABS(diferenca), NULL) IGNORE NULLS "
        "ORDER BY ano DESC LIMIT 1)[SAFE_OFFSET(0)]"), limite=0.05, unidade="pontos"),
    regra("cidades com diferenca de ate 0,5 ponto", "Precisão", (
        "ROUND(100 * SAFE_DIVIDE(COUNTIF(nivel = 'municipio' AND ABS(diferenca) <= 0.5), "
        "COUNTIF(nivel = 'municipio' AND diferenca IS NOT NULL)), 2)"),
        limite=98, sentido="no minimo", unidade="%"),
])

resultados_gold += medir("gold", "desigualdade_regional", f"`{G}.desigualdade_regional`", [
    regra("agrupamentos com totais de cidades diferentes", "Consistência", (
        "ABS(SUM(IF(agrupamento = 'regiao', cidades, 0)) - SUM(IF(agrupamento = 'brasil', cidades, 0))) "
        "+ ABS(SUM(IF(agrupamento = 'uf', cidades, 0)) - SUM(IF(agrupamento = 'brasil', cidades, 0)))")),
])

resultados_gold += medir("gold", "base_modelagem", f"`{G}.base_modelagem`", [
    regra("cidade repetida", "Unicidade", "COUNT(*) - COUNT(DISTINCT id_municipio)"),
    regra("pronta para treino sem entrada ou sem alvo", "Completude",
          "COUNTIF(pronta_para_treino AND (taxa_2023 IS NULL OR alvo_taxa_2024 IS NULL))"),
    # Cruzamento com o território: eh_capital vazio = cidade sem par.
    regra("cidade sem territorio", "Completude", "COUNTIF(eh_capital IS NULL)"),
])
# Etapa 4.6: papel de cada coluna (ajuda a evitar vazamento de alvo, mas não o garante).
resultados_gold += [
    conferencia_direta("gold", "base_modelagem", "coluna sem papel declarado", "Validade",
                       len(COLUNAS_SEM_PAPEL)),
    conferencia_direta("gold", "base_modelagem", "nome da coluna incompatível com o papel",
                       "Validade", len(COLUNAS_MAL_NOMEADAS)),
]

df_validacao_gold = gravar_conferencias(resultados_gold)
mostrar_conferencias(df_validacao_gold)
print(f"\n{len(df_validacao_gold)} resultados gravados em {TABELA_CONFERENCIAS}")
validacao_de_qualidade(df_validacao_gold, "4.7 validação da Gold")


# ETAPA 4.8 · Exportação da Gold
# Exporta cada tabela da Gold em Parquet e CSV, com um manifesto, num único zip para a
# Fase 3.
# list_rows baixa a tabela sem rodar consulta (não consome a cota de leitura).
# No Sandbox as tabelas expiram em menos de 60 dias, então o arquivo é a cópia
# permanente.
import os
import zipfile
from datetime import datetime, timezone

TABELAS_GOLD = [
    "indicador_municipio",
    "meta_vs_resultado",
    "evolucao_temporal",
    "validacao_taxa",
    "desigualdade_regional",
    "base_modelagem",
]

PASTA_EXPORTACAO = "/content/gold_export"
os.makedirs(PASTA_EXPORTACAO, exist_ok=True)

manifesto = []
for tabela in TABELAS_GOLD:
    referencia = client.get_table(f"{G}.{tabela}")
    # Baixa a tabela inteira sem consulta SQL (não entra na cota de leitura).
    df = client.list_rows(referencia).to_dataframe(create_bqstorage_client=False)

    caminho_parquet = f"{PASTA_EXPORTACAO}/gold_{tabela}.parquet"
    caminho_csv = f"{PASTA_EXPORTACAO}/gold_{tabela}.csv"
    df.to_parquet(caminho_parquet, index=False)
    df.to_csv(caminho_csv, index=False)

    # Regra bloqueante: o arquivo gravado tem de ter as mesmas linhas da tabela.
    linhas_arquivo = len(pd.read_parquet(caminho_parquet))
    if linhas_arquivo != referencia.num_rows:
        raise ValueError(
            f"gold_{tabela}: arquivo com {linhas_arquivo} linhas e tabela com "
            f"{referencia.num_rows}. Exportação interrompida."
        )

    manifesto.append({
        "tabela": f"gold.{tabela}",
        "linhas": referencia.num_rows,
        "colunas": len(df.columns),
        "parquet_kib": round(os.path.getsize(caminho_parquet) / 1024, 1),
        "csv_kib": round(os.path.getsize(caminho_csv) / 1024, 1),
    })
    print(f"[gold_{tabela}] exportada: {referencia.num_rows:,} linhas")

df_manifesto = pd.DataFrame(manifesto)
df_manifesto["exportado_em_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
df_manifesto.to_csv(f"{PASTA_EXPORTACAO}/manifesto.csv", index=False)

# Um único .zip, para baixar tudo de uma vez.
caminho_zip = "/content/gold_export.zip"
with zipfile.ZipFile(caminho_zip, "w", zipfile.ZIP_DEFLATED) as pacote:
    for nome in sorted(os.listdir(PASTA_EXPORTACAO)):
        pacote.write(f"{PASTA_EXPORTACAO}/{nome}", arcname=f"gold_export/{nome}")

print("\n=== MANIFESTO DA EXPORTAÇÃO ===")
print(df_manifesto.to_string(index=False))
total_parquet_kib = df_manifesto.parquet_kib.sum()
total_csv_kib = df_manifesto.csv_kib.sum()
print(
    f"\nTotal em Parquet: {total_parquet_kib:,.1f} KiB | Total em CSV: {total_csv_kib:,.1f} KiB "
    f"(1 KiB = 1.024 bytes)"
)
print(f"O Parquet ocupa cerca de {total_csv_kib / total_parquet_kib:.1f} vezes menos espaço que o CSV.")
print(f"Pacote: {caminho_zip} ({os.path.getsize(caminho_zip) / 1024:,.1f} KiB)")

# Download do pacote.
from google.colab import files
files.download(caminho_zip)


# ETAPA 4.9 · Gráficos dos resultados da Gold
# Gera os cinco gráficos da apresentação, lendo as tabelas da Gold (meta_vs_resultado,
# evolucao_temporal, desigualdade_regional e base_modelagem). Cada um responde a uma pergunta da Gold e é
# precedido da tabela de onde saem os números. Se faltar uma categoria esperada, a célula para: um gráfico não pode
# esconder dado.
# As imagens ficam em /content/graficos e vão num único zip.
import os
import zipfile

import matplotlib
import matplotlib.pyplot as plt

print(f"matplotlib {matplotlib.__version__}")

PASTA_GRAFICOS = "/content/graficos"
os.makedirs(PASTA_GRAFICOS, exist_ok=True)

COR_ATINGIU, COR_NAO_ATINGIU, COR_SEM_META = "#3f7f4f", "#b5533c", "#8e9ca3"
COR_TAXA, COR_META = "#2d7f7b", "#d9a21b"


def milhar(n) -> str:
    """Número inteiro com ponto de milhar (2788 -> 2.788)."""
    return f"{int(n):,}".replace(",", ".")


def virgula(x, casas: int = 1) -> str:
    """Número decimal com vírgula (73.1 -> 73,1)."""
    return f"{x:.{casas}f}".replace(".", ",")


def salvar(figura, nome: str) -> str:
    caminho = f"{PASTA_GRAFICOS}/{nome}"
    figura.savefig(caminho, dpi=200, bbox_inches="tight")
    return caminho


# Gráfico 1 · Municípios em 2024 frente à meta do ano (rede Municipal)
df_situacao = consulta(f"""
    SELECT situacao_meta, COUNT(*) AS municipios
    FROM `{G}.meta_vs_resultado`
    WHERE nivel = 'municipio' AND ano = 2024
    GROUP BY situacao_meta
""").set_index("situacao_meta")

ORDEM_SITUACAO = [
    ("atingiu", "Atingiu", COR_ATINGIU),
    ("nao atingiu", "Não atingiu", COR_NAO_ATINGIU),
    ("sem meta", "Sem meta publicada", COR_SEM_META),
]
inesperadas = set(df_situacao.index) - {c for c, _, _ in ORDEM_SITUACAO}
ausentes = {c for c, _, _ in ORDEM_SITUACAO} - set(df_situacao.index)
if inesperadas or ausentes:
    raise ValueError(
        f"Situações inesperadas: {sorted(inesperadas)}; ausentes: {sorted(ausentes)}. "
        "O gráfico 1 não foi gerado."
    )

print("=== GRÁFICO 1 · Municípios em 2024, rede Municipal ===")
print(df_situacao.loc[[c for c, _, _ in ORDEM_SITUACAO]].to_string())

fig1, ax1 = plt.subplots(figsize=(8, 3.6))
rotulos = [nome for _, nome, _ in ORDEM_SITUACAO]
valores = [int(df_situacao.loc[c, "municipios"]) for c, _, _ in ORDEM_SITUACAO]
barras = ax1.barh(rotulos, valores, color=[cor for _, _, cor in ORDEM_SITUACAO])
ax1.invert_yaxis()
for barra, valor in zip(barras, valores):
    ax1.text(valor, barra.get_y() + barra.get_height() / 2, f" {milhar(valor)}", va="center")
ax1.set_xlim(0, max(valores) * 1.15)
ax1.set_xlabel(f"Número de municípios ({milhar(sum(valores))} com resultado da rede Municipal em 2024)")
ax1.set_ylabel("Situação frente à meta do ano")
ax1.set_title("Municípios em 2024, rede Municipal: situação frente à meta do ano")
ax1.spines[["top", "right"]].set_visible(False)
print(salvar(fig1, "grafico_1_municipios_frente_a_meta_2024.png"))
plt.show()

# Gráfico 2 · Brasil: resultado observado x meta pactuada do ano
df_brasil = consulta(f"""
    SELECT ano, taxa_alfabetizacao, meta_do_ano
    FROM `{G}.evolucao_temporal`
    WHERE nivel = 'brasil'
    ORDER BY ano
""")
if df_brasil.empty or df_brasil["taxa_alfabetizacao"].notna().sum() == 0:
    raise ValueError("evolucao_temporal sem linhas do Brasil. O gráfico 2 não foi gerado.")

print("\n=== GRÁFICO 2 · Brasil, rede Pública ===")
print(df_brasil.to_string(index=False))

observado = df_brasil.dropna(subset=["taxa_alfabetizacao"])
meta = df_brasil.dropna(subset=["meta_do_ano"])
fig2, ax2 = plt.subplots(figsize=(8, 4.2))
ax2.plot(observado["ano"], observado["taxa_alfabetizacao"], color=COR_META, marker="o",
         linewidth=3, label="Resultado observado")
ax2.plot(meta["ano"], meta["meta_do_ano"], color="#14343b", marker="D", linestyle="--",
         linewidth=2, label="Meta pactuada do ano")
obs_por_ano = dict(zip(observado["ano"], observado["taxa_alfabetizacao"]))
meta_por_ano = dict(zip(meta["ano"], meta["meta_do_ano"]))
# O rótulo do valor mais alto vai acima e o do mais baixo abaixo, para não cobrir o outro ponto.
for ano, valor in obs_por_ano.items():
    acima = ano not in meta_por_ano or valor >= meta_por_ano[ano]
    ax2.annotate(virgula(valor), (ano, valor), textcoords="offset points",
                 xytext=(0, 8 if acima else -16), ha="center", color=COR_META)
for ano, valor in meta_por_ano.items():
    acima = ano not in obs_por_ano or valor > obs_por_ano[ano]
    ax2.annotate(virgula(valor), (ano, valor), textcoords="offset points",
                 xytext=(0, 8 if acima else -16), ha="center", color="#14343b")
ax2.set_ylim(0, 100)
ax2.set_xticks(list(df_brasil["ano"]))
ax2.set_xlabel("Ano")
ax2.set_ylabel("% de alunos alfabetizados (rede Pública)")
ax2.set_title("Brasil, rede Pública: resultado observado x meta pactuada")
ax2.legend(loc="lower right")
ax2.spines[["top", "right"]].set_visible(False)
fig2.text(0.01, -0.05,
          "De 2026 a 2030 há só a meta pactuada, não previsão. O resultado de 2025 é o valor "
          "publicado na tabela de meta (sem microdados).",
          fontsize=8, color="#555555", ha="left")
print(salvar(fig2, "grafico_2_brasil_observado_x_meta.png"))
plt.show()

# Gráfico 3 · Melhorou ou piorou? Variação de 2023 para 2024 por região (rede Municipal)
df_variacao = consulta(f"""
    SELECT
        nome_regiao                                         AS regiao,
        COUNT(*)                                            AS cidades,
        ROUND({percentil_sql('variacao_anual', 0.5)}, 2)    AS variacao_mediana,
        ROUND(100 * COUNTIF(variacao_anual < 0) / COUNT(*), 1) AS pct_que_piorou
    FROM `{G}.evolucao_temporal`
    WHERE nivel = 'municipio' AND ano = 2024 AND variacao_anual IS NOT NULL
    GROUP BY nome_regiao
    ORDER BY variacao_mediana DESC
""")
if len(df_variacao) != 5 or df_variacao[["variacao_mediana", "pct_que_piorou"]].isna().any().any():
    raise ValueError(
        f"evolucao_temporal trouxe {len(df_variacao)} regiões (esperado: 5) ou valores vazios. "
        "O gráfico 3 não foi gerado."
    )

print("\n=== GRÁFICO 3 · Variação de 2023 para 2024, rede Municipal ===")
print(df_variacao.to_string(index=False))

fig3, (ax3a, ax3b) = plt.subplots(1, 2, figsize=(10, 4.4), sharey=True)
y = list(range(len(df_variacao)))
cores = [COR_ATINGIU if v >= 0 else COR_NAO_ATINGIU for v in df_variacao["variacao_mediana"]]
ax3a.barh(y, df_variacao["variacao_mediana"], color=cores)
ax3a.axvline(0, color="#14343b", linewidth=1)
for yi, v in zip(y, df_variacao["variacao_mediana"]):
    ax3a.text(v, yi, f" {virgula(v)}" if v >= 0 else f"{virgula(v)} ", va="center",
              ha="left" if v >= 0 else "right", fontsize=9)
ax3a.set_yticks(y)
ax3a.set_yticklabels(df_variacao["regiao"])
ax3a.invert_yaxis()
ax3a.margins(x=0.2)
ax3a.set_xlabel("Variação mediana da taxa (pontos percentuais)")
ax3a.set_ylabel("Região")
ax3a.set_title("Mudança típica da cidade", fontsize=10)
ax3b.barh(y, df_variacao["pct_que_piorou"], color=COR_META)
for yi, v in zip(y, df_variacao["pct_que_piorou"]):
    ax3b.text(v, yi, f" {virgula(v)}", va="center", fontsize=9)
ax3b.set_xlim(0, 100)
ax3b.set_xlabel("% das cidades em que a taxa caiu")
ax3b.set_title("Cidades que pioraram", fontsize=10)
for eixo in (ax3a, ax3b):
    eixo.spines[["top", "right"]].set_visible(False)
fig3.suptitle("Melhorou ou piorou? Cidades por região, de 2023 para 2024 (rede Municipal)")
print(salvar(fig3, "grafico_3_variacao_por_regiao_2023_2024.png"))
plt.show()

# Gráfico 4 · Regiões (rede Municipal, 2024): nível x cumprimento da meta
df_regioes = consulta(f"""
    SELECT grupo AS regiao, cidades_com_taxa, taxa_do_meio, percentual_que_atingiu
    FROM `{G}.desigualdade_regional`
    WHERE agrupamento = 'regiao' AND ano = 2024
    ORDER BY taxa_do_meio DESC
""")
if len(df_regioes) != 5 or df_regioes[["taxa_do_meio", "percentual_que_atingiu"]].isna().any().any():
    raise ValueError(
        f"desigualdade_regional trouxe {len(df_regioes)} regiões (esperado: 5) ou valores vazios. "
        "O gráfico 4 não foi gerado."
    )

print("\n=== GRÁFICO 4 · Regiões, rede Municipal, 2024 ===")
print(df_regioes.to_string(index=False))

fig4, ax4 = plt.subplots(figsize=(9, 4.6))
posicao = range(len(df_regioes))
altura = 0.38
b_taxa = ax4.barh([p - altura / 2 for p in posicao], df_regioes["taxa_do_meio"], altura,
                  color=COR_TAXA, label="Taxa mediana das cidades (nível de alfabetização)")
b_meta = ax4.barh([p + altura / 2 for p in posicao], df_regioes["percentual_que_atingiu"], altura,
                  color=COR_META, label="% das cidades que atingiram a própria meta")
for barras_grupo in (b_taxa, b_meta):
    for barra in barras_grupo:
        ax4.text(barra.get_width(), barra.get_y() + barra.get_height() / 2,
                 f" {virgula(barra.get_width())}", va="center", fontsize=9)
ax4.set_yticks(list(posicao))
ax4.set_yticklabels(df_regioes["regiao"])
ax4.invert_yaxis()
ax4.set_xlim(0, 100)
ax4.set_xlabel("% (taxa mediana das cidades; % das cidades que atingiram a meta)")
ax4.set_ylabel("Região")
ax4.set_title("Regiões, rede Municipal, 2024: nível de alfabetização x cumprimento da meta")
ax4.legend(loc="lower right", fontsize=8)
ax4.spines[["top", "right"]].set_visible(False)
fig4.text(0.01, -0.05,
          "Cada cidade tem a própria meta. O percentual mede o cumprimento da meta, "
          "não o nível de alfabetização.",
          fontsize=8, color="#555555", ha="left")
print(salvar(fig4, "grafico_4_regioes_nivel_x_meta_2024.png"))
plt.show()

# Gráfico 5 · Base para a Fase 3: taxa de 2023 x taxa de 2024 de cada município
df_base = consulta(f"""
    SELECT id_municipio, taxa_2023, alvo_taxa_2024
    FROM `{G}.base_modelagem`
    WHERE pronta_para_treino
""")
if df_base.empty or df_base[["taxa_2023", "alvo_taxa_2024"]].isna().any().any():
    raise ValueError("base_modelagem sem linhas prontas ou com valores vazios. O gráfico 5 não foi gerado.")

correlacao = df_base["taxa_2023"].corr(df_base["alvo_taxa_2024"])
print("\n=== GRÁFICO 5 · base_modelagem: municípios prontos para treino ===")
print(f"municípios: {milhar(len(df_base))} | correlação entre a taxa de 2023 e a de 2024: {correlacao:.3f}")
print(df_base[["taxa_2023", "alvo_taxa_2024"]].describe().round(2).to_string())

fig5, ax5 = plt.subplots(figsize=(6.4, 6))
ax5.scatter(df_base["taxa_2023"], df_base["alvo_taxa_2024"], s=8, alpha=0.25, color=COR_TAXA)
ax5.plot([0, 100], [0, 100], color="#14343b", linestyle="--", linewidth=1.2,
         label="Sem mudança (taxa de 2024 = taxa de 2023)")
ax5.set_xlim(0, 100)
ax5.set_ylim(0, 100)
ax5.set_xlabel("Taxa de alfabetização em 2023 (%)")
ax5.set_ylabel("Taxa de alfabetização em 2024 (%)")
ax5.set_title("Base para a Fase 3: taxa de 2023 x taxa de 2024 por município")
ax5.legend(loc="upper left", fontsize=8)
ax5.text(0.97, 0.04, f"{milhar(len(df_base))} municípios\ncorrelação {virgula(correlacao, 3)}",
         transform=ax5.transAxes, ha="right", fontsize=10)
ax5.spines[["top", "right"]].set_visible(False)
fig5.text(0.01, -0.02,
          "Cada ponto é um município da rede Municipal. Acima da linha, a taxa subiu de 2023 para 2024.",
          fontsize=8, color="#555555", ha="left")
print(salvar(fig5, "grafico_5_base_modelagem_2023_x_2024.png"))
plt.show()

# Um único .zip, para baixar as imagens de uma vez.
caminho_zip_graficos = "/content/graficos.zip"
with zipfile.ZipFile(caminho_zip_graficos, "w", zipfile.ZIP_DEFLATED) as pacote:
    for nome in sorted(os.listdir(PASTA_GRAFICOS)):
        pacote.write(f"{PASTA_GRAFICOS}/{nome}", arcname=f"graficos/{nome}")
print(f"\nPacote: {caminho_zip_graficos} ({os.path.getsize(caminho_zip_graficos) / 1024:,.1f} KiB)")

# Download do pacote.
from google.colab import files
files.download(caminho_zip_graficos)
