# Gerado a partir de notebooks/NexusAI_techfase2.ipynb (mesmo código, separado por camada).
# A execução é feita pelo notebook, na ordem das etapas: as células compartilham
# variáveis (client, PROJECT_ID e as funções das Etapas 1.1 e 1.2).

# ETAPA 7 · Demonstração de falha (rode por último)
# Esta célula termina em erro de propósito, para demonstrar a falha.
# Cria uma tabela de teste com 3 defeitos (taxa 150, rede sem tradução e linha repetida)
# e aplica as regras de qualidade da resultado_uf.
# O esperado: a validação interrompe a célula, o vigia envia o aviso [TESTE] e grava a
# falha como "<execução>_teste". As tabelas reais não são alteradas.
TABELA_TESTE = f"{Q}.teste_resultado_uf_com_defeito"

# O aviso sai com [TESTE] e a falha é gravada como <execução>_teste, separada da
# execução real.
MODO_DEMONSTRACAO = True

# 1) Cópia da resultado_uf com 3 defeitos (SELECT * de propósito).
sql_tabela_teste = f"""
    SELECT * REPLACE (
        IF(sigla_uf = 'AM' AND ano = 2024 AND rede_codigo = '5', 150.0, taxa_alfabetizacao)
            AS taxa_alfabetizacao,
        IF(sigla_uf = 'CE' AND ano = 2024 AND rede_codigo = '5', 'desconhecido', rede_nome)
            AS rede_nome
    )
    FROM `{S}.resultado_uf`
    UNION ALL
    SELECT *
    FROM `{S}.resultado_uf`
    WHERE sigla_uf = 'RJ' AND ano = 2024 AND rede_codigo = '5'
"""
config_teste = bigquery.QueryJobConfig(
    destination=TABELA_TESTE,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)
client.query(sql_tabela_teste, job_config=config_teste).result()
print("Tabela de teste criada, com 3 defeitos plantados:", TABELA_TESTE)

# 2) As mesmas regras que a validação da Silver aplica à resultado_uf.
resultados_teste = medir("teste", "teste_resultado_uf_com_defeito", f"`{TABELA_TESTE}`", [
    regra("linhas repetidas na chave", "Unicidade", repetidas("ano", "sigla_uf", "rede_codigo")),
    regra("codigo sem traducao", "Validade",
          "COUNTIF(rede_nome = 'desconhecido' OR serie_nome = 'desconhecido')"),
    regra("taxa fora de 0 a 100", "Validade",
          "COUNTIF(taxa_alfabetizacao < 0 OR taxa_alfabetizacao > 100)"),
    regra("lugar sem nome", "Completude", "COUNTIF(nome_uf IS NULL)"),
])

# 3) Grava antes da validação, para a reprovação ficar registrada.
df_teste = gravar_conferencias(resultados_teste, cenario="falha proposital")
print()
mostrar_conferencias(df_teste)

print("\n=== HISTÓRICO GRAVADO EM quality_reports.conferencias ===")
print(consulta(f"""
    SELECT
        execucao_id,
        cenario,
        COUNT(*)                                     AS regras,
        COUNTIF(passou)                              AS passaram,
        COUNTIF(NOT passou AND tipo = 'BLOQUEANTE')  AS bloqueantes_que_falharam
    FROM `{TABELA_CONFERENCIAS}`
    GROUP BY execucao_id, cenario
    ORDER BY MIN(executado_em)
""").to_string(index=False))

# 4) Validação: o erro levantado aqui interrompe a célula.
print("\nPassando a tabela com defeitos pela validação de qualidade...")
validacao_de_qualidade(df_teste, "7 falha proposital (TESTE, sem efeito nos dados reais)")
print("Linha não alcançada: se for impressa, a validação deixou passar dado com defeito.")
