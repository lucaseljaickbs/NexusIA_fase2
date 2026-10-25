# Gerado a partir de notebooks/NexusAI_techfase2.ipynb (mesmo código, separado por camada).
# A execução é feita pelo notebook, na ordem das etapas: as células compartilham
# variáveis (client, PROJECT_ID e as funções das Etapas 1.1 e 1.2).

# ETAPA 1.1 · Ferramentas de qualidade
# Cria as funções de qualidade usadas no resto do notebook: montar uma regra, medir,
# gravar o resultado em quality_reports.conferencias e interromper a execução se uma
# regra bloqueante falhar. Usadas nas Etapas 2.3, 2.4, 3.11, 4.7, 6.3 e 7.
# Também define consulta() (roda uma consulta) e simular_custo() (estima quantos MB uma
# consulta vai ler, sem executá-la).
import pandas as pd
from datetime import datetime, timezone

TABELA_CONFERENCIAS = f"{Q}.conferencias"

# Guarda as conferências feitas nesta execução (o diário de bordo as resume depois).
CONFERENCIAS_DA_EXECUCAO = []

B_, I_ = "BLOQUEANTE", "INFORMATIVA"


class ErroDeQualidade(Exception):
    """Erro levantado quando uma regra bloqueante falha."""


def consulta(sql: str) -> pd.DataFrame:
    """Roda uma consulta de leitura e devolve o resultado como tabela."""
    return client.query(sql).to_dataframe()


def simular_custo(sql: str) -> float:
    """Estima quantos MB a consulta vai ler, sem executá-la e sem cobrança (dry run)."""
    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=config)
    return round(job.total_bytes_processed / 1024**2, 2)


def percentil_sql(coluna: str, p: float) -> str:
    """Devolve o trecho de SQL que calcula um percentil exato de uma coluna.

    O resultado é sempre o mesmo: os valores são colocados em ordem e escolhe-se a
    posição do percentil. A função APPROX_QUANTILES do BigQuery é aproximada e poderia
    mudar um pouco a cada execução. Valores vazios ficam de fora; sem nenhum valor,
    devolve vazio.
    """
    posicao = f"CAST(ROUND({p} * (COUNTIF({coluna} IS NOT NULL) - 1)) AS INT64)"
    return f"ARRAY_AGG({coluna} IGNORE NULLS ORDER BY {coluna})[SAFE_OFFSET({posicao})]"


# Montagem das regras
def regra(nome, dimensao, expressao, tipo=B_, **extras):
    """Monta uma regra. 'extras' aceita limite, sentido e unidade."""
    return {"regra": nome, "dimensao": dimensao, "expressao": expressao, "tipo": tipo, **extras}


def repetidas(*colunas):
    """Devolve o trecho de SQL que conta as linhas que repetem a mesma chave.

    A chave é montada com TO_JSON_STRING(STRUCT(...)) porque, ao contrário do CONCAT,
    ela não descarta chaves com valor vazio (CONCAT com vazio devolve vazio).
    """
    campos = ", ".join(colunas)
    return f"COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT({campos})))"


def percentual_com_par(peso="1"):
    """Devolve o trecho de SQL com a porcentagem das linhas (ou do peso) que acharam par no
    cruzamento.
    """
    return f"ROUND(100 * SAFE_DIVIDE(SUM(IF(par IS NOT NULL, {peso}, 0)), SUM({peso})), 2)"


# Avaliação
def avaliar(camada: str, tabela: str, r: dict, valor) -> dict:
    """Compara o valor medido com o limite da regra e diz se passou."""
    valor = None if valor is None or pd.isna(valor) else float(valor)
    limite = float(r.get("limite", 0))
    sentido = r.get("sentido", "no maximo")
    if valor is None:
        passou = False          # sem valor medido, a regra não é considerada cumprida
    elif sentido == "no maximo":
        passou = valor <= limite
    else:
        passou = valor >= limite
    return {
        "camada": camada,
        "tabela": tabela,
        "regra": r["regra"],
        "dimensao": r["dimensao"],
        "tipo": r["tipo"],
        "valor_medido": valor,
        "sentido": sentido,
        "limite": limite,
        "unidade": r.get("unidade", "linhas"),
        "passou": passou,
    }


def medir(camada: str, tabela: str, fonte_sql: str, regras: list) -> list:
    """Mede todas as regras de uma tabela numa única consulta (uma leitura só)."""
    colunas = ",\n        ".join(f"{r['expressao']} AS m{i}" for i, r in enumerate(regras))
    linha = consulta(f"SELECT\n        {colunas}\n    FROM {fonte_sql}").iloc[0]
    return [avaliar(camada, tabela, r, linha[f"m{i}"]) for i, r in enumerate(regras)]


def conferencia_direta(camada, tabela, nome, dimensao, valor, tipo=B_, **extras) -> dict:
    """Avalia uma regra cujo valor já foi calculado em Python (ex.: contagem da Bronze)."""
    return avaliar(camada, tabela, regra(nome, dimensao, "", tipo, **extras), valor)


# Gravação, exibição e validação
def gravar_conferencias(resultados: list, cenario: str = "execucao normal") -> pd.DataFrame:
    """Acrescenta os resultados em quality_reports.conferencias e devolve a tabela."""
    df = pd.DataFrame(resultados)
    df.insert(0, "execucao_id", EXECUCAO_ID)
    df.insert(1, "executado_em", datetime.now(timezone.utc))
    df.insert(2, "cenario", cenario)
    # Colunas novas entram no fim da tabela.
    df = df[[c for c in df.columns if c != "dimensao"] + ["dimensao"]]
    config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        schema=[
            bigquery.SchemaField("execucao_id", "STRING"),
            bigquery.SchemaField("executado_em", "TIMESTAMP"),
            bigquery.SchemaField("cenario", "STRING"),
            bigquery.SchemaField("camada", "STRING"),
            bigquery.SchemaField("tabela", "STRING"),
            bigquery.SchemaField("regra", "STRING"),
            bigquery.SchemaField("tipo", "STRING"),
            bigquery.SchemaField("valor_medido", "FLOAT"),
            bigquery.SchemaField("sentido", "STRING"),
            bigquery.SchemaField("limite", "FLOAT"),
            bigquery.SchemaField("unidade", "STRING"),
            bigquery.SchemaField("passou", "BOOLEAN"),
            bigquery.SchemaField("dimensao", "STRING"),
        ],
    )
    # Gravação por carga de arquivo (load job): é gratuita e funciona no Sandbox.
    client.load_table_from_dataframe(df, TABELA_CONFERENCIAS, job_config=config).result()
    if cenario == "execucao normal":
        CONFERENCIAS_DA_EXECUCAO.append(df)
    return df


def mostrar_conferencias(df: pd.DataFrame) -> None:
    """Mostra o resultado das conferências, com as falhas primeiro."""
    visao = df.sort_values(["passou", "camada", "tabela"])[
        ["camada", "tabela", "regra", "dimensao", "tipo", "valor_medido", "sentido", "limite", "unidade", "passou"]
    ]
    print(visao.to_string(index=False))
    bloqueantes = df[df["tipo"] == B_]
    informativas = df[df["tipo"] == I_]
    print(f"\nBloqueantes: {int(bloqueantes['passou'].sum())} de {len(bloqueantes)} passaram.")
    print(f"Informativas: {int(informativas['passou'].sum())} de {len(informativas)} passaram.")
    # Score = porcentagem de regras aprovadas.
    print(f"Score de qualidade: {round(100 * df['passou'].mean(), 1)}%")


def validacao_de_qualidade(df: pd.DataFrame, etapa: str) -> None:
    """Interrompe a execução (ErroDeQualidade) se alguma regra bloqueante falhou.

    Não usa try/except de propósito: o erro precisa parar a execução.
    """
    falhas = df[(df["tipo"] == B_) & (~df["passou"])]
    if len(falhas) > 0:
        lista = "; ".join(
            f"{f.tabela}: {f.regra} (medido "
            + ("sem medida" if pd.isna(f.valor_medido) else f"{f.valor_medido:g}")
            + f", limite {f.sentido} {f.limite:g} {f.unidade})"
            for f in falhas.itertuples()
        )
        raise ErroDeQualidade(f"[{etapa}] {len(falhas)} regra(s) bloqueante(s) falharam. {lista}")
    print(f"\nValidação de qualidade ({etapa}): todas as regras bloqueantes passaram.")


print("Ferramentas de qualidade prontas.")
print("Execução:", EXECUCAO_ID)
print("Resultados das conferências vão para:", TABELA_CONFERENCIAS)


# ETAPA 1.3 · Testes das funções de qualidade
# Testes automáticos (unittest) das funções que decidem se um dado passa ou falha:
# avaliar, repetidas, percentual_com_par e validacao_de_qualidade.
# Rodam sem consultar o BigQuery. Se algum teste falhar, a execução para aqui.
import contextlib
import io
import sys
import unittest


class TestesDeQualidade(unittest.TestCase):
    """Verifica se as regras e a validação se comportam como esperado."""

    def test_limite_maximo(self):
        r = regra("linhas repetidas", "Unicidade", "")          # limite 0: no máximo zero linhas repetidas
        self.assertTrue(avaliar("silver", "t", r, 0)["passou"])
        self.assertFalse(avaliar("silver", "t", r, 1)["passou"])

    def test_limite_minimo(self):
        r = regra("linhas que acharam par", "Consistência", "", limite=95,
                  sentido="no minimo", unidade="%")
        self.assertTrue(avaliar("silver", "t", r, 95.0)["passou"])
        self.assertFalse(avaliar("silver", "t", r, 94.99)["passou"])

    def test_sem_medida_reprova(self):
        # Sem valor medido, a regra não é considerada cumprida.
        r = regra("qualquer regra", "Validade", "")
        self.assertFalse(avaliar("silver", "t", r, None)["passou"])
        self.assertFalse(avaliar("silver", "t", r, float("nan"))["passou"])

    def test_validacao_para_na_bloqueante(self):
        reprovada = pd.DataFrame([avaliar("silver", "t", regra("r", "Validade", ""), 3)])
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(ErroDeQualidade):
                validacao_de_qualidade(reprovada, "teste")

    def test_validacao_deixa_passar_informativa_reprovada(self):
        info = pd.DataFrame([avaliar("silver", "t", regra("r", "Validade", "", tipo=I_), 3)])
        with contextlib.redirect_stdout(io.StringIO()):
            validacao_de_qualidade(info, "teste")                   # não pode levantar erro

    def test_expressoes_sql(self):
        self.assertIn("TO_JSON_STRING(STRUCT(ano, sigla_uf))", repetidas("ano", "sigla_uf"))
        self.assertIn("SUM(alunos)", percentual_com_par("alunos"))


suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestesDeQualidade)
resultado_dos_testes = unittest.TextTestRunner(stream=sys.stdout, verbosity=2).run(suite)
if not resultado_dos_testes.wasSuccessful():
    raise ErroDeQualidade("Os testes das funções de qualidade falharam. Pipeline interrompido.")
print("\nTestes das funções de qualidade: todos passaram.")
