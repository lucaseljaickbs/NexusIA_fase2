# Gerado a partir de notebooks/NexusAI_techfase2.ipynb (mesmo código, separado por camada).
# A execução é feita pelo notebook, na ordem das etapas: as células compartilham
# variáveis (client, PROJECT_ID e as funções das Etapas 1.1 e 1.2).

# ETAPA 1.2 · Ferramentas de monitoramento
# Cria as ferramentas de monitoramento:
# - enviar_alerta: manda o aviso de erro para o Discord e registra em
#   quality_reports.alertas. O endereço do Discord fica nos Secrets do Colab
#   (ALERTA_WEBHOOK_URL), nunca no código.
# - registrar_execucao: grava o diário de bordo (tabelas metricas_por_tabela e
#   execucoes).
# - vigia de erros: quando qualquer célula dá erro, avisa e grava a falha. O erro
#   continua aparecendo na tela e a execução para normalmente.
import os
import re
import requests
from google.api_core.exceptions import NotFound

TABELA_METRICAS = f"{Q}.metricas_por_tabela"
TABELA_EXECUCOES = f"{Q}.execucoes"
TABELA_ALERTAS = f"{Q}.alertas"

# Ativa o registro de falhas e o envio de alertas durante a execução.
VIGIA_LIGADO = True

# Só vira True na Etapa 7 (demonstração de falha): o aviso sai com [TESTE] e a execução
# ganha um id próprio.
MODO_DEMONSTRACAO = False


# Aviso de erro
def ler_endereco_do_webhook():
    """Lê o endereço do Discord nos Secrets do Colab. Devolve None se não existir."""
    try:
        from google.colab import userdata
        endereco = userdata.get("ALERTA_WEBHOOK_URL")
    except Exception:
        endereco = None          # segredo não existe ou o acesso não foi liberado
    return endereco or os.environ.get("ALERTA_WEBHOOK_URL")


def enviar_alerta(titulo: str, mensagem: str, gravidade: str = "ERRO") -> str:
    """Envia o aviso pelo webhook (se houver) e sempre grava na tabela de alertas."""
    if MODO_DEMONSTRACAO:
        titulo = f"[TESTE] {titulo}"
    criado_em = datetime.now(timezone.utc)
    texto = (
        f"**[{gravidade}] {titulo}**\n"
        f"Execução: {EXECUCAO_ID}\n"
        f"Horário (UTC): {criado_em:%d/%m/%Y %H:%M:%S}\n"
        f"{mensagem}"
    )
    endereco = ler_endereco_do_webhook()
    if not endereco:
        canal, envio = "nenhum", "desligado: nenhum endereço configurado"
    else:
        canal = "webhook"
        try:
            # O Discord aceita mensagens de até 2.000 caracteres.
            resposta = requests.post(endereco, json={"content": texto[:1900]}, timeout=10)
            envio = "enviado" if resposta.status_code in (200, 204) else f"falhou: código {resposta.status_code}"
        except Exception as erro:
            # Se o envio do aviso falhar, isso não pode esconder o erro original.
            envio = f"falhou: {type(erro).__name__}"

    registro = pd.DataFrame([{
        "execucao_id": EXECUCAO_ID,
        "criado_em": criado_em,
        "gravidade": gravidade,
        "titulo": titulo,
        "mensagem": mensagem,
        "canal": canal,
        "envio_externo": envio,
    }])
    config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[bigquery.SchemaField(n, t) for n, t in [
            ("execucao_id", "STRING"), ("criado_em", "TIMESTAMP"), ("gravidade", "STRING"),
            ("titulo", "STRING"), ("mensagem", "STRING"), ("canal", "STRING"),
            ("envio_externo", "STRING"),
        ]],
    )
    client.load_table_from_dataframe(registro, TABELA_ALERTAS, job_config=config).result()
    print(f"Aviso gravado em {TABELA_ALERTAS} | envio externo: {envio}")
    return envio


# Quantas linhas cada consulta leu
def linhas_de_entrada(job) -> tuple:
    """Devolve as tabelas lidas por uma consulta e a soma das suas linhas.

    O número de linhas vem dos metadados da tabela (get_table, sem custo). Na Bronze,
    que guarda várias cópias, usa-se a contagem da cópia que foi lida (LINHAS_DA_COPIA,
    Etapa 3.0.6).
    """
    copias = globals().get("LINHAS_DA_COPIA", {})
    total, lidas = 0, []
    for ref in job.referenced_tables or []:
        lidas.append(f"{ref.dataset_id}.{ref.table_id}")
        if ref.project == PROJECT_ID and ref.dataset_id == "bronze" and ref.table_id in copias:
            total += copias[ref.table_id]
        else:
            total += client.get_table(ref).num_rows or 0
    return total, ", ".join(sorted(set(lidas)))


# Diário de bordo
CAMPOS_DIARIO_TABELAS = [
    ("execucao_id", "STRING"), ("registrado_em", "TIMESTAMP"), ("camada", "STRING"),
    ("tabela", "STRING"), ("linhas", "INTEGER"), ("mb_estimados", "FLOAT"),
    ("mb_lidos", "FLOAT"), ("duracao_s", "FLOAT"), ("job_id", "STRING"),
    # Colunas novas entram no fim da tabela (ALLOW_FIELD_ADDITION).
    ("linhas_entrada", "INTEGER"), ("tabelas_lidas", "STRING"), ("status", "STRING"),
]
CAMPOS_DIARIO_EXECUCOES = [
    ("execucao_id", "STRING"), ("registrado_em", "TIMESTAMP"), ("tabelas_gravadas", "INTEGER"),
    ("linhas_gravadas", "INTEGER"), ("mb_lidos", "FLOAT"), ("duracao_s", "FLOAT"),
    ("regras_medidas", "INTEGER"), ("regras_que_passaram", "INTEGER"),
    ("bloqueantes_que_falharam", "INTEGER"), ("situacao", "STRING"),
    # Colunas novas entram no fim da tabela.
    ("etapa_da_falha", "STRING"), ("erro", "STRING"),
]


def _config_acrescimo(campos: list):
    """Carga em modo de acréscimo, aceitando colunas novas no fim da tabela."""
    return bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        schema=[bigquery.SchemaField(nome, tipo) for nome, tipo in campos],
    )


def _valores_ja_gravados(tabela: str, coluna: str, execucao: str) -> set:
    """Devolve os valores de uma coluna já gravados para esta execução (para não gravar
    duas vezes).
    """
    try:
        sql = f"SELECT DISTINCT {coluna} AS v FROM `{tabela}` WHERE execucao_id = '{execucao}'"
        return set(consulta(sql)["v"].dropna())
    except NotFound:
        return set()     # a tabela ainda não existe: é a primeira gravação


def medicoes_da_sessao() -> pd.DataFrame:
    """Junta as medições que a Bronze, a Silver e a Gold guardaram nesta sessão."""
    linhas = []
    for camada, lista in [("bronze", "metricas_bronze"), ("silver", "metricas_silver"),
                          ("gold", "metricas_gold")]:
        for m in globals().get(lista, []):
            if not m.get("job_id"):
                continue     # tabela que não foi gravada nesta execução
            nome = m["tabela"] if "." in m["tabela"] else f"{camada}.{m['tabela']}"
            linhas.append({
                "camada": camada,
                "tabela": nome,
                "linhas": m.get("linhas", m.get("linhas_gravadas")),
                "mb_estimados": m.get("mb_estimados"),
                "mb_lidos": m.get("mb_lidos"),
                "duracao_s": m.get("duracao_s"),
                "job_id": m.get("job_id"),
                "linhas_entrada": m.get("linhas_entrada"),
                "tabelas_lidas": m.get("tabelas_lidas"),
                "status": m.get("status", "OK"),
            })
    colunas = [nome for nome, _ in CAMPOS_DIARIO_TABELAS if nome not in ("execucao_id", "registrado_em")]
    df = pd.DataFrame(linhas, columns=colunas)
    # Se uma etapa rodou mais de uma vez na sessão, vale a última medição.
    df = df.drop_duplicates(subset="tabela", keep="last").reset_index(drop=True)
    for coluna in ["linhas", "linhas_entrada"]:
        df[coluna] = df[coluna].astype("Int64")        # aceita campo vazio
    for coluna in ["mb_estimados", "mb_lidos", "duracao_s"]:
        df[coluna] = df[coluna].astype("float64")
    return df


def registrar_execucao(situacao: str, etapa_da_falha: str = None, erro: str = None):
    """Grava o diário de bordo desta execução e devolve (tabelas, resumo).

    - Uma linha por tabela gravada em quality_reports.metricas_por_tabela
      (só as que ainda não estavam lá, pelo job_id).
    - Uma linha de resumo em quality_reports.execucoes, com a situação
      ("sucesso" ou "falha") e, na falha, a etapa e a mensagem do erro.
    """
    execucao = f"{EXECUCAO_ID}_teste" if MODO_DEMONSTRACAO else EXECUCAO_ID
    agora = datetime.now(timezone.utc)

    # Tabelas gravadas nesta sessão (na demonstração, nenhuma).
    df_tabelas = medicoes_da_sessao()
    if MODO_DEMONSTRACAO:
        df_tabelas = df_tabelas.iloc[0:0]
    novas = df_tabelas[~df_tabelas["job_id"].isin(_valores_ja_gravados(TABELA_METRICAS, "job_id", execucao))]
    if len(novas) > 0:
        novas = novas.copy()
        novas.insert(0, "execucao_id", execucao)
        novas.insert(1, "registrado_em", agora)
        client.load_table_from_dataframe(
            novas, TABELA_METRICAS, job_config=_config_acrescimo(CAMPOS_DIARIO_TABELAS)).result()

    # Regras medidas nesta execução (de todas as camadas), sem repetir a mesma regra.
    if CONFERENCIAS_DA_EXECUCAO and not MODO_DEMONSTRACAO:
        regras = (pd.concat(CONFERENCIAS_DA_EXECUCAO)
                  .drop_duplicates(subset=["camada", "tabela", "regra"], keep="last"))
        n_regras, n_passaram = len(regras), int(regras["passou"].sum())
        n_bloq = int(((regras["tipo"] == B_) & (~regras["passou"])).sum())
    else:
        n_regras = n_passaram = n_bloq = None

    df_execucao = pd.DataFrame([{
        "execucao_id": execucao,
        "registrado_em": agora,
        "tabelas_gravadas": int(len(df_tabelas)),
        "linhas_gravadas": int(df_tabelas["linhas"].sum()) if len(df_tabelas) else 0,
        "mb_lidos": round(float(df_tabelas["mb_lidos"].sum()), 2) if len(df_tabelas) else 0.0,
        "duracao_s": round(float(df_tabelas["duracao_s"].sum()), 2) if len(df_tabelas) else 0.0,
        "regras_medidas": n_regras,
        "regras_que_passaram": n_passaram,
        "bloqueantes_que_falharam": n_bloq,
        "situacao": situacao,
        "etapa_da_falha": etapa_da_falha,
        "erro": erro,
    }])
    for coluna in ["regras_medidas", "regras_que_passaram", "bloqueantes_que_falharam"]:
        df_execucao[coluna] = df_execucao[coluna].astype("Int64")

    if situacao == "sucesso" and "sucesso" in _valores_ja_gravados(TABELA_EXECUCOES, "situacao", execucao):
        print(f"A execução {execucao} já estava no diário como sucesso. Resumo não regravado.")
    else:
        client.load_table_from_dataframe(
            df_execucao, TABELA_EXECUCOES, job_config=_config_acrescimo(CAMPOS_DIARIO_EXECUCOES)).result()
        print(f"Diário gravado: {len(novas)} tabela(s) em {TABELA_METRICAS} "
              f"e 1 resumo ({situacao}) em {TABELA_EXECUCOES}")
    return df_tabelas, df_execucao


# Vigia de erros
def etapa_da_celula(shell) -> str:
    """Lê a primeira linha ("# ETAPA ...") da célula que estava rodando, para saber em que
    etapa o erro ocorreu.
    """
    entradas = shell.user_ns.get("In") or [""]
    for linha in entradas[-1].splitlines():
        achou = re.match(r"\s*#\s*(ETAPA\s+[\d.]+.*)", linha)
        if achou:
            return achou.group(1).strip()
    return "célula sem cabeçalho de etapa"


def vigia_de_erros(shell, etype, value, tb, tb_offset=None):
    """Função chamada pelo IPython sempre que uma célula dá erro: mostra o erro, envia o
    aviso e grava a falha.

    O try/except protege só o registro da falha; o erro original já foi exibido e a
    célula termina com erro.
    """
    shell.showtraceback((etype, value, tb), tb_offset=tb_offset)
    if not VIGIA_LIGADO:
        return None
    etapa = etapa_da_celula(shell)
    mensagem = f"{etype.__name__}: {value}"
    try:
        enviar_alerta("Pipeline de alfabetização: execução interrompida", f"Etapa: {etapa}\n{mensagem}")
        registrar_execucao("falha", etapa_da_falha=etapa, erro=mensagem[:1500])
    except Exception as erro_do_vigia:
        print(f"[vigia] não foi possível registrar a falha: {type(erro_do_vigia).__name__}: {erro_do_vigia}")
    return None


get_ipython().set_custom_exc((Exception,), vigia_de_erros)

print("Ferramentas de monitoramento prontas.")
print("Endereço de webhook configurado?", "Sim" if ler_endereco_do_webhook() else "Não")
print("Vigia de erros:", "ligado" if VIGIA_LIGADO else "desligado")
