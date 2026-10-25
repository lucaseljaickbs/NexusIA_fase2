# Gerado a partir de notebooks/NexusAI_techfase2.ipynb (mesmo código, separado por camada).
# A execução é feita pelo notebook, na ordem das etapas: as células compartilham
# variáveis (client, PROJECT_ID e as funções das Etapas 1.1 e 1.2).

# ETAPA 6.0 · Streaming: Kafka no Colab
# Instala e liga o Kafka 3.7.0 (com ZooKeeper) no Colab e cria o tópico com 3 partições.
# O Kafka só existe durante a sessão; os eventos aceitos ficam gravados no BigQuery.
# O download do archive.apache.org leva cerca de 10 min na primeira vez da sessão.
# O arquivo é conferido pelo SHA-512 publicado pela Apache.
import subprocess, os, time, logging

logging.getLogger("kafka").setLevel(logging.CRITICAL)

KAFKA_VERSION = "3.7.0"
SCALA_VERSION = "2.13"
KAFKA_DIR     = f"/opt/kafka_{SCALA_VERSION}-{KAFKA_VERSION}"
KAFKA_URL     = f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/kafka_{SCALA_VERSION}-{KAFKA_VERSION}.tgz"
KAFKA_ARCHIVE = "/tmp/kafka.tgz"
BIN           = f"{KAFKA_DIR}/bin"


def porta_aberta(porta):
    """Diz se algum programa já está escutando nesta porta."""
    return subprocess.run(["nc", "-z", "-w1", "localhost", str(porta)], capture_output=True).returncode == 0


def aguardar_porta(porta, nome, max_seg=35):
    """Espera o serviço ligar, testando a porta uma vez por segundo."""
    print(f"   Aguardando {nome}", end="")
    for _ in range(max_seg):
        time.sleep(1)
        print(".", end="", flush=True)
        if porta_aberta(porta):
            print(" OK")
            return True
    print(" ERRO")
    return False


# 1) Java: o Kafka é escrito em Java e precisa dele para rodar.
if subprocess.run(["java", "-version"], capture_output=True).returncode != 0:
    os.system("apt-get install -y -q default-jre-headless 2>/dev/null")
print("1/5 Java OK")

# 2) Netcat: ferramenta usada para testar se as portas já abriram.
if subprocess.run(["which", "nc"], capture_output=True).returncode != 0:
    os.system("apt-get install -y -q netcat-openbsd 2>/dev/null")
print("2/5 Netcat OK")

# 3) Instalação: usa a cópia da sessão atual; se não houver, o espelho; depois o Google
#    Drive; por último o archive.apache.org.
#    Qualquer cópia é conferida pelo SHA-512 oficial antes de ser usada.

# Espelho opcional do .tgz (ex.: arquivo de um GitHub Release). Vazio = não usa.
KAFKA_ESPELHO_URL = ""

USAR_CACHE_NO_DRIVE = False     # True: guarda e reaproveita o Kafka no seu Google Drive
PASTA_CACHE = "/content/drive/MyDrive/kafka_cache"
NOME_ARQUIVO = f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}.tgz"
COPIA_NO_DRIVE = f"{PASTA_CACHE}/{NOME_ARQUIVO}"
HASH_URL = f"{KAFKA_URL}.sha512"   # arquivo pequeno (~1 KB) com a impressão digital oficial


def baixar(url: str, destino: str) -> None:
    """Baixa o arquivo mostrando a barra de progresso na célula.

    os.system manda a saída do wget para o terminal do Colab, onde ela não aparece;
    get_ipython().system (o mesmo que "!wget") mostra a barra aqui.
    """
    get_ipython().system(f"wget --progress=bar:force:noscroll {url} -O {destino}")


def hash_oficial():
    """Baixa a impressão digital SHA-512 publicada pela Apache (arquivo de 1 KB)."""
    import re, urllib.request
    try:
        texto = urllib.request.urlopen(HASH_URL, timeout=60).read().decode()
    except Exception as erro:
        print(f"   Aviso: não consegui baixar o hash oficial ({erro}). Conferindo só o tamanho.")
        return None
    # O arquivo da Apache vem como "nome.tgz: 1A2B 3C4D ...". Tira o nome e
    # fica só com os caracteres hexadecimais, em minúsculas.
    return re.sub(r"[^0-9a-f]", "", texto.replace(NOME_ARQUIVO, "").lower())


def arquivo_confere(caminho, esperado):
    """Confere tamanho mínimo e, se houver hash oficial, a impressão digital."""
    import hashlib
    if not os.path.isfile(caminho) or os.path.getsize(caminho) < 50 * 1024**2:
        return False                     # ausente ou pequeno demais: download incompleto
    if esperado is None:
        return True
    h = hashlib.sha512()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloco)
    return h.hexdigest() == esperado


if os.path.isfile(f"{BIN}/zookeeper-server-start.sh"):
    print("3/5 Kafka já instalado nesta sessão")
else:
    esperado = hash_oficial()
    origem = None

    # b) Espelho (KAFKA_ESPELHO_URL).
    if KAFKA_ESPELHO_URL:
        print("3/5 Baixando o Kafka da cópia do grupo (GitHub)...")
        baixar(KAFKA_ESPELHO_URL, KAFKA_ARCHIVE)
        if arquivo_confere(KAFKA_ARCHIVE, esperado):
            origem = "cópia do grupo (GitHub)"
        else:
            print("   A cópia do grupo não passou na conferência. Tentando outro caminho.")

    # c) Cache no Google Drive.
    if origem is None and USAR_CACHE_NO_DRIVE:
        from google.colab import drive
        drive.mount("/content/drive")      # pede autorização na primeira vez
        if os.path.isfile(COPIA_NO_DRIVE):
            print("3/5 Copiando o Kafka do seu Google Drive...")
            os.system(f"cp {COPIA_NO_DRIVE} {KAFKA_ARCHIVE}")
            if arquivo_confere(KAFKA_ARCHIVE, esperado):
                origem = "seu Google Drive"

    # d) Servidor oficial.
    if origem is None:
        print(f"3/5 Baixando o Kafka {KAFKA_VERSION} de archive.apache.org (lento, cerca de 10 min)...")
        baixar(KAFKA_URL, KAFKA_ARCHIVE)
        if not arquivo_confere(KAFKA_ARCHIVE, esperado):
            raise RuntimeError("O download do Kafka falhou ou veio incompleto. Rode de novo.")
        origem = "archive.apache.org"
        if USAR_CACHE_NO_DRIVE:
            os.makedirs(PASTA_CACHE, exist_ok=True)
            os.system(f"cp {KAFKA_ARCHIVE} {COPIA_NO_DRIVE}")
            print(f"   Cópia guardada no Drive: {COPIA_NO_DRIVE}")

    os.system(f"tar -xzf {KAFKA_ARCHIVE} -C /opt/")
    conferido = "SHA-512 conferido" if esperado else "só tamanho conferido"
    tamanho_mb = os.path.getsize(KAFKA_ARCHIVE) / 1024**2
    print(f"3/5 Kafka instalado a partir de: {origem} ({tamanho_mb:.0f} MB, {conferido})")

# 4) Configura o servidor para aceitar conexões do próprio Colab.
os.system("pkill -f kafka.Kafka 2>/dev/null; pkill -f zookeeper 2>/dev/null")
time.sleep(3)
os.system("rm -rf /tmp/kafka-logs /tmp/zookeeper")   # começa com a fila vazia
props = f"{KAFKA_DIR}/config/server.properties"
os.system(f"sed -i '/^advertised\\.listeners/d;/^listeners=/d;/^log\\.dirs/d' {props}")
with open(props, "a") as f:
    f.write("\nlisteners=PLAINTEXT://0.0.0.0:9092\n")
    f.write("advertised.listeners=PLAINTEXT://localhost:9092\n")
    f.write("log.dirs=/tmp/kafka-logs\n")
print("4/5 Configuração OK")

# 5) Sobe o ZooKeeper e depois o broker.
subprocess.Popen(f"{BIN}/zookeeper-server-start.sh {KAFKA_DIR}/config/zookeeper.properties".split(),
                 stdout=open("/tmp/zookeeper.log", "w"), stderr=subprocess.STDOUT, preexec_fn=os.setsid)
if not aguardar_porta(2181, "ZooKeeper"):
    raise RuntimeError("O ZooKeeper não ligou. Veja /tmp/zookeeper.log")
subprocess.Popen(f"{BIN}/kafka-server-start.sh {props}".split(),
                 stdout=open("/tmp/kafka.log", "w"), stderr=subprocess.STDOUT, preexec_fn=os.setsid)
if not aguardar_porta(9092, "Kafka"):
    raise RuntimeError("O Kafka não ligou. Veja /tmp/kafka.log")

# Cliente Python do Kafka.
os.system("pip install -q kafka-python-ng==2.2.3")
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = "localhost:9092"
TOPICO = "medicoes-alfabetizacao"

# Cria o tópico: a fila onde as novas medições vão chegar.
# Com 3 partições, a mesma cidade vai sempre para a mesma partição.
for tentativa in range(10):
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        try:
            admin.create_topics([NewTopic(name=TOPICO, num_partitions=3, replication_factor=1)])
            print(f"5/5 Tópico '{TOPICO}' criado (3 partições)")
        except TopicAlreadyExistsError:
            print(f"5/5 Tópico '{TOPICO}' já existia")
        break
    except Exception:
        time.sleep(2)   # o servidor pode levar alguns segundos para aceitar pedidos
else:
    raise RuntimeError("Não foi possível criar o tópico no Kafka.")

print(f"\nKafka pronto. kafka-python-ng {versao('kafka-python-ng')}")


# ETAPA 6.1 · Streaming: produtor
# Cria os eventos "nova_medicao" de 2025, no mesmo formato de uma linha de bronze.municipio
# (ano, município, série, rede, taxa, média e proporções por nível, estas vazias),
# simulados a partir da taxa municipal de 2024
# de 200 cidades (seed fixa e consulta ordenada, para dar sempre a mesma amostra). Cada evento tem
# evento_id e criado_em; a hora de chegada é anotada pelo consumidor, para medir a
# latência de verdade.
# 8% dos eventos são enviados de novo (duplicatas, como acontece na entrega "pelo menos
# uma vez" do Kafka) e 6% recebem um defeito (taxa fora da faixa, cidade inexistente ou
# taxa vazia). O sorteio usa semente fixa para reproduzir a amostra e os defeitos.
import json, uuid, random
from datetime import datetime, timezone
import pandas as pd
from kafka import KafkaProducer

QTD_CIDADES = 200
PERC_REPETIDOS = 0.08   # 8% dos eventos serão reenviados
PERC_INVALIDOS = 0.06   # 6% dos eventos terão defeito

# As medições inventadas partem da taxa oficial de 2024 da rede municipal (lê só 2
# colunas de uma tabela pequena da Silver).
base_2024 = client.query(f"""
    SELECT id_municipio, taxa_alfabetizacao
    FROM `{S}.resultado_municipio`
    WHERE ano = 2024 AND comparavel_com_meta AND taxa_alfabetizacao IS NOT NULL
    ORDER BY id_municipio
""").to_dataframe()
amostra = base_2024.sample(n=QTD_CIDADES, random_state=42).reset_index(drop=True)


def criar_evento(id_municipio, taxa_2024, sorteio):
    """Monta uma nova medição: a taxa de 2024 com uma variação sorteada."""
    taxa = min(100.0, max(0.0, taxa_2024 + sorteio.gauss(3.0, 5.0)))
    return {
        "evento_id": str(uuid.uuid4()),
        "tipo_evento": "nova_medicao",
        "ano": 2025,
        "id_municipio": id_municipio,
        "serie": "2",
        "rede": "3",
        "taxa_alfabetizacao": round(taxa, 2),
        "media_portugues": None,                              # vazio, nunca zero
        **{coluna: None for coluna in COLUNAS_NIVEL},          # proporções por nível: vazias
        "criado_em": None,   # preenchido no momento do envio
    }


def estragar(evento, sorteio):
    """Coloca um defeito no evento, para testar a validação do consumidor."""
    defeito = sorteio.choice(["taxa impossivel", "cidade inexistente", "taxa faltando"])
    if defeito == "taxa impossivel":
        evento["taxa_alfabetizacao"] = 150.0
    elif defeito == "cidade inexistente":
        evento["id_municipio"] = "0000000"
    else:
        evento.pop("taxa_alfabetizacao")
    return evento


def montar_fila_de_envio(semente=42):
    """Lista, em ordem, os eventos que o produtor vai mandar (com repetidos e inválidos)."""
    sorteio = random.Random(semente)
    eventos = [criar_evento(r.id_municipio, r.taxa_alfabetizacao, sorteio) for r in amostra.itertuples()]
    for e in sorteio.sample(eventos, int(len(eventos) * PERC_INVALIDOS)):
        estragar(e, sorteio)
    fila = list(eventos)
    # Repetidos: o mesmo evento volta mais adiante na fila.
    for e in sorteio.sample(eventos, int(len(eventos) * PERC_REPETIDOS)):
        fila.insert(sorteio.randint(fila.index(e) + 1, len(fila)), e)
    return fila


def criar_produtor():
    """Produtor que espera a confirmação do Kafka a cada envio (acks=all) e tenta de novo
    em caso de erro.
    """
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",     # só considera enviado quando o Kafka confirma
        retries=3,      # tenta de novo em caso de erro
    )


fila_previa = montar_fila_de_envio()
ids = [e["evento_id"] for e in fila_previa]
print(f"Cidades sorteadas: {QTD_CIDADES}")
print(f"Mensagens que serão enviadas: {len(fila_previa)}")
print(f"  eventos diferentes: {len(set(ids))}")
print(f"  repetidos plantados: {len(ids) - len(set(ids))}")
print(f"  inválidos plantados: {int(QTD_CIDADES * PERC_INVALIDOS)}")
# Exemplo: o primeiro evento sem defeito. criado_em ainda está vazio, porque o produtor
# só o preenche na hora do envio (Etapa 6.2).
exemplo = next(e for e in fila_previa
               if e.get("taxa_alfabetizacao") is not None
               and e["taxa_alfabetizacao"] <= 100 and e["id_municipio"] != "0000000")
print("\nExemplo de evento válido (criado_em é preenchido no envio):")
print(json.dumps(exemplo, indent=2, ensure_ascii=False))


# ETAPA 6.2 · Streaming: produção e consumo em micro-lotes
# Envia e recebe os eventos em micro-lotes. O produtor roda em uma thread; o consumidor
# só confirma a leitura (commit) depois de gravar, o que garante a entrega "pelo menos
# uma vez".
# Em cada lote: remove repetidos por evento_id, valida, grava os aceitos em
# bronze.streaming_municipio e os rejeitados (com o motivo) em
# quality_reports.eventos_rejeitados, e registra as métricas do lote.
import threading, time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from google.api_core.exceptions import NotFound

TAMANHO_DO_LOTE = 25
PAUSA_ENTRE_ENVIOS = (0.02, 0.08)   # segundos, sorteado a cada envio
TABELA_EVENTOS = f"{B}.streaming_municipio"
TABELA_REJEITADOS = f"{Q}.eventos_rejeitados"
TABELA_METRICAS_STREAMING = f"{Q}.metricas_streaming"
EXECUCAO_STREAMING = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# Dados de referência para validar os eventos
# Lista de cidades que existem (list_rows lê a tabela sem custo de consulta).
cidades_validas = {
    r["id_municipio"] for r in client.list_rows(f"{S}.territorio_municipio",
                                               selected_fields=[bigquery.SchemaField("id_municipio", "STRING")])
}
# Eventos já gravados em execuções anteriores (para não aceitar de novo).
try:
    ja_vistos = set(client.query(f"SELECT DISTINCT evento_id FROM `{TABELA_EVENTOS}`").to_dataframe().evento_id)
except NotFound:
    ja_vistos = set()   # primeira execução: a tabela ainda não existe
print(f"Cidades conhecidas: {len(cidades_validas)} | eventos já gravados antes: {len(ja_vistos)}")


def motivo_de_rejeicao(evento):
    """Devolve o motivo se o evento for inválido; None se estiver bom."""
    for campo in ["evento_id", "id_municipio", "ano", "serie", "rede", "taxa_alfabetizacao", "criado_em"]:
        if evento.get(campo) is None:
            return f"campo obrigatorio vazio: {campo}"
    if not 0 <= evento["taxa_alfabetizacao"] <= 100:
        return "taxa fora de 0 a 100"
    if evento["id_municipio"] not in cidades_validas:
        return "cidade inexistente"
    if not 2023 <= int(evento["ano"]) <= 2030:
        return "ano fora do periodo"
    return None


# Testes da validação, antes de ela decidir o destino de qualquer evento.
# Uma cidade conhecida com taxa válida passa; cada defeito tem o seu motivo.
_cidade_ok = next(iter(cidades_validas))
_base = {"evento_id": "t", "id_municipio": _cidade_ok, "ano": 2025, "serie": "2", "rede": "3",
         "taxa_alfabetizacao": 50.0, "criado_em": "2025-01-01T00:00:00+00:00"}
assert motivo_de_rejeicao(_base) is None
assert motivo_de_rejeicao({**_base, "taxa_alfabetizacao": 150.0}) == "taxa fora de 0 a 100"
assert motivo_de_rejeicao({**_base, "id_municipio": "0000000"}) == "cidade inexistente"
assert motivo_de_rejeicao({**_base, "taxa_alfabetizacao": None}).startswith("campo obrigatorio vazio")
assert motivo_de_rejeicao({**_base, "ano": 1999}) == "ano fora do periodo"
print("Testes da validação de eventos: 5 de 5 passaram")


def acrescentar(df, tabela, campos):
    """Grava em modo de acréscimo, por carga de arquivo (sem custo)."""
    if df.empty:
        return
    config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[bigquery.SchemaField(n, t) for n, t in campos],
    )
    client.load_table_from_dataframe(df, tabela, job_config=config).result()


# Mesmas colunas de bronze.municipio, mais as de controle do evento.
COLUNAS_FLOAT_EVENTO = ["media_portugues"] + COLUNAS_NIVEL
CAMPOS_ENTRADA = (["evento_id", "tipo_evento", "ano", "id_municipio", "serie", "rede",
                   "taxa_alfabetizacao"] + COLUNAS_FLOAT_EVENTO)
CAMPOS_EVENTOS = ([("evento_id", "STRING"), ("tipo_evento", "STRING"), ("ano", "INTEGER"),
                   ("id_municipio", "STRING"), ("serie", "STRING"), ("rede", "STRING"),
                   ("taxa_alfabetizacao", "FLOAT")]
                  + [(c, "FLOAT") for c in COLUNAS_FLOAT_EVENTO]
                  + [("criado_em", "TIMESTAMP"), ("recebido_em", "TIMESTAMP"), ("latencia_s", "FLOAT"),
                     ("particao_kafka", "INTEGER"), ("offset_kafka", "INTEGER"), ("lote", "INTEGER"),
                     ("origem", "STRING"), ("_execucao_streaming", "STRING")])
CAMPOS_REJEITADOS = [("evento_id", "STRING"), ("motivo", "STRING"), ("conteudo", "STRING"),
                     ("recebido_em", "TIMESTAMP"), ("lote", "INTEGER"), ("_execucao_streaming", "STRING")]
CAMPOS_METRICAS = [("execucao", "STRING"), ("lote", "INTEGER"), ("recebidos", "INTEGER"),
                   ("aceitos", "INTEGER"), ("repetidos", "INTEGER"), ("invalidos", "INTEGER"),
                   ("latencia_media_s", "FLOAT"), ("latencia_maxima_s", "FLOAT"),
                   ("segundos_para_gravar", "FLOAT"), ("registrado_em", "TIMESTAMP")]


# Produtor em thread
envio_terminou = threading.Event()


def produzir():
    """Manda a fila de eventos, um por vez, com pequenas pausas."""
    produtor = criar_produtor()
    sorteio = random.Random(7)
    for evento in montar_fila_de_envio():
        # O horário de criação é anotado na hora do primeiro envio. O evento repetido é
        # o mesmo evento enviado de novo, então chega com o mesmo evento_id e o mesmo
        # horário.
        if evento["criado_em"] is None:
            evento["criado_em"] = datetime.now(timezone.utc).isoformat()
        produtor.send(TOPICO, key=evento.get("id_municipio"), value=evento)
        time.sleep(sorteio.uniform(*PAUSA_ENTRE_ENVIOS))
    produtor.flush()
    envio_terminou.set()


# Consumidor
consumidor = KafkaConsumer(
    TOPICO,
    bootstrap_servers=BOOTSTRAP,
    group_id="pipeline-alfabetizacao",
    auto_offset_reset="earliest",
    enable_auto_commit=False,     # confirmação manual, só depois de gravar
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)
threading.Thread(target=produzir, daemon=True).start()
print(f"Produtor iniciado. Consumindo em lotes de até {TAMANHO_DO_LOTE} eventos...\n")

metricas_lotes, lote, vazios = [], 0, 0
while True:
    pacote = consumidor.poll(timeout_ms=2000, max_records=TAMANHO_DO_LOTE)
    mensagens = [m for msgs in pacote.values() for m in msgs]
    if not mensagens:
        vazios += 1
        if envio_terminou.is_set() and vazios >= 2:
            break           # o produtor acabou e a fila esvaziou
        continue
    vazios, lote = 0, lote + 1
    recebido_em = datetime.now(timezone.utc)
    aceitos, rejeitados, repetidos = [], [], 0

    for m in mensagens:
        e = m.value
        if e.get("evento_id") in ja_vistos:
            repetidos += 1
            rejeitados.append({"evento_id": e.get("evento_id"), "motivo": "repetido",
                               "conteudo": json.dumps(e), "recebido_em": recebido_em, "lote": lote})
            continue
        motivo = motivo_de_rejeicao(e)
        ja_vistos.add(e.get("evento_id"))
        if motivo:
            rejeitados.append({"evento_id": e.get("evento_id"), "motivo": motivo,
                               "conteudo": json.dumps(e), "recebido_em": recebido_em, "lote": lote})
            continue
        criado = pd.Timestamp(e["criado_em"])
        aceitos.append({**{k: e.get(k) for k in CAMPOS_ENTRADA},
                        "criado_em": criado, "recebido_em": recebido_em,
                        "latencia_s": round((recebido_em - criado).total_seconds(), 3),
                        "particao_kafka": m.partition, "offset_kafka": m.offset, "lote": lote,
                        "origem": "simulado"})

    inicio = time.time()
    df_aceitos = pd.DataFrame(aceitos)
    df_rejeitados = pd.DataFrame(rejeitados)
    if not df_aceitos.empty:
        for coluna in COLUNAS_FLOAT_EVENTO:     # coluna toda vazia chega como texto: vira número
            df_aceitos[coluna] = pd.to_numeric(df_aceitos[coluna]).astype("float64")
    for df in (df_aceitos, df_rejeitados):
        if not df.empty:
            df["_execucao_streaming"] = EXECUCAO_STREAMING
    acrescentar(df_aceitos, TABELA_EVENTOS, CAMPOS_EVENTOS)
    acrescentar(df_rejeitados, TABELA_REJEITADOS, CAMPOS_REJEITADOS)
    consumidor.commit()           # só agora a leitura é confirmada
    segundos = round(time.time() - inicio, 2)

    invalidos = len(rejeitados) - repetidos
    lat = df_aceitos.latencia_s if not df_aceitos.empty else pd.Series(dtype=float)
    metricas_lotes.append({"execucao": EXECUCAO_STREAMING, "lote": lote, "recebidos": len(mensagens),
                           "aceitos": len(aceitos), "repetidos": repetidos, "invalidos": invalidos,
                           "latencia_media_s": round(lat.mean(), 3) if len(lat) else None,
                           "latencia_maxima_s": round(lat.max(), 3) if len(lat) else None,
                           "segundos_para_gravar": segundos, "registrado_em": datetime.now(timezone.utc)})
    print(f"Lote {lote:2d}: {len(mensagens):3d} recebidos | {len(aceitos):3d} aceitos | "
          f"{repetidos} repetidos | {invalidos} inválidos | gravado em {segundos}s")

consumidor.close()
df_metricas_streaming = pd.DataFrame(metricas_lotes)
for coluna in ["latencia_media_s", "latencia_maxima_s"]:
    df_metricas_streaming[coluna] = df_metricas_streaming[coluna].astype(float)   # lote sem aceitos fica vazio
acrescentar(df_metricas_streaming, TABELA_METRICAS_STREAMING, CAMPOS_METRICAS)

print("\n=== RESUMO DO STREAMING ===")
print(f"Execução: {EXECUCAO_STREAMING} | lotes: {lote}")
print(df_metricas_streaming[["recebidos", "aceitos", "repetidos", "invalidos"]].sum().to_string())


# ETAPA 6.3.1 · Streaming: validação da Bronze
# Mesmo caminho do lote: a Bronze é conferida ANTES de a Silver ser criada. Uma regra
# bloqueante reprovada interrompe a execução. O resultado é gravado em
# quality_reports.conferencias antes da decisão.
totais_stream = df_metricas_streaming[["recebidos", "aceitos", "repetidos", "invalidos"]].sum()
FONTE_BRONZE_STREAM = f"`{B}.streaming_municipio`"
FONTE_SILVER_STREAM = f"`{S}.streaming_municipio`"


def um_valor(sql):
    """Roda uma consulta que devolve um único número."""
    return consulta(sql).iloc[0, 0]


def regra_de_origem():
    """Regra comum: toda linha das tabelas de streaming precisa ser 'simulado'."""
    return regra("linha sem origem 'simulado'", "Consistência",
                 "COUNTIF(origem IS NULL OR origem != 'simulado')")


resultados_bronze_stream = []
# Bronze de streaming
resultados_bronze_stream += medir("streaming", "bronze.streaming_municipio", FONTE_BRONZE_STREAM, [
    regra("evento_id repetido", "Unicidade", "COUNT(*) - COUNT(DISTINCT evento_id)"),
    regra_de_origem(),
    regra("evento sem horário de criação ou de chegada", "Completude",
          "COUNTIF(criado_em IS NULL OR recebido_em IS NULL)"),
    regra("latência negativa", "Validade", "COUNTIF(latencia_s < 0)"),
])
resultados_bronze_stream += [
    conferencia_direta(
        "streaming", "bronze.streaming_municipio",
        "recebidas diferentes de aceitas + repetidas + invalidas", "Completude",
        abs(int(totais_stream.recebidos)
            - int(totais_stream.aceitos + totais_stream.repetidos + totais_stream.invalidos))),
    conferencia_direta(
        "streaming", "bronze.streaming_municipio",
        "aceitas diferentes das linhas gravadas nesta execucao", "Completude",
        abs(int(totais_stream.aceitos) - int(um_valor(
            f"SELECT COUNT(*) FROM {FONTE_BRONZE_STREAM} "
            f"WHERE _execucao_streaming = '{EXECUCAO_STREAMING}'")))),
]


df_conf_bronze_stream = gravar_conferencias(resultados_bronze_stream, cenario="streaming")
mostrar_conferencias(df_conf_bronze_stream)
validacao_de_qualidade(df_conf_bronze_stream, "6.3 validação do streaming (Bronze)")


# ETAPA 6.3.2 · Streaming: Silver
# Mesmo caminho do lote (Bronze, Silver e Gold), em tabelas separadas de streaming.
# Silver: silver.streaming_municipio, com a mesma receita de silver.resultado_municipio
# (códigos traduzidos pelo dicionário, nome, UF e região do município). Fica com a
# medição mais recente de cada município, ano, série e rede.
# (A Gold de streaming vem na célula seguinte, depois de a Silver ser validada.)
# Gold: quatro tabelas de streaming, uma para cada resposta da Gold que o resultado do
# município alimenta: indicador_municipio, meta_vs_resultado, evolucao_temporal e
# desigualdade_regional. validacao_taxa (não há alunos de 2025) e base_modelagem (usa só
# 2023 e 2024) não mudam com os eventos e não têm versão de streaming.
# As tabelas do lote só são lidas, nunca gravadas. Tudo leva origem = "simulado".
# Os números desta Gold não são análise: servem para levar o evento até a camada
# analítica final.
metricas_streaming_tabelas = []
REDE_MUN = REDE_COM_META["municipio"]


def gravar_streaming(destino, sql):
    """Simula o custo, grava (substituindo a tabela) e devolve as linhas."""
    mb_estimados = simular_custo(sql)
    config = bigquery.QueryJobConfig(destination=destino,
                                     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = client.query(sql, job_config=config)
    job.result()
    linhas = client.get_table(destino).num_rows
    entrada, lidas = linhas_de_entrada(job)
    metricas_streaming_tabelas.append({
        "tabela": destino.split(".", 1)[1],
        "linhas": linhas,
        "linhas_entrada": entrada,
        "tabelas_lidas": lidas,
        "status": "OK",
        "mb_estimados": mb_estimados,
        "mb_lidos": round(job.total_bytes_processed / 1024**2, 2),
        "duracao_s": round((job.ended - job.started).total_seconds(), 2),
        "job_id": job.job_id,
    })
    print(f"[{destino.split('.', 1)[1]}] gravada: {linhas} linhas | simulação: {mb_estimados} MB")
    return linhas


METADADOS_GOLD_STREAMING = (
    "'simulado' AS origem,\n"
    "        CURRENT_TIMESTAMP() AS _gold_processed_at,\n"
    f"        '{PIPELINE_VERSION}' AS _pipeline_version"
)
niveis_stream = ",\n        ".join(f"o.{c}" for c in COLUNAS_NIVEL)
todos_niveis_stream = " AND ".join(f"o.{c} IS NOT NULL" for c in COLUNAS_NIVEL)

# Silver
gravar_streaming(f"{S}.streaming_municipio", f"""
    WITH origem AS (
        -- Uma medição por município, ano, série e rede: fica a mais recente.
        SELECT * EXCEPT (ordem)
        FROM (
            SELECT
                e.*,
                ROW_NUMBER() OVER (
                    PARTITION BY e.ano, TRIM(e.id_municipio), TRIM(e.serie), TRIM(e.rede)
                    ORDER BY e.criado_em DESC, e.recebido_em DESC
                ) AS ordem
            FROM `{B}.streaming_municipio` AS e
        )
        WHERE ordem = 1
    )
    SELECT
        o.ano,
        TRIM(o.id_municipio) AS id_municipio,
        t.nome_municipio,
        t.sigla_uf,
        t.nome_uf,
        t.nome_regiao,
        TRIM(o.serie) AS serie_codigo,
        {nome_traduzido('d_serie', 'serie')} AS serie_nome,
        TRIM(o.rede) AS rede_codigo,
        {nome_traduzido('d_rede', 'rede')} AS rede_nome,
        IFNULL(TRIM(o.rede) = '{REDE_MUN}', FALSE) AS comparavel_com_meta,
        o.taxa_alfabetizacao,
        o.media_portugues,
        {niveis_stream},
        ({todos_niveis_stream}) AS tem_distribuicao_niveis,
        o.evento_id,
        o.criado_em,
        o.recebido_em,
        o.latencia_s,
        o.origem,
        CURRENT_TIMESTAMP() AS _silver_processed_at,
        '{PIPELINE_VERSION}' AS _pipeline_version
    FROM origem AS o
    {juncao_dicionario('d_serie', 'municipio', 'serie')}
    {juncao_dicionario('d_rede', 'municipio', 'rede')}
    LEFT JOIN `{S}.territorio_municipio` AS t
        ON t.id_municipio = TRIM(o.id_municipio)
""")


# Validação da Silver de streaming, ANTES de a Gold ser criada.
# Tipos das chaves de cruzamento (INFORMATION_SCHEMA lê só metadados).
tipos_chaves = consulta(f"""
    SELECT table_name, column_name, data_type
    FROM `{S}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name IN ('streaming_municipio', 'territorio_municipio', 'resultado_municipio')
      AND column_name IN ('id_municipio', 'ano')
""")


def tipos_diferentes(coluna, tabelas):
    """Tipos diferentes a mais, mais colunas não encontradas (esperado: zero)."""
    achadas = tipos_chaves[(tipos_chaves["column_name"] == coluna)
                           & (tipos_chaves["table_name"].isin(tabelas))]
    return max(achadas["data_type"].nunique() - 1, 0) + (len(tabelas) - len(achadas))


resultados_silver_stream = []
# Silver de streaming
soma_niveis_stream = " + ".join(f"proporcao_aluno_nivel_{n}" for n in range(9))
resultados_silver_stream += medir("streaming", "silver.streaming_municipio", FONTE_SILVER_STREAM, [
    regra("linhas repetidas na chave", "Unicidade",
          repetidas("ano", "id_municipio", "serie_codigo", "rede_codigo")),
    regra("codigo sem traducao", "Validade",
          "COUNTIF(rede_nome = 'desconhecido' OR serie_nome = 'desconhecido')"),
    regra("serie diferente de 2 (2o ano)", "Validade",
          "COUNTIF(serie_codigo IS NULL OR serie_codigo != '2')"),
    regra("taxa fora de 0 a 100", "Validade",
          "COUNTIF(taxa_alfabetizacao < 0 OR taxa_alfabetizacao > 100)"),
    regra("ano fora do periodo (2023 a 2030)", "Validade",
          "COUNTIF(ano < 2023 OR ano > 2030)"),
    regra("lugar sem nome", "Completude", "COUNTIF(nome_municipio IS NULL)"),
    regra_de_origem(),
    regra("proporcoes por nivel nao somam 100", "Consistência",
          f"COUNTIF(tem_distribuicao_niveis AND ABS(({soma_niveis_stream}) - 100) > 0.5)", tipo=I_),
    regra("codigo da cidade sem 7 digitos", "Validade", CODIGO_CIDADE_INVALIDO, tipo=I_),
])
linhas_bronze_unicas = um_valor(f"""
    SELECT COUNT(*) FROM (
        SELECT DISTINCT ano, TRIM(id_municipio) AS id_municipio,
                        TRIM(serie) AS serie, TRIM(rede) AS rede
        FROM {FONTE_BRONZE_STREAM}
    )
""")
linhas_silver = um_valor(f"SELECT COUNT(*) FROM {FONTE_SILVER_STREAM}")
com_territorio = um_valor(f"""
    SELECT ROUND(100 * SAFE_DIVIDE(COUNTIF(nome_municipio IS NOT NULL), COUNT(*)), 2)
    FROM {FONTE_SILVER_STREAM}
""")
resultados_silver_stream += [
    conferencia_direta("streaming", "silver.streaming_municipio",
                       "linhas diferentes da Bronze (chaves distintas)", "Completude",
                       abs(int(linhas_bronze_unicas) - int(linhas_silver))),
    conferencia_direta("streaming", "silver.streaming_municipio",
                       "tipo diferente na chave id_municipio", "Validade",
                       tipos_diferentes("id_municipio", ["streaming_municipio",
                                                         "territorio_municipio",
                                                         "resultado_municipio"])),
    conferencia_direta("streaming", "silver.streaming_municipio",
                       "tipo diferente na chave ano", "Validade",
                       tipos_diferentes("ano", ["streaming_municipio", "resultado_municipio"])),
    conferencia_direta("cruzamento", "streaming_municipio com territorio_municipio",
                       "linhas que acharam par", "Consistência", com_territorio,
                       limite=100, sentido="no minimo", unidade="%"),
]


df_conf_silver_stream = gravar_conferencias(resultados_silver_stream, cenario="streaming")
mostrar_conferencias(df_conf_silver_stream)
validacao_de_qualidade(df_conf_silver_stream, "6.3 validação do streaming (Silver)")


# ETAPA 6.3.3 · Streaming: Gold
# Quatro tabelas de streaming, uma para cada resposta da Gold que o resultado do município
# alimenta. validacao_taxa e base_modelagem não mudam com os eventos e não têm versão de
# streaming. As tabelas do lote só são lidas. Tudo leva origem = "simulado".
# Depois de gravar, a Gold de streaming é validada; os números dela não são análise.
# Gold 1 · streaming_indicador_municipio (sem alunos em 2025: essas colunas ficam vazias)
niveis_gold_stream = ",\n        ".join(f"s.{c}" for c in COLUNAS_NIVEL)
gravar_streaming(f"{G}.streaming_indicador_municipio", f"""
    SELECT
        s.ano,
        s.id_municipio,
        s.nome_municipio,
        s.sigla_uf,
        s.nome_uf,
        s.nome_regiao,
        s.rede_codigo,
        s.rede_nome,
        s.comparavel_com_meta,
        s.taxa_alfabetizacao,
        s.media_portugues,
        {niveis_gold_stream},
        s.tem_distribuicao_niveis,
        CAST(NULL AS INT64)   AS alunos_total,
        CAST(NULL AS INT64)   AS alunos_avaliados,
        CAST(NULL AS FLOAT64) AS percentual_avaliados,
        FALSE                 AS tem_dados_de_alunos,
        {METADADOS_GOLD_STREAMING}
    FROM `{S}.streaming_municipio` AS s
""")

# Gold 2 · streaming_meta_vs_resultado (a meta do ano vem da trajetória oficial, só lida)
gravar_streaming(f"{G}.streaming_meta_vs_resultado", f"""
    WITH comparado AS (
        SELECT
            s.ano,
            s.id_municipio     AS codigo_local,
            s.nome_municipio   AS nome_local,
            s.sigla_uf,
            s.nome_regiao,
            s.rede_codigo,
            s.taxa_alfabetizacao,
            m.meta             AS meta_do_ano,
            m30.meta           AS meta_2030
        FROM `{S}.streaming_municipio` AS s
        LEFT JOIN (
            SELECT id_municipio, ano_meta, meta
            FROM `{S}.meta_municipio_trajetoria`
            WHERE ano_referencia = 2024 AND rede_codigo = '{REDE_MUN}'
        ) AS m
            ON m.id_municipio = s.id_municipio
           AND m.ano_meta = s.ano
        LEFT JOIN (
            SELECT id_municipio, meta
            FROM `{S}.meta_municipio_trajetoria`
            WHERE ano_referencia = 2024 AND rede_codigo = '{REDE_MUN}'
              AND ano_meta = 2030
        ) AS m30
            ON m30.id_municipio = s.id_municipio
        WHERE s.comparavel_com_meta
    )
    SELECT
        'municipio' AS nivel,
        ano,
        codigo_local,
        nome_local,
        sigla_uf,
        nome_regiao,
        rede_codigo,
        taxa_alfabetizacao,
        'evento simulado'     AS fonte_da_taxa,
        CAST(NULL AS FLOAT64) AS taxa_na_tabela_de_meta,
        FALSE                 AS taxas_divergem,
        meta_do_ano,
        meta_2030,
        CASE
            WHEN taxa_alfabetizacao IS NULL AND meta_do_ano IS NULL THEN 'sem resultado e sem meta'
            WHEN taxa_alfabetizacao IS NULL THEN 'sem resultado'
            WHEN meta_do_ano IS NULL THEN 'sem meta'
            WHEN taxa_alfabetizacao >= meta_do_ano THEN 'atingiu'
            ELSE 'nao atingiu'
        END AS situacao_meta,
        ROUND(taxa_alfabetizacao - meta_do_ano, 2) AS distancia_meta_do_ano,
        ROUND(taxa_alfabetizacao - meta_2030, 2)   AS distancia_meta_2030,
        {METADADOS_GOLD_STREAMING}
    FROM comparado
""")

# Gold 3 · streaming_evolucao_temporal (variação em relação à taxa oficial do ano anterior)
gravar_streaming(f"{G}.streaming_evolucao_temporal", f"""
    WITH anterior AS (
        SELECT id_municipio, ano, taxa_alfabetizacao AS taxa_ano_anterior
        FROM `{S}.resultado_municipio`
        WHERE rede_codigo = '{REDE_MUN}'
    )
    SELECT
        m.nivel,
        m.codigo_local,
        m.nome_local,
        m.sigla_uf,
        m.nome_regiao,
        m.rede_codigo,
        m.ano,
        'simulado'                AS tipo_de_linha,
        m.taxa_alfabetizacao,
        m.fonte_da_taxa,
        m.meta_do_ano,
        2024                      AS ano_de_publicacao_da_meta,
        a.taxa_ano_anterior,
        ROUND(m.taxa_alfabetizacao - a.taxa_ano_anterior, 2) AS variacao_anual,
        m.meta_2030,
        ROUND(m.meta_2030 - m.taxa_alfabetizacao, 2)         AS falta_para_meta_2030,
        2030 - m.ano              AS anos_ate_2030,
        CASE
            WHEN m.taxa_alfabetizacao IS NULL OR m.meta_2030 IS NULL OR m.ano >= 2030 THEN NULL
            WHEN m.taxa_alfabetizacao >= m.meta_2030 THEN 0
            ELSE ROUND((m.meta_2030 - m.taxa_alfabetizacao) / (2030 - m.ano), 2)
        END AS cenario_ritmo_necessario_por_ano,
        {METADADOS_GOLD_STREAMING}
    FROM `{G}.streaming_meta_vs_resultado` AS m
    LEFT JOIN anterior AS a
        ON a.id_municipio = m.codigo_local
       AND a.ano = m.ano - 1
""")

# Gold 4 · streaming_desigualdade_regional (mesmo resumo do lote, com as cidades simuladas)
gravar_streaming(f"{G}.streaming_desigualdade_regional", f"""
    WITH cidades AS (
        SELECT
            m.ano,
            m.codigo_local AS id_municipio,
            m.sigla_uf,
            m.nome_regiao,
            m.taxa_alfabetizacao AS taxa,
            m.situacao_meta,
            CAST(NULL AS INT64) AS alunos_avaliados
        FROM `{G}.streaming_meta_vs_resultado` AS m
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
        ROUND(taxa_maxima - taxa_minima, 2) AS distancia_entre_extremos,
        media_simples_entre_cidades,
        cidades_atingiram,
        cidades_nao_atingiram,
        cidades_sem_meta,
        ROUND(100 * SAFE_DIVIDE(cidades_atingiram,
                                cidades_atingiram + cidades_nao_atingiram), 2) AS percentual_que_atingiu,
        RANK() OVER (PARTITION BY agrupamento, ano ORDER BY taxa_do_meio DESC) AS posicao_pela_taxa_do_meio,
        {METADADOS_GOLD_STREAMING}
    FROM resumos
""")


# Validação da Gold de streaming.
resultados_gold_stream = []
# Gold de streaming
resultados_gold_stream += medir("streaming", "gold.streaming_indicador_municipio",
                              f"`{G}.streaming_indicador_municipio`", [
    regra("linhas repetidas na chave", "Unicidade", repetidas("ano", "id_municipio", "rede_codigo")),
    regra_de_origem(),
])
resultados_gold_stream.append(conferencia_direta(
    "streaming", "gold.streaming_indicador_municipio", "linhas diferentes da Silver de streaming",
    "Consistência",
    abs(int(um_valor(f"SELECT COUNT(*) FROM `{G}.streaming_indicador_municipio`")) - int(linhas_silver))))

resultados_gold_stream += medir("streaming", "gold.streaming_meta_vs_resultado",
                              f"`{G}.streaming_meta_vs_resultado`", [
    regra("linhas repetidas na chave", "Unicidade", repetidas("nivel", "ano", "codigo_local")),
    regra("situacao fora da lista prevista", "Validade", (
        "COUNTIF(situacao_meta NOT IN ('atingiu', 'nao atingiu', 'sem meta', "
        "'sem resultado', 'sem resultado e sem meta'))")),
    regra("atingiu ou nao atingiu sem taxa ou sem meta", "Consistência", (
        "COUNTIF(situacao_meta IN ('atingiu', 'nao atingiu') "
        "AND (taxa_alfabetizacao IS NULL OR meta_do_ano IS NULL))")),
    regra_de_origem(),
])
com_meta = um_valor(f"""
    SELECT ROUND(100 * SAFE_DIVIDE(COUNTIF(meta_do_ano IS NOT NULL), COUNT(*)), 2)
    FROM `{G}.streaming_meta_vs_resultado`
""")
resultados_gold_stream.append(conferencia_direta(
    "cruzamento", "streaming_municipio com meta_municipio_trajetoria", "linhas que acharam par",
    "Consistência", com_meta, limite=85, sentido="no minimo", unidade="%"))

resultados_gold_stream += medir("streaming", "gold.streaming_evolucao_temporal",
                              f"`{G}.streaming_evolucao_temporal`", [
    regra("linhas repetidas na chave", "Unicidade", repetidas("nivel", "codigo_local", "ano")),
    regra("lugar sem nome", "Completude", "COUNTIF(nome_local IS NULL)"),
    regra_de_origem(),
    regra("variacao sem taxa oficial do ano anterior", "Completude",
          "COUNTIF(taxa_ano_anterior IS NULL)", tipo=I_),
])

resultados_gold_stream += medir("streaming", "gold.streaming_desigualdade_regional",
                              f"`{G}.streaming_desigualdade_regional`", [
    regra("agrupamentos com totais de cidades diferentes", "Consistência", (
        "ABS(SUM(IF(agrupamento = 'regiao', cidades, 0)) - SUM(IF(agrupamento = 'brasil', cidades, 0))) "
        "+ ABS(SUM(IF(agrupamento = 'uf', cidades, 0)) - SUM(IF(agrupamento = 'brasil', cidades, 0)))")),
    regra_de_origem(),
])

# As seis tabelas oficiais do lote não podem ter recebido nada simulado.
TABELAS_OFICIAIS_GOLD = ["indicador_municipio", "meta_vs_resultado", "evolucao_temporal",
                         "validacao_taxa", "desigualdade_regional", "base_modelagem"]
simuladas_nas_oficiais = um_valor("SELECT SUM(n) FROM (\n" + "\nUNION ALL\n".join(
    f"SELECT COUNTIF(origem != 'oficial') AS n FROM `{G}.{t}`" for t in TABELAS_OFICIAIS_GOLD
) + "\n)")
resultados_gold_stream.append(conferencia_direta(
    "streaming", "gold (6 tabelas oficiais)", "linha que nao e oficial nas tabelas oficiais",
    "Consistência", simuladas_nas_oficiais))


df_conf_gold_stream = gravar_conferencias(resultados_gold_stream, cenario="streaming")
mostrar_conferencias(df_conf_gold_stream)
validacao_de_qualidade(df_conf_gold_stream, "6.3 validação do streaming (Gold)")
df_conferencias_streaming = pd.concat([df_conf_bronze_stream, df_conf_silver_stream, df_conf_gold_stream],
                                      ignore_index=True)
print(f"\n{len(df_conferencias_streaming)} regras de streaming aprovadas (Bronze, Silver e Gold)")
print("Nenhum repetido, tudo marcado como simulado e nenhuma tabela oficial alterada.")

# Métricas das tabelas de streaming, persistidas junto com as do lote (acumulam execuções).
metricas_bronze_stream = {
    "tabela": "bronze.streaming_municipio",
    "linhas": int(totais_stream.aceitos), "linhas_entrada": int(totais_stream.recebidos),
    "tabelas_lidas": "kafka", "status": "OK", "mb_estimados": None, "mb_lidos": None,
    "duracao_s": float(df_metricas_streaming["segundos_para_gravar"].sum()), "job_id": None,
}
df_metricas_tabelas_stream = pd.DataFrame([metricas_bronze_stream] + metricas_streaming_tabelas)
df_metricas_tabelas_stream["camada"] = "streaming"
df_metricas_tabelas_stream.insert(0, "execucao_id", EXECUCAO_STREAMING)
df_metricas_tabelas_stream.insert(1, "registrado_em", datetime.now(timezone.utc))
df_metricas_tabelas_stream = df_metricas_tabelas_stream[
    [nome for nome, _ in CAMPOS_DIARIO_TABELAS]]
df_metricas_tabelas_stream["linhas"] = df_metricas_tabelas_stream["linhas"].astype("Int64")
df_metricas_tabelas_stream["linhas_entrada"] = df_metricas_tabelas_stream["linhas_entrada"].astype("Int64")
for coluna in ["mb_estimados", "mb_lidos", "duracao_s"]:
    df_metricas_tabelas_stream[coluna] = df_metricas_tabelas_stream[coluna].astype("float64")
client.load_table_from_dataframe(
    df_metricas_tabelas_stream, TABELA_METRICAS,
    job_config=_config_acrescimo(CAMPOS_DIARIO_TABELAS)).result()
print("\n=== TABELAS GRAVADAS PELO STREAMING (também em quality_reports.metricas_por_tabela) ===")
print(df_metricas_tabelas_stream.drop(columns=["registrado_em", "mb_estimados", "tabelas_lidas"]).to_string(index=False))

print("\n=== GOLD DE STREAMING: O QUE A MEDIÇÃO SIMULADA INDICARIA PARA 2025 ===")
print(consulta(f"""
    SELECT situacao_meta, COUNT(*) AS cidades,
           ROUND(AVG(distancia_meta_do_ano), 2) AS distancia_media_da_meta
    FROM `{G}.streaming_meta_vs_resultado`
    GROUP BY situacao_meta
    ORDER BY cidades DESC
""").to_string(index=False))


# ETAPA 6.4 · Streaming: latência e comparação com o batch
# Calcula a latência (recebido_em - criado_em: média, mediana, p95 e máxima) só dos
# eventos desta execução do streaming, porque a tabela acumula execuções.
# Gera um aviso se os inválidos passarem de 10% dos recebidos; repetidos não contam,
# pois são esperados na entrega "pelo menos uma vez".
# Compara também com o tempo do processamento batch.
lat = client.query(f"""
    SELECT
        COUNT(*) AS eventos_aceitos,
        ROUND(AVG(latencia_s), 3) AS media_s,
        ROUND({percentil_sql('latencia_s', 0.50)}, 3) AS mediana_s,
        ROUND({percentil_sql('latencia_s', 0.95)}, 3) AS percentil_95_s,
        ROUND(MAX(latencia_s), 3) AS maxima_s,
        TIMESTAMP_DIFF(MAX(recebido_em), MIN(criado_em), SECOND) AS janela_s
    FROM `{B}.streaming_municipio`
    WHERE _execucao_streaming = '{EXECUCAO_STREAMING}'
""").to_dataframe()
print(f"=== LATÊNCIA (execução de streaming {EXECUCAO_STREAMING}): DO EVENTO CRIADO ATÉ CHEGAR AO PIPELINE ===")
print(lat.T.to_string(header=False))

rej = client.query(f"""
    SELECT motivo, COUNT(*) AS eventos
    FROM `{Q}.eventos_rejeitados`
    WHERE _execucao_streaming = '{EXECUCAO_STREAMING}'
    GROUP BY motivo
    ORDER BY eventos DESC
""").to_dataframe()
print("\n=== EVENTOS SEPARADOS NESTA EXECUÇÃO ===")
print(rej.to_string(index=False))

# Frequência e garantias medidas nesta execução do streaming.
janela_s = max(int(lat.janela_s[0]), 1)
lotes_stream = len(df_metricas_streaming)
eventos_por_min = round(60 * int(df_metricas_streaming['recebidos'].sum()) / janela_s, 1)
perdidos = int(df_metricas_streaming['recebidos'].sum() - df_metricas_streaming['aceitos'].sum()
               - df_metricas_streaming['repetidos'].sum() - df_metricas_streaming['invalidos'].sum())

# Duração do processamento batch desta sessão, somada do diário de bordo.
duracao_batch_s = round(sum(
    m.get("duracao_s") or 0
    for lista in (metricas_bronze, metricas_silver, metricas_gold) for m in lista
), 1)

totais = df_metricas_streaming[["recebidos", "aceitos", "repetidos", "invalidos"]].sum()
# Repetidos são esperados na entrega "pelo menos uma vez"; o sinal de problema na origem
# é o evento inválido.
perc_invalido = round(100 * totais.invalidos / totais.recebidos, 2)
print(f"\nRecebidos: {totais.recebidos} | aceitos: {totais.aceitos} | "
      f"repetidos: {totais.repetidos} | inválidos: {totais.invalidos} ({perc_invalido}%)")

print("\n=== BATCH x STREAMING (números medidos) ===")
print(pd.DataFrame([
    {"aspecto": "como o dado chega", "batch": "tudo de uma vez, tabela inteira",
     "streaming": f"evento por evento, em {len(df_metricas_streaming)} lotes de até {TAMANHO_DO_LOTE}"},
    {"aspecto": "frequência", "batch": f"1 carga completa por execução ({duracao_batch_s} s); uso previsto: mensal",
     "streaming": f"contínua: {int(df_metricas_streaming['recebidos'].sum())} eventos em {janela_s} s "
                  f"(cerca de {eventos_por_min} por minuto), em {lotes_stream} micro-lotes"},
    {"aspecto": "latência", "batch": f"até a próxima execução (+ {duracao_batch_s} s de processamento)",
     "streaming": f"mediana de {lat.mediana_s[0]} s, máxima de {lat.maxima_s[0]} s"},
    {"aspecto": "garantia", "batch": "contagem de cada tabela igual à da fonte (conferida tabela a tabela)",
     "streaming": f"pelo menos uma vez: {int(totais.repetidos)} repetidos descartados pelo evento_id, "
                  f"{int(totais.invalidos)} inválidos separados, {perdidos} perdidos"},
    {"aspecto": "dado inválido", "batch": "a validação de qualidade para o pipeline",
     "streaming": "o evento é separado com o motivo e o resto segue"},
    {"aspecto": "verificações", "batch": "88 regras (8 Bronze, 61 Silver, 19 Gold)",
     "streaming": f"{len(df_conferencias_streaming)} regras (Bronze, Silver e Gold de streaming)"},
]).to_string(index=False))

# Aviso informativo: muitos eventos inválidos indicam problema na origem dos dados.
LIMITE_INVALIDOS = 10.0
if perc_invalido <= LIMITE_INVALIDOS:
    print(f"\nInválidos: {perc_invalido}%, dentro do limite de {LIMITE_INVALIDOS}%. Nenhum aviso enviado.")
elif "enviar_alerta" in globals():
    enviar_alerta("Streaming: eventos inválidos acima do limite",
                  f"{perc_invalido}% dos eventos eram inválidos (limite {LIMITE_INVALIDOS}%).",
                  gravidade="AVISO")
else:
    print(f"\nInválidos: {perc_invalido}%, acima do limite de {LIMITE_INVALIDOS}%. Rode a Etapa 1.2 para o aviso ir ao Discord.")
