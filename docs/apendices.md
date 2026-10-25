# Apêndices

Material de apoio do [README](../README.md): como cada camada é gravada, regras de negócio, consumo da execução de referência, notas sobre os prints, a proposta de histórico da Bronze, detalhes da Gold e da modelagem e as limitações em detalhe.

## Apêndice A. Como cada camada é gravada

| Camada | Conteúdo | Regra de gravação |
|---|---|---|
| **Bronze** | **Lote (Etapas 2 e 2.4): 8 tabelas**, cópia fiel das 7 do dataset (as 6 entidades do enunciado mais o `dicionario`) e do diretório de municípios (de outro dataset da Base dos Dados, usado para nome, UF e região das cidades). **Streaming (Etapa 6): `streaming_municipio`**, no mesmo formato de `municipio`, criada só no fim | Lote: uma cópia por dia, gravada na partição do dia (`WRITE_TRUNCATE` em `tabela$AAAAMMDD`). Rodar de novo no mesmo dia troca a cópia daquele dia e preserva as outras. As partições ficam salvas por 59 dias no Sandbox. Streaming: `WRITE_APPEND`. As 8 tabelas do lote são divididas por `_ingestion_date` e têm `_ingestion_timestamp`, `_source_table` e `_record_hash`. A `streaming_municipio` não é dividida e traz `_execucao_streaming` para identificar a rodada. A Silver lê a cópia mais recente do lote |
| **Silver** | 12 tabelas (as 7 da fonte tratadas, 2 de território, uma de cidades e outra de UFs, e as 3 trajetórias de meta em formato longo) e `streaming_municipio` (mesma receita de `resultado_municipio`, com a medição mais recente de cada município) | `WRITE_TRUNCATE`: refeita a cada execução a partir da cópia mais recente da Bronze |
| **Gold** | 6 tabelas oficiais e 4 de streaming (`streaming_indicador_municipio`, `streaming_meta_vs_resultado`, `streaming_evolucao_temporal`, `streaming_desigualdade_regional`) | `WRITE_TRUNCATE`. A coluna `origem` existe em todas |
| **quality_reports** | `conferencias`, `metricas_por_tabela`, `execucoes`, `alertas`, `eventos_rejeitados` e `metricas_streaming` | `WRITE_APPEND`: acumulam todas as execuções |

## Apêndice B. Regras de negócio

**Regras que evitam erros silenciosos**

- Vazio nunca vira zero. Roraima, sem metas em 2023 e 2024, aparece nesses anos como "sem meta".
- `situacao_meta` tem cinco estados: `atingiu`, `nao atingiu`, `sem meta`, `sem resultado` e `sem resultado e sem meta`.
- Resultado e meta só são comparados no mesmo recorte: rede Municipal (3) para cidades e Pública (5) para UF e Brasil (`comparavel_com_meta`).
- A taxa usa como base apenas os alunos **avaliados** (presentes, com caderno preenchido e nota), com o peso `peso_aluno`. Ausentes ficam com `esta_alfabetizado` vazio.
- Códigos são traduzidos só pelo dicionário oficial. Código sem tradução vira `desconhecido`, e uma regra bloqueante exige zero ocorrências.
- A média entre cidades se chama `media_simples_entre_cidades` e não é apresentada como indicador oficial.
- A projeção até 2030 é um cenário (`cenario_ritmo_necessario_por_ano`), não uma previsão.

**Os sete tipos de regra.** As 88 regras da execução normal verificam sete coisas:

| O que verifica | Dimensão | Exemplo | Tipo |
|---|---|---|---|
| Perda ou criação indevida de linhas | Completude | Linhas diferentes da Bronze (Silver); mesmas linhas da Silver (Gold) | Bloqueante |
| Linhas repetidas | Unicidade | Linhas repetidas na chave | Bloqueante |
| Campo essencial faltando | Completude | Aluno avaliado sem peso; lugar sem nome | Bloqueante; informativa para cidade ou lugar sem nome em `alunos`, `meta_uf` e `meta_municipio` |
| Valores possíveis | Validade | Taxa fora de 0 a 100; série diferente de 2; código sem tradução | Bloqueante; informativas para nota fora de 0 a 1000 e código de cidade sem 7 dígitos |
| Coerência entre campos | Consistência | Aluno não avaliado com nota; "alfabetizado" diferente da nota de corte | Bloqueante; informativas para meta de 2030 diferente de 80, meta que diminui e proporções por nível que não somam 100 |
| Correspondência nas junções | Consistência | Taxa de correspondência e tipo das chaves (Silver); alunos achados pelo resultado municipal (Gold) | Bloqueante; informativa para 2023 na Gold |
| Recálculo igual ao oficial | Precisão | Diferença do Brasil no ano mais recente; cidades a até 0,5 ponto (Gold, `validacao_taxa`) | Bloqueante |

Uma regra é bloqueante quando o defeito invalida o resultado final. A lista completa, com valor medido e limite, fica em `quality_reports.conferencias` e nas saídas das Etapas 2.3, 2.4, 3.11 e 4.7.

**Taxa de correspondência dos joins (Silver).** Medida depois de cada junção, com limite definido a partir do valor medido:

| Junção | Medido | Limite | Motivo da diferença |
|---|---|---|---|
| resultado_uf → meta_uf | 100% | 100% | Toda UF com resultado tem meta |
| resultado_municipio → meta_municipio | 97,78% | 95% | 242 cidades-ano sem meta publicada (Investigação 4) |
| alunos → resultado_municipio | 99,89% | 99% | 4.199 alunos em recortes sem resultado publicado (Investigação 3) |

**Limites da Gold.** Exige-se que 99% dos resultados municipais de 2024 tenham alunos correspondentes (medido: 100%) e que 98% das cidades fiquem a até 0,5 ponto da taxa recalculada (medido: 99,03%). Para 2023, a regra de correspondência com alunos é **informativa**, com limite de 85%: São Paulo não tem alunos na base daquele ano, e o valor medido é 88,49%. O tipo de cada chave de junção (`ano`, `id_municipio`, `sigla_uf`, `rede_codigo`) também é conferido, e uma diferença de tipo reprova uma regra bloqueante.

## Apêndice C. Consumo da execução de referência

| Execução | MB lidos nas gravações | GiB faturados (todas as consultas) |
|---|---|---|
| `20261024_070829` (**referência deste README**) | 826,43 (Bronze 260,22 · Silver 354,07 · Gold 212,14) | **3,993** (147 consultas e 6 cargas) |

**Gravações que mais leem.** `silver.alunos` (348,5 MB, 42%), `gold.validacao_taxa` e `gold.indicador_municipio` (122 e 81 MB) e a cópia de `bronze.alunos` (256 MB). As gravações que passam pela tabela `alunos` somam 807 dos 826 MB lidos (98%). As demais tabelas, somadas, pesam menos de 20 MB.

**Faturado e lido.** O valor faturado é maior que os 826 MB lidos nas gravações porque inclui as consultas de conferência e de medição, que releem tabelas já gravadas. Não isolamos consulta a consulta de onde vem a diferença.

**Por que o custo de consulta não cresce com o histórico.** As Etapas 3.0.1 a 3.0.6 descobrem a cópia mais recente com `INFORMATION_SCHEMA.PARTITIONS` (informação sobre a tabela, sem ler os dados), e as leituras filtram por `_ingestion_date`, que lê uma partição só. A execução de referência lê somente a partição `20261024`; a existência de outras partições não exige que elas sejam lidas por essas consultas. A política de uma cópia por partição está na [seção 11.1 do README](../README.md#111-política-de-histórico-da-bronze).

Os valores medidos incluem as conferências, que releem tabelas já gravadas de propósito, para verificar a qualidade antes de seguir. A medição da Etapa 5.2 acontece antes do streaming. As consultas da Etapa 6 (Silver e Gold de streaming) ficam fora desse total.

## Apêndice D. Notas sobre os prints

- **03:** A cada dia em que o notebook roda, nasce uma partição nova e as anteriores permanecem (política da seção 11.1 do README). A execução de referência (`20261024_070829`) foi feita em um projeto novo e limpo, por isso a Etapa 5.3 lista uma só partição de `bronze.alunos` (`20261024`, 3.867.999 linhas e 0,736 GiB).
- **04:** a tabela e o texto abaixo dela (45 bloqueantes e 16 informativas) são duas saídas da mesma célula (Etapa 3.11), unidas em uma imagem.
- **07 e 08:** a saída da Etapa 5.1 é larga demais para um print só do Colab, então a captura de tela da saída foi dividida em dois recortes (08 em cima: entrada e saída por tabela e consumo por camada; 07 embaixo: resumo da execução e histórico). O conteúdo é o da execução `20261024_070829`. No momento do print o histórico tinha só a execução de sucesso; a falha proposital (Etapa 7) fica registrada nos prints 05 e 06.
- **13:** o buscador do console, com o filtro de 12 horas, logo depois da execução `20261024_070829`, mostra os jobs concluídos e 3 jobs com erro. Os 3 são consultas de checagem da primeira execução em projeto novo, que falham com `Not found` porque a tabela consultada ainda não existe, e o código trata isso (`except NotFound`) e segue: (1) Etapa 5.1, `src/02_monitoramento.py` linha 192, confere se as métricas da execução já estão em `quality_reports.metricas_por_tabela`; (2) Etapa 5.1, linha 226, confere se a execução já está em `quality_reports.execucoes`; (3) Etapa 6.2, `src/50_streaming.py` linha 320, lê os `evento_id` já gravados em `bronze.streaming_municipio` (esta é a da mensagem `Table fresh-lens-512607-c2:bronze.streaming_municipio was not found`, e aparece como "eventos já gravados antes: 0" no print 09). Em um projeto que já rodou antes as tabelas existem e esses erros não aparecem. Não vêm da falha proposital da Etapa 7 e não afetaram nenhum dado. O total de jobs é maior que o da Etapa 5.2 (153: 147 consultas e 6 cargas) porque o console conta tudo o que o projeto fez na hora: também o streaming, a Etapa 7 e consultas manuais. A coluna do proprietário foi coberta no print.
- **10:** os 43,3 s do quadro batch × streaming são a soma das durações dos jobs de gravação, não o tempo total do pipeline. A latência do streaming vai da criação do evento até o recebimento pelo consumidor, antes da gravação do lote no BigQuery.
- **17:** reúne as saídas das três células da validação do streaming (6.3.1 Bronze, 6.3.2 Silver e 6.3.3 Gold) numa só imagem, de cima para baixo. As 34 regras são do streaming inteiro (6 na Bronze, 13 na Silver e 15 na Gold); o texto impresso pelo código ainda traz o prefixo "6.3".
- **Versão do notebook:** os prints são da execução `20261024_070829`, a versão atual do notebook. O consumo está no Apêndice C.

## Apêndice E. Guardar todas as execuções em um ambiente pago (proposta)

Esta é uma proposta de evolução da [política de histórico da Bronze](../README.md#111-política-de-histórico-da-bronze). Ela **não foi implementada nem testada** neste trabalho.

Sem o limite de 10 GiB do Sandbox, guardar todas as execuções fica barato: pela tabela de custos do README, uma cópia de 0,75 GiB custa cerca de US$ 0,015 por mês de armazenamento (0,75 × US$ 0,020). O desenho seria:

1. **Acrescentar, não substituir.** Cada execução recebe um identificador (`run_id`, que pode ser o próprio carimbo) e é gravada ao lado das anteriores.
2. **Separar uma execução da outra no disco**, para a Silver ler só a que precisa. Há duas formas: agrupar a tabela pelo `run_id` (*clustering*) ou criar uma tabela por execução. A tabela por execução é a mais segura. O agrupamento muda menos o código, mas o ganho de leitura precisa ser medido antes de adotar.
3. **Uma tabela de controle** com a execução vigente. A Silver consulta essa tabela primeiro (sem ler os dados) e lê só aquela execução.
4. **Política de retenção.** Manter as últimas N execuções (por exemplo, 10) ou os últimos X dias e apagar o resto, para o espaço não crescer sem parar.
5. **Opcional: definir o "dia" no horário de Brasília**, para que "um dia" signifique o mesmo que para quem opera o pipeline.

## Apêndice F. Detalhes da Gold e da modelagem

### Sinal das colunas de distância

As colunas de distância não seguem todas a mesma convenção:

| Coluna | Cálculo | Valor positivo | Valor negativo |
|---|---|---|---|
| `distancia_meta_do_ano` (`meta_vs_resultado`) | taxa − meta do ano | acima da meta | abaixo da meta |
| `distancia_meta_2030` (`meta_vs_resultado`) | taxa − meta de 2030 | acima da meta | abaixo da meta |
| `falta_para_meta_2030` (`evolucao_temporal`) | meta de 2030 − taxa | ainda falta | já passou da meta |
| `distancia_taxa_2023_para_meta_2024` (`base_modelagem`) | meta de 2024 − taxa de 2023 | ainda falta | já passou da meta |

Exemplo, a cidade do Rio de Janeiro em 2024: `distancia_meta_2030 = −16,25` e `falta_para_meta_2030 = +16,25` descrevem o mesmo fato. A mesma tabela está no notebook, depois da Etapa 4.3.

### Composição da base de modelagem

Há 5.500 municípios: 5.396 aparecem nos dois anos (taxa de 2023 e alvo de 2024), 52 só em 2023 e 52 só em 2024. A correlação entre as taxas de 2023 e 2024 é 0,635.

### Disponibilidade temporal das metas

`meta_2024` e `meta_2030` vêm da publicação com `ano_referencia = 2024`, a mesma que traz o resultado de 2024. Se o INEP tivesse calculado essas metas depois de conhecer o resultado, elas não seriam entradas legítimas. Uma célula do notebook (depois da Etapa 4.6) compara as metas das publicações de 2023 e de 2024. Na execução `20261024_070829`, entre as **5.352 cidades presentes nas duas publicações**: `meta_2024` é igual em 5.232 e diferente em nenhuma (as outras 120 não têm meta de 2024 em nenhuma das duas), e `meta_2030` é igual nas 5.352. Nenhuma meta aparece só na publicação de 2024. Conclusão: nessas cidades, as metas já constavam da publicação de 2023. A comparação não cobre as cidades que não estão nas duas publicações.

### Cuidados na modelagem

- **São Paulo.** As 627 cidades de SP não têm notas de alunos em 2023, então `alunos_total_2023`, `alunos_avaliados_2023` e `percentual_avaliados_2023` ficam vazias. Com as 52 cidades que só existem em 2024, são 679 linhas (12,3%). A ausência não é aleatória: um modelo pode tratar "ser de SP" como sinal. Vale treinar com e sem SP ou criar uma coluna indicadora.
- **Cidades pequenas.** Com poucas centenas de alunos, a taxa oscila muito de um ano para o outro (Costa Marques/RO foi de 80,68 para 59,63). Parte da variação do alvo é ruído de amostra. Vale usar `alunos_avaliados_2023` como peso de treino ou excluir cidades com poucos avaliados.
- **Alvo `alvo_atingiu_meta_2024`.** Fica vazio para 268 cidades: 216 sem meta publicada e 52 sem resultado em 2024.

## Apêndice G. Limitações em detalhe

- **Streaming simulado.** O Kafka roda dentro do Colab e os eventos de 2025 são criados a partir de 2024. Só a Bronze de streaming é incremental (só acréscimo): a Silver e a Gold de streaming são refeitas a cada execução, a partir da Bronze acumulada. A latência vai da criação do evento até a chegada ao consumidor (`recebido_em`), antes de o micro-lote ser gravado no BigQuery, o que acrescenta alguns segundos. As tabelas do streaming acumulam eventos de execuções anteriores; na execução de referência, que partiu de um projeto limpo, a Silver do streaming tem 188 linhas.
- **Histórico da Bronze por dia.** Uma nova execução no mesmo dia troca a cópia daquele dia. O "dia" é o dia em UTC (Brasília está 3 horas atrás): uma execução depois das 21h em Brasília cai na partição do dia seguinte. Repetir só a célula de ingestão na mesma sessão mantém o identificador: o pipeline avisa que a execução já estava gravada e não troca a carga. No Sandbox, cada cópia expira em menos de 60 dias (ficam 59) e o armazenamento é de 10 GiB (cerca de 0,75 GiB por cópia); com cobrança ativada, a expiração é removida.
- **Execução manual.** Não há agendamento nem nova tentativa automática, e não há alerta de custo nem monitoramento nativo da nuvem (Cloud Monitoring). O monitoramento é feito pelas tabelas `quality_reports` e pelo Discord.
- **Orquestração, infraestrutura como código e integração contínua (não implementadas).** As etapas dependem umas das outras apenas pela ordem das células; os datasets, as tabelas e a expiração das partições são criados pelo notebook, com as permissões da conta do Colab, e não há ambiente recriável por um comando. Evolução prevista: um DAG no Cloud Composer (Airflow) ou um job no Cloud Scheduler encadeando Bronze → validação → Silver → validação → Gold, com nova tentativa por etapa; Terraform para projeto, datasets e tabelas de controle, com estado e diff revisável; e os testes da Etapa 1.3 e da validação de eventos (6.2) executados em integração contínua a cada alteração. Os datasets de streaming seguiriam o mesmo caminho, com um cluster Kafka gerenciado no lugar da instância da sessão.
- **Lacunas da fonte.** Três estados sem resultado da rede Pública em 2023 (AC, DF e RR) e dois em 2024 (DF e RR). `nivel_alfabetizacao` sem tradução no dicionário. O dicionário não cobre as tabelas de meta: o mapeamento "Pública" = 5 e "Municipal" = 3 foi confirmado pela coincidência das taxas. O resultado do Brasil em 2025 (66,0) vem da tabela de meta, sem microdados para conferir.
- **Conferência pelos microdados.** 100 linhas de município (0,4%) com diferença acima de 1 ponto, sem causa apurada. Hipóteses ainda não testadas: poucos alunos avaliados nessas cidades ou divergência entre a coluna `alfabetizado` da fonte e o corte de 743 pontos. A regra bloqueante exige ao menos 98% das linhas a até 0,5 ponto da taxa oficial; o valor medido é 99,03%. O limiar foi escolhido com folga sobre o medido e tolera cerca de 460 linhas fora, então uma fonte nova com mais divergência poderia passar sem alerta até esse ponto.
- **Desigualdade regional sem ponderação.** Em `desigualdade_regional`, mínimo, máximo e quartis tratam todas as cidades igual. Os extremos (4,40 e 100,00 em 2024) refletem em parte cidades com poucos alunos. A soma de `alunos_avaliados` em 2023 não inclui as cidades de SP.
- **Metas na `base_modelagem`.** A verificação de que `meta_2024` e `meta_2030` já existiam na publicação de 2023 cobre 5.352 cidades; as demais não foram comparadas.
- **Validação sem número mínimo de regras.** Se uma camada chegasse à validação com zero regras medidas, a função `validacao_de_qualidade` a aprovaria, porque não há bloqueante reprovada. Hoje todas as execuções medem 88 regras (8 na Bronze, 61 na Silver e 19 na Gold) e o diário de bordo registra esse número, mas a função não impõe um mínimo. Uma correção simples seria levantar erro quando a lista vier vazia.
- **Comparação de taxas entre tabelas da Gold.** A conferência entre `indicador_municipio` e `meta_vs_resultado` (tolerância 0,001) considera só os pares em que as duas taxas existem. Uma taxa vazia de um lado e preenchida do outro não é contada. Esse reforço não foi implementado.
- **Robustez do streaming.** A validação de cada mensagem (Etapa 6.2) não cobre todos os formatos: taxa em texto pode gerar `TypeError`, ano em texto pode gerar `ValueError` e o formato do horário não é validado nessa função. JSON malformado também não é tratado por mensagem. Além disso, se a thread do produtor falhar antes de sinalizar o fim, o consumidor pode esperar sem avisar a execução principal. Esses casos estão fora da cobertura da validação atual e não ocorreram na execução demonstrada.
- **Taxas de correspondência só exibidas.** A Etapa 3.9.3 mede cinco cruzamentos; a Etapa 3.11 grava em `quality_reports.conferencias` e decide três deles, com limite. Os dois no sentido inverso, `meta_uf (anos da tabela uf) → resultado_uf` (54 linhas, 49 com par, 90,74%) e `meta_municipio → resultado_municipio` (10.704 linhas, 10.654 com par, 99,53%), são impressos na saída da 3.9.3 e explicados na Investigação 4 (5 UFs-ano e 50 cidades com meta e sem resultado), mas não são gravados nem têm limite. A correção simples seria registrá-los como regras informativas.
- **Testes.** Os testes automáticos cobrem as funções de qualidade e a validação de eventos. As consultas em SQL são conferidas pelas regras de qualidade a cada execução.
