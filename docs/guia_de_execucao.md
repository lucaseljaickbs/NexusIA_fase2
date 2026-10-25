# Guia de execução (detalhes)

Complemento da [seção 12 do README](../README.md#12-como-executar). O passo a passo curto está no README; aqui ficam os avisos de permissão, o alerta no Discord e a solução de problemas.

## Avisos de permissão

Aparecem a cada sessão nova. Se a tela ficar parada, procure a janela do aviso.

| # | Onde | O que pede | O que fazer |
|---|---|---|---|
| 1 | Etapa 1, célula de ambiente | Acesso do notebook à sua conta Google, para usar o BigQuery | Escolha a conta dona do projeto e clique em **Permitir**. Sem isso, a execução para na Etapa 1 |
| 2 | Etapa 1.2, monitoramento | "O notebook não tem acesso ao Secret 'ALERTA_WEBHOOK_URL'. Permitir acesso?" | Clique em **Permitir acesso**. O aviso volta ao abrir uma cópia nova do arquivo |

Se o aviso 2 for cancelado, ou se o secret não existir, o pipeline roda normalmente. A falha só é gravada em `quality_reports.alertas` e não chega ao Discord. A Etapa 1.2 mostra `Endereço de webhook configurado? Sim` ou `Não`.

## Alerta no Discord (opcional)

1. No Discord, abra o canal que vai receber os avisos: **Editar canal → Integrações → Webhooks → Novo webhook → Copiar URL do webhook**.
2. No Colab, clique no **ícone de chave (Secrets)**, na barra lateral esquerda.
3. Clique em **Adicionar novo secret**.
4. Em **Nome**, digite exatamente `ALERTA_WEBHOOK_URL`.
5. Em **Valor**, cole a URL copiada no passo 1.
6. Ligue a chave **Acesso ao notebook** da linha do secret.
7. Rode o notebook e, quando o aviso 2 aparecer, clique em **Permitir acesso**.

## O que esperar

- **Duração:** cerca de 10 minutos no total. O download do Kafka (114 MB, de `archive.apache.org`) pode levar de alguns segundos a 10 minutos. É normal a Etapa 6.0 parecer parada: aguarde a mensagem "Kafka pronto".
- **Aviso `[sandbox]` na Etapa 2:** é normal. O Sandbox não aceita partição sem prazo de expiração, então o pipeline usa 59 dias, o máximo.
- **Etapa 4.8:** baixa `gold_export.zip`, o dataset da Fase 3. O navegador pode pedir permissão.
- **Etapa 4.9:** gera 5 gráficos dos resultados e baixa `graficos.zip`. O navegador pode pedir permissão de novo para o segundo download.
- **A execução termina em erro na Etapa 7, de propósito.**
- **Rodar de novo (nova execução, com novo identificador):** no mesmo dia (em UTC), a cópia da Bronze daquele dia é trocada. Em outro dia, nasce uma partição nova e as anteriores permanecem. Silver e Gold são refeitas, e `quality_reports` acumula mais uma execução.
- **Campos vazios na Gold são de propósito.** Exemplos: `sigla_uf` vazia na linha do Brasil e `meta_do_ano` vazia em 2023 (ano sem meta). Vazio significa "não se aplica" ou "a fonte não informou", nunca zero.

## Se algo der errado

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| "Não consegui usar o projeto ..." na Etapa 1 | ID errado ou conta diferente da dona do projeto | Confira o ID (não o nome) e a conta escolhida no aviso 1 |
| "Quota exceeded: free storage" | O Sandbox limita o armazenamento a 10 GiB e o projeto acumulou execuções | Apague o conteúdo dos datasets no console, ou crie um projeto novo e use o ID dele |
| Aviso de "sessões ativas" no Colab | Muitos notebooks abertos | Encerre as antigas em **Ambiente de execução → Gerenciar sessões** |
| O navegador pede permissão para baixar vários arquivos e o aviso não responde | O Chrome bloqueia o segundo download automático (gold_export.zip na Etapa 4.8 e graficos.zip na 4.9) | O download é opcional: o pipeline roda do mesmo jeito. Para pegar os arquivos, abra o painel **Arquivos** (ícone de pasta, à esquerda) no Colab, clique com o botão direito em gold_export.zip ou graficos.zip e escolha **Fazer download**. Se preferir o download automático, clique em **Permitir** no aviso ou libere downloads múltiplos para colab.research.google.com nas configurações do site |
| Download do Kafka parado | Servidor da Apache lento | Aguarde até 10 minutos. A Etapa 6.0 confere o arquivo no final |
| `503 ServiceUnavailable` em alguma célula do BigQuery | Indisponibilidade momentânea do serviço, sem relação com o código | Espere alguns minutos e use **Ambiente de execução → Reiniciar sessão e executar tudo**. A falha fica registrada no diário (`quality_reports.execucoes`) com situação `falha`, e a nova execução recomeça do zero |
