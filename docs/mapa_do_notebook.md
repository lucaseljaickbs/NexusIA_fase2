# Mapa do notebook

Cada etapa do notebook `notebooks/NexusAI_techfase2.ipynb`, a camada a que pertence e o arquivo de `src/` onde o mesmo código aparece separado.

| Etapa | Conteúdo | Camada | Arquivo em src/ |
|---|---|---|---|
| Configuração | ID do projeto no Google Cloud (a única célula que o usuário edita) | Ambiente | `00_ambiente.py` |
| 1 | Ambiente | Ambiente | `00_ambiente.py` |
| 1.1 | Ferramentas de qualidade | Qualidade | `01_qualidade.py` |
| 1.2 | Ferramentas de monitoramento | Monitoramento | `02_monitoramento.py` |
| 1.3 | Testes das funções de qualidade | Qualidade | `01_qualidade.py` |
| 2 | Bronze: ingestão batch | Bronze | `10_bronze.py` |
| 2.3 | Validação de qualidade da Bronze: conferência da cópia (fonte x Bronze) | Bronze | `10_bronze.py` |
| 2.3.1 | Investigação 1: metas por UF (só leitura) | Bronze | `10_bronze.py` |
| 2.4 | Bronze: tabela de apoio territorial (com validação de qualidade) | Bronze | `10_bronze.py` |
| 3.0.1 | Como achar a cópia mais recente da Bronze | Silver | `20_silver.py` |
| 3.0.2 | Reconhecimento do dicionário (só leitura) | Silver | `20_silver.py` |
| 3.0.3 | Preparação da Silver (funções `bronze()` e `gravar_silver()`) | Silver | `20_silver.py` |
| 3.0.4 | Reconhecimento da tabela de apoio (só leitura) | Silver | `20_silver.py` |
| 3.0.5 | Reconhecimento dos alunos (só leitura) | Silver | `20_silver.py` |
| 3.0.6 | Cópia da Bronze usada pela Silver | Silver | `20_silver.py` |
| 3.1 | Silver: dicionário e território | Silver | `20_silver.py` |
| 3.2 | Silver: resultado_uf | Silver | `20_silver.py` |
| 3.3 | Silver: resultado_municipio | Silver | `20_silver.py` |
| 3.4 | Silver: alunos | Silver | `20_silver.py` |
| 3.5 | Silver: meta_brasil | Silver | `20_silver.py` |
| 3.6 | Silver: meta_uf | Silver | `20_silver.py` |
| 3.7 | Silver: meta_municipio | Silver | `20_silver.py` |
| 3.8 | Silver: trajetória das metas | Silver | `20_silver.py` |
| 3.9.1 | Medições de qualidade da Silver: regras por tabela | Silver | `20_silver.py` |
| 3.9.2 | Medições de qualidade da Silver: tipos das chaves | Silver | `20_silver.py` |
| 3.9.3 | Medições de qualidade da Silver: correspondência dos cruzamentos | Silver | `20_silver.py` |
| 3.9.4 | Medições de qualidade da Silver: coerência das taxas | Silver | `20_silver.py` |
| 3.9.5 | Medições de qualidade da Silver: cobertura de estados | Silver | `20_silver.py` |
| 3.10 | Investigações da Silver (só leitura) | Silver | `20_silver.py` |
| 3.11 | Validação de qualidade da Silver | Silver | `20_silver.py` |
| 4.0 | Preparação da Gold | Gold | `30_gold.py` |
| 4.1 | Gold: indicador_municipio | Gold | `30_gold.py` |
| 4.2 | Gold: meta_vs_resultado | Gold | `30_gold.py` |
| 4.3 | Gold: evolucao_temporal | Gold | `30_gold.py` |
| 4.4 | Gold: validacao_taxa | Gold | `30_gold.py` |
| 4.5 | Gold: desigualdade_regional | Gold | `30_gold.py` |
| 4.6 | Gold: base_modelagem (parte 1) | Gold | `30_gold.py` |
| 4.6 | Gold: base_modelagem (parte 2): conferências | Gold | `30_gold.py` |
| 4.6 | Verificação de vazamento: metas de `base_modelagem` x publicação de 2023 (só leitura) | Gold | `30_gold.py` |
| 4.7 | Validação de qualidade da Gold | Gold | `30_gold.py` |
| 4.8 | Exportação da Gold | Gold | `30_gold.py` |
| 4.9 | Gráficos dos resultados da Gold (5 imagens em PNG, num `.zip`) | Gold | `30_gold.py` |
| 5.1 | Diário de bordo | Monitoramento / FinOps | `40_diario_e_finops.py` |
| 5.2 | FinOps: consumo medido e custo projetado (**tabela de custos**) | Monitoramento / FinOps | `40_diario_e_finops.py` |
| 5.3 | Evidências do ambiente (datasets e partições) | Monitoramento / FinOps | `40_diario_e_finops.py` |
| 6.0 | Streaming: Kafka no Colab | Streaming | `50_streaming.py` |
| 6.1 | Streaming: produtor | Streaming | `50_streaming.py` |
| 6.2 | Streaming: produção e consumo em micro-lotes | Streaming | `50_streaming.py` |
| 6.3.1 | Streaming: validação da Bronze de streaming | Streaming | `50_streaming.py` |
| 6.3.2 | Streaming: Silver de streaming e validação | Streaming | `50_streaming.py` |
| 6.3.3 | Streaming: Gold de streaming (4 tabelas), validação e resumo das 34 regras | Streaming | `50_streaming.py` |
| 6.4 | Streaming: latência e comparação com o batch | Streaming | `50_streaming.py` |
| 7 | Demonstração de falha (rode por último) | Qualidade (demonstração) | `90_falha_proposital.py` |
