# MC859 — Predição de Links em Redes de Colaboração Científica

Este projeto tem como objetivo construir e analisar uma rede de colaboração entre pesquisadores da Unicamp, utilizando dados coletados da base Scopus, para aplicar e avaliar técnicas de predição de links. A rede é dividida em períodos de treino (2018–2023), validação (2024) e teste (2025), permitindo verificar se modelos conseguem prever novas colaborações futuras entre autores.

## Estrutura

```
MC859/
├── data_collection/
│   ├── construtor_grafo.py                  # Helpers de construção e subgrafos (biblioteca)
│   ├── 01_scopus.py                         # Coleta publicações da API Scopus → SQLite
│   ├── 02_obter_metricas_autores.py         # Enriquece autores com h-index, citações e áreas
│   └── 03_recalcular_features_treino.py     # Recalcula atributos com corte temporal 2018-2023
├── data/
│   ├── raw/
│   │   ├── unicamp_network.db               # Banco SQLite com autores e publicações
│   │   ├── autores_brutos.csv
│   │   ├── publicacoes.csv
│   │   └── autor_publicacao.csv
│   ├── processed/
│   │   ├── autores_grafo.csv                # Snapshot Scopus original (carreira completa)
│   │   └── autores_grafo_treino.csv         # Atributos recalculados com corte 2018-2023
│   ├── graphs/
│   │   ├── grafo_unico.graphml              # Grafo completo 2018-2025 com bitmask de anos ativos
│   │   └── grafo_treino_msg.graphml         # Subgrafo de mensagem (90% das arestas de treino)
│   ├── features/
│   │   ├── train_features.csv
│   │   ├── val_features.csv
│   │   └── test_features.csv
│   ├── splits/
│   │   ├── train_pairs.csv                  # Pares positivos (holdout 10%) e negativos de treino
│   │   ├── val_pairs.csv                    # Pares positivos e negativos de validação (2024)
│   │   └── test_pairs.csv                   # Pares positivos e negativos de teste (2025)
│   └── results/
│       ├── results_v1_leaky.csv             # Baseline com vazamentos (cutoff + holdout)
│       ├── results.csv                      # Resultado final (cutoff + holdout aplicados)
│       └── heuristics/                      # Scores das heurísticas por método e split
├── models/
│   ├── 00_generate_splits.ipynb             # Gera pares pos/neg e aplica o holdout 90/10
│   ├── 01_heuristics.ipynb                  # Heurísticas estruturais (CN, Adamic-Adar, Jaccard, Katz)
│   ├── 02_ml_features.ipynb                 # Features de par para ML clássico
│   └── 03_ml_algorithms.ipynb               # Random Forest e XGBoost
├── notebooks/
│   └── analise_inicial.ipynb                # Análise exploratória e visualizações do grafo
└── spec/
    ├── features_temporal_cutoff.md
    ├── message_supervision_split.md
    └── gnn.md
```

## Ordem de execução do pipeline

Os scripts em `data_collection/` devem ser rodados na ordem indicada pelo prefixo numérico:

1. `01_scopus.py`: coleta papers e relações autor-publicação na base Scopus, popula `data/raw/unicamp_network.db`.
2. `02_obter_metricas_autores.py`: enriquece os autores com atributos de carreira (h-index, citações, áreas) via API `AuthorRetrieval`, gera `data/processed/autores_grafo.csv`.
3. `03_recalcular_features_treino.py`: recalcula `document_count`, `citation_count` e `h_index` a partir do SQLite com corte temporal em 31/12/2023, gera `data/processed/autores_grafo_treino.csv`. Justificativa em `spec/features_temporal_cutoff.md`.

`construtor_grafo.py` é uma biblioteca importada pelos scripts acima e pelos notebooks em `models/`; não tem ordem de execução própria.

Em seguida, rodar os notebooks em `models/` na ordem `00 → 01 → 02 → 03`.
