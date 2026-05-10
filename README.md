# MC859 — Predição de Links em Redes de Colaboração Científica

Este projeto tem como objetivo construir e analisar uma rede de colaboração entre pesquisadores da Unicamp, utilizando dados coletados da base Scopus, para aplicar e avaliar técnicas de predição de links. A rede é dividida em períodos de treino (2018–2023), validação (2024) e teste (2025), permitindo verificar se modelos conseguem prever novas colaborações futuras entre autores.

## Estrutura

```
MC859/
├── data_collection/
│   ├── scopus.py                  # Coleta publicações da API Scopus → SQLite
│   ├── obter_metricas_autores.py  # Enriquece autores com h-index, citações e áreas
│   └── construtor_grafo.py        # Constrói o grafo e define helpers de subgrafo
├── data/
│   ├── raw/
│   │   ├── unicamp_network.db     # Banco SQLite com autores e publicações
│   │   ├── autores_brutos.csv
│   │   ├── publicacoes.csv
│   │   └── autor_publicacao.csv
│   ├── processed/
│   │   └── autores_grafo.csv      # Atributos enriquecidos por autor (h-index, citações, áreas)
│   ├── graphs/
│   │   └── grafo_unico.graphml    # Grafo completo 2018–2025 com bitmask de anos ativos
│   ├── splits/
│   │   ├── val_pairs.csv          # Pares de arestas positivas e negativas para validação (2024)
│   │   └── test_pairs.csv         # Pares de arestas positivas e negativas para teste (2025)
│   └── results/
│       └── heuristics/            # Scores das heurísticas por método e split
├── models/
│   ├── 00_generate_splits.ipynb   # Gera os pares positivos e negativos de val/test
│   └── 01_heuristics.ipynb        # Heurísticas estruturais (CN, Adamic-Adar, Jaccard, Katz)
└── notebooks/
    └── analise_inicial.ipynb      # Análise exploratória e visualizações do grafo
```
