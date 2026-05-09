# MC859 — Predição de Links em Redes de Colaboração Científica

Este projeto tem como objetivo construir e analisar uma rede de colaboração entre pesquisadores da Unicamp, utilizando dados coletados da base Scopus, para aplicar e avaliar técnicas de predição de links. A rede é dividida em períodos de treino (2018–2023), validação (2024) e teste (2025), permitindo verificar se modelos conseguem prever novas colaborações futuras entre autores.

## Estrutura

```
MC859/
├── data_collection/   # Scripts de coleta (Scopus) e construção do grafo
├── data/
│   ├── raw/           # Banco de dados SQLite e .csv com os dados brutos
│   ├── processed/     # Banco de dados com autores processados e informações adicionais (h-index, citações, áreas de pesquisa, etc.)
│   └── graphs/        # Grafos exportados em formato GraphML
└── notebooks/         # Análises exploratórias e visualizações
```
