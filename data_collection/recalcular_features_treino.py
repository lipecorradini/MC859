"""
Recalcula as features de atributo dos autores aplicando corte temporal em 31/12/2023.

Substitui o snapshot atual do Scopus (citation_count, document_count, h_index),
que herda papers e citações de 2024-2025, por valores derivados das tabelas
locais publicacoes e autor_publicacao restritas ao período de treino.

areas e pub_year_first são reaproveitadas do CSV original conforme spec
spec/features_temporal_cutoff.md (seções 3.1 e 4).

coauthor_count foi removido do feature set (vide spec, seção 3.1) por se tornar
redundante com local_degree após o corte temporal: autor_publicacao só contém
relações entre autores Unicamp, e a base não cobre papers anteriores a 2018.

Saída: data/processed/autores_grafo_treino.csv
"""

import csv
import sqlite3
import sys
from pathlib import Path

import networkx as nx

sys.path.insert(0, str(Path(__file__).parent))
from construtor_grafo import ANO_TREINO_INI, ANO_TREINO_FIM

BASE_DIR   = Path(__file__).parent.parent
DB_PATH    = BASE_DIR / "data" / "raw" / "unicamp_network.db"
GRAPH_PATH = BASE_DIR / "data" / "graphs" / "grafo_unico.graphml"
CSV_ORIG   = BASE_DIR / "data" / "processed" / "autores_grafo.csv"
CSV_OUT    = BASE_DIR / "data" / "processed" / "autores_grafo_treino.csv"

CSV_COLS = ["auth_id", "areas", "citation_count", "document_count",
            "h_index", "pub_year_first"]


def calcular_h_index(citacoes_desc):
    """h-index: maior h tal que pelo menos h papers têm >= h citações.
    Recebe lista de citações já ordenada em ordem decrescente."""
    h = 0
    for i, c in enumerate(citacoes_desc, start=1):
        if c >= i:
            h = i
        else:
            break
    return h


def carregar_atributos_estaveis():
    """Lê areas e pub_year_first do CSV original.
    Esses campos são reaproveitados conforme spec (areas é snapshot aceito,
    pub_year_first é seguro pelo filtro do grafo)."""
    if not CSV_ORIG.exists():
        print(f"AVISO: {CSV_ORIG} não encontrado — areas e pub_year_first ficarão vazios.")
        return {}

    estaveis = {}
    with open(CSV_ORIG, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            estaveis[row["auth_id"]] = {
                "areas":          row.get("areas", "") or "",
                "pub_year_first": row.get("pub_year_first", "") or "",
            }
    return estaveis


def consultar_doc_e_citacoes(cursor):
    """document_count e citation_count agregados por autor no período de treino."""
    cursor.execute("""
        SELECT ap.auth_id, COUNT(*), COALESCE(SUM(p.citedby_count), 0)
        FROM autor_publicacao ap
        JOIN publicacoes p ON ap.eid = p.eid
        WHERE p.ano BETWEEN ? AND ?
        GROUP BY ap.auth_id
    """, (ANO_TREINO_INI, ANO_TREINO_FIM))
    return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}


def consultar_h_indices(cursor):
    """h-index por autor computado sobre citedby_count dos papers no período."""
    cursor.execute("""
        SELECT ap.auth_id, COALESCE(p.citedby_count, 0)
        FROM autor_publicacao ap
        JOIN publicacoes p ON ap.eid = p.eid
        WHERE p.ano BETWEEN ? AND ?
        ORDER BY ap.auth_id, p.citedby_count DESC
    """, (ANO_TREINO_INI, ANO_TREINO_FIM))

    citacoes_por_autor = {}
    for auth_id, citedby in cursor.fetchall():
        citacoes_por_autor.setdefault(auth_id, []).append(citedby)

    return {auth_id: calcular_h_index(cits) for auth_id, cits in citacoes_por_autor.items()}


def imprimir_estatisticas(auth_ids, doc_cit, h_index):
    print("\nEstatísticas das features recalculadas:")
    def stats(nome, valores):
        n = len(valores)
        ordenados = sorted(valores)
        med = ordenados[n // 2]
        media = sum(ordenados) / n
        print(f"  {nome:25s}  min={min(ordenados):>5}  mediana={med:>5}  máx={max(ordenados):>7}  média={media:>8.1f}")

    stats("document_count", [doc_cit.get(a, (0, 0))[0] for a in auth_ids])
    stats("citation_count", [doc_cit.get(a, (0, 0))[1] for a in auth_ids])
    stats("h_index",        [h_index.get(a, 0)         for a in auth_ids])


def main():
    CSV_OUT.parent.mkdir(parents=True, exist_ok=True)

    print(f"Lendo grafo: {GRAPH_PATH}")
    G = nx.read_graphml(GRAPH_PATH)
    auth_ids_grafo = list(G.nodes())
    print(f"  Autores no grafo: {len(auth_ids_grafo)}")

    print(f"\nLendo atributos estáveis (areas, pub_year_first) de: {CSV_ORIG}")
    estaveis = carregar_atributos_estaveis()
    print(f"  Autores com areas/pub_year_first no CSV original: {len(estaveis)}")

    print(f"\nCalculando métricas com filtro p.ano BETWEEN {ANO_TREINO_INI} AND {ANO_TREINO_FIM}...")
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    doc_cit = consultar_doc_e_citacoes(cursor)
    h_index = consultar_h_indices(cursor)
    conn.close()
    print(f"  Autores com ao menos 1 paper no período: {len(doc_cit)}")

    print("\n=== Diagnóstico de cobertura ===")
    sem_estaveis = sum(1 for a in auth_ids_grafo if a not in estaveis)
    sem_metricas = sum(1 for a in auth_ids_grafo if a not in doc_cit)
    docs_pequenos = sum(1 for a in auth_ids_grafo if doc_cit.get(a, (0, 0))[0] < 2)

    print(f"  Sem areas/pub_year_first no CSV original          : {sem_estaveis}")
    print(f"  Sem papers em {ANO_TREINO_INI}-{ANO_TREINO_FIM} no banco            : {sem_metricas} (esperado: 0)")
    print(f"  document_count < 2 (filtro do grafo violado)      : {docs_pequenos} (esperado: 0)")

    if sem_metricas > 0:
        print(f"\nERRO: {sem_metricas} autores no grafo sem papers no período.")
        print("Inconsistência entre o grafo e o banco. Verifique se construtor_grafo.py foi rodado contra o banco atual.")
        sys.exit(1)
    if docs_pequenos > 0:
        print(f"\nERRO: {docs_pequenos} autores com document_count < 2.")
        print("O filtro de pelo menos 2 publicações em 2018-2023 deveria impedir isso.")
        sys.exit(1)

    print(f"\nEscrevendo {CSV_OUT}...")
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLS)
        writer.writeheader()
        for auth_id in auth_ids_grafo:
            doc, cit = doc_cit.get(auth_id, (0, 0))
            estavel  = estaveis.get(auth_id, {})
            writer.writerow({
                "auth_id":        auth_id,
                "areas":          estavel.get("areas", ""),
                "citation_count": cit,
                "document_count": doc,
                "h_index":        h_index.get(auth_id, 0),
                "pub_year_first": estavel.get("pub_year_first", ""),
            })
    print(f"  {len(auth_ids_grafo)} linhas escritas.")

    imprimir_estatisticas(auth_ids_grafo, doc_cit, h_index)


if __name__ == "__main__":
    main()
