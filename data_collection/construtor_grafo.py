import sqlite3
from pathlib import Path
import networkx as nx

DB_PATH    = Path(__file__).parent.parent / "data" / "raw" / "unicamp_network.db"
GRAPHS_DIR = Path(__file__).parent.parent / "data" / "graphs"

# Decisões metodológicas documentadas
# Treino: 2018-2023 | Validação: 2024 | Teste: 2025
# Filtro: >= 2 publicações no período de treino (análogo ao "Core" de Liben-Nowell & Kleinberg, 2007)
# Grafo não-direcionado: coautoria é relação simétrica (Newman, 2001)
# Anonimização: auth_id Scopus como identificador — nome não exportado (Makarov et al., 2019)
# Nota: autores_brutos já contém apenas pesquisadores Unicamp, pré-filtrados pelo afid
# 60029570 durante a coleta no scopus.py. O refinador_unicamp.py será executado na Fase 2
# para enriquecer os nós com metadados (h-index, áreas, etc.).
ANO_TREINO_INI  = 2018
ANO_TREINO_FIM  = 2023
MIN_PUBLICACOES = 2

def construir_grafo(db_path=DB_PATH):
    conn   = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Autores com >= MIN_PUBLICACOES no período de treino
    # Fonte: autores_brutos, já pré-filtrados por afid Unicamp no scopus.py
    cursor.execute('''
        SELECT ab.auth_id
        FROM autores_brutos ab
        WHERE (
            SELECT COUNT(*)
            FROM autor_publicacao ap
            JOIN publicacoes p ON ap.eid = p.eid
            WHERE ap.auth_id = ab.auth_id
              AND CAST(p.ano AS INTEGER) BETWEEN ? AND ?
        ) >= ?
    ''', (ANO_TREINO_INI, ANO_TREINO_FIM, MIN_PUBLICACOES))

    auth_ids_validos = {row[0] for row in cursor.fetchall()}

    total_brutos = cursor.execute('SELECT COUNT(*) FROM autores_brutos').fetchone()[0]
    total_1_pub  = cursor.execute('''
        SELECT COUNT(*) FROM autores_brutos ab
        WHERE (
            SELECT COUNT(*) FROM autor_publicacao ap
            JOIN publicacoes p ON ap.eid = p.eid
            WHERE ap.auth_id = ab.auth_id
              AND CAST(p.ano AS INTEGER) BETWEEN ? AND ?
        ) >= 1
    ''', (ANO_TREINO_INI, ANO_TREINO_FIM)).fetchone()[0]

    print(f"\n=== Diagnóstico de autores ===")
    print(f"  Total em autores_brutos (todos os anos)          : {total_brutos}")
    print(f"  Com >= 1 pub em {ANO_TREINO_INI}-{ANO_TREINO_FIM}                    : {total_1_pub}")
    print(f"  Com >= {MIN_PUBLICACOES} pubs em {ANO_TREINO_INI}-{ANO_TREINO_FIM} (nós do grafo)  : {len(auth_ids_validos)}")
    print(f"  Excluídos por < {MIN_PUBLICACOES} pubs no treino                : {total_brutos - len(auth_ids_validos)}")
    print(f"  Apenas em 2024/2025 (fora do treino)             : {total_brutos - total_1_pub}")

    G = nx.Graph()

    # Nós — sem atributos por enquanto; serão enriquecidos na Fase 2 pelo refinador_unicamp.py
    for auth_id in auth_ids_validos:
        G.add_node(auth_id)

    # Arestas: pares de autores que co-publicaram no período de treino
    cursor.execute('''
        SELECT ap1.auth_id, ap2.auth_id,
               COUNT(*)                    AS peso,
               MIN(CAST(p.ano AS INTEGER)) AS ano_primeira_colab
        FROM autor_publicacao ap1
        JOIN autor_publicacao ap2
          ON ap1.eid = ap2.eid AND ap1.auth_id < ap2.auth_id
        JOIN publicacoes p ON ap1.eid = p.eid
        WHERE CAST(p.ano AS INTEGER) BETWEEN ? AND ?
        GROUP BY ap1.auth_id, ap2.auth_id
    ''', (ANO_TREINO_INI, ANO_TREINO_FIM))

    for auth1, auth2, peso, ano_primeira_colab in cursor.fetchall():
        if auth1 in auth_ids_validos and auth2 in auth_ids_validos:
            G.add_edge(auth1, auth2, weight=peso, ano_primeira_colab=ano_primeira_colab)

    conn.close()
    return G


if __name__ == "__main__":
    G = construir_grafo()

    n    = G.number_of_nodes()
    e    = G.number_of_edges()
    grau = (2 * e / n) if n > 0 else 0
    comp = nx.number_connected_components(G)

    print(f"\n=== Grafo completo de treino ({ANO_TREINO_INI}-{ANO_TREINO_FIM}) ===")
    print(f"  Vértices  : {n}")
    print(f"  Arestas   : {e}")
    print(f"  Grau médio: {grau:.2f}")
    print(f"  Componentes conexas: {comp}")

    nx.write_graphml(G, GRAPHS_DIR / "grafo_treino.graphml")
    print(f"\nExportado: {GRAPHS_DIR / 'grafo_treino.graphml'}")

    # LCC — usada na Fase 2 (modelagem); exportada separadamente
    lcc_nodes = max(nx.connected_components(G), key=len)
    G_lcc     = G.subgraph(lcc_nodes).copy()

    print(f"\n=== Maior componente conexa (LCC) ===")
    print(f"  Vértices : {G_lcc.number_of_nodes()} ({G_lcc.number_of_nodes()/n*100:.1f}% do total)")
    print(f"  Arestas  : {G_lcc.number_of_edges()}")

    nx.write_graphml(G_lcc, GRAPHS_DIR / "grafo_treino_lcc.graphml")
    print(f"Exportado: {GRAPHS_DIR / 'grafo_treino_lcc.graphml'}")
