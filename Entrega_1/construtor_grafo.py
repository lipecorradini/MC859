import sqlite3
import networkx as nx

# Decisões metodológicas documentadas
# Treino: 2018-2023 | Validação: 2024 | Teste: 2025
# Filtro: >= 2 publicações no período de treino (análogo ao "Core" de Liben-Nowell & Kleinberg, 2007)
# Grafo não-direcionado: coautoria é relação simétrica (Newman, 2001)
# Anonimização: auth_id Scopus como identificador — nome não exportado (Makarov et al., 2019)
ANO_TREINO_INI  = 2018
ANO_TREINO_FIM  = 2023
MIN_PUBLICACOES = 2

def construir_grafo(db_path='unicamp_network.db'):
    conn   = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Autores Unicamp com >= MIN_PUBLICACOES no período de treino
    cursor.execute('''
        SELECT au.auth_id, au.areas, au.citation_count, au.document_count,
               au.h_index, au.pub_year_first, au.coauthor_count
        FROM autores_unicamp au
        WHERE (
            SELECT COUNT(*)
            FROM autor_publicacao ap
            JOIN publicacoes p ON ap.eid = p.eid
            WHERE ap.auth_id = au.auth_id
              AND CAST(p.ano AS INTEGER) BETWEEN ? AND ?
        ) >= ?
    ''', (ANO_TREINO_INI, ANO_TREINO_FIM, MIN_PUBLICACOES))

    autores = cursor.fetchall()
    auth_ids_validos = {row[0] for row in autores}
    print(f"Autores válidos (>= {MIN_PUBLICACOES} publicações em {ANO_TREINO_INI}-{ANO_TREINO_FIM}): {len(auth_ids_validos)}")

    G = nx.Graph()

    # Nós com atributos — nome excluído (anonimização)
    for auth_id, areas, citation_count, document_count, h_index, pub_year_first, coauthor_count in autores:
        G.add_node(
            auth_id,
            areas          = areas          or "",
            citation_count = citation_count or 0,
            document_count = document_count or 0,
            h_index        = h_index        or 0,
            pub_year_first = pub_year_first or 0,
            coauthor_count = coauthor_count or 0,
        )

    # Arestas: pares de autores Unicamp que co-publicaram no período de treino
    # O JOIN com autores_unicamp garante que ambos os extremos são pesquisadores da Unicamp
    cursor.execute('''
        SELECT ap1.auth_id, ap2.auth_id,
               COUNT(*)                    AS peso,
               MIN(CAST(p.ano AS INTEGER)) AS ano_primeira_colab
        FROM autor_publicacao ap1
        JOIN autor_publicacao ap2
          ON ap1.eid = ap2.eid AND ap1.auth_id < ap2.auth_id
        JOIN publicacoes p  ON ap1.eid = p.eid
        JOIN autores_unicamp u1 ON ap1.auth_id = u1.auth_id
        JOIN autores_unicamp u2 ON ap2.auth_id = u2.auth_id
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

    nx.write_graphml(G, "grafo_treino.graphml")
    print("\nExportado: grafo_treino.graphml")

    # LCC — usada na Fase 2 (modelagem); exportada separadamente
    lcc_nodes = max(nx.connected_components(G), key=len)
    G_lcc     = G.subgraph(lcc_nodes).copy()

    print(f"\n=== Maior componente conexa (LCC) ===")
    print(f"  Vértices : {G_lcc.number_of_nodes()} ({G_lcc.number_of_nodes()/n*100:.1f}% do total)")
    print(f"  Arestas  : {G_lcc.number_of_edges()}")

    nx.write_graphml(G_lcc, "grafo_treino_lcc.graphml")
    print("Exportado: grafo_treino_lcc.graphml")
