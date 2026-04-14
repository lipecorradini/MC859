import sqlite3
from pathlib import Path
import networkx as nx

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
ANO_VAL         = 2024
ANO_TESTE       = 2025
MIN_PUBLICACOES = 2

DB_PATH    = Path(__file__).parent.parent / "data" / "raw" / "unicamp_network.db"
GRAPHS_DIR = Path(__file__).parent.parent / "data" / "graphs"


def get_auth_ids_validos(cursor):
    """Retorna o conjunto de autores com >= MIN_PUBLICACOES no período de treino."""
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
    return {row[0] for row in cursor.fetchall()}


def diagnostico_autores(cursor, auth_ids_validos):
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
    print(f"  Apenas em {ANO_VAL}/{ANO_TESTE} (fora do treino)             : {total_brutos - total_1_pub}")


def construir_grafo_treino(cursor, auth_ids_validos):
    """Grafo de coautoria no período de treino (2018-2023)."""
    G = nx.Graph()
    for auth_id in auth_ids_validos:
        G.add_node(auth_id)

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

    return G


def construir_grafo_periodo(cursor, auth_ids_validos, ano, arestas_existentes):
    """
    Grafo de novas colaborações em `ano`, considerando apenas pares de autores
    que ainda não colaboraram nos períodos anteriores (arestas_existentes).
    Representa os links positivos a serem previstos naquele período.
    """
    G = nx.Graph()
    for auth_id in auth_ids_validos:
        G.add_node(auth_id)

    cursor.execute('''
        SELECT ap1.auth_id, ap2.auth_id,
               COUNT(*)                    AS peso,
               MIN(CAST(p.ano AS INTEGER)) AS ano_primeira_colab
        FROM autor_publicacao ap1
        JOIN autor_publicacao ap2
          ON ap1.eid = ap2.eid AND ap1.auth_id < ap2.auth_id
        JOIN publicacoes p ON ap1.eid = p.eid
        WHERE CAST(p.ano AS INTEGER) = ?
        GROUP BY ap1.auth_id, ap2.auth_id
    ''', (ano,))

    for auth1, auth2, peso, ano_primeira_colab in cursor.fetchall():
        par = (min(auth1, auth2), max(auth1, auth2))
        if (auth1 in auth_ids_validos and auth2 in auth_ids_validos
                and par not in arestas_existentes):
            G.add_edge(auth1, auth2, weight=peso, ano_primeira_colab=ano_primeira_colab)

    return G


def salvar(G, nome):
    path = GRAPHS_DIR / nome
    nx.write_graphml(G, path)
    print(f"  Exportado: {path}")


if __name__ == "__main__":
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    auth_ids_validos = get_auth_ids_validos(cursor)
    diagnostico_autores(cursor, auth_ids_validos)

    # --- Grafo de treino ---
    G_treino = construir_grafo_treino(cursor, auth_ids_validos)
    print(f"\n=== Grafo de treino ({ANO_TREINO_INI}-{ANO_TREINO_FIM}) ===")
    print(f"  Vértices: {G_treino.number_of_nodes()}")
    print(f"  Arestas : {G_treino.number_of_edges()}")
    salvar(G_treino, "grafo_treino.graphml")

    # Conjunto de arestas já existentes no treino (para filtrar novidades em val/teste)
    arestas_treino = {(min(u, v), max(u, v)) for u, v in G_treino.edges()}

    # --- Grafo de validação (novas colaborações em 2024) ---
    G_val = construir_grafo_periodo(cursor, auth_ids_validos, ANO_VAL, arestas_treino)
    print(f"\n=== Grafo de validação ({ANO_VAL}) ===")
    print(f"  Vértices: {G_val.number_of_nodes()}")
    print(f"  Arestas (novas colaborações): {G_val.number_of_edges()}")
    salvar(G_val, "grafo_validacao.graphml")

    # --- Grafo de teste (novas colaborações em 2025) ---
    arestas_treino_val = arestas_treino | {(min(u, v), max(u, v)) for u, v in G_val.edges()}
    G_teste = construir_grafo_periodo(cursor, auth_ids_validos, ANO_TESTE, arestas_treino_val)
    print(f"\n=== Grafo de teste ({ANO_TESTE}) ===")
    print(f"  Vértices: {G_teste.number_of_nodes()}")
    print(f"  Arestas (novas colaborações): {G_teste.number_of_edges()}")
    salvar(G_teste, "grafo_teste.graphml")

    conn.close()
