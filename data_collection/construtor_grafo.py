import sqlite3
from pathlib import Path
from collections import defaultdict
import networkx as nx

# Treino: 2018-2023 | Validação: 2024 | Teste: 2025
# Filtro: >= 2 pubs no treino (Liben-Nowell & Kleinberg, 2007)
# anos_ativos: bitmask inteiro — bit i = 1 se colaboração ocorreu em ANOS[i]
ANO_TREINO_INI  = 2018
ANO_TREINO_FIM  = 2023
ANO_VAL         = 2024
ANO_TESTE       = 2025
MIN_PUBLICACOES = 2
ANOS            = list(range(ANO_TREINO_INI, ANO_TESTE + 1))
NUM_ANOS        = len(ANOS)
MASK_TREINO     = (1 << (ANO_TREINO_FIM - ANO_TREINO_INI + 1)) - 1  # bits 0–5

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

    so_val_teste   = total_brutos - total_1_pub
    filtro_min_pub = total_1_pub - len(auth_ids_validos)

    print(f"\n=== Diagnóstico de autores ===")
    print(f"  Total em autores_brutos (todos os anos)                  : {total_brutos}")
    print(f"  Com publicações no treino ({ANO_TREINO_INI}-{ANO_TREINO_FIM})               : {total_1_pub}")
    print(f"  No grafo (>= {MIN_PUBLICACOES} pubs em {ANO_TREINO_INI}-{ANO_TREINO_FIM})                : {len(auth_ids_validos)}")
    print(f"  Cortados pelo filtro (exatamente 1 pub no treino)        : {filtro_min_pub}")
    print(f"  Sem publicação no treino (só em {ANO_VAL}/{ANO_TESTE})          : {so_val_teste}")
    print(f"  Verificação (soma dos três grupos == total)              : {len(auth_ids_validos) + filtro_min_pub + so_val_teste == total_brutos}")


def construir_grafo_unico(cursor, auth_ids_validos):
    """
    Grafo único com todas as colaborações (ANO_TREINO_INI–ANO_TESTE).
    Cada aresta armazena:
      anos_ativos — inteiro bitmask; bit i = 1 indica colaboração em ANOS[i]
      weight      — número total de coautorias ao longo de todos os anos
    """
    G = nx.Graph()

    for auth_id in auth_ids_validos:
        G.add_node(auth_id)

    cursor.execute('''
        SELECT ap1.auth_id, ap2.auth_id,
               CAST(p.ano AS INTEGER) AS ano,
               COUNT(*)              AS peso_ano
        FROM autor_publicacao ap1
        JOIN autor_publicacao ap2
          ON ap1.eid = ap2.eid AND ap1.auth_id < ap2.auth_id
        JOIN publicacoes p ON ap1.eid = p.eid
        WHERE CAST(p.ano AS INTEGER) BETWEEN ? AND ?
        GROUP BY ap1.auth_id, ap2.auth_id, ano
    ''', (ANO_TREINO_INI, ANO_TESTE))

    bitmasks = defaultdict(int)
    pesos    = defaultdict(int)

    for auth1, auth2, ano, peso_ano in cursor.fetchall():
        if auth1 in auth_ids_validos and auth2 in auth_ids_validos:
            par = (auth1, auth2)
            bitmasks[par] |= 1 << (ano - ANO_TREINO_INI)
            pesos[par]    += peso_ano

    for (auth1, auth2), anos_ativos in bitmasks.items():
        G.add_edge(auth1, auth2, anos_ativos=anos_ativos, weight=pesos[(auth1, auth2)])

    return G


def subgrafo_treino(G):
    """Arestas ativas em pelo menos um ano do período de treino."""
    arestas = [(u, v) for u, v, d in G.edges(data=True)
               if d['anos_ativos'] & MASK_TREINO]
    return G.edge_subgraph(arestas).copy()


def subgrafo_novos(G, ano):
    """Arestas que aparecem pela primeira vez em `ano` (nunca existiram antes)."""
    idx          = ano - ANO_TREINO_INI
    mask_ano     = 1 << idx
    mask_anterior = mask_ano - 1  # bits 0..idx-1
    arestas = [(u, v) for u, v, d in G.edges(data=True)
               if (d['anos_ativos'] & mask_ano)
               and not (d['anos_ativos'] & mask_anterior)]
    return G.edge_subgraph(arestas).copy()


def filtrar_lcc_treino(G):
    """Restringe G aos nós da maior componente conexa do subgrafo de treino."""
    G_treino = subgrafo_treino(G)
    lcc_nodes = max(nx.connected_components(G_treino), key=len)
    return G.subgraph(lcc_nodes).copy()


def salvar(G, nome):
    path = GRAPHS_DIR / nome
    nx.write_graphml(G, path)
    print(f"  Exportado: {path}")


if __name__ == "__main__":
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    auth_ids_validos = get_auth_ids_validos(cursor)
    diagnostico_autores(cursor, auth_ids_validos)

    G = construir_grafo_unico(cursor, auth_ids_validos)
    print(f"\n=== Grafo único ({ANO_TREINO_INI}-{ANO_TESTE}) — antes do filtro LCC ===")
    print(f"  Vértices: {G.number_of_nodes()}")
    print(f"  Arestas : {G.number_of_edges()}")

    G = filtrar_lcc_treino(G)
    print(f"\n=== Grafo único ({ANO_TREINO_INI}-{ANO_TESTE}) — após filtro LCC do treino ===")
    print(f"  Vértices: {G.number_of_nodes()}")
    print(f"  Arestas : {G.number_of_edges()}")
    salvar(G, "grafo_unico.graphml")

    G_treino = subgrafo_treino(G)
    print(f"\n=== Subgrafo de treino ({ANO_TREINO_INI}-{ANO_TREINO_FIM}) ===")
    print(f"  Arestas: {G_treino.number_of_edges()}")

    G_val = subgrafo_novos(G, ANO_VAL)
    print(f"\n=== Subgrafo de validação ({ANO_VAL}) ===")
    print(f"  Arestas novas: {G_val.number_of_edges()}")

    G_teste = subgrafo_novos(G, ANO_TESTE)
    print(f"\n=== Subgrafo de teste ({ANO_TESTE}) ===")
    print(f"  Arestas novas: {G_teste.number_of_edges()}")

    # Métricas acumuladas por período (para tabela do relatório)
    mask_treino_val = (1 << (ANO_VAL - ANO_TREINO_INI + 1)) - 1  # bits 0-6
    n = G.number_of_nodes()
    arestas_treino     = sum(1 for _, _, d in G.edges(data=True)
                             if int(d['anos_ativos']) & MASK_TREINO)
    arestas_treino_val = sum(1 for _, _, d in G.edges(data=True)
                             if int(d['anos_ativos']) & mask_treino_val)
    arestas_total      = G.number_of_edges()

    print(f"\n=== Métricas acumuladas por período ({n} vértices) ===")
    print(f"{'Período':<12} {'Split':<22} {'Arestas':>10} {'Grau médio':>12}")
    print("-" * 58)
    print(f"{'2018-2023':<12} {'Treinamento':<22} {arestas_treino:>10,} {2*arestas_treino/n:>12.2f}")
    print(f"{'2018-2024':<12} {'Treino+Validação':<22} {arestas_treino_val:>10,} {2*arestas_treino_val/n:>12.2f}")
    print(f"{'2018-2025':<12} {'Total (instância)':<22} {arestas_total:>10,} {2*arestas_total/n:>12.2f}")

    conn.close()