from pybliometrics.scopus import AuthorRetrieval, init
from pathlib import Path
import networkx as nx
import sqlite3
import time

DB_PATH    = Path(__file__).parent.parent / "data" / "raw" / "unicamp_network.db"
GRAPH_PATH = Path(__file__).parent.parent / "data" / "graphs" / "grafo_unico.graphml"

NOVAS_COLUNAS = [
    ("nome",           "TEXT"),
    ("areas",          "TEXT"),
    ("citation_count", "INTEGER"),
    ("document_count", "INTEGER"),
    ("h_index",        "INTEGER"),
    ("pub_year_first", "INTEGER"),
    ("coauthor_count", "INTEGER"),
    ("is_unicamp",     "INTEGER DEFAULT 0"),
    ("in_graph",       "INTEGER DEFAULT 0"),
    ("refinado",       "INTEGER DEFAULT 0"),
]

def adicionar_colunas(cursor):
    colunas_existentes = {row[1] for row in cursor.execute("PRAGMA table_info(autores_brutos)")}
    for nome, tipo in NOVAS_COLUNAS:
        if nome not in colunas_existentes:
            cursor.execute(f"ALTER TABLE autores_brutos ADD COLUMN {nome} {tipo}")

def filtrar_apenas_unicamp():
    G = nx.read_graphml(GRAPH_PATH)
    graph_nodes = list(G.nodes())
    print(f"Nós no grafo: {len(graph_nodes)}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    adicionar_colunas(cursor)
    conn.commit()

    cursor.executemany("INSERT OR IGNORE INTO autores_brutos (auth_id) VALUES (?)", [(n,) for n in graph_nodes])
    cursor.executemany("UPDATE autores_brutos SET in_graph = 1 WHERE auth_id = ?", [(n,) for n in graph_nodes])
    conn.commit()

    autores_para_verificar = cursor.execute(
        "SELECT auth_id FROM autores_brutos WHERE in_graph = 1 AND refinado = 0"
    ).fetchall()

    total       = len(autores_para_verificar)
    unicamp     = 0
    processado  = 0
    erros       = 0
    batch_start = time.time()

    print(f"Total de autores a verificar: {total}")

    for (auth_id,) in autores_para_verificar:
        try:
            au = AuthorRetrieval(auth_id)

            is_unicamp = (
                au.affiliation_history
                and any(aff.id == '60007324' for aff in au.affiliation_history)
            )

            areas          = ";".join([area.area for area in au.subject_areas]) if au.subject_areas else ""
            pub_year_first = au.publication_range[0] if au.publication_range else None

            cursor.execute('''UPDATE autores_brutos
                              SET nome=?, areas=?, citation_count=?, document_count=?,
                                  h_index=?, pub_year_first=?, coauthor_count=?,
                                  is_unicamp=?, refinado=1
                              WHERE auth_id=?''',
                           (au.given_name + " " + au.surname,
                            areas,
                            au.citation_count,
                            au.document_count,
                            au.h_index,
                            pub_year_first,
                            au.coauthor_count,
                            int(is_unicamp),
                            auth_id))
            conn.commit()
            processado += 1
            if is_unicamp:
                unicamp += 1

        except Exception as e:
            msg = str(e)
            if "cannot be found" in msg or "404" in msg:
                cursor.execute("UPDATE autores_brutos SET refinado=1 WHERE auth_id=?", (auth_id,))
                conn.commit()
            else:
                erros += 1
                print(f"Erro no autor {auth_id}: {e}")
                time.sleep(1)

        if (processado + erros) % 100 == 0:
            elapsed = time.time() - batch_start
            print(f"  [{processado + erros}/{total}] processados | Unicamp: {unicamp} | Erros: {erros} | último batch: {elapsed:.1f}s")
            batch_start = time.time()

    print(f"\nConcluído: {processado} processados, {unicamp} Unicamp, {erros} erros.")

if __name__ == "__main__":
    init()
    filtrar_apenas_unicamp()
