import csv
import sqlite3
import time
from pathlib import Path

import networkx as nx
from pybliometrics.scopus import AuthorRetrieval, init

DB_PATH    = Path(__file__).parent.parent / "data" / "raw" / "unicamp_network.db"
GRAPH_PATH = Path(__file__).parent.parent / "data" / "graphs" / "grafo_unico.graphml"
CSV_PATH   = Path(__file__).parent.parent / "data" / "processed" / "autores_grafo.csv"

CSV_COLS = ["auth_id", "areas", "citation_count",
            "document_count", "h_index", "pub_year_first", "coauthor_count"]


def preparar_tabela(cursor):
    for col, tipo in [
        ("areas",          "TEXT"),
        ("citation_count", "INTEGER"),
        ("document_count", "INTEGER"),
        ("h_index",        "INTEGER"),
        ("pub_year_first", "INTEGER"),
        ("coauthor_count", "INTEGER"),
        ("processado",     "INTEGER DEFAULT 0"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE autores_unicamp ADD COLUMN {col} {tipo}")
        except Exception:
            pass


def carregar_ids_csv():
    """Returns the set of auth_ids already written to the CSV."""
    if not CSV_PATH.exists():
        return set()
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["auth_id"] for row in reader}


def sincronizar_csv(cursor, auth_ids_grafo, ids_no_csv):
    """Writes to CSV any graph authors that are already processed in DB but missing from CSV."""
    placeholders = ",".join("?" * len(auth_ids_grafo))
    ja_processados = cursor.execute(
        f"""SELECT auth_id, areas, citation_count, document_count,
                   h_index, pub_year_first, coauthor_count
            FROM autores_unicamp
            WHERE processado = 1
              AND citation_count IS NOT NULL
              AND auth_id IN ({placeholders})""",
        list(auth_ids_grafo),
    ).fetchall()

    faltando = [row for row in ja_processados if row[0] not in ids_no_csv]
    if not faltando:
        return 0

    escrever_csv = not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if escrever_csv:
            writer.writerow(CSV_COLS)
        writer.writerows(faltando)

    return len(faltando)


def appendar_csv(row: dict):
    escrever_header = not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLS)
        if escrever_header:
            writer.writeheader()
        writer.writerow(row)


def obter_metricas():
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    G = nx.read_graphml(GRAPH_PATH)
    auth_ids_grafo = set(G.nodes())

    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    preparar_tabela(cursor)
    conn.commit()

    ids_no_csv = carregar_ids_csv()
    sincronizados = sincronizar_csv(cursor, auth_ids_grafo, ids_no_csv)
    ids_no_csv = carregar_ids_csv()  # reload after sync

    pendentes = cursor.execute(
        "SELECT auth_id FROM autores_unicamp WHERE processado = 0 AND auth_id IN ({})".format(
            ",".join("?" * len(auth_ids_grafo))
        ),
        list(auth_ids_grafo),
    ).fetchall()

    total      = len(pendentes)
    processado = 0
    erros      = 0
    batch_start = time.time()

    print(f"Autores no grafo       : {len(auth_ids_grafo)}")
    print(f"Já no CSV (após sync)  : {len(ids_no_csv)} (+{sincronizados} sincronizados agora)")
    print(f"Pendentes de fetch     : {total}\n")

    for (auth_id,) in pendentes:
        done = processado + erros
        print(f"[{done + 1}/{total}] {auth_id}", end=" ... ", flush=True)

        try:
            au = AuthorRetrieval(auth_id)

            areas          = ";".join(a.area for a in au.subject_areas) if au.subject_areas else ""
            pub_year_first = au.publication_range[0] if au.publication_range else None

            cursor.execute(
                """UPDATE autores_unicamp
                   SET areas = ?, citation_count = ?, document_count = ?,
                       h_index = ?, pub_year_first = ?, coauthor_count = ?,
                       processado = 1
                   WHERE auth_id = ?""",
                (areas, au.citation_count, au.document_count,
                 au.h_index, pub_year_first, au.coauthor_count,
                 auth_id),
            )
            conn.commit()

            if auth_id not in ids_no_csv:
                appendar_csv({
                    "auth_id":        auth_id,
                    "areas":          areas,
                    "citation_count": au.citation_count,
                    "document_count": au.document_count,
                    "h_index":        au.h_index,
                    "pub_year_first": pub_year_first,
                    "coauthor_count": au.coauthor_count,
                })
                ids_no_csv.add(auth_id)

            processado += 1
            print(f"ok (h={au.h_index}, cit={au.citation_count}, docs={au.document_count})")

        except Exception as e:
            msg = str(e)
            if "cannot be found" in msg or "404" in msg:
                cursor.execute(
                    "UPDATE autores_unicamp SET processado = 1 WHERE auth_id = ?", (auth_id,)
                )
                conn.commit()
                processado += 1
                print("não encontrado — ignorado")
            else:
                erros += 1
                print(f"ERRO: {e}")
                time.sleep(1)

        done = processado + erros
        if done % 100 == 0:
            elapsed = time.time() - batch_start
            print(f"\n--- {done}/{total} novos | CSV total: {len(ids_no_csv)} | erros: {erros} | último batch: {elapsed:.1f}s ---\n")
            batch_start = time.time()

    conn.close()
    print(f"\nConcluído: {processado} processados, {erros} erros.")
    print(f"CSV final: {len(ids_no_csv)} autores em {CSV_PATH}")


if __name__ == "__main__":
    init()
    obter_metricas()
