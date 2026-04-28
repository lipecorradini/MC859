"""
Parseia o cache local do pybliometrics para identificar autores com
afiliação Unicamp e cria/popula a tabela autores_unicamp — sem chamadas de API.

Usa o afid 60029570 (Universidade Estadual de Campinas).
"""

import json
import sqlite3
from pathlib import Path
from collections import defaultdict

CACHE_DIR = Path.home() / ".cache" / "pybliometrics" / "Scopus" / "scopus_search" / "COMPLETE"
DB_PATH   = Path(__file__).parent.parent / "data" / "raw" / "unicamp_network.db"
AFID_UNICAMP = "60029570"


def extrair_autores_unicamp_do_cache():
    autores = {}          # auth_id -> nome mais recente
    autor_pubs = defaultdict(set)  # auth_id -> set de eids
    total_pubs = 0

    for cache_file in CACHE_DIR.iterdir():
        with open(cache_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total_pubs += 1
                try:
                    doc = json.loads(line)
                except json.JSONDecodeError:
                    continue

                eid = doc.get("eid", "")
                lista_autores = doc.get("author") or []

                for autor in lista_autores:
                    afids = autor.get("afid") or []
                    afid_values = {a.get("$") for a in afids if a.get("$")}

                    if AFID_UNICAMP in afid_values:
                        auth_id = autor.get("authid")
                        if not auth_id:
                            continue
                        nome = autor.get("authname", "")
                        autores[auth_id] = nome
                        if eid:
                            autor_pubs[auth_id].add(eid)

    print(f"  Publicações no cache            : {total_pubs:,}")
    print(f"  Autores Unicamp identificados   : {len(autores):,}")
    return autores, autor_pubs


def popular_banco(autores, autor_pubs):
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS autores_unicamp")
    cursor.execute("""CREATE TABLE autores_unicamp (
        auth_id  TEXT PRIMARY KEY,
        nome     TEXT
    )""")

    for auth_id, nome in autores.items():
        cursor.execute(
            "INSERT OR IGNORE INTO autores_unicamp (auth_id, nome) VALUES (?, ?)",
            (auth_id, nome),
        )

    # Limpa autores_brutos e autor_publicacao para manter só Unicamp
    unicamp_ids = set(autores.keys())
    placeholders = ",".join("?" * len(unicamp_ids))
    id_list = list(unicamp_ids)

    removidos_ap = 0
    removidos_ab = 0
    if id_list:
        cursor.execute(
            f"DELETE FROM autor_publicacao WHERE auth_id NOT IN ({placeholders})",
            id_list,
        )
        removidos_ap = cursor.rowcount

        cursor.execute(
            f"DELETE FROM autores_brutos WHERE auth_id NOT IN ({placeholders})",
            id_list,
        )
        removidos_ab = cursor.rowcount

    conn.commit()

    total_unicamp = cursor.execute("SELECT COUNT(*) FROM autores_unicamp").fetchone()[0]
    total_brutos  = cursor.execute("SELECT COUNT(*) FROM autores_brutos").fetchone()[0]
    total_ap      = cursor.execute("SELECT COUNT(*) FROM autor_publicacao").fetchone()[0]
    conn.close()

    print(f"\n  Tabela autores_unicamp criada    : {total_unicamp:,} autores")
    print(f"  Removidos de autor_publicacao    : {removidos_ap:,}")
    print(f"  Removidos de autores_brutos      : {removidos_ab:,}")
    print(f"  autores_brutos restantes         : {total_brutos:,}")
    print(f"  autor_publicacao restantes        : {total_ap:,}")


if __name__ == "__main__":
    print("=== Etapa 1: lendo cache do pybliometrics ===")
    autores, autor_pubs = extrair_autores_unicamp_do_cache()

    print("\n=== Etapa 2: populando autores_unicamp e limpando banco ===")
    popular_banco(autores, autor_pubs)

    print("\nConcluído.")