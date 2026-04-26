"""
Re-parseia o cache local do pybliometrics para identificar autores com
afiliação Unicamp (afid 60029570) sem consumir chamadas de API.

Remove de autores_brutos (e em cascata de autor_publicacao) todos os
autores que não possuem afid 60029570 em nenhuma publicação cacheada.
"""

import json
import sqlite3
from pathlib import Path

CACHE_DIR = Path.home() / ".cache" / "pybliometrics" / "Scopus" / "scopus_search" / "COMPLETE"
DB_PATH   = Path(__file__).parent.parent / "data" / "raw" / "unicamp_network.db"
AFID_UNICAMP = "60029570"


def extrair_unicamp_do_cache():
    unicamp_ids = set()
    total_pubs  = 0
    sem_afid    = 0

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

                autores = doc.get("author", [])
                if not autores:
                    sem_afid += 1
                    continue

                tem_afid = False
                for autor in autores:
                    afids = autor.get("afid", [])
                    if afids:
                        tem_afid = True
                        if any(a.get("$") == AFID_UNICAMP for a in afids):
                            unicamp_ids.add(autor["authid"])

                if not tem_afid:
                    sem_afid += 1

    print(f"  Publicações no cache          : {total_pubs:,}")
    print(f"  Publicações sem afid          : {sem_afid:,}")
    print(f"  Publicações com afid          : {total_pubs - sem_afid:,}")
    print(f"  Auth_ids Unicamp identificados: {len(unicamp_ids):,}")
    return unicamp_ids


def filtrar_banco(unicamp_ids, dry_run=True):
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_brutos = cursor.execute("SELECT COUNT(*) FROM autores_brutos").fetchone()[0]
    confirmados  = cursor.execute(
        f"SELECT COUNT(*) FROM autores_brutos WHERE auth_id IN ({','.join('?' * len(unicamp_ids))})",
        list(unicamp_ids)
    ).fetchone()[0]
    remover = total_brutos - confirmados

    print(f"\n  Total em autores_brutos       : {total_brutos:,}")
    print(f"  Confirmados Unicamp (cache)   : {confirmados:,}")
    print(f"  A remover (sem afid Unicamp)  : {remover:,}")

    if dry_run:
        print("\n  [DRY RUN] Nenhuma alteração feita. Rode com dry_run=False para aplicar.")
        conn.close()
        return

    # Remove de autor_publicacao primeiro (FK)
    cursor.execute(
        f"DELETE FROM autor_publicacao WHERE auth_id NOT IN ({','.join('?' * len(unicamp_ids))})",
        list(unicamp_ids)
    )
    pubs_removidas = cursor.rowcount

    # Remove de autores_brutos
    cursor.execute(
        f"DELETE FROM autores_brutos WHERE auth_id NOT IN ({','.join('?' * len(unicamp_ids))})",
        list(unicamp_ids)
    )

    conn.commit()
    conn.close()

    print(f"\n  Linhas removidas de autor_publicacao : {pubs_removidas:,}")
    print(f"  Autores removidos de autores_brutos  : {remover:,}")
    print("  Banco atualizado com sucesso.")


if __name__ == "__main__":
    print("=== Etapa 1: lendo cache do pybliometrics ===")
    unicamp_ids = extrair_unicamp_do_cache()

    print("\n=== Etapa 2: diagnóstico do banco ===")
    filtrar_banco(unicamp_ids, dry_run=True)

    print("\nPara aplicar a limpeza, altere dry_run=False na última linha e rode novamente.")
