from pybliometrics.scopus import AuthorRetrieval, init
from pathlib import Path
import sqlite3
import time

DB_PATH = Path(__file__).parent.parent / "data" / "raw" / "unicamp_network.db"

def filtrar_apenas_unicamp():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS autores_unicamp (
        auth_id         TEXT PRIMARY KEY,
        nome            TEXT,
        areas           TEXT,
        citation_count  INTEGER,
        document_count  INTEGER,
        h_index         INTEGER,
        pub_year_first  INTEGER,
        coauthor_count  INTEGER
    )''')

    autores_para_verificar = cursor.execute('SELECT auth_id FROM autores_brutos WHERE processado = 0').fetchall()

    total       = len(autores_para_verificar)
    unicamp     = 0
    processado  = 0
    erros       = 0
    batch_start = time.time()

    print(f"Total de autores a verificar: {total}")

    for (auth_id,) in autores_para_verificar:
        try:
            au = AuthorRetrieval(auth_id)

            is_unicamp = False
            if au.affiliation_history:
                if any(aff.id == '60007324' for aff in au.affiliation_history):
                    is_unicamp = True

            if is_unicamp:
                areas = ";".join([area.area for area in au.subject_areas]) if au.subject_areas else ""
                pub_year_first = au.publication_range[0] if au.publication_range else None

                cursor.execute('''INSERT OR IGNORE INTO autores_unicamp
                                  (auth_id, nome, areas,
                                   citation_count, document_count, h_index,
                                   pub_year_first, coauthor_count)
                                  VALUES (?,?,?,?,?,?,?,?)''',
                               (auth_id,
                                au.given_name + " " + au.surname,
                                areas,
                                au.citation_count,
                                au.document_count,
                                au.h_index,
                                pub_year_first,
                                au.coauthor_count))
                unicamp += 1

            cursor.execute('UPDATE autores_brutos SET processado = 1 WHERE auth_id = ?', (auth_id,))
            conn.commit()
            processado += 1

        except Exception as e:
            msg = str(e)
            if "cannot be found" in msg or "404" in msg:
                # Perfil inexistente no Scopus — marca como processado para não retentar
                cursor.execute('UPDATE autores_brutos SET processado = 1 WHERE auth_id = ?', (auth_id,))
                conn.commit()
            else:
                # Erro recuperável (limite de API, rede) — retenta na próxima execução
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