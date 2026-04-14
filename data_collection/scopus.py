import sqlite3
from pathlib import Path
from pybliometrics.scopus import ScopusSearch, init
import time

DB_PATH = Path(__file__).parent.parent / "data" / "raw" / "unicamp_network.db"

def coletar_artigos_unicamp(ano_inicio, ano_fim, ano_lote):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('CREATE TABLE IF NOT EXISTS autores_brutos (auth_id TEXT PRIMARY KEY, processado INTEGER DEFAULT 0)')

    cursor.execute('''CREATE TABLE IF NOT EXISTS publicacoes (
        eid             TEXT PRIMARY KEY,
        titulo          TEXT,
        ano             INTEGER,
        author_count    INTEGER,
        ano_lote        INTEGER,
        citedby_count   INTEGER,
        source_id       TEXT,
        authkeywords    TEXT
    )''')

    cursor.execute('CREATE TABLE IF NOT EXISTS autor_publicacao (auth_id TEXT, eid TEXT, PRIMARY KEY (auth_id, eid))')

    query = f"AFFIL(Unicamp OR \"Universidade Estadual de Campinas\" OR \"University of Campinas\") AND PUBYEAR > {ano_inicio-1} AND PUBYEAR < {ano_fim+1}"
    search = ScopusSearch(query, download=True, refresh=False)

    for doc in search.results or []:
        authkeywords = ";".join(doc.authkeywords) if doc.authkeywords else None

        cursor.execute('''INSERT OR IGNORE INTO publicacoes
                          (eid, titulo, ano, author_count, ano_lote,
                           citedby_count, source_id, authkeywords)
                          VALUES (?,?,?,?,?,?,?,?)''',
                       (doc.eid,
                        doc.title,
                        doc.coverDate[:4] if doc.coverDate else None,
                        doc.author_count,
                        ano_lote,
                        doc.citedby_count,
                        doc.source_id,
                        authkeywords))
        
        # Pré-filtro: guarda apenas autores cuja afiliação na publicação é a Unicamp (ID 60007324).
        # doc.author_afids tem um entry por autor, separados por ';'. Cada entry pode conter
        # múltiplas afiliações separadas por '-'. Ex: "60007324-60003999;55432100"
        # Fallback: se author_afids não estiver disponível no cache, inclui todos os autores
        # e delega a filtragem ao refinador_unicamp.py.
        if doc.author_ids:
            ids = doc.author_ids.split(';')
            if doc.author_afids:
                afids = doc.author_afids.split(';')
                for auth_id, afid_entry in zip(ids, afids):
                    if '60029570' in afid_entry: # Número afid referente à unicamp
                        cursor.execute('INSERT OR IGNORE INTO autores_brutos (auth_id) VALUES (?)', (auth_id.strip(),))
                        cursor.execute('INSERT OR IGNORE INTO autor_publicacao VALUES (?,?)', (auth_id.strip(), doc.eid))
            else:
                for auth_id in ids:
                    cursor.execute('INSERT OR IGNORE INTO autores_brutos (auth_id) VALUES (?)', (auth_id.strip(),))
                    cursor.execute('INSERT OR IGNORE INTO autor_publicacao VALUES (?,?)', (auth_id.strip(), doc.eid))
    
    conn.commit()

    total_publicacoes = cursor.execute('SELECT COUNT(*) FROM publicacoes WHERE ano_lote = ?', (ano_lote,)).fetchone()[0]
    total_autores     = cursor.execute('SELECT COUNT(*) FROM autores_brutos').fetchone()[0]
    conn.close()
    print(f"Fase 1 completa: {total_publicacoes} publicações e {total_autores} autores únicos (acumulado) coletados.")

if __name__ == "__main__":
    init()

    # Coleta completa: 2018-2023 (treino), 2024 (validação), 2025 (teste)
    anos = list(range(2025, 2017, -1))
    
    for ano in anos:
        print("="*50)
        print(f"Iniciando coleta para o ano {ano}...")
        start = time.time()
        coletar_artigos_unicamp(ano_inicio=ano, ano_fim=ano, ano_lote=ano)
        end = time.time() - start
        print(f"Coleta para o ano {ano} concluída em {end:.2f} segundos!")