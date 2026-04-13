import sqlite3
from pybliometrics.scopus import ScopusSearch, init
import time

def coletar_artigos_unicamp(ano_inicio=2020, ano_fim=2025, ano_lote=2020):
    conn = sqlite3.connect('unicamp_network.db')
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
        subject_areas   TEXT,
        authkeywords    TEXT
    )''')

    cursor.execute('CREATE TABLE IF NOT EXISTS autor_publicacao (auth_id TEXT, eid TEXT, PRIMARY KEY (auth_id, eid))')

    query = f"AFFIL(Unicamp OR \"Universidade Estadual de Campinas\" OR \"University of Campinas\") AND PUBYEAR > {ano_inicio-1} AND PUBYEAR < {ano_fim+1}"
    search = ScopusSearch(query, download=True, refresh=False)

    for doc in search.results or []:
        # Serializa listas em strings separadas por ponto-e-vírgula
        subject_areas = ";".join([s.area for s in doc.subject_areas]) if doc.subject_areas else None
        authkeywords  = ";".join(doc.authkeywords) if doc.authkeywords else None

        cursor.execute('''INSERT OR IGNORE INTO publicacoes
                          (eid, titulo, ano, author_count, ano_lote,
                           citedby_count, source_id, subject_areas, authkeywords)
                          VALUES (?,?,?,?,?,?,?,?,?)''',
                       (doc.eid,
                        doc.title,
                        doc.coverDate[:4] if doc.coverDate else None,
                        doc.author_count,
                        ano_lote,
                        doc.citedby_count,
                        doc.source_id,
                        subject_areas,
                        authkeywords))
        
        if doc.author_ids:
            ids = doc.author_ids.split(';')
            for auth_id in ids:
                cursor.execute('INSERT OR IGNORE INTO autores_brutos (auth_id) VALUES (?)', (auth_id,))
                cursor.execute('INSERT OR IGNORE INTO autor_publicacao VALUES (?,?)', (auth_id, doc.eid))
    
    conn.commit()
    conn.close()
    print("Fase 1 completa: Artigos e lista bruta de autores coletados.")

if __name__ == "__main__":
    init()

    anos = [2025, 2024, 2023, 2022]
    
    for ano in anos:
        print("="*50)
        print(f"Iniciando coleta para o ano {ano}...")
        start = time.time()
        coletar_artigos_unicamp(ano_inicio=ano, ano_fim=ano, ano_lote=ano)
        end = time.time() - start
        print(f"Coleta para o ano {ano} concluída em {end:.2f} segundos!")