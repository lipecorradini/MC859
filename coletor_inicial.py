import sqlite3
from pybliometrics.scopus import ScopusSearch

def coletar_artigos_unicamp(ano_inicio=2018, ano_fim=2025):
    conn = sqlite3.connect('unicamp_network.db')
    cursor = conn.cursor()
    
    # Criamos a tabela de autores_brutos para decidir depois quem fica
    cursor.execute('CREATE TABLE IF NOT EXISTS autores_brutos (auth_id TEXT PRIMARY KEY, processado INTEGER DEFAULT 0)')
    cursor.execute('CREATE TABLE IF NOT EXISTS publicacoes (eid TEXT PRIMARY KEY, titulo TEXT, ano INTEGER, author_count INTEGER)')
    cursor.execute('CREATE TABLE IF NOT EXISTS autor_publicacao (auth_id TEXT, eid TEXT, PRIMARY KEY (auth_id, eid))')

    query = f"AF-ID(60007324) AND PUBYEAR > {ano_inicio-1} AND PUBYEAR < {ano_fim+1}"
    search = ScopusSearch(query, download=True)

    for doc in search.results:
        cursor.execute('INSERT OR IGNORE INTO publicacoes VALUES (?,?,?,?)', 
                       (doc.eid, doc.title, doc.coverDate[:4], doc.author_count))
        
        if doc.author_ids:
            ids = doc.author_ids.split(';')
            for auth_id in ids:
                cursor.execute('INSERT OR IGNORE INTO autores_brutos (auth_id) VALUES (?)', (auth_id,))
                cursor.execute('INSERT OR IGNORE INTO autor_publicacao VALUES (?,?)', (auth_id, doc.eid))
    
    conn.commit()
    conn.close()
    print("Fase 1 completa: Artigos e lista bruta de autores coletados.")