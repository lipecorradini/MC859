import sqlite3
from pybliometrics.scopus import ScopusSearch, init
import time

def coletar_artigos_unicamp(ano_inicio=2020, ano_fim=2025, ano_lote=2020):
    conn = sqlite3.connect('unicamp_network.db')
    cursor = conn.cursor()
    
    # Criamos a tabela de autores_brutos
    cursor.execute('CREATE TABLE IF NOT EXISTS autores_brutos (auth_id TEXT PRIMARY KEY, processado INTEGER DEFAULT 0)')
    
    # Criamos a tabela inicial se não existir
    cursor.execute('CREATE TABLE IF NOT EXISTS publicacoes (eid TEXT PRIMARY KEY, titulo TEXT, ano INTEGER, author_count INTEGER)')
    
    # Tenta adicionar a nova coluna caso o banco seja de uma execução antiga
    try:
        cursor.execute('ALTER TABLE publicacoes ADD COLUMN ano_lote INTEGER')
    except sqlite3.OperationalError:
        pass # A coluna já existe
        
    cursor.execute('CREATE TABLE IF NOT EXISTS autor_publicacao (auth_id TEXT, eid TEXT, PRIMARY KEY (auth_id, eid))')

    query = f"AFFIL(Unicamp OR \"Universidade Estadual de Campinas\" OR \"University of Campinas\") AND PUBYEAR > {ano_inicio-1} AND PUBYEAR < {ano_fim+1}"
    search = ScopusSearch(query, download=True, refresh=False)

    for doc in search.results or []:
        # Especificamos as colunas no INSERT para evitar erros estruturais
        cursor.execute('''INSERT OR IGNORE INTO publicacoes 
                          (eid, titulo, ano, author_count, ano_lote) 
                          VALUES (?,?,?,?,?)''', 
                       (doc.eid, doc.title, doc.coverDate[:4] if doc.coverDate else None, doc.author_count, ano_lote))
        
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