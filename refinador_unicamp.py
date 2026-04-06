from pybliometrics.scopus import AuthorRetrieval
import sqlite3
import time

def filtrar_apenas_unicamp():
    conn = sqlite3.connect('unicamp_network.db')
    cursor = conn.cursor()
    
    # Tabela final dos autores que passaram no filtro
    cursor.execute('''CREATE TABLE IF NOT EXISTS autores_unicamp 
                      (auth_id TEXT PRIMARY KEY, nome TEXT, areas TEXT)''')

    # Pegamos autores que ainda não foram processados
    autores_para_verificar = cursor.execute('SELECT auth_id FROM autores_brutos WHERE processado = 0').fetchall()

    for (auth_id,) in autores_para_verificar:
        try:
            au = AuthorRetrieval(auth_id)
            
            # Verificamos se a afiliação atual ou recente é Unicamp (ID 60007324)
            is_unicamp = False
            if au.affiliation_history:
                if any(aff.id == '60007324' for aff in au.affiliation_history):
                    is_unicamp = True
            
            if is_unicamp:
                # Pegamos as áreas de pesquisa (Subject Areas) para a GNN/ML
                areas = ";".join([area.area for area in au.subject_areas]) if au.subject_areas else ""
                cursor.execute('INSERT OR IGNORE INTO autores_unicamp VALUES (?,?,?)',
                               (auth_id, au.given_name + " " + au.surname, areas))
            
            cursor.execute('UPDATE autores_brutos SET processado = 1 WHERE auth_id = ?', (auth_id,))
            conn.commit()
            
        except Exception as e:
            print(f"Erro no autor {auth_id}: {e}")
            time.sleep(1) # Pausa para não ser bloqueado pela API

    conn.close()
    print("Limpeza concluída. Apenas pesquisadores Unicamp restaram na tabela autores_unicamp.")