from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy
import os
import pandas as pd
import datetime

"""
Classe criada para gerar uma tabela SQL SERVER a partir de um pandas DataFrame
"""
class criar_tabela_NS:
    
    def __init__(self):
        
        
    def conexao_atuario(self):
        self.server = input(f'Servidor: ') 
        self.database = input(f'Database: ')
        username = input(f'Usuário:') 
        password = input(f'Senha: ') 

        a = "{"
        b = "}"
        connection_string = f"DRIVER={a}ODBC Driver 17 for SQL Server{b};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        self.engine = create_engine(connection_url, fast_executemany=True)
        print('Conexão criada.')
        return self

    def criar_tabela_RVR(self, nome, df):
        
        t1 = datetime.datetime.now()
        print(f'Escrevendo dados em [{self.database}].[dbo].[{nome}]...')
        
        formatos = {
                   'datefld': sqlalchemy.types.DateTime(), 
                   'intfld': sqlalchemy.types.INTEGER(),
                   'strfld': sqlalchemy.types.NVARCHAR(length=50),
                   'floatfld': sqlalchemy.types.Float(precision=4, asdecimal=True),
                   'booleanfld': sqlalchemy.types.Boolean
                    }
        
        df.to_sql(nome, con=self.engine, if_exists='replace', index=False,chunksize=10000,  #method='multi', chunksize=150,
                       dtype=formatos)
        
        tempo = datetime.datetime.now() - t1
        print(f'Tabela criada. Tempo de execução: {tempo}')
