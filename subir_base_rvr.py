from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy
import os
import pandas as pd
import datetime

class pre_processar:
    
    def __init__(self):
        
        caminho = input('Usar pasta atual? (S/N) ')
        if (caminho.upper() == 'S'):
            self.pasta = os.getcwd()
        else:
            self.pasta = input('Pasta: ')
            
    
    def concatenar(self):
        t1 = datetime.datetime.now()
        
        classe = input('Tipo (ABERTO, PG, CAN): ')

        csvs = [file for file in os.listdir(self.pasta) if classe in file]
        dfs = {}
        for base in csvs:
            dfs[base[:-4]] = pd.read_csv(f'{self.pasta}/{base}', 
                                         encoding='latin-1', 
                                         sep=';', 
                                         decimal=',')

        df = pd.concat(dfs.values())
        del dfs
        
        #NVARCHAR50
        varchar = ['TIPO_DOC',
                     'COD_DOC',
                     'CNPJ_CLIENTE',
                     'NOME_CLIENTE',
                     'STA_NS',
                     ]

        #floats
        floats = ['VLR_PRE_TOT_PGTO', 'VLR_PRE_TOT',
               'VLR_PRE_LIQ', 'VLR_SALDO_CREDITO', 'VLR_EM_ABERTO']

        #ints
        ints = ['PROPOSTA', 'NUM_PARC']

        #datetimes
        dtimes = ['DT_EMISS','DT_VCTO_ORIG', 'DT_VCTO',
               'DT_ATU', 'DT_VIG_INI', 'DT_VIG_FIM']

        df[varchar] = df[varchar].astype(str).astype('category')
        df[floats] = df[floats].astype(float)
        df[ints] = df[ints].astype(int)


        for d in dtimes:
            df[d] = pd.to_datetime(df[d], format='%d/%m/%Y')
        
        tempo = datetime.datetime.now() - t1
        print(f'Arquivos tratados e concatenados. Tempo de execução: {tempo}. \n Total de linhas: {df.shape[0]}')
        return df



class criar_tabela_NS:
    
    def __init__(self):
        self.df = pre_processar().concatenar()
        
    def conexao_atuario(self):
        server = 'PSSQLTABLEAU\TABLEAU' 
        database = 'Atuario'
        username = input(f'Usuário:') #PottencialBI 
        password = input(f'Senha: ') #2536$#MPtte20

        a = "{"
        b = "}"
        connection_string = f"DRIVER={a}ODBC Driver 17 for SQL Server{b};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        self.engine = create_engine(connection_url, fast_executemany=True)
        print('Conexão criada.')
        return self

    def criar_tabela_RVR(self, nome):
        
        t1 = datetime.datetime.now()
        print(f'Escrevendo dados em [Atuario].[dbo].[{nome}]...')
        self.df.to_sql(nome, con=self.engine, if_exists='replace', index=False,chunksize=10000,  #method='multi', chunksize=150,
                       dtype={'datefld': sqlalchemy.types.DateTime(), 
                                 'intfld': sqlalchemy.types.INTEGER(),
                                 'strfld': sqlalchemy.types.NVARCHAR(length=50),
                                 'floatfld': sqlalchemy.types.Float(precision=4, asdecimal=True),
                                 'booleanfld': sqlalchemy.types.Boolean})
        
        tempo = datetime.datetime.now() - t1
        print(f'Tabela criada. Tempo de execução: {tempo}')
    
#2536$#MPtte20
if __name__ == '__main__':
    tabela = criar_tabela_NS()
    tabela.conexao_atuario()
    tabela.criar_tabela_RVR(nome=input('Nome da tabela: ')) #