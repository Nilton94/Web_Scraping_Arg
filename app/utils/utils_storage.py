import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass
import pandas as pd
import duckdb
import re, datetime, os

def get_paths():
    '''
        Retorna paths importantes da pasta data
    '''
    try:
        return {
            'argenprop': {
                'imoveis': os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'imoveis') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'imoveis'), 
                'paginas': os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'paginas'), 
                'bronze': os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'imoveis', 'bronze') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'imoveis', 'bronze'), 
                'silver': os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'imoveis', 'silver') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'imoveis', 'silver')
            },

            'zonaprop': {
                'imoveis': os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'imoveis') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'imoveis'), 
                'paginas': os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'paginas'), 
                'bronze': os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'imoveis', 'bronze') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'imoveis', 'bronze'), 
                'silver': os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'imoveis', 'silver') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'imoveis', 'silver')
            }
        }
    except Exception as e:
        print(f'Erro: {e}')

@dataclass
class ParquetStorage:

    _path: str = None
    _df: pd.DataFrame = None
    _tipos: list = None
    _locais: list = None

    def check_types(self):
        '''
            Checa quais tipos de imóveis constam na base
        '''

        try:
            tipos = pq.read_table(
                self._path, 
                filters = [('cidade', 'in', self._locais)]
            )['tipo_imovel'].unique().to_pylist()

            return tipos
        
        except:
            return []

    def check_parquet(self):
        '''
            Checa se existe algum arquivo na pasta com a data atual, caso contrário, apaga o arquivo
        '''

        try:
            if re.match('(.*?)_(.*)_.*', os.listdir(self._path)[0]).group(1) == str(datetime.datetime.now().date()):
                print('Arquivo já existe!')
            else:
                os.remove(os.path.join(self._path, os.listdir(self._path)[0]))
                print('Arquivo anterior removido!')
        except Exception as e:
            print('Não existe arquivo!')

    def check_files(self):
        '''
            Checa se existe algum arquivo na pasta
        '''

        return len(os.listdir(self._path))
    
    def types_intersection(self):
        '''
            Checa quais tipos de imóveis constam na base atualmente
        '''

        # Tipos que nao existem na base
        if self.check_types() == [] or self.check_types() is None:
            tipos = []
        else:
            tipos = [x for x in self._tipos if x not in self.check_types()]

        # Tipos com 1 ou mais imóveis
        try:
            if 'argenprop' in self._path:
                lista_nao_null = (
                    pd.read_parquet(
                        os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'paginas'),
                        filters = [('cidade', 'in', self._locais)]
                        
                    )
                    .pipe(lambda df: df.loc[df.imoveis > 0])
                    ['tipo_imovel']
                    .to_list()
                )
            else:
                lista_nao_null = (
                    pd.read_parquet(
                        os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'paginas'),
                        filters = [('cidade', 'in', self._locais)]
                    )
                    .pipe(lambda df: df.loc[df.imoveis > 0])
                    ['tipo_imovel']
                    .to_list()
                )
        except:
            lista_nao_null = []

        # Retorna apenas os tipos de imóveis que não constam na base do dia de hoje e que possuem ao menos um imóvel
        # try:
        resultado = [x for x in tipos if x in lista_nao_null]
        # except:
            # resultado = []

        return resultado
    
@dataclass
class DuckDBtStorage:
    '''
        Classe para realizar operações no DB do DuckDB.

        Parâmetros:
        ----------
        _path: str
            path do DB
        _df: pd.DataFrame
            Dataframe para ser inserido, nas operações de INSERT, CREATE
        _tipos: list
            Lista com os tipos selecionados no frontend (Streamlit)
        _locais: list
            Lista com os locais selecionados no frontend (Streamlit)
        _tabela: str
            Tabela em que a operação irá ocorrer

    '''

    _path: str = None
    _df: pd.DataFrame = None
    _tipos: list = None
    _locais: list = None
    _tabela: str = None

    def create_table(self):
        '''
            Cria a tabela, caso não exista
        '''

        try:
            # Conexão
            cursor = duckdb.connect(self._path)

            # Criando tabela baseado no dataframe
            df_create = self._df
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {self._tabela} AS SELECT * FROM df_create')
            cursor.close()

        except Exception as e:
            print(f'Erro: {e}')

    def drop_old_data(self, timedelta: int = 1):
        '''
            Limpa os dados anteriores ao dia atual, a fim de reduzir quantidade de dados.
        '''

        try:
            # Conexão
            cursor = duckdb.connect(self._path)

            # Deletando dados anteriores a hoje
            data = str(datetime.datetime.now().date() - datetime.timedelta(timedelta))
            cursor.execute(f"DELETE FROM {self._tabela} WHERE data::DATE <= ? ", (data,))
            cursor.commit()
            cursor.close()
            print(f'Dados antigos excluídos!')

        except Exception as e:
            print(f'Erro: {e}')
    
    def  check_table(self):
        '''
            Checa se os locais e tipos passados no frontend já existem na base
        '''

        try:
            # Conexão
            cursor = duckdb.connect(self._path)

            # Checando quais dados constam na tabela
            df = cursor.execute(
                f'''
                    SELECT 
                            DISTINCT tipo_imovel, 
                            cidade 
                    FROM {self._tabela}
                    WHERE 
                            cidade IN ({"'"+"', '".join([i for i in self._locais])+"'"})
                            AND tipo_imovel IN ({"'"+"', '".join([i for i in self._tipos])+"'"})
                '''
            ).df().pipe(lambda df: df.tipo_imovel.unique())
            cursor.close()

            # Lista com os tipos passados que não constam na base
            lista_tipos = list(set(self._tipos) - set(df))

            return lista_tipos
        except Exception as e:
            print(f'Erro: {e}')
    
    def insert_data(self):
        '''
            Insere os dados na tabela especificada com base em um dataframe passado.
        '''

        # Conexão
        cursor = duckdb.connect(self._path)

        # Inserindo dados
        df_insert = self._df
        cursor.execute(
            f"""
                INSERT INTO {self._tabela} 
                SELECT * FROM df_insert
            """
        )
        cursor.close()



