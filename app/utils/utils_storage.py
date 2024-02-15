import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass
import pandas as pd
import re, datetime, os

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
                filters = [('local', 'in', self._locais)]
            )['tipo_imovel'].unique().to_pylist()

            return tipos
        
        except:
            return []

    def check_parquet(self):
        '''
            Checa se existe algum arquivo na pasta com a data atual, caso contrário, apaga o arquivo
        '''

        if re.match('(.*?)_paginas_.*', os.listdir(self._path)[0]).group(1) == str(datetime.datetime.now().date()):
            print('Arquivo já existe!')
        else:
            os.remove(os.path.join(self._path, os.listdir(self._path)[0]))
            print('Arquivo anterior removido!')

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
        tipos = [x for x in self._tipos if x not in self.check_types()]

        # Tipos com 1 ou mais imóveis
        try:
            if 'argenprop' in self._path:
                lista_nao_null = (
                    pd.read_parquet(
                        os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'paginas'),
                        filters = [('local', 'in', self._locais)]
                        
                    )
                    .pipe(lambda df: df.loc[df.imoveis > 0])
                    ['tipo']
                    .to_list()
                )
            else:
                lista_nao_null = (
                    pd.read_parquet(
                        os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'paginas'),
                        filters = [('local', 'in', self._locais)]
                    )
                    .pipe(lambda df: df.loc[df.imoveis > 0])
                    ['tipo']
                    .to_list()
                )
        except:
            lista_nao_null = []

        # Retorna apenas os tipos de imóveis que não constam na base do dia de hoje e que possuem ao menos um imóvel
        return [x for x in tipos if x in lista_nao_null]