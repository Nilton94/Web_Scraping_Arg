import pandas as pd
from functools import reduce
from utils.utils_scraper import ScraperArgenProp, ScraperZonaProp
import asyncio

class TiposImoveis:
    
    def argenprop_tipos(self) -> list[str]:
        tipos = ['departamentos','casas','campos','cocheras','fondos-de-comercio','galpones','hoteles','locales','negocios-especiales','oficinas','ph','quintas','terrenos']
        return tipos

    def zonaprop_tipos(self) -> list[str]:
        tipos = [
            'casas','departamentos','ph','locales-comerciales','oficinas-comerciales','bodegas-galpones','cocheras','depositos',
            'terrenos','edificios','quintas-vacacionales','campos','fondos-de-comercio','hoteles','consultorios','cama-nautica','bovedas-nichos-y-parcelas'
        ]

        return tipos

    def total_tipos(self) -> list[str]:
        
        tipos_argenprop = self.argenprop_tipos()
        tipos_zonaprop = self.zonaprop_tipos()

        lista_tipos_agg = list(
            set(tipos_argenprop).union(tipos_zonaprop)
        )

        lista_tipos_agg.sort()

        return lista_tipos_agg
    
def get_columns_intersection(*dataframes: pd.DataFrame) -> list[str]:
    '''
        *** Objetivo:
        - Dados dois ou mais dataframes, encontra as colunas com os nomes comuns entre eles
        *** Parâmetros:
        - dataframes: conjunto de 1 ou mais dataframes a serem avaliados
    '''

    # Checa se ao menos dois dataframes foram passados
    if len(dataframes) < 2:
        raise ValueError('Necessário ao menos dois dataframes')
    
    # Aplica a intersecção a cada par de dataframes (1 & 2 -> resultado1, resultado1 & 3 -> resultado2, etc)
    colunas = reduce(
        lambda x, y: set(x.columns) & set(y.columns), 
        dataframes
    )

    return list(colunas)

async def fetch_argenprop_data(locais: list[str] = None, tipos: list[str] = None):
    
    df_argenprop = await ScraperArgenProp(_tipo = tipos, _local = locais).get_final_dataframe()
    
    return df_argenprop

def fetch_zonaprop_data(locais: list[str] = None, tipos: list[str] = None):
    
    df_argenprop = ScraperArgenProp(_tipo = tipos, _local = locais).get_final_dataframe()
    
    return df_argenprop