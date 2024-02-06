import pandas as pd
from functools import reduce
from utils.utils_scraper import ScraperArgenProp, ScraperZonaProp
import asyncio
import geopandas as gpd
from shapely.geometry import Point
import unidecode
import re

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
    '''
        ### Objetivo:
            - Retorna a base do site Argenprop de acordo com os parâmetros de local e tipo
        ### Parâmetros:
            - locais: local do imóvel
            - tipos: tipos do imóvel
    '''
    
    df_argenprop = await ScraperArgenProp(_tipo = tipos, _local = locais).get_final_dataframe()
    return df_argenprop

def fetch_zonaprop_data(locais: list[str] = None, tipos: list[str] = None):
    '''
        ### Objetivo:
            - Retorna a base do site Argenprop de acordo com os parâmetros de local e tipo
        ### Parâmetros:
            - locais: local do imóvel
            - tipos: tipos do imóvel
    '''

    df_zonaprop = ScraperZonaProp(_tipo = tipos, _local = locais).get_final_dataframe()
    return df_zonaprop

def df_to_geopandas(df: pd.DataFrame):
    '''
        ### Objetivo:
            - Recebe como parâmetro um pd.DataFrame e o converte em o GeoDataFrame
    '''

    df.bairro = df.bairro.apply(lambda x: unidecode.unidecode(str(re.sub(' +', ' ', x.strip())).lower()))

    if 'coordenadas' in df.columns:
        df['coordenadas'] = df['coordenadas'].apply(Point)
        geodf = (
            gpd.GeoDataFrame(data = df, geometry = 'coordenadas')
            .pipe(
                lambda df: df.dropna(subset = ['latitude', 'longitude', 'coordenadas'])
            )
        )
        return geodf
    else:
        raise NameError('DataFrame não possui a coluna de coordenadas!')