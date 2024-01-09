from geopy.geocoders import Nominatim
import geocoder
import pandas as pd
import os
import unidecode
from utils.log_config import get_logger
import time
from concurrent.futures import ProcessPoolExecutor

# Criando logger
logger = get_logger()

def get_state(cidade: str = '', pais: str = 'argentina'):
    try:
        
        logger.info('Tratando nome da cidade!')
        
        # Tratando strings
        cidade = unidecode.unidecode(cidade).lower()

        # Bases
        df_dpt = pd.read_parquet(
            os.path.join(os.getcwd(),'data','geograficos','departamentos') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','departamentos')
        )

        df_prov_muni = pd.read_parquet(
            os.path.join(os.getcwd(),'data','geograficos','provincias_municipios') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','provincias_municipios')
        )

        logger.info(f'Procurando estado para a cidade {cidade} - {pais}!')

        # Procurando a cidade nas bases e retornando o estado/provincia
        base_muni = df_prov_muni[df_prov_muni['municipio'] == cidade][['municipio','provincia']]
        base_dpt = df_dpt.loc[df_dpt['departamento'] == cidade][['departamento','provincia']]

        if base_muni.shape[0] > 0: 
            estado = base_muni['provincia'].iloc[0]
        elif base_dpt.shape[0] > 0: 
            estado = base_dpt['provincia'].iloc[0]
        else: 
            estado = ''

        # Retornando Estado
        return estado
    
    except Exception as e:
        # logger.error(f'Erro na obtenção dos dados: {e}')
        return ''

def get_geocoding(endereco: str = '', bairro: str = '', cidade: str = '', estado: str = '', pais: str = 'Argentina', user_agent: str = 'web_scraping_arg'):
    '''
        ### Objetivo
        - Função para retornar a latitude e longitude de um endereço
    '''

    endereco = '' if (endereco == None or endereco.lower() == 'sem info') else endereco
    bairro = '' if (bairro == None or bairro.lower() == 'sem info') else bairro
    cidade = '' if (cidade == None or cidade.lower() == 'sem info') else cidade
    
    # time.sleep(1)

    try:
        
        logger.info('Tratando nome da cidade!')
        
        # Tratando strings
        cidade = unidecode.unidecode(cidade).lower()

        # Bases
        df_dpt = pd.read_parquet(
            os.path.join(os.getcwd(),'data','geograficos','departamentos') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','departamentos')
        )

        df_prov_muni = pd.read_parquet(
            os.path.join(os.getcwd(),'data','geograficos','provincias_municipios') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','provincias_municipios')
        )

        logger.info(f'Procurando estado para a cidade {cidade} - {pais}!')

        # Procurando a cidade nas bases e retornando o estado/provincia
        base_muni = df_prov_muni[df_prov_muni['municipio'] == cidade][['municipio','provincia']]
        base_dpt = df_dpt.loc[df_dpt['departamento'] == cidade][['departamento','provincia']]

        if base_muni.shape[0] > 0: 
            estado = base_muni['provincia'].iloc[0]
        elif base_dpt.shape[0] > 0: 
            estado = base_dpt['provincia'].iloc[0]
        else: 
            estado = ''

        # Retornando latitude e longitude
        geo_string = f'{endereco},{bairro},{cidade},{estado},{pais}'.replace(',,',',')
        # geolocator = Nominatim(user_agent = f"{user_agent}")
        response = geocoder.arcgis(geo_string)

        logger.info(f'Tentando obter coordenadas do local {geo_string}!')

        # location = geolocator.geocode(geo_string)
        # return location.raw
        return response.json
    
    except Exception as e:
        # logger.error(f'Erro na obtenção dos dados: {e}')
        return {'lat': None, 'lon': None}
    
# Função para aplicar a get_geocoding em um Dataframe
def apply_geocoding(row):
    result = get_geocoding(
        endereco = row['endereco'],
        bairro = row['bairro'],
        cidade = row['cidade']
    )
    return pd.Series({'latitude': result.get('lat'), 'longitude': result.get('lon')})