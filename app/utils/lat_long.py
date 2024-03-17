from geopy.geocoders import Nominatim
from geopy.distance import distance
import geocoder
import pandas as pd
import os
import unidecode
from utils.log_config import get_logger
import time
from concurrent.futures import ProcessPoolExecutor

# Criando logger
logger = get_logger()

# Retorna estado com base na cidade passada
def get_state(cidade: str = ''):
    try:
        
        logger.info('Agrupando dados de província e departamentos!')
        
        # # Tratando strings
        # cidade = unidecode.unidecode(cidade).lower()

        # # Bases
        # df_dpt = pd.read_parquet(
        #     os.path.join(os.getcwd(),'data','geograficos','departamentos') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','departamentos')
        # )

        # df_prov_muni = pd.read_parquet(
        #     os.path.join(os.getcwd(),'data','geograficos','provincias_municipios') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','provincias_municipios')
        # )

        # logger.info(f'Procurando estado para a cidade {cidade} - {pais}!')

        # # Procurando a cidade nas bases e retornando o estado/provincia
        # base_muni = df_prov_muni[df_prov_muni['municipio'] == cidade][['municipio','provincia']]
        # base_dpt = df_dpt.loc[df_dpt['departamento'] == cidade][['departamento','provincia']]

        # if base_muni.shape[0] > 0: 
        #     estado = base_muni['provincia'].iloc[0]
        # elif base_dpt.shape[0] > 0: 
        #     estado = base_dpt['provincia'].iloc[0]
        # else: 
        #     estado = ''
        
        # JSON COM DADOS DE MUNICIPIO/DEPARTAMENTO E PROVINCIA
        df_mun = pd.read_parquet(
            os.path.join(os.getcwd(),'data','geograficos','provincias_municipios') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','provincias_municipios')
        )
        df_mun_final = df_mun[['municipio_tratado','provincia_tratada']].drop_duplicates()

        df_dpt = pd.read_parquet(
            os.path.join(os.getcwd(),'data','geograficos','departamentos') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','departamentos')
        )
        df_dpt_final = df_dpt[['departamento', 'provincia']].drop_duplicates()

        json_mun = {row['municipio_tratado']:row['provincia_tratada'] for index, row in df_mun_final.iterrows()}
        json_dpt = {row['departamento']:row['provincia'] for index, row in df_dpt_final.iterrows()}

        estado = {**json_dpt, **json_mun}

        # Retornando Estado
        return estado[cidade]
    
    except Exception as e:
        # logger.error(f'Erro na obtenção dos dados: {e}')
        return ''

def get_all_states():

    logger.info('Agrupando dados de província e departamentos!')
    
    # JSON COM DADOS DE MUNICIPIO/DEPARTAMENTO E PROVINCIA
    df_mun = pd.read_parquet(
        os.path.join(os.getcwd(),'data','geograficos','provincias_municipios') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','provincias_municipios')
    )
    df_mun_final = df_mun[['municipio_tratado','provincia_tratada']].drop_duplicates()

    df_dpt = pd.read_parquet(
        os.path.join(os.getcwd(),'data','geograficos','departamentos') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','departamentos')
    )
    df_dpt_final = df_dpt[['departamento', 'provincia']].drop_duplicates()

    json_mun = {row['municipio_tratado']:row['provincia_tratada'] for index, row in df_mun_final.iterrows()}
    json_dpt = {row['departamento']:row['provincia'] for index, row in df_dpt_final.iterrows()}

    estado = {**json_dpt, **json_mun}

    # Retornando Estado
    return estado
    
def get_geocoding(endereco: str = '', bairro: str = '', cidade: str = '', estado: str = '', pais: str = 'Argentina', user_agent: str = 'web_scraping_arg'):
    '''
        ### Objetivo
        - Função para retornar a latitude e longitude de um endereço
        ### Parâmetros
        - endereco: endereço do imóvel
        - bairro: bairro do imóvel
        - cidade: cidade do imóvel
        - estado: estado de localização do imóvel
        - pais: país de localização do imóvel, default 'Argentina'
    '''

    endereco = '' if (endereco == None or endereco.lower() == 'sem info') else endereco
    bairro = '' if (bairro == None or bairro.lower() == 'sem info') else bairro
    cidade = '' if (cidade == None or cidade.lower() == 'sem info') else cidade
    
    # time.sleep(1)

    try:
        
        logger.info('Tratando nome da cidade!')
        
        # Tratando strings
        cidade = unidecode.unidecode(cidade).lower()

        # Retornando latitude e longitude
        geo_string = f'{endereco},{bairro},{cidade},{estado},{pais}'.replace(',,',',')

        logger.info(f'Tentando obter coordenadas do local {geo_string}!')
        # geolocator = Nominatim(user_agent = f"{user_agent}")
        response = geocoder.arcgis(geo_string)

        # location = geolocator.geocode(geo_string)
        # return location.raw
        return response.json
    
    except Exception as e:
        return {'lat': None, 'lon': None}
    
# Função para aplicar a get_geocoding em um Dataframe
def apply_geocoding(row, max_tentativas: int = 1):
    '''
        ### Objetivo:
        - Aplica a função a um dataframe e retorna a latitude e longitude com base no endereço 
        ### Parâmetros:
        - row: linha de um dataframe retornada durante uma iteração
        - max_tentativas: máximo de tentativas caso a requisição falhe ou veja nula
    '''
    
    # time.sleep(1)
    counter = 0

    for _ in range(1, max_tentativas + 1):
        # Resultado de longitude e latitude
        result = get_geocoding(
            endereco = row['endereco'],
            bairro = row['bairro'],
            cidade = row['cidade'],
            estado = row['estado']
        )

        if result.get('lat') is not None and result.get('lng') is not None:
            resultado = {
                    'id': row['id'],
                    'latitude': result.get('lat'), 
                    'longitude': result.get('lng')
            }

            return resultado
        
        counter += 1
        time.sleep(counter)

    resultado = {
                'id': row['id'],
                'latitude': None, 
                'longitude': None
        }
    
    return resultado

# Função para pegar a distancia lidando com erros
def get_distance_unr(coordenada: list = []):
    try:
        distancia = round(
            distance((-32.94002703733129, -60.66512777075645), tuple(coordenada)).km, 
            3
        )
    except:
        distancia = 9999.0
    return distancia

def get_distance_provincial(coordenada: list = []):
    try:
        distancia = round(
            distance((-32.95532893189587, -60.629488104849905), tuple(coordenada)).km, 
            3
        )
    except:
        distancia = 9999.0
    return distancia

def get_distance_baigorria(coordenada: list = []):
    try:
        distancia = round(
            distance((-32.855384310532656, -60.704628940365566), tuple(coordenada)).km, 
            3
        )
    except:
        distancia = 9999.0
    return distancia

def get_distance_ninos(coordenada: list = []):
    try:
        distancia = round(
            distance((-32.96587782771106, -60.65125791535292), tuple(coordenada)).km, 
            3
        )
    except:
        distancia = 9999.0
    return distancia

def get_distance_carrasco(coordenada: list = []):
    try:
        distancia = round(
            distance((-32.94595943742149, -60.679916866738836), tuple(coordenada)).km, 
            3
        )
    except:
        distancia = 9999.0
    return distancia