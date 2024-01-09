import pandas as pd
from pandas import json_normalize
import requests
from bs4 import BeautifulSoup
from random_user_agent.user_agent import UserAgent
import pyarrow as pa
import pyarrow.parquet as pq
import re
import os
import asyncio
import aiohttp
from dataclasses import dataclass
import datetime
import pytz
from utils.log_config import get_logger
from utils.lat_long import get_geocoding, get_state

# Criando logger
logger = get_logger()

# CLASSE DO ARGENPROP
@dataclass
class ScraperArgenProp:
    
    # Parametros da classe
    _tipo: list = None
    _local: list = None


    # Retorna a página async
    async def get_page(self, session, url):
        '''
            ### Objetivo 
            - Função para retornar o codigo html da página com base na url passada de forma assincrona usando asyncio e aiohttp
            ### Parâmetros
            - session: aiohttp session
            - url: url do site desejado
        '''

        try:
            async with session.get(url) as response:
                
                logger.info(f'Obtendo código html da url {url}')

                html_source = await response.text()
                return {
                    'local': re.compile(r'\.com/(.*?)/alquiler/(.*?)\?').search(url).group(2),
                    'tipo': re.compile(r'\.com/(.*?)/alquiler/(.*?)\?').search(url).group(1),
                    'imoveis': float(re.sub('[^0-9]','',BeautifulSoup(html_source, 'html.parser').find('p','listing-header__results').text)) if BeautifulSoup(html_source, 'html.parser').find('p','listing-header__results') != None else 0.0,
                    'base': re.search(r'www\.(.*?)\.com', url).group(1),
                    'url': url, 
                    'status code': response.status,
                    'html': html_source,
                    'data': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')),
                    'ano': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).year),
                    'mes': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).month),
                    'dia': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).day)
                }
        except Exception as e:
            logger.error(f'Erro na operação: {e}')

        
    # Estimativa do total de páginas do tipo e local especificado, com base no total de imóveis retornado (base com 20 imóveis por página)
    async def total_pages(self):
        '''
            ### Objetivo 
            - Função para tratar e salvar dados das páginas com base nos parâmetros passados, obtendo estimativa do total de paginas de cada tipo
            - Serve como fonte para a geração das urls no método que obtém todos os dados dos imóveis
        '''

        try:
            logger.info('Obtendo lista de htmls para definir total de páginas por tipo de imóvel!')

            # HTML
            urls = [
                    f'https://www.argenprop.com/{tipo}/alquiler/{local}?pagina-1' 
                    for tipo in self._tipo 
                    for local in self._local
                ]

            logger.info('Iniciando execução assíncrona das tasks')

            async with aiohttp.ClientSession() as session:
                tasks = [self.get_page(session, url) for url in urls]
                html_pages = await asyncio.gather(*tasks)

                logger.info('Retornando resultado com todos os códigos html!')
            
            html_source = html_pages

            logger.info('Criando o Dataframe com dados de imóveis das páginas!')

            # Criando Dataframe
            page_df = (
                pd.DataFrame(data = html_source)
                .assign(paginas = lambda x: round(x['imoveis']/20 + 2.0))
            )

            # Salvando como parquet
            # pa_df = pa.Table.from_pandas(page_df)
            # file_path = os.path.join(os.getcwd(),'data','imoveis','paginas','argenprop','bronze') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','imoveis','paginas','argenprop','bronze')
            # info(f'Salvando o Dataframe como Parquet no caminho {file_path}')
            # pq.write_to_dataset(
            #     pa_df, 
            #     root_path = file_path,
            #     partition_cols = ['ano', 'mes', 'dia']
            # )

            return page_df
        
        except Exception as e:
            logger.error(f'Erro na operação: {e}')

    # Retorna todas as páginas de forma async
    async def get_all_pages(self):
        '''
            ### Objetivo 
            - Função para retornar o codigo html de todas as página com base na tipo de imóvel e local
            ### Parâmetros
            - _tipo: tipo de imóvel com base nos valores do constructor
            - _local: local do imóvel com base nos valores do constructor
        '''
        
        # Carregando dados da estimativa do total de páginas de cada tipo e local
        page_df = await self.total_pages()

        try:
            logger.info('Obtendo lista de urls!')

            # Retorna dados de todas as páginas e não apenas da primeira página
            urls = [
                f'https://www.argenprop.com/{tipo}/alquiler/{local}?pagina-{pagina}' 
                for tipo in self._tipo 
                for local in self._local 
                for max_pages in [int(page_df[(page_df['tipo'] == tipo) & (page_df['local'] == local)]['paginas'].max())]
                for pagina in range(1, max_pages + 1)
            ]

            logger.info('Iniciando execução assíncrona das tasks')

            async with aiohttp.ClientSession() as session:
                tasks = [self.get_page(session, url) for url in urls]
                html_pages = await asyncio.gather(*tasks)

                logger.info('Retornando resultado com todos os códigos html!')

                return html_pages
            
        except Exception as e:
            logger.error(f'Erro na operação: {e}')

    async def get_property_data(self):
        '''
            ### Objetivo
            - Tratamento dos dados de imóveis das códigos das páginas retornadas no método get_all_pages()
        '''

        logger.info('Obtendo lista de htmls para extrair dados de imóveis!')

        # Lista de htmls
        html = await self.get_all_pages()

        # Lista que guardará os dados dos imóveis
        dados = []

        logger.info('Iniciando iteração sobre lista de htmls!')

        # Iterando sobre os códigos das páginas e extraindo os dados dos imóveis
        for x in html:
            try:
                soup = BeautifulSoup(x['html'], 'html.parser')

                res = soup.find('div', {"class":"listing__items"})

                for i in res:
                    # CIDADE
                    try:
                        # cidade = re.search(r',\s*([^,]+)', i.find('p','card__title--primary show-mobile').text).group(1).replace('Departamento de','').strip()
                        cidade = x['local']
                    except:
                        cidade = 'Sem info'

                    # TIPO IMOVEL
                    try:
                        tipo_imo = x['tipo']
                    except:
                        tipo_imo = 'Sem info'

                    # BASE DOS DADOS
                    try:
                        base = x['base']
                    except:
                        base = 'Sem info'

                    # DATA
                    data = x['data']
                    ano = x['ano']
                    mes = x['mes']
                    dia = x['dia']

                    # ID DO IMOVEL - OK
                    try:
                        id = i.find('a').get('data-item-card')
                    except:
                        id = 'Sem info'

                    # URL - OK
                    try:
                        url = 'https://www.argenprop.com' + i.find('a').get('href')
                    except:
                        url = 'Sem info'

                    # NUMERO DE FOTOS - OK
                    try:
                        num_fotos = int(re.search(r'(?<=/)\d+', i.find('div','counter-box').text).group(0))
                    except:
                        num_fotos = 0.0

                    # ENDERECO - OK
                    try:
                        endereco = (i.find('p', 'card__address').text).strip().lower().replace(' al', '')
                    except:
                        endereco = 'Sem info'

                    # BAIRRO - OK
                    try:
                        bairro = re.match(r'^([^,]+)', (i.find('p','card__title--primary show-mobile').text).strip()).group(0)
                    except:
                        bairro = 'Sem info'

                    # ESTADO
                    try:
                        estado = get_state(cidade = cidade)
                    except:
                        estado = ''

                    # ALUGUEL - MOEDA
                    try:
                        aluguel_moeda = i.find('span','card__currency').text
                    except:
                        aluguel_moeda = 'Sem info'
                    
                    # ALUGUEL - VALOR
                    try:
                        aluguel = float(re.sub(r'[^0-9]', '', i.find('span','card__currency').next_sibling.strip()))
                    except:
                        aluguel = 0.0

                    # EXPENSAS
                    try:
                        expensas = float(re.sub(r'[^0-9]', '', i.find('span','card__expenses').text.strip()))
                    except:
                        expensas = 0.0

                    # IMOBILIARIA
                    try:
                        imobiliaria = i.find('div', 'card__agent').find('img').get('alt').strip().lower()
                    except:
                        imobiliaria = 'Sem info'

                    # FOTOS
                    try:
                        fotos = [p.find('img').get('data-src') for p in i.find('ul', 'card__photos').find_all('li')]
                    except:
                        fotos = 'Sem info'

                    # CARD POINTS - DEFINE A ORDEM EM QUE OS IMOVEIS APARECEM NO SITE (APARENTEMENTE)
                    try:
                        card_points = float(re.sub('[^0-9]','',i.find('p','card__points').text.strip()))
                    except:
                        card_points = 0.0

                    # DESCRICAO
                    try:
                        descricao = i.find('p', 'card__info').text.strip()
                    except:
                        descricao = 'Sem info'
                    
                    # TITULO
                    try:
                        titulo = i.find('h2', 'card__title').text.strip()
                    except:
                        titulo = 'Sem info'

                    # AMENIDADES
                    try:
                        # Lista com todas as amenidades para facilitar extração das existentes
                        amenidades = i.find('ul', 'card__main-features').find_all('li')
                    except:
                        amenidades = None

                    # AREA
                    try:
                        area_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-superficie_cubierta') != None]
                        area = float(re.match('[0-9]{1,}', amenidades[area_indice[0]].find('span').text.strip()).group(0))
                    except:
                        area = 0.0

                    # ANTIGUIDADE
                    try:
                        antiguidade_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-antiguedad') != None]
                        antiguidade = float(re.match('[0-9]{1,}', amenidades[antiguidade_indice[0]].find('span').text.strip()).group(0)) if 'estrenar' not in amenidades[antiguidade_indice[0]].find('span').text.strip().lower() else 0.0
                    except:
                        antiguidade = 0.0

                    # BANHEIROS
                    try:
                        banheiros_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-cantidad_banos') != None]
                        banheiros = float(re.match('[0-9]{1,}', amenidades[banheiros_indice[0]].find('span').text.strip()).group(0))
                    except:
                        banheiros = 0.0

                    # TIPO BANHEIROS
                    try:
                        tipo_banheiro_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-tipo_baño') != None]
                        tipo_banheiro = amenidades[tipo_banheiro_indice[0]].find('span').text.strip()
                    except:
                        tipo_banheiro = 'Sem info'

                    # AMBIENTES
                    try:
                        ambientes_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-cantidad_ambientes') != None]
                        ambientes = amenidades[ambientes_indice[0]].find('span').text.strip()
                    except:
                        ambientes = 'Sem info'
                    
                    # DORMITORIOS
                    try:
                        dormitorios_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-cantidad_dormitorios') != None]
                        dormitorios = float(re.match('[0-9]{1,}', amenidades[dormitorios_indice[0]].find('span').text.strip()).group(0))
                    except:
                        dormitorios = 0.0

                    # ORIENTACAO
                    try:
                        orientacao_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-orientacion') != None]
                        orientacao = amenidades[orientacao_indice[0]].find('span').text.strip()
                    except:
                        orientacao = 'Sem info'

                    # GARAGENS
                    try:
                        garagens_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-ambiente_cochera') != None]
                        garagens = float(re.match('[0-9]{1,}', amenidades[garagens_indice[0]].find('span').text.strip()).group(0))
                    except:
                        garagens = 0.0
                    
                    # ESTADO PROPRIEDADE
                    try:
                        estado_prop_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-estado_propiedad') != None]
                        estado_prop = amenidades[estado_prop_indice[0]].find('span').text.strip()
                    except:
                        estado_prop = 'Sem info'
                    
                    # TIPO DE LOCAL
                    try:
                        tipo_local_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-tipo_local') != None]
                        tipo_local = amenidades[tipo_local_indice[0]].find('span').text.strip()
                        # print(tipo_local)
                    except:
                        tipo_local = 'Sem info'

                    # WHATSAPP
                    try:
                        wsp = re.findall(r'https://wa\.me/\d+', i.find('div', 'card-contact-group').find('span')['data-href'])[0]
                    except:
                        wsp = 'Sem info'

                    # # Latitude
                    # try:
                    #     latitude = get_geocoding(endereco = endereco, bairro = bairro, cidade = cidade)['lat']
                    # except:
                    #     latitude = 0.0

                    # # Latitude
                    # try:
                    #     longitude = get_geocoding(endereco = endereco, bairro = bairro, cidade = cidade)['lon']
                    # except:
                    #     longitude = 0.0

                    # logger.info(f'Guardando os dados da url {x["url"]}')

                    # Adicionando à lista
                    dados.append(
                        [
                            id,
                            base,
                            tipo_imo,
                            estado,
                            cidade, 
                            bairro,
                            endereco,
                            # latitude,
                            # longitude,
                            url, 
                            descricao,
                            titulo,
                            aluguel_moeda,
                            aluguel,
                            expensas,
                            area,
                            antiguidade, 
                            banheiros,
                            tipo_banheiro,
                            ambientes,
                            dormitorios,
                            orientacao,
                            garagens,
                            estado_prop,
                            tipo_local, 
                            imobiliaria, 
                            num_fotos,
                            fotos,
                            card_points,
                            wsp,
                            data,
                            ano,
                            mes,
                            dia
                        ]
                    )
            except Exception as e:
                logger.error(f'Erro na extração dos dados da url {x["url"]}: {e}')
                continue
        
        logger.info('Iteração finalizada. Guardando os dados em um Dataframe!')

        # Criando Dataframe
        df = pd.DataFrame(
            dados, 
            columns = [
                'id',
                'base',
                'tipo_imovel',
                'estado',
                'cidade', 
                'bairro',
                'endereco',
                # 'latitude',
                # 'longitude',
                'url', 
                'descricao',
                'titulo',
                'aluguel_moeda',
                'aluguel_valor',
                'expensas',
                'area',
                'antiguidade', 
                'banheiros',
                'tipo_banheiro',
                'ambientes',
                'dormitorios',
                'orientacao',
                'garagens',
                'estado_propriedade',
                'tipo_local', 
                'imobiliaria', 
                'num_fotos',
                'fotos',
                'card_points',
                'wsp',
                'data',
                'ano',
                'mes',
                'dia'
            ]
        )
        df_final = (
            df
            .drop(index = df[(df['id'].isnull()) | (df['id'] == 'Sem info')].index)
            .drop_duplicates(subset = ['id','cidade','tipo_imovel'])
            .sort_values(by = ['cidade','tipo_imovel','bairro'])
            .reset_index(drop = True)
        )

        return df_final





