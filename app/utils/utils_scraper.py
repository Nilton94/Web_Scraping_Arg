import pandas as pd
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq
import re
import os
import asyncio
import aiohttp
from dataclasses import dataclass
import datetime
import pytz
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from utils.log_config import get_logger
from utils.lat_long import apply_geocoding, get_state, get_distance_unr, get_distance_provincial, get_distance_baigorria, get_distance_ninos, get_distance_carrasco
from geopy.distance import distance
import streamlit as st
from aiocache import cached
from aiocache.serializers import PickleSerializer
import unidecode

# Criando logger
logger = get_logger()

# CLASSE DO ARGENPROP
@dataclass
class ScraperArgenProp:
    
    # Parametros da classe
    _tipo: list = None
    _local: list = None


    # Retorna a página async
    # @st.cache_data(ttl = 86400, max_entries = 100)
    @cached(ttl = 86400, serializer=PickleSerializer())
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
    # @st.cache_data(ttl = 86400, max_entries = 100)
    @cached(ttl = 86400, serializer=PickleSerializer())
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

            return page_df
        
        except Exception as e:
            logger.error(f'Erro na operação: {e}')

    # Retorna todas as páginas de forma async
    # @st.cache_data(ttl = 86400, max_entries = 100)
    @cached(ttl = 86400, serializer=PickleSerializer())
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

    # @st.cache_data(ttl = 86400, max_entries = 100)
    @cached(ttl = 86400, serializer=PickleSerializer())
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

                # ESTADO
                try:
                    estado = get_state(cidade = x['local'])
                except:
                    estado = 'Sem info'
                
                # Dados do dicionário
                cidade, tipo_imo, base = x['local'], x['tipo'], x['base']

                # Datas
                data, ano, mes, dia = x['data'], x['ano'], x['mes'], x['dia']

                for i in res:
                    # # CIDADE
                    # try:
                    #     # cidade = re.search(r',\s*([^,]+)', i.find('p','card__title--primary show-mobile').text).group(1).replace('Departamento de','').strip()
                    #     cidade = x['local']
                    # except:
                    #     cidade = 'Sem info'

                    # # TIPO IMOVEL
                    # try:
                    #     tipo_imo = x['tipo']
                    # except:
                    #     tipo_imo = 'Sem info'

                    # # BASE DOS DADOS
                    # try:
                    #     base = x['base']
                    # except:
                    #     base = 'Sem info'

                    # # DATA
                    # data = x['data']
                    # ano = x['ano']
                    # mes = x['mes']
                    # dia = x['dia']

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
                        # bairro = unidecode.unidecode(str(re.sub(' +', ' ', bairro.strip())).lower())
                    except:
                        bairro = 'Sem info'

                    # ALUGUEL - MOEDA
                    try:
                        aluguel_moeda = i.find('span','card__currency').text
                        aluguel_moeda = 'Sem info' if 'consultar' in aluguel_moeda.lower() else aluguel_moeda
                    except:
                        aluguel_moeda = 'Sem info'
                    
                    # ALUGUEL - VALOR
                    try:
                        aluguel = float(re.sub(r'[^0-9]', '', i.find('span','card__currency').next_sibling.strip()))
                    except:
                        aluguel = 0.0

                    # EXPENSAS MOEDAS
                    try:
                        expensas_moeda = re.findall(r'\$', i.find('span','card__expenses').text.strip())[0]
                    except:
                        expensas_moeda = 'Sem info'

                    # EXPENSAS
                    try:
                        expensas = float(re.sub(r'[^0-9]', '', i.find('span','card__expenses').text.strip()))
                    except:
                        expensas = 0.0

                    # VALOR TOTAL (ALUGUEL + EXPENSAS)
                    if aluguel_moeda == expensas_moeda:
                        valor_total_aluguel = (aluguel + expensas)
                    elif aluguel_moeda != expensas_moeda and aluguel_moeda == 'USD':
                        valor_total_aluguel = (aluguel + expensas/1000)
                    elif aluguel_moeda != expensas_moeda and aluguel_moeda == '$':
                        valor_total_aluguel = (aluguel + expensas*1000)
                    else:
                        valor_total_aluguel = 0.0

                    # IMOBILIARIA
                    try:
                        imobiliaria = i.find('div', 'card__agent').find('img').get('alt').strip().lower()
                    except:
                        imobiliaria = 'Sem info'

                    # FOTOS
                    try:
                        fotos = [p.find('img').get('data-src') for p in i.find('ul', 'card__photos').find_all('li')]
                    except:
                        fotos = ['Sem info']

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
                        ambientes = 1.0 if ambientes.lower().__contains__('mono') else float(re.sub('[^0-9]', '', ambientes))
                    except:
                        ambientes = 0.0
                    
                    # DORMITORIOS
                    try:
                        dormitorios_indice = [i for i, s in enumerate(amenidades) if s.find('i','icono-cantidad_dormitorios') != None]
                        dormitorios = 1.0 if 'mono' in amenidades[dormitorios_indice[0]].find('span').text.lower().strip() else float(re.match('[0-9]{1,}', amenidades[dormitorios_indice[0]].find('span').text.strip()).group(0))
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
                    except:
                        tipo_local = 'Sem info'

                    # WHATSAPP
                    try:
                        wsp = re.findall(r'https://wa\.me/\d+', i.find('div', 'card-contact-group').find('span')['data-href'])[0]
                    except:
                        wsp = 'Sem info'

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
                            url, 
                            descricao,
                            titulo,
                            aluguel_moeda,
                            aluguel,
                            expensas_moeda,
                            expensas,
                            valor_total_aluguel,
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
        df = (
            pd.DataFrame(
                dados, 
                columns = [
                    'id',
                    'base',
                    'tipo_imovel',
                    'estado',
                    'cidade', 
                    'bairro',
                    'endereco',
                    'url', 
                    'descricao',
                    'titulo',
                    'aluguel_moeda',
                    'aluguel_valor',
                    'expensas_moeda',
                    'expensas_valor',
                    'valor_total_aluguel',
                    'area_util',
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
            .pipe(
                lambda df: df.loc[df.id != 'Sem info'].reset_index(drop = True)
            )
            .drop_duplicates(subset = ['id', 'tipo_imovel', 'endereco'])
        )
        
        return df
    
    # Função para obter dados geográficos (40 min -> 6 min)
    # @st.cache_data(ttl = 86400, max_entries = 100)
    @cached(ttl = 86400, serializer=PickleSerializer())
    async def get_final_dataframe(self):
        '''
            ### Objetivo
            - Inclusão de dados geográficos aos dados dos imóveis
        '''
        
        # Dataframe com dados dos imóveis de todas as páginas
        df = await self.get_property_data()

        # Obtendo dados de latitude e longitude
        logger.info('Obtendo dados de latitude e longitude!')

        res = []

        with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
            # Criando a sequência de tasks que serão submetidas para a thread pool
            rows = {executor.submit(apply_geocoding, row): row for index, row in df.iterrows()}
            
            # Loop para executar as tasks de forma concorrente. Também seria possível criar uma list comprehension que esperaria todos os resultados para retornar os valores.
            for future in concurrent.futures.as_completed(rows):
                try:
                    resultado = future.result()
                    res.append(resultado)
                except Exception as exc:
                    continue

        # Juntando dados de latitude e longitude
        df_lat_long = (
            pd.merge(
                left = df,
                right = pd.DataFrame(res),
                how = 'left',
                on = 'id'
            )
            .assign(coordenadas = lambda df: df[['latitude','longitude']].values.tolist())
            .assign(
                distancia_unr = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_unr(x)
                )
            )
            .assign(
                distancia_hospital_provincial = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_provincial(x)
                )
            )
            .assign(
                distancia_hospital_baigorria = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_baigorria(x)
                )
            )
            .assign(
                distancia_hospital_ninos = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_ninos(x)
                )
            )
            .assign(
                distancia_hospital_carrasco = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_carrasco(x)
                )
            )
            .assign(bairro = lambda df: df.bairro.apply(lambda x: unidecode.unidecode(str(re.sub(' +', ' ', x.strip())).lower())))
        )

        # Dataframe final
        logger.info('Criando dataframe final!')

        df_final = (
            df_lat_long
            .drop(
                index = df_lat_long[(df_lat_long['id'].isnull()) | (df_lat_long['id'] == 'Sem info')].index
            )
            .drop_duplicates(subset = ['id','cidade', 'tipo_imovel', 'endereco'])
            .sort_values(by = ['cidade','tipo_imovel','bairro'])
            .reset_index(drop = True)
        )

        # Salvando como parquet
        logger.info('Salvando dataframe final como parquet!')

        pq.write_to_dataset(
            table = pa.Table.from_pandas(df_final),
            root_path = os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'bronze') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'bronze'),
            partition_cols = ['ano','mes','dia'],
            existing_data_behavior = 'delete_matching',
            use_legacy_dataset = False
        )

        return df_final


# CLASSE DO ZONAPROP
@dataclass
class ScraperZonaProp:
    
    # Parametros da classe
    _tipo: list = None
    _local: list = None


    # @st.cache_data(ttl = 86400, max_entries = 100)
    def extract_pages(self, url):
        '''
            * Retorna o total de imóveis do tipo passado
        '''

        # Browser
        logger.info('Criando o browser e coletando o HTML')

        options = Options()
        options.add_argument("--headless")
        browser = webdriver.Firefox(options = options)
        browser.get(url)
        source_code = browser.find_element(By.XPATH, '//*').get_attribute("innerHTML")
        browser.quit()

        logger.info('Tratando os dados da HTML')

        # Dados
        try:
            local = re.search(r'www\.(.*)\.com\.ar/(.*?)-alquiler-(.*?)(-pagina.*|).html', url).group(3)
        except:
            local = 'Sem info'
        
        try:
            tipo_imovel = re.search(r'www\.(.*)\.com\.ar/(.*?)-alquiler-(.*?)(-pagina.*|).html', url).group(2)
        except:
            tipo_imovel = 'Sem info'

        try:
            imoveis = float(re.sub(r'[^0-9]', '', BeautifulSoup(source_code, 'html.parser').find('h1','sc-1oqs0ed-0 idmkkS').text))
        except:
            imoveis = 0.0

        
        return {
            'local':local,
            'base': re.search(r'www\.(.*)\.com\.ar/(.*?)-alquiler-(.*?)(-pagina.*|).html', url).group(1),
            'tipo_imovel':tipo_imovel,
            'imoveis':imoveis,
            'paginas': (imoveis//20 + 1) if imoveis > 0 else 0.0,
            'url':url,
            'html': source_code,
            'data': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')),
            'ano': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).year),
            'mes': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).month),
            'dia': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).day)
        }

    # @st.cache_data(ttl = 86400, max_entries = 100)
    def get_pages(self):
        '''
            * Recebe os tipos de imóveis e os locais e obtém o total de páginas puxando a função extract_pages usando threads
        '''

        logger.info('Criando lista de urls')

        # Lista de urls com base nos dados de tipo e local
        urls = [f'https://www.zonaprop.com.ar/{tipo}-alquiler-{local}-pagina-1.html' for tipo in self._tipo for local in self._local]

        dados = []
        
        logger.info('Obtendo dados com ThreadPool')

        with concurrent.futures.ThreadPoolExecutor(max_workers = 5) as executor:
            rows = {executor.submit(self.extract_pages, url): url for url in urls}

            for future in concurrent.futures.as_completed(rows):
                try:
                    resultado = future.result()
                    dados.append(resultado)
                except Exception as exc:
                    continue
        
        return pd.DataFrame(dados).sort_values('imoveis', ascending = False).reset_index(drop=True)

    # @st.cache_data(ttl = 86400, max_entries = 100)
    def get_all_pages(self):
        '''
            * Retorna o código fonte de todas as páginas com base nos critérios selecionados
        '''

        # DF com o total de páginas por tipo e local do imóvel
        page_df = self.get_pages()

        # URLS
        urls = [
                f'https://www.zonaprop.com.ar/{tipo}-alquiler-{local}-pagina-{pagina}.html' 
                for tipo in self._tipo 
                for local in self._local 
                for max_pages in [int(page_df[(page_df['tipo_imovel'] == tipo) & (page_df['local'] == local)]['paginas'].max())]
                for pagina in range(1, max_pages + 1)
        ]

        # Thread
        dados = []
        with concurrent.futures.ThreadPoolExecutor(max_workers = 5) as executor:
            rows = {executor.submit(self.extract_pages, url): url for url in urls}

            for future in concurrent.futures.as_completed(rows):
                try:
                    resultado = future.result()
                    dados.append(resultado)
                except Exception as exc:
                    continue
        
        return dados

    # @st.cache_data(ttl = 86400, max_entries = 100)
    def get_property_data(self):
        '''
            Com base nos códigos html de cada página, extrai os dados dos imóveis
        '''

        # dados das páginas
        x = self.get_all_pages()

        # Dados dos imóveis
        dados = []

        for i in x:

            try:
                # PARSE DO HTML
                soup = BeautifulSoup(i['html'], 'html.parser')
                divs = soup.find('div','postings-container').find_all('div')

                # Dados gerais do link
                cidade, tipo_imovel, base = i['local'], i['tipo_imovel'], i['base']

                # ESTADO
                try:
                    estado = get_state(cidade = cidade)
                except:
                    estado = 'Sem info'

                # DATA
                data = i['data']
                ano = i['ano']
                mes = i['mes']
                dia = i['dia']

                for i in divs:
                    # ID DO IMÓVEL
                    try:
                        id_imovel = i.find('div', {'data-qa': "posting PROPERTY"}).get('data-id').strip()
                    except:
                        id_imovel = 'Sem info'

                    # URL DO IMOVEL
                    try:
                        url_imovel = 'https://www.zonaprop.com.ar' + i.find('div', {'data-qa': "posting PROPERTY"}).get('data-to-posting').strip()
                    except:
                        url_imovel = 'Sem info'

                    # FOTOS
                    try:
                        fotos = [x.get('src') or x.get('data-flickity-lazyload') for x in i.find('div', 'flickity-slider').find_all('img')]
                    except:
                        fotos = []

                    # MOEDA DO ALUGUEL
                    try:
                        aluguel_moeda = re.sub(r'[0-9.]', '', i.find('div', {'data-qa': "POSTING_CARD_PRICE"}).text).strip()
                        aluguel_moeda = '$' if aluguel_moeda == 'Pesos' else aluguel_moeda
                        aluguel_moeda = 'Sem info' if 'consultar' in aluguel_moeda.lower() else aluguel_moeda
                    except:
                        aluguel_moeda = 'Sem info'

                    # VALOR DO ALUGUEL
                    try:
                        aluguel_valor = float(re.sub(r'[^0-9]', '', i.find('div', {'data-qa': "POSTING_CARD_PRICE"}).text))
                    except:
                        aluguel_valor = 0.0
                        
                    # DESCONTO NO ALUGUEL
                    try:
                        desconto_aluguel = i.find('div', 'sc-12dh9kl-5 foYfuB').text.strip()
                    except:
                        desconto_aluguel = 'Sem info'
                        
                    # MOEDA DAS EXPENSAS
                    try:
                        expensas_moeda = re.sub(r'[0-9.Expensas]', '', i.find('div', {'data-qa': "expensas"}).text).strip()
                        expensas_moeda = '$' if expensas_moeda == 'Pesos' else expensas_moeda
                    except:
                        expensas_moeda = 'Sem info'
                        
                    # VALOR DAS EXPENSAS
                    try:
                        expensas_valor = float(re.sub(r'[^0-9]', '', i.find('div', {'data-qa': "expensas"}).text))
                    except:
                        expensas_valor = 0.0

                    # VALOR TOTAL (ALUGUEL + EXPENSAS)
                    if aluguel_moeda == expensas_moeda:
                        valor_total_aluguel = (aluguel_valor + expensas_valor)
                    elif aluguel_moeda != expensas_moeda and aluguel_moeda == 'USD':
                        valor_total_aluguel = (aluguel_valor + expensas_valor/1000)
                    elif aluguel_moeda != expensas_moeda and aluguel_moeda == '$':
                        valor_total_aluguel = (aluguel_valor + expensas_valor*1000)
                    else:
                        valor_total_aluguel = 0.0
                    
                    # ENDERECO
                    try:
                        endereco = i.find('div', 'sc-ge2uzh-0 eXwAuU').text.strip()
                    except:
                        endereco = 'Sem info'

                    # BAIRRO
                    try:
                        bairro = re.match(r'^([^,]+)', (i.find('div', {'data-qa': "POSTING_CARD_LOCATION"}).text.strip()).strip()).group(0).strip()
                        # bairro = unidecode.unidecode(str(re.sub(' +', ' ', bairro.strip())).lower())
                    except:
                        bairro = 'Sem info'
                    
                    # DESTAQUE
                    try:
                        destaque = i.find('span', 'sc-ryls1p-0 bzIPYI').text.strip()
                    except:
                        destaque = 'Sem info'
                    
                    # LISTA AMENIDADES
                    try:
                        amenidades = i.find('div', {'data-qa': "POSTING_CARD_FEATURES"}).find_all('span')
                    except:
                        amenidades = []

                    # AREA TOTAL
                    try:
                        area_total = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 eLhfrW') != None][0]
                    except:
                        area_total = 0.0

                    # AREA UTIL (cubierta)
                    try:
                        area_util = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 dRoEma') != None][0]
                    except:
                        area_util = 0.0

                    # AMBIENTES
                    try:
                        # ambientes = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 jkEBRn') != None][0]
                        ambientes = [
                            1.0 if 'mono' in amenidades[i].find('span').text.lower().strip()
                            else float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) 
                            for i, s in enumerate(amenidades) 
                            if s.find('img','sc-1uhtbxc-1 jkEBRn') != None
                        ][0]
                    except:
                        ambientes = 0.0

                    # DORMITORIOS
                    try:
                        dormitorios = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 ljuqxM') != None][0]
                    except:
                        dormitorios = 0.0

                    # BANHEIROS
                    try:
                        banheiros = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 foetjI') != None][0]
                    except:
                        banheiros = 0.0
                    
                    # GARAGENS
                    try:
                        garagens = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 eykaou') != None][0]
                    except:
                        garagens = 0.0

                    # TITULO
                    try:
                        titulo = i.find('h2', 'sc-i1odl-11 kvKUxE').text.strip()
                    except:
                        titulo = 'Sem info'

                    # DESCRICAO
                    try:
                        descricao = i.find('div', {'data-qa': "POSTING_CARD_DESCRIPTION"}).text.strip()
                    except:
                        descricao = 'Sem info'

                    # IMOBILIARIA
                    try:
                        dono_direto = i.find('span', 'sc-hlm4rl-4 ihiYoF').text.strip() 
                    except:
                        dono_direto = 'Sem info'
                    try:
                        imobiliaria = re.search('logo_(\w.*)_', i.find('img', {'data-qa': "POSTING_CARD_PUBLISHER"}).get('src')).group(1).replace('-',' ')
                        imobiliaria = dono_direto if dono_direto != 'Sem info' else imobiliaria
                    except:
                        imobiliaria = dono_direto if dono_direto != 'Sem info' else 'Sem info'

                    # LISTA FINAL
                    dados.append(
                        [
                            id_imovel, 
                            base,
                            tipo_imovel,
                            estado,
                            cidade,
                            bairro, 
                            endereco, 
                            url_imovel,
                            descricao, 
                            titulo, 
                            aluguel_moeda, 
                            aluguel_valor, 
                            desconto_aluguel,
                            expensas_moeda, 
                            expensas_valor, 
                            valor_total_aluguel,
                            area_total, 
                            area_util, 
                            ambientes, 
                            dormitorios, 
                            banheiros, 
                            garagens, 
                            destaque,
                            imobiliaria,
                            data,
                            ano,
                            mes,
                            dia
                        ]
                    )
            except:
                continue

        df = (
            pd.DataFrame(
                dados,
                columns = [
                    'id', 
                    'base',
                    'tipo_imovel',
                    'estado',
                    'cidade',
                    'bairro', 
                    'endereco', 
                    'url',
                    'descricao', 
                    'titulo', 
                    'aluguel_moeda', 
                    'aluguel_valor', 
                    'desconto_aluguel',
                    'expensas_moeda', 
                    'expensas_valor', 
                    'valor_total_aluguel',
                    'area_total', 
                    'area_util', # No argenprop, a área disponível já é a útil
                    'ambientes', 
                    'dormitorios', 
                    'banheiros', 
                    'garagens', 
                    'destaque',
                    'imobiliaria',
                    'data',
                    'ano',
                    'mes',
                    'dia'
                ]
            )
            .pipe(
                lambda df: df.loc[df.id != 'Sem info'].reset_index(drop = True)
            )
            .drop_duplicates(subset = ['id', 'tipo_imovel', 'endereco'])
        )

        return df

    # @st.cache_data(ttl = 86400, max_entries = 100)
    def get_final_dataframe(self):

        # Dataframe com dados dos imóveis
        df = self.get_property_data()
        
        # Obtendo dados de longitude e latitude
        res = []

        with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
            # Criando a sequência de tasks que serão submetidas para a thread pool
            rows = {executor.submit(apply_geocoding, row): row for index, row in df.iterrows()}
            
            # Loop para executar as tasks de forma concorrente. Também seria possível criar uma list comprehension que esperaria todos os resultados para retornar os valores.
            for future in concurrent.futures.as_completed(rows):
                try:
                    resultado = future.result()
                    res.append(resultado)
                except Exception as exc:
                    continue

        # Juntando dados de latitude e longitude
        df_lat_long = (
            pd.merge(
                left = df,
                right = pd.DataFrame(res),
                how = 'left',
                on = 'id'
            )
            .assign(coordenadas = lambda df: df[['latitude','longitude']].values.tolist())
            .assign(
                distancia_unr = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_unr(x)
                )
            )
            .assign(
                distancia_hospital_provincial = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_provincial(x)
                )
            )
            .assign(
                distancia_hospital_baigorria = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_baigorria(x)
                )
            )
            .assign(
                distancia_hospital_ninos = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_ninos(x)
                )
            )
            .assign(
                distancia_hospital_carrasco = lambda df: df['coordenadas'].apply(
                    lambda x: get_distance_carrasco(x)
                )
            )
            .assign(bairro = lambda df: df.bairro.apply(lambda x: unidecode.unidecode(str(re.sub(' +', ' ', x.strip())).lower())))
        )

        # Dataframe final
        df_final = (
            df_lat_long
            .drop(
                index = df_lat_long[(df_lat_long['id'].isnull()) | (df_lat_long['id'] == 'Sem info')].index
            )
            .drop_duplicates(subset = ['id','cidade','tipo_imovel', 'endereco'])
            .sort_values(by = ['cidade','tipo_imovel','bairro'])
            .reset_index(drop = True)
        )

        # Salvando como parquet
        pq.write_to_dataset(
            table = pa.Table.from_pandas(df_final),
            root_path = os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'bronze') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'bronze'),
            partition_cols = ['ano','mes','dia'],
            existing_data_behavior = 'delete_matching',
            use_legacy_dataset = False
        )
        return df_final