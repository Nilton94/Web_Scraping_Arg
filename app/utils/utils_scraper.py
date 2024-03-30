import pandas as pd
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
from utils.utils_storage import get_paths, DuckDBtStorage
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
    # @cached(ttl = 86400, serializer = PickleSerializer(), cache=Cache.MEMORY)
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
                    'cidade': re.compile(r'\.com/(.*?)/alquiler/(.*?)\?').search(url).group(2),
                    'tipo_imovel': re.compile(r'\.com/(.*?)/alquiler/(.*?)\?').search(url).group(1),
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
    # @cached(ttl = 86400, serializer=PickleSerializer(), cache=Cache.MEMORY)
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
                    # for tipo in self._tipo 
                    for tipo in ['departamentos','casas','campos','cocheras','fondos-de-comercio','galpones','hoteles','locales','negocios-especiales','oficinas','ph','quintas','terrenos']
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

            # Salvando os dados em um db
            DuckDBtStorage(
                _path = os.path.join(get_paths()['argenprop']['imoveis'], 'argenprop.db'), 
                _tabela = 'paginas_argenprop',
                _df = page_df
            ).create_table()

            DuckDBtStorage(
                _path = os.path.join(get_paths()['argenprop']['imoveis'], 'argenprop.db'), 
                _tabela = 'paginas_argenprop',
                _df = page_df
            ).insert_data()

            # Salvando os dados de página - Parquet
            # path = os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'paginas')
            
            # pq.write_to_dataset(
            #     table = pa.Table.from_pandas(page_df),
            #     root_path = path,
            #     existing_data_behavior = 'delete_matching',
            #     basename_template = f"{datetime.datetime.now(tz=pytz.timezone('America/Sao_Paulo')).date()}_paginas_argenprop_"+"{i}.parquet",
            #     use_legacy_dataset = False
            # )

            # Retornando df 
            return page_df
        
        except Exception as e:
            logger.error(f'Erro na operação: {e}')

    # Retorna todas as páginas de forma async
    # @cached(ttl = 86400, serializer=PickleSerializer(), cache=Cache.MEMORY)
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
                for max_pages in [int(page_df[(page_df['tipo_imovel'] == tipo) & (page_df['cidade'] == local)]['paginas'].max())]
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

    # @cached(ttl = 86400, serializer=PickleSerializer(), cache=Cache.MEMORY)
    async def get_property_data(self):
        '''
            ### Objetivo
            - Tratamento dos dados de imóveis das códigos das páginas retornadas no método get_all_pages()
        '''

        # Lista de htmls
        html = await self.get_all_pages()

        logger.info('Obtendo lista de htmls para extrair dados de imóveis!')

        # Lista que guardará os dados dos imóveis
        dados = []

        logger.info('Iniciando iteração sobre lista de htmls!')

        # Iterando sobre os códigos das páginas e extraindo os dados dos imóveis
        for x in html:
            if x['imoveis'] > 0.0:
                try:
                    soup = BeautifulSoup(x['html'], 'html.parser')

                    res = soup.find('div', {"class":"listing__items"})

                    # ESTADO
                    try:
                        estado = get_state(cidade = x['cidade'])
                    except:
                        estado = 'Sem info'
                    
                    # Dados do dicionário
                    cidade, tipo_imo, base = x['cidade'], x['tipo_imovel'], x['base']

                    # Datas
                    data, ano, mes, dia = x['data'], x['ano'], x['mes'], x['dia']

                    for i in res:
                        
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
            else:
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
        
        logger.info(f'Saldos dados Argenprop na pasta bronze')

        # Salvando dados iniciais - db
        DuckDBtStorage(
            _path = os.path.join(get_paths()['argenprop']['imoveis'], 'argenprop.db'), 
            _tabela = 'bronze_imoveis_argenprop',
            _df = df
        ).create_table()

        DuckDBtStorage(
            _path = os.path.join(get_paths()['argenprop']['imoveis'], 'argenprop.db'), 
            _tabela = 'bronze_imoveis_argenprop',
            _df = df
        ).insert_data()

        # Salvando dados iniciais - Parquet
        # path = os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'imoveis', 'bronze') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'imoveis', 'bronze')
            
        # pq.write_to_dataset(
        #     table = pa.Table.from_pandas(df),
        #     root_path = path,
        #     existing_data_behavior = 'delete_matching',
        #     basename_template = f"{datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).date()}_imoveis_bronze_argenprop_"+"{i}.parquet",
        #     use_legacy_dataset = False
        # )

        # Retornando df
        return df
    
    # Função para obter dados geográficos (40 min -> 6 min)
    # @cached(ttl = 86400, serializer=PickleSerializer(), cache=Cache.MEMORY)
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

        while True:
            
            # Ids sem latitude e longitude
            if len(res) == 0:
                df_i = df
            else:
                df_i = (
                    pd.merge(
                        left = df,
                        right = pd.DataFrame(res),
                        how = 'left',
                        on = 'id'
                    )
                    .pipe(
                        lambda df: df.loc[df.latitude.isna()]
                    )
                )

                if df_i.empty or (df_i.id.nunique() / df.id.nunique() < 0.10):
                    break
                else:
                    continue

            with concurrent.futures.ThreadPoolExecutor(max_workers = 6) as executor:
                # Criando a sequência de tasks que serão submetidas para a thread pool
                rows = {executor.submit(apply_geocoding, row, max_tentativas = 1): row for index, row in df_i.iterrows()}
                
                # Loop para executar as tasks de forma concorrente. Também seria possível criar uma list comprehension que esperaria todos os resultados para retornar os valores.
                for future in concurrent.futures.as_completed(rows):
                    try:
                        resultado = future.result()
                        res.append(resultado)
                    except Exception as exc:
                        continue

        logger.info(f'Obtenção de dados geográficos finalizados!')

        # Juntando dados de latitude e longitude
        df_lat_long = (
            pd.merge(
                left = df,
                right = pd.DataFrame(res),
                how = 'inner',
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

        # # Salvando como parquet
        # logger.info('Salvando dataframe final Argenprop Silver como parquet!')

        # Salvando dados iniciais - db
        DuckDBtStorage(
            _path = os.path.join(get_paths()['argenprop']['imoveis'], 'argenprop.db'), 
            _tabela = 'silver_imoveis_argenprop',
            _df = df_final
        ).create_table()

        DuckDBtStorage(
            _path = os.path.join(get_paths()['argenprop']['imoveis'], 'argenprop.db'), 
            _tabela = 'silver_imoveis_argenprop',
            _df = df_final
        ).insert_data()

        # # Salvando dados brutos
        # path = os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'imoveis', 'silver') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'imoveis', 'silver')
            
        # pq.write_to_dataset(
        #     table = pa.Table.from_pandas(df_final),
        #     root_path = path,
        #     existing_data_behavior = 'delete_matching',
        #     basename_template = f"{datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).date()}_imoveis_silver_argenprop_" + "{i}.parquet",
        #     use_legacy_dataset = False
        # )

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
            imoveis = float(re.sub(r'[^0-9]', '', BeautifulSoup(source_code, 'html.parser').find('h1','sc-1oqs0ed-0 cvTPma').text))
            # Tag h1 mudada em 20/02/24 de sc-1oqs0ed-0 idmkkS para sc-1oqs0ed-0 cvTPma
        except:
            imoveis = 0.0

        
        return {
            'cidade': local,
            'base': re.search(r'www\.(.*)\.com\.ar/(.*?)-alquiler-(.*?)(-pagina.*|).html', url).group(1),
            'tipo_imovel':tipo_imovel,
            'imoveis': imoveis,
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
        
        df = pd.DataFrame(dados).sort_values('imoveis', ascending = False).reset_index(drop=True)

        # Salvando os dados em um db
        DuckDBtStorage(
            _path = os.path.join(get_paths()['zonaprop']['imoveis'], 'zonaprop.db'), 
            _tabela = 'paginas_zonaprop',
            _df = df
        ).create_table()

        DuckDBtStorage(
            _path = os.path.join(get_paths()['zonaprop']['imoveis'], 'zonaprop.db'), 
            _tabela = 'paginas_zonaprop',
            _df = df
        ).insert_data()

        # # Salvando os dados de página
        # path = os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'paginas')
        
        # pq.write_to_dataset(
        #     table = pa.Table.from_pandas(df),
        #     root_path = path,
        #     existing_data_behavior = 'delete_matching',
        #     basename_template = f"{datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).date()}_paginas_zonaprop_" + "{i}.parquet",
        #     use_legacy_dataset = False
        # )

        # Retornando df 
        return df

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
                for max_pages in [int(page_df[(page_df['tipo_imovel'] == tipo) & (page_df['cidade'] == local)]['paginas'].max())]
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

        logger.info('Iniciando iteração sobre o hmtl das páginas')

        for j in x:

            try:
                # PARSE DO HTML
                soup = BeautifulSoup(j['html'], 'html.parser')
                divs = soup.find('div','postings-container').find_all('div')

                # Dados gerais do link
                cidade, tipo_imovel, base = j['cidade'], j['tipo_imovel'], j['base']

                # ESTADO
                try:
                    estado = get_state(cidade = cidade)
                except:
                    estado = 'Sem info'

                # DATA
                data = j['data']
                ano = j['ano']
                mes = j['mes']
                dia = j['dia']

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
                        # endereco = i.find('div', 'sc-ge2uzh-0 eXwAuU').text.strip()
                        endereco = i.find('div', 'sc-ge2uzh-0 eWOwnE postingAddress').text.strip()
                    except:
                        endereco = 'Sem info'

                    # BAIRRO
                    try:
                        bairro = re.match(r'^([^,]+)', (i.find('h2', {'data-qa': "POSTING_CARD_LOCATION"}).text.strip()).strip()).group(0).strip()
                        # bairro = unidecode.unidecode(str(re.sub(' +', ' ', bairro.strip())).lower())
                    except:
                        bairro = 'Sem info'
                    
                    # DESTAQUE
                    try:
                        # destaque = i.find('span', 'sc-ryls1p-0 bzIPYI').text.strip()
                        destaque = i.find('div', 'sc-i1odl-10 goAqQX').text.strip()
                    except:
                        destaque = 'Sem info'
                    
                    # LISTA AMENIDADES
                    try:
                        # amenidades = i.find('div', {'data-qa': "POSTING_CARD_FEATURES"}).find_all('span')
                        amenidades = i.find('h3', {'data-qa': "POSTING_CARD_FEATURES"}).find_all('span')
                    except:
                        amenidades = []

                    # AREA TOTAL
                    try:
                        # area_total = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 eLhfrW') != None][0]
                        area_total = [float(re.match('[0-9]{1,}', amenidades[i].text.strip()).group(0)) for i, s in enumerate(amenidades) if s.text.__contains__('m²')][0]
                    except:
                        area_total = 0.0

                    # # AREA UTIL (cubierta)
                    # try:
                    #     area_util = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 dRoEma') != None][0]
                    # except:
                    #     area_util = 0.0
                    area_util = area_total

                    # AMBIENTES
                    try:
                        # ambientes = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 jkEBRn') != None][0]
                        # ambientes = [
                        #     1.0 if 'mono' in amenidades[i].find('span').text.lower().strip()
                        #     else float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) 
                        #     for i, s in enumerate(amenidades) 
                        #     if s.find('img','sc-1uhtbxc-1 jkEBRn') != None
                        # ][0]
                        ambientes = [
                            1.0 if 'mono' in amenidades[i].text.lower().strip()
                            else float(re.match('[0-9]{1,}', amenidades[i].text.strip()).group(0)) 
                            for i, s in enumerate(amenidades) 
                            if s.text.__contains__('amb')
                        ][0]
                    except:
                        ambientes = 0.0

                    # DORMITORIOS
                    try:
                        # dormitorios = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 ljuqxM') != None][0]
                        dormitorios = [float(re.match('[0-9]{1,}', amenidades[i].text.strip()).group(0)) for i, s in enumerate(amenidades) if s.text.__contains__('dorm')][0]
                    except:
                        dormitorios = 0.0

                    # BANHEIROS
                    try:
                        # banheiros = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 foetjI') != None][0]
                        banheiros = [float(re.match('[0-9]{1,}', amenidades[i].text.strip()).group(0)) for i, s in enumerate(amenidades) if s.text.__contains__('baño')][0]
                    except:
                        banheiros = 0.0
                    
                    # GARAGENS
                    try:
                        # garagens = [float(re.match('[0-9]{1,}', amenidades[i].find('span').text.strip()).group(0)) for i, s in enumerate(amenidades) if s.find('img','sc-1uhtbxc-1 eykaou') != None][0]
                        garagens = [float(re.match('[0-9]{1,}', amenidades[i].text.strip()).group(0)) for i, s in enumerate(amenidades) if s.text.__contains__('coch')][0]
                    except:
                        garagens = 0.0

                    # TITULO
                    try:
                        titulo = i.find('h2', 'sc-i1odl-11 kvKUxE').text.strip()
                    except:
                        titulo = 'Sem info'

                    # DESCRICAO
                    try:
                        # descricao = i.find('div', {'data-qa': "POSTING_CARD_DESCRIPTION"}).text.strip()
                        descricao = i.find('h3', {'data-qa': "POSTING_CARD_DESCRIPTION"}).text.strip()
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

        logger.info('Obtendo dataframe da pasta bronze do Zonaprop')

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
            # .drop_duplicates(subset = ['id', 'tipo_imovel', 'endereco'])
        )

        # Salvando dados iniciais - db
        DuckDBtStorage(
            _path = os.path.join(get_paths()['zonaprop']['imoveis'], 'zonaprop.db'), 
            _tabela = 'bronze_imoveis_zonaprop',
            _df = df
        ).create_table()

        DuckDBtStorage(
            _path = os.path.join(get_paths()['zonaprop']['imoveis'], 'zonaprop.db'), 
            _tabela = 'bronze_imoveis_zonaprop',
            _df = df
        ).insert_data()

        # # Salvando dados iniciais brutos
        # path = os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'imoveis', 'bronze') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'imoveis', 'bronze')
            
        # pq.write_to_dataset(
        #     table = pa.Table.from_pandas(df),
        #     root_path = path,
        #     existing_data_behavior = 'delete_matching',
        #     basename_template = f"{datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).date()}_imoveis_bronze_zonaprop_" + "{i}.parquet",
        #     use_legacy_dataset = False
        # )

        return df
        # else:
        #     return None

    # @st.cache_data(ttl = 86400, max_entries = 100)
    def get_final_dataframe(self):

        # Dataframe com dados dos imóveis
        df = self.get_property_data()
        
        # Obtendo dados de longitude e latitude
        res = []

        logger.info('Iniciando iteração para obter dados geográficos')

        # Loop para obter todos os dados geográficos dos imóveis
        while True:
            
            # Ids sem latitude e longitude
            if len(res) == 0:
                df_i = df
            else:
                df_i = (
                    pd.merge(
                        left = df,
                        right = pd.DataFrame(res),
                        how = 'left',
                        on = 'id'
                    )
                    .pipe(
                        lambda df: df.loc[df.latitude.isna()]
                    )
                )

                if df_i.empty or df_i.id.nunique() / df.id.nunique() < 0.10:
                    break
                else:
                    continue

            # Thread
            with concurrent.futures.ThreadPoolExecutor(max_workers = 6) as executor:
                # Criando a sequência de tasks que serão submetidas para a thread pool
                rows = {executor.submit(apply_geocoding, row): row for index, row in df_i[['id', 'estado', 'cidade', 'bairro', 'endereco']].iterrows()}
                
                # Loop para executar as tasks de forma concorrente. Também seria possível criar uma list comprehension que esperaria todos os resultados para retornar os valores.
                for future in concurrent.futures.as_completed(rows):
                    try:
                        resultado = future.result()
                        res.append(resultado)
                    except Exception as exc:
                        continue

            logger.info(f'DF Bronze: {df.id.nunique}, DF Sem Lat/Long: {pd.DataFrame(res).id.nunique()}, Tamanho Lista: {len(res)}')

        logger.info('Obtendo dataframe final com dados geográficos da pasta silver Zonaprop')

        # Juntando dados de latitude e longitude
        df_lat_long = (
            pd.merge(
                left = df,
                right = pd.DataFrame(res),
                how = 'inner',
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

        # Salvando dados iniciais - db
        DuckDBtStorage(
            _path = os.path.join(get_paths()['zonaprop']['imoveis'], 'zonaprop.db'), 
            _tabela = 'silver_imoveis_zonaprop',
            _df = df_final
        ).create_table()

        DuckDBtStorage(
            _path = os.path.join(get_paths()['zonaprop']['imoveis'], 'zonaprop.db'), 
            _tabela = 'silver_imoveis_zonaprop',
            _df = df_final
        ).insert_data()

        # # Salvando os dados enriquecidos como parquet
        # path = os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'imoveis', 'silver') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'imoveis', 'silver')
            
        # pq.write_to_dataset(
        #     table = pa.Table.from_pandas(df_final),
        #     root_path = path,
        #     existing_data_behavior = 'delete_matching',
        #     basename_template = f"{datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).date()}_imoveis_silver_zonaprop_" + '{i}.parquet',
        #     use_legacy_dataset = False
        # )

        return df_final