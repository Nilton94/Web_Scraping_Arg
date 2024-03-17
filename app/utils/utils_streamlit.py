import streamlit as st
import folium
import pandas as pd
import os
from utils.lat_long import get_all_states
from utils.utils_transformations import TiposImoveis, get_columns_intersection
from folium.plugins import GroupedLayerControl
import asyncio
from utils.utils_scraper import ScraperArgenProp, ScraperZonaProp
import smtplib
import email.message
import re
import textwrap
from dotenv import load_dotenv
from io import BytesIO
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import openpyxl
from utils.utils_storage import ParquetStorage

# Carregando variáveis de ambiente
load_dotenv()

def get_widgets():
    # WIDGETS
    locais = list(get_all_states().keys())
    locais.sort()

    with st.expander('### Principais Filtros'):
        # with st.container(height = 120, border=True):
        col1_r1, col2_r1, col3_r1, col4_r1, col5_r1 = st.columns(5)
        col1_r2, col2_r2, col3_r2, col4_r2, col5_r2 = st.columns(5)
        
        col1_r1.multiselect(
            label = 'Base de Busca',
            options = ['Argenprop','Zonaprop'],
            key = 'base_busca'
        )

        col2_r1.multiselect(
            label = 'Locais',
            options = locais,
            key = 'locais'
        )

        col3_r1.multiselect(
            label = 'Tipos de Imóveis',
            options = TiposImoveis().total_tipos(),
            key = 'tipos'
        )

        col4_r1.multiselect(
            'Moeda do Aluguel',
            options = ['USD', '$', 'Sem info'],
            key = 'aluguel_moeda'
        )

        col1_r2.slider(
            'Aluguel',
            min_value = 0,
            max_value = 999_999,
            value = (0, 999_999),
            step = 1000,
            key = 'aluguel_valor'
        )

        col5_r1.multiselect(
            'Moeda das Expensas',
            options = ['USD', '$', 'Sem info'],
            key = 'expensas_moeda'
        )

        col2_r2.slider(
            'Expensas',
            min_value = 0,
            max_value = 999_999,
            value = (0, 999_999),
            step = 1000,
            key = 'expensas_valor'
        )

        col3_r2.slider(
            'Ambientes',
            min_value = 0,
            max_value = 100,
            value = (0, 100),
            key = 'ambientes'
        )

        col4_r2.slider(
            'Dormitórios',
            min_value = 0,
            max_value = 100,
            value = (0, 100),
            key = 'dormitorios'
        )

        col5_r2.slider(
            'Distância para UNR (km)',
            min_value = 0.0,
            max_value = 60.0,
            value = (0.0, 100.0),
            key = 'distancia_unr'
        )

    st.sidebar.markdown('### Outros Filtros')

    st.sidebar.slider(
        'Área Útil (m2)',
        min_value = 0.0,
        max_value = 999.0,
        value = (0.0, 999.0),
        key = 'area_util'
    )

    st.sidebar.slider(
        'Banheiros',
        min_value = 0,
        max_value = 100,
        value = (0, 100),
        key = 'banheiros'
    )
    
    st.sidebar.slider(
        'Distância para Hospital Provincial (km)',
        min_value = 0.0,
        max_value = 60.0,
        value = (0.0, 100.0),
        key = 'distancia_provincial'
    )

    st.sidebar.slider(
        'Distância para Hospital de Niños (km)',
        min_value = 0.0,
        max_value = 60.0,
        value = (0.0, 100.0),
        key = 'distancia_ninos'
    )

    st.sidebar.slider(
        'Distância para Hospital Carrasco (km)',
        min_value = 0.0,
        max_value = 60.0,
        value = (0.0, 100.0),
        key = 'distancia_carrasco'
    )

    st.sidebar.slider(
        'Distância para Hospital Baigorria (km)',
        min_value = 0.0,
        max_value = 60.0,
        value = (0.0, 100.0),
        key = 'distancia_baigorria'
    )

def get_email_widgets():

    st.text_input(
        label = 'E-mails (separados por vírgula)',
        key = 'email_usuario',
         help = 'E-mail para enviar os imóveis selecionados.'
    )

    e1, e2 = st.columns(2)
    e1.button(
        label = '## Limpar Lista de Ids',
        key = 'limpar',
        type = 'primary',
        use_container_width = True,
        help = "Ao clicar em um imóvel, este é incluido em uma lista de interesse. Para reiniciar a lista, basta clicar no botão."
    )
    

    e2.button(
        label = 'Enviar E-mail',
        key = 'enviar_email',
        type = 'primary',
        use_container_width = True,
        help = '(WIP) Envia a lista de imóveis selecionados para o e-mail passado.'
    )

def get_map(df: pd.DataFrame, tipo_layer: str = 'bairro'):
    '''
        Cria um mapa com base no dataframe passado

        Parâmetros:
        ----------
        df: pd.DataFrame
            dataframe com dados geográficos
        tipo_layer: 'bairro' ou 'base'
            define qual layer será definido no mapa
            impossibilidade de colocar um mesmo marker em layers diferentes
    '''

    # Inicializando o mapa
    m = folium.Map(
        location = [-32.945, -60.667],
        zoom_start = 15
    )

    # Criando grupos de layers para filtrar, dentro do mapa, a base
    zonaprop_group = folium.FeatureGroup("Zonaprop")
    argenprop_group = folium.FeatureGroup("Argenprop")

    # Layers dos bairros. cria uma layer pra cada bairro distinto
    dict_bairros = {}

    for i in df.bairro.sort_values().unique():
        dict_bairros[i] = folium.FeatureGroup(i)

    for _, row in df.iterrows():
        # Definindo o ícone
        zona_icon = [
            folium.features.CustomIcon(os.path.join(os.getcwd(), 'assets', 'argenprop.jpg') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'assets', 'argenprop.jpg'), icon_size=(22, 26))
            if row['base'] == 'argenprop'
            else folium.features.CustomIcon(os.path.join(os.getcwd(), 'assets', 'zonaprop.png') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'assets', 'zonaprop.png'), icon_size=(22, 26))
        ][0]
        
        # Criando marker
        marker = folium.Marker(
            location = [row.latitude, row.longitude],
            popup = f'''
                <b>Id</b>: {row['id']} <br>
                <b>Base</b>: {row['base']} <br> 
                <b>Endereço</b>:  {row['endereco']} <br> 
                <b>Imobiliária</b>:  {row['imobiliaria']} <br> 
                <b>Área Útil</b>:  {row['area_util']} <br> 
                <b>Dormitórios</b>:  {row['dormitorios']} <br> 
                <b>Ambientes</b>:  {row['ambientes']} <br> 
                <b>Banheiros</b>:  {row['banheiros']} <br> 
                <b>Link</b>:  <a href={row['url']}>{row['url']}</a> <br> 
            ''',
            # hoover
            tooltip = f'''
                <b>Tipo de Imóvel</b>: {row['tipo_imovel']} <br>
                <b>Bairro</b>: {row['bairro']} <br> 
                <b>Aluguel ({row.aluguel_moeda})</b>: {row['aluguel_valor']} <br> 
                <b>Expensas ({row.expensas_moeda})</b>: {row['expensas_valor']} <br> 
                <b>Valor Total ({row.aluguel_moeda})</b>: {row.valor_total_aluguel}
            ''',
            icon = zona_icon
        )

        if tipo_layer == 'bairro':
            marker.add_to(dict_bairros[row['bairro']])
        else:
            if row['base'] == 'argenprop':
                marker.add_to(argenprop_group)
            else:
                marker.add_to(zonaprop_group)
    
    if tipo_layer == 'bairro':
        for i in dict_bairros:
            m.add_child(dict_bairros[i])
        
        GroupedLayerControl(
            groups = {'Bairros': [dict_bairros[i] for i in dict_bairros]},
            collapsed = True,
            exclusive_groups = False
        ).add_to(m)
    else:
        m.add_child(zonaprop_group)
        m.add_child(argenprop_group)
    
        GroupedLayerControl(
            groups = {'Base de Busca': [zonaprop_group, argenprop_group]},
            collapsed = True,
            exclusive_groups=False
        ).add_to(m)

    return m


def get_paths():

    return {
        'path_page_argenprop': os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'paginas'),
        'path_page_zonaprop': os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'paginas') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'paginas'),
        'bronze_argenprop': os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'imoveis', 'bronze') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'imoveis', 'bronze'),
        'silver_argenprop': os.path.join(os.getcwd(), 'data', 'imoveis', 'argenprop', 'imoveis', 'silver') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'argenprop', 'imoveis', 'silver'),
        'bronze_zonaprop': os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'imoveis', 'bronze') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'imoveis', 'bronze'),
        'silver_zonaprop': os.path.join(os.getcwd(), 'data', 'imoveis', 'zonaprop', 'imoveis', 'silver') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'data', 'imoveis', 'zonaprop', 'imoveis', 'silver')
    }

def get_zonaprop():
    
    # Paths
    path_page_zonaprop = get_paths()['path_page_zonaprop']
    bronze_zonaprop = get_paths()['bronze_zonaprop']
    silver_zonaprop = get_paths()['silver_zonaprop']

    # Checando se os dados do dia atual existem
    ParquetStorage(_path = path_page_zonaprop, _locais = st.session_state.locais).check_parquet()
    ParquetStorage(_path = bronze_zonaprop, _locais = st.session_state.locais).check_parquet()
    ParquetStorage(_path = silver_zonaprop, _locais = st.session_state.locais).check_parquet()

    # Checando se existem arquivos
    check_page_zonaprop = ParquetStorage(_path = path_page_zonaprop, _locais = st.session_state.locais).check_files()
    check_bronze_zonaprop = ParquetStorage(_path = bronze_zonaprop, _locais = st.session_state.locais).check_files()

    # Caso não exista arquivo na página bronze, carrega tudo
    if check_bronze_zonaprop == 0:
        df_zonaprop = ScraperZonaProp(
                _tipo = [
                    'casas','departamentos','ph','locales-comerciales','oficinas-comerciales','bodegas-galpones','cocheras','depositos','terrenos',
                    'edificios','quintas-vacacionales','campos','fondos-de-comercio','hoteles', 'consultorios','cama-nautica','bovedas-nichos-y-parcelas'
                ], 
                _local = st.session_state.locais
            ).get_final_dataframe()

        df_zonaprop.loc[df_zonaprop.tipo_imovel.isin(st.session_state.tipos)]

    else:
        df_zonaprop = pd.read_parquet(
                path = get_paths()['silver_zonaprop'],
                filters = [
                    ('cidade', 'in', st.session_state.locais),
                    ('tipo_imovel', 'in', st.session_state.tipos)
                ]
            )
    
    return df_zonaprop
    
def get_argenprop():
    
    # Paths
    path_page_argenprop = get_paths()['path_page_argenprop']
    bronze_argenprop = get_paths()['bronze_argenprop']
    silver_argenprop = get_paths()['silver_argenprop']

    # Checando se os dados do dia atual existem
    ParquetStorage(_path = path_page_argenprop, _locais = st.session_state.locais).check_parquet()
    ParquetStorage(_path = bronze_argenprop, _locais = st.session_state.locais).check_parquet()
    ParquetStorage(_path = silver_argenprop, _locais = st.session_state.locais).check_parquet()

    # Checando se existem arquivos
    check_page_argenprop = ParquetStorage(_path = path_page_argenprop, _locais = st.session_state.locais).check_files()
    check_bronze_argenprop = ParquetStorage(_path = bronze_argenprop, _locais = st.session_state.locais).check_files()

    # Caso não exista arquivo na página bronze, carrega tudo
    if check_bronze_argenprop == 0:
        df_argenprop = asyncio.run(
            ScraperArgenProp(
                _tipo = ['departamentos','casas','campos','cocheras','fondos-de-comercio','galpones','hoteles','locales','negocios-especiales','oficinas','ph','quintas','terrenos'], 
                _local = st.session_state.locais
            ).get_final_dataframe()
        )

        df_argenprop.loc[df_argenprop.tipo_imovel.isin(st.session_state.tipos)]

    else:
        df_argenprop = pd.read_parquet(
                path = get_paths()['silver_argenprop'],
                filters = [
                    ('cidade', 'in', st.session_state.locais),
                    ('tipo_imovel', 'in', st.session_state.tipos)
                ]
            )
    
    return df_argenprop

def get_dataframe(df_argenprop: pd.DataFrame = None, df_zonaprop: pd.DataFrame = None):

    # DADOS
    if ('Argenprop' in st.session_state.base_busca) and ('Zonaprop' in st.session_state.base_busca):
        df_argenprop = get_argenprop()
        df_zonaprop = get_zonaprop()

        df_final = (
            pd.concat(
                [
                    df_argenprop.loc[:, get_columns_intersection(df_argenprop, df_zonaprop)],
                    df_zonaprop.loc[:, get_columns_intersection(df_argenprop, df_zonaprop)]
                ],
                ignore_index = True
            )
            .pipe(
                lambda df: df.loc[
                    (df.distancia_unr.between(st.session_state.distancia_unr[0], st.session_state.distancia_unr[1]))
                    & (df.distancia_hospital_provincial.between(st.session_state.distancia_provincial[0], st.session_state.distancia_provincial[1]))
                    & (df.distancia_hospital_ninos.between(st.session_state.distancia_ninos[0], st.session_state.distancia_ninos[1]))
                    & (df.distancia_hospital_carrasco.between(st.session_state.distancia_carrasco[0], st.session_state.distancia_carrasco[1]))
                    & (df.distancia_hospital_baigorria.between(st.session_state.distancia_baigorria[0], st.session_state.distancia_baigorria[1]))
                    & (df.area_util.between(st.session_state.area_util[0], st.session_state.area_util[1]))
                    & (df.banheiros.between(st.session_state.banheiros[0], st.session_state.banheiros[1]))
                    & (df.ambientes.between(st.session_state.ambientes[0], st.session_state.ambientes[1]))
                    & (df.dormitorios.between(st.session_state.dormitorios[0], st.session_state.dormitorios[1]))
                    & (df.aluguel_valor.between(st.session_state.aluguel_valor[0], st.session_state.aluguel_valor[1]))
                    & (df.expensas_valor.between(st.session_state.expensas_valor[0], st.session_state.expensas_valor[1]))
                    & (df.aluguel_moeda.isin(
                            ['$', 'USD', 'Sem info', 'Consultar precio'] if len(st.session_state.aluguel_moeda) == 0 else st.session_state.aluguel_moeda
                        )
                    )
                    & (df.expensas_moeda.isin(
                            ['$', 'USD', 'Sem info', 'Consultar precio'] if len(st.session_state.expensas_moeda) == 0 else st.session_state.expensas_moeda
                        )
                    )
                ]
            )
            .reset_index(drop = True)
        )

    elif 'Argenprop' in st.session_state.base_busca:
        df_argenprop = get_argenprop()
        
        df_final = (
            df_argenprop
            .pipe(
                lambda df: df.loc[
                    (df.distancia_unr.between(st.session_state.distancia_unr[0], st.session_state.distancia_unr[1]))
                    & (df.distancia_hospital_provincial.between(st.session_state.distancia_provincial[0], st.session_state.distancia_provincial[1]))
                    & (df.distancia_hospital_ninos.between(st.session_state.distancia_ninos[0], st.session_state.distancia_ninos[1]))
                    & (df.distancia_hospital_carrasco.between(st.session_state.distancia_carrasco[0], st.session_state.distancia_carrasco[1]))
                    & (df.distancia_hospital_baigorria.between(st.session_state.distancia_baigorria[0], st.session_state.distancia_baigorria[1]))
                    & (df.area_util.between(st.session_state.area_util[0], st.session_state.area_util[1]))
                    & (df.banheiros.between(st.session_state.banheiros[0], st.session_state.banheiros[1]))
                    & (df.ambientes.between(st.session_state.ambientes[0], st.session_state.ambientes[1]))
                    & (df.dormitorios.between(st.session_state.dormitorios[0], st.session_state.dormitorios[1]))
                    & (df.aluguel_valor.between(st.session_state.aluguel_valor[0], st.session_state.aluguel_valor[1]))
                    & (df.expensas_valor.between(st.session_state.expensas_valor[0], st.session_state.expensas_valor[1]))
                    & (df.aluguel_moeda.isin(
                            ['$', 'USD', 'Sem info', 'Consultar precio'] if len(st.session_state.aluguel_moeda) == 0 else st.session_state.aluguel_moeda
                        )
                    )
                    & (df.expensas_moeda.isin(
                            ['$', 'USD', 'Sem info', 'Consultar precio'] if len(st.session_state.expensas_moeda) == 0 else st.session_state.expensas_moeda
                        )
                    )
                ]
            )
            .reset_index(drop = True)
        )

    elif 'Zonaprop' in st.session_state.base_busca:
        df_zonaprop = get_zonaprop()
        
        df_final = (
            df_zonaprop
            .pipe(
                lambda df: df.loc[
                    (df.distancia_unr.between(st.session_state.distancia_unr[0], st.session_state.distancia_unr[1]))
                    & (df.distancia_hospital_provincial.between(st.session_state.distancia_provincial[0], st.session_state.distancia_provincial[1]))
                    & (df.distancia_hospital_ninos.between(st.session_state.distancia_ninos[0], st.session_state.distancia_ninos[1]))
                    & (df.distancia_hospital_carrasco.between(st.session_state.distancia_carrasco[0], st.session_state.distancia_carrasco[1]))
                    & (df.distancia_hospital_baigorria.between(st.session_state.distancia_baigorria[0], st.session_state.distancia_baigorria[1]))
                    & (df.area_util.between(st.session_state.area_util[0], st.session_state.area_util[1]))
                    & (df.banheiros.between(st.session_state.banheiros[0], st.session_state.banheiros[1]))
                    & (df.ambientes.between(st.session_state.ambientes[0], st.session_state.ambientes[1]))
                    & (df.dormitorios.between(st.session_state.dormitorios[0], st.session_state.dormitorios[1]))
                    & (df.aluguel_valor.between(st.session_state.aluguel_valor[0], st.session_state.aluguel_valor[1]))
                    & (df.expensas_valor.between(st.session_state.expensas_valor[0], st.session_state.expensas_valor[1]))
                    & (df.aluguel_moeda.isin(
                            ['$', 'USD', 'Sem info', 'Consultar precio'] if len(st.session_state.aluguel_moeda) == 0 else st.session_state.aluguel_moeda
                        )
                    )
                    & (df.expensas_moeda.isin(
                            ['$', 'USD', 'Sem info', 'Consultar precio'] if len(st.session_state.expensas_moeda) == 0 else st.session_state.expensas_moeda
                        )
                    )
                ]
            )
            .reset_index(drop = True)
        )

    else:
        st.text('Selecione uma ou mais bases!')

    return df_final

def send_mail(df: pd.DataFrame, emails: str = None):
    '''
        Envia e-mail com os dados dos imóveis selecionados.

        Parâmetros:
        -----------
        df: pd.Dataframe
            Base de imóveis selecionados.
        email: str
            E-mail passado no sidebar.
    '''

    # Criando arquivo excel em memória
    excel_file = BytesIO()
    df.to_excel(excel_file, index=False)
    excel_file.seek(0)  # movendo cursor para o início do arquivo

    # Definindo o corpo do email
    corpo_email = textwrap.dedent(
        'Olá, confira abaixo a lista com os imóveis selecionados.'
    )

    # lista de emails
    lista_emails = [x for x in re.sub('\s+','', st.session_state.email_usuario).split(',')]

    # Loop para enviar um email por vez
    for i in range(0,len(lista_emails)):
        msg = MIMEMultipart()
        msg['Subject'] = f"Lista de imóveis selecionados"
        msg['From'] = os.getenv(key = "EMAIL")
        msg['To'] = lista_emails[i]
        senha = os.getenv(key = "EMAIL_PASSWORD")
        msg.add_header('Content-Type','text/html')
        # msg.set_payload(corpo_email)

        part = MIMEApplication(excel_file.getvalue(), Name = 'dataframe.xlsx')
        part['Content-Disposition'] = f'attachment; filename="dataframe.xlsx"'
        msg.attach(part)
        msg.attach(MIMEText(corpo_email, "plain"))

        # Definindo a conexão e enviando
        s = smtplib.SMTP('smtp.gmail.com: 587')
        s.starttls()
        s.login(msg['From'], senha)
        
        s.sendmail(
            msg['From'],
            [msg['To']],
            msg.as_string().encode('utf-8')
        )