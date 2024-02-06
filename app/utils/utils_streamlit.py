import streamlit as st
import folium
import pandas as pd
import os
from utils.lat_long import get_all_states
from utils.utils_transformations import TiposImoveis, get_columns_intersection
from folium.plugins import GroupedLayerControl

def get_widgets():
    # WIDGETS
    locais = list(get_all_states().keys())
    locais.sort()


    st.sidebar.multiselect(
        label = 'Base de Busca',
        options = ['Argenprop','Zonaprop'],
        key = 'base_busca'
    )

    st.sidebar.multiselect(
        label = 'Locais',
        options = locais,
        key = 'locais'
    )

    st.sidebar.multiselect(
        label = 'Tipos de Imóveis',
        options = TiposImoveis().total_tipos(),
        key = 'tipos'
    )

    st.sidebar.multiselect(
        label = 'Colunas da tabela',
        options = ['id', 'base','tipo_imovel','estado','cidade','bairro', 'endereco', 'url','descricao', 'titulo', 'aluguel_moeda', 'aluguel_valor', 'expensas_moeda', 'expensas_valor', 'valor_total_aluguel',
                'area_util','ambientes', 'dormitorios', 'banheiros', 'garagens', 'imobiliaria','distancia_hospital_baigorria','distancia_hospital_carrasco','distancia_hospital_ninos','distancia_hospital_provincial',
                'distancia_unr','latitude','longitude','coordenadas','data','ano','mes','dia'],
        key = 'colunas_tabela'
    )

    
    st.sidebar.slider(
        'Distância para UNR (km)',
        min_value = 0.0,
        max_value = 60.0,
        key = 'distancia_unr'
    )

    st.sidebar.slider(
        'Distância para Hospital Provincial (km)',
        min_value = 0.0,
        max_value = 60.0,
        key = 'distancia_provincial'
    )

    st.sidebar.slider(
        'Distância para Hospital de Niños (km)',
        min_value = 0.0,
        max_value = 60.0,
        key = 'distancia_ninos'
    )

    st.sidebar.slider(
        'Distância para Hospital Carrasco (km)',
        min_value = 0.0,
        max_value = 60.0,
        key = 'distancia_carrasco'
    )

    st.sidebar.slider(
        'Distância para Hospital Baigorria (km)',
        min_value = 0.0,
        max_value = 60.0,
        key = 'distancia_baigorria'
    )

    st.sidebar.button(
        label = 'Atualizar',
        key = 'atualizar'
    )

    # area util
    # banheiros
    # ambientes
    # dormitorios
    # aluguel moeda
    # aluguel valor
    # expensas moeda
    # expensas valor

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