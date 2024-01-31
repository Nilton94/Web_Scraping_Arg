import streamlit as st
from utils.lat_long import get_all_states
from utils.utils_transformations import TiposImoveis, get_columns_intersection

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