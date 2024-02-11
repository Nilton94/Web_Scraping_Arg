import streamlit as st 
from utils.utils_scraper import ScraperArgenProp, ScraperZonaProp
from utils.utils_streamlit import get_widgets, get_dataframe, get_map
import asyncio
import folium
from streamlit_folium import st_folium, folium_static
import re

# CONFIGURACOES STREAMLIT
st.set_page_config(
    page_title = "Dados de Imóveis",
    page_icon = 'house',
    layout = 'wide',
    menu_items = {
        'about': 'App para consultar dados de imóveis das bases do Zonaprop e Argenprop. Criado por josenilton1878@gmail.com'
    }

)
# HEADER
# st.header("MAPA DE IMÓVEIS")

# WIDGETS
get_widgets()

if "lista_ids" not in st.session_state:
    st.session_state.lista_ids = []

if 'map' not in st.session_state:
    st.session_state.map = None

if st.session_state.limpar:
    st.session_state.lista_ids = []

# DADOS
df_final = get_dataframe()

with st.expander('### Mapa'):
    col1, col2 = st.columns([0.85, 0.15])
    with col1:
        st_data = st_folium(
            get_map(df_final),
            use_container_width = True,
            height = 480
        )
    with col2:
        st.markdown('### Resumo')
        st.metric(label = 'Imóveis', value = df_final.id.nunique())
        st.metric(label = 'Média Aluguel ($)', value = df_final.loc[df_final['aluguel_moeda'] == '$', 'aluguel_valor'].mean().round(1))
        st.metric(label = 'Média Expensas ($)', value = df_final.loc[df_final['expensas_moeda'] == '$', 'expensas_valor'].mean().round(1))
        st.metric(label = 'Média Ambientes', value = df_final.loc[df_final.ambientes != 0.0, 'ambientes'].mean().round(1))
        st.metric(label = 'Média Dormitórios', value = df_final.loc[df_final.dormitorios != 0.0, 'dormitorios'].mean().round(1))

    st.session_state.map = st_data

    try:
        if re.search(r'Id: (\d+)', st_data['last_object_clicked_popup']).group(1) not in st.session_state.lista_ids:
            st.session_state.lista_ids.append(
                str(re.search(r'Id: (\d+)', st_data['last_object_clicked_popup']).group(1))
            )
    except:
        st.write('')
        
with st.expander('### Base'):
    st.dataframe(
        df_final[
            [
                'id', 'base','tipo_imovel','estado','cidade','bairro', 'endereco', 'url','descricao', 'titulo', 'aluguel_moeda', 'aluguel_valor', 'expensas_moeda', 'expensas_valor', 'valor_total_aluguel',
                'area_util','ambientes', 'dormitorios', 'banheiros', 'garagens', 'imobiliaria','distancia_hospital_baigorria','distancia_hospital_carrasco','distancia_hospital_ninos','distancia_hospital_provincial',
                'distancia_unr','latitude','longitude','coordenadas','data','ano','mes','dia'
            ]
        ]
    )

with st.expander('### Lista de Imóveis Selecionados'):
    try:
        st.dataframe(
            (
                df_final
                .loc[df_final.id.isin(st.session_state.lista_ids)]
                [[
                    'id', 'base','tipo_imovel','estado','cidade','bairro', 'endereco', 'url','descricao', 'titulo', 'aluguel_moeda', 'aluguel_valor', 'expensas_moeda', 'expensas_valor', 'valor_total_aluguel',
                    'area_util','ambientes', 'dormitorios', 'banheiros', 'garagens', 'imobiliaria','distancia_hospital_baigorria','distancia_hospital_carrasco','distancia_hospital_ninos','distancia_hospital_provincial',
                    'distancia_unr','latitude','longitude','coordenadas','data','ano','mes','dia'
                ]]
            )
        )
    except:
        st.write('Selecione ao menos um imóvel!')