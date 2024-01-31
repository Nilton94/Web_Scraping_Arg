import streamlit as st 
from utils.utils_scraper import ScraperArgenProp, ScraperZonaProp
from utils.utils_transformations import TiposImoveis, get_columns_intersection
from utils.lat_long import get_all_states
from utils.utils_streamlit import get_widgets
import pandas as pd
import asyncio

# CONFIGURACOES STREAMLIT
st.set_page_config(
    page_title = "Dados de Imóveis",
    layout = 'wide',
    menu_items = {
        'about': 'App para consultar dados de imóveis das bases do Zonaprop e Argenprop. Criado por josenilton1878@gmail.com'
    }

)

# WIDGETS
get_widgets()

if st.session_state.atualizar:

    # DADOS
    if ('Argenprop' in st.session_state.base_busca) and ('Zonaprop' in st.session_state.base_busca):
        df_argenprop = asyncio.run(ScraperArgenProp(st.session_state.tipos, st.session_state.locais).get_final_dataframe())
        df_zonaprop = ScraperZonaProp(_tipo = st.session_state.tipos, _local = st.session_state.locais).get_final_dataframe()

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
                    (df.distancia_unr.between(0.0, st.session_state.distancia_unr if st.session_state.distancia_unr > 0.0 else 60.0))
                    & (df.distancia_hospital_provincial.between(0.0, (st.session_state.distancia_provincial if st.session_state.distancia_provincial > 0.0 else 60.0)))
                    & (df.distancia_hospital_ninos.between(0.0, (st.session_state.distancia_ninos if st.session_state.distancia_ninos > 0.0 else 60.0)))
                    & (df.distancia_hospital_carrasco.between(0.0, (st.session_state.distancia_carrasco if st.session_state.distancia_carrasco > 0.0 else 60.0)))
                    & (df.distancia_hospital_baigorria.between(0.0, (st.session_state.distancia_baigorria if st.session_state.distancia_baigorria > 0.0 else 60.0)))
                ]
            )
            .reset_index(drop = True)
        )

    elif 'Argenprop' in st.session_state.base_busca:
        df_argenprop = asyncio.run(ScraperArgenProp(st.session_state.tipos, st.session_state.locais).get_final_dataframe())
        
        df_final = (
            df_argenprop
            .pipe(
                lambda df: df.loc[
                    (df.distancia_unr.between(0.0, st.session_state.distancia_unr if st.session_state.distancia_unr > 0.0 else 60.0))
                    & (df.distancia_hospital_provincial.between(0.0, (st.session_state.distancia_provincial if st.session_state.distancia_provincial > 0.0 else 60.0)))
                    & (df.distancia_hospital_ninos.between(0.0, (st.session_state.distancia_ninos if st.session_state.distancia_ninos > 0.0 else 60.0)))
                    & (df.distancia_hospital_carrasco.between(0.0, (st.session_state.distancia_carrasco if st.session_state.distancia_carrasco > 0.0 else 60.0)))
                    & (df.distancia_hospital_baigorria.between(0.0, (st.session_state.distancia_baigorria if st.session_state.distancia_baigorria > 0.0 else 60.0)))
                ]
            )
            .reset_index(drop = True)
        )

    elif 'Zonaprop' in st.session_state.base_busca:
        df_zonaprop = ScraperZonaProp(_tipo = st.session_state.tipos, _local = st.session_state.locais).get_final_dataframe()
        
        df_final = (
            df_zonaprop
            .pipe(
                lambda df: df.loc[
                    (df.distancia_unr.between(0.0, st.session_state.distancia_unr if st.session_state.distancia_unr > 0.0 else 60.0))
                    & (df.distancia_hospital_provincial.between(0.0, (st.session_state.distancia_provincial if st.session_state.distancia_provincial > 0.0 else 60.0)))
                    & (df.distancia_hospital_ninos.between(0.0, (st.session_state.distancia_ninos if st.session_state.distancia_ninos > 0.0 else 60.0)))
                    & (df.distancia_hospital_carrasco.between(0.0, (st.session_state.distancia_carrasco if st.session_state.distancia_carrasco > 0.0 else 60.0)))
                    & (df.distancia_hospital_baigorria.between(0.0, (st.session_state.distancia_baigorria if st.session_state.distancia_baigorria > 0.0 else 60.0)))
                ]
            )
            .reset_index(drop = True)
        )

    else:
        st.text('Selecione uma ou mais bases!')

    # imoveis = df_final['id'].nunique()
    # aluguel_mediana = df_final['aluguel_valor'].median()
    # expensas_mediana = df_final['expensas_valor'].median()

    st.dataframe(
        df_final[st.session_state.colunas_tabela], 
        use_container_width = True
    )