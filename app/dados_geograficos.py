import requests
import pandas as pd
from pandas import json_normalize
import os
import pyarrow as pa
import pyarrow.parquet as pq
import unidecode
import re


# DADOS DE PROVINCIAS/ESTADOS ARGENTINOS #
url = ' https://apis.datos.gob.ar/georef/api/provincias?campos=id,nombre,centroide'
r = requests.get(url)
dados = r.json()['provincias']

df = (
    json_normalize(dados)
    .rename(
        columns = {
            'nombre':'provincia',
            'centroide.lat':'latitude',
            'centroide.lon':'longitude'
        }
    )
    .sort_values(by = 'id')
    .reset_index(drop = True)
)

pa_df = pa.Table.from_pandas(df)

file_path = os.path.join(os.getcwd(),'data','geograficos','provincias') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','provincias')

# pq.write_to_dataset(
#     pa_df, 
#     root_path = file_path
# )


# DADOS DOS MUNICIPIOS
provincias = pd.read_parquet(
        os.path.join(os.getcwd(),'data','geograficos','provincias') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','provincias')
)
ids_provincias = {value['id']:value['provincia'] for name, value in provincias.iterrows()}

list_muni = []
for id in ids_provincias.keys():
    url = f'https://apis.datos.gob.ar/georef/api/municipios?provincia={id}&campos=id,nombre,centroide&max=1000'
    r = requests.get(url)
    municipios = r.json()
    list_muni.append(municipios)

muni_df = pd.DataFrame(columns = ['id_provincia', 'municipios_provincia', 'id_municipio', 'municipio', 'latitude', 'longitude'])

for i in range(len(list_muni)):
    try:
        df_auxiliar = (
            json_normalize(list_muni[i]['municipios'])
            .rename(
                columns = {
                    'id':'id_municipio',
                    'nombre':'municipio',
                    'centroide.lat':'latitude',
                    'centroide.lon':'longitude'
                }
            )
        )
        df_auxiliar['id_provincia'] = list_muni[i]['parametros']['provincia'][0]
        df_auxiliar['municipios_provincia'] = list_muni[i]['cantidad']
        muni_df = pd.concat([muni_df, df_auxiliar[['id_provincia', 'municipios_provincia', 'id_municipio', 'municipio', 'latitude', 'longitude']]], ignore_index = True)
    except:
        continue

pa_muni_df = pa.Table.from_pandas(muni_df)

file_path_muni = os.path.join(os.getcwd(),'data','geograficos','municipios') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','municipios')

# pq.write_to_dataset(
#     pa_muni_df, 
#     root_path = file_path_muni
# )

# CRIANDO DATAFRAME FINAL COM DADOS DE MUNICIPIO E PROVINCIA
df_muni = pd.read_parquet(
    os.path.join(os.getcwd(),'data','geograficos','municipios') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','municipios')
)

df_prov = pd.read_parquet(
    os.path.join(os.getcwd(),'data','geograficos','provincias') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','provincias')
)

muni_prov_df = (
    pd.merge(
        left = df_muni,
        right = df_prov,
        how = 'left',
        left_on = ['id_provincia'],
        right_on = ['id']
    )
    .drop('id', axis = 1)
    .assign(municipio_tratado = lambda df: df['municipio'].apply(lambda x: unidecode.unidecode(str(re.sub(' +', ' ', x.strip())).lower())))
    .assign(provincia_tratada = lambda df: df['provincia'].apply(lambda x: unidecode.unidecode(str(re.sub(' +', ' ', x.strip())).lower())))
    .rename(
        columns ={
            'latitude_x': 'latitude_municipio',
            'longitude_x': 'longitude_municipio',
            'latitude_y': 'latitude_provincia',
            'longitude_y': 'longitude_provincia',
        }
    )
    [['id_municipio', 'municipio', 'latitude_municipio', 'longitude_municipio', 'id_provincia', 'provincia', 'latitude_provincia', 'longitude_provincia', 'municipios_provincia', 'municipio_tratado', 'provincia_tratada']]
)

pa_muni_prov_df = pa.Table.from_pandas(muni_prov_df)

file_path_muni_prov = os.path.join(os.getcwd(),'data','geograficos','provincias_municipios') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','provincias_municipios')

# pq.write_to_dataset(
#     pa_muni_prov_df, 
#     root_path = file_path_muni_prov
# )

# DEPARTAMENTOS (NAO ENCONTRADOS NA API)
df_dpt = (
    pd.read_excel(r'G:\Otros ordenadores\Meu modelo Laptop\Ciência de Dados\8. Projetos\web_scraping_arg\departamentos_argentina.xlsx')
    .assign(Nombre = lambda x: x['Nombre'].str.lower().apply(lambda y: unidecode.unidecode(y)))
    .assign(Provincia = lambda x: x['Provincia'].str.lower().apply(lambda y: unidecode.unidecode(y)))
    .rename(
        columns = {
            'Nombre':'departamento',
            'Latitud':'latitude', 
            'Longitud':'longitude',
            'Provincia':'provincia'
        }
    )
    [['departamento','latitude', 'longitude','provincia']]
)

df_dpt = (
        pd.merge(
            left = df_dpt,
            right = df_prov.assign(provincia = lambda x: x['provincia'].str.lower().apply(lambda y: unidecode.unidecode(y))),
            how = 'left',
            left_on = 'provincia',
            right_on = 'provincia'
        )
        .rename(
            columns = {
                'latitude_x': 'latitude_municipio',		
                'longitude_x': 'longitude_municipio',
                'id': 'id_provincia',	
                'latitude_y': 'latitude_provincia',	
                'longitude_y': 'longitude_provincia'
            }
        )
    )

pa_dpt_df = pa.Table.from_pandas(df_dpt)

file_path_dpt = os.path.join(os.getcwd(),'data','geograficos','departamentos') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','geograficos','departamentos')

pq.write_to_dataset(
    pa_dpt_df, 
    root_path = file_path_dpt
)

# JSON COM DADOS CONSOLIDADOS DE PROVINCIAS E MUNICIPIOS/DEPARTAMENTOS

print(pa_muni_prov_df)