# Definindo imagem do python
FROM python:3.8

# Definindo a pasta de trabalho do app
WORKDIR /code

# Copiando os arquivps
COPY ./requirements.txt /code/requirements.txt

# Instalando as dependências, sem cache do pip na pasta requirements do container
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copiando os dados da pasta app para o folder app no container
COPY ./app /code/app