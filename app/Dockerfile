# Definindo imagem do python
FROM python:3.9

# Definindo a pasta de trabalho do app
WORKDIR /app

# Instalando as dependências do SO
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copiando repositório
RUN git clone https://github.com/Nilton94/Web_Scraping_Arg/tree/main/app .

# Instalando dependencias do python
RUN pip install -r requirements.txt

# Expondo porta necessária para rodar o streamlit
EXPOSE 8501

# Checando se a porta está disponível
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app_streamlit.py", "--server.port=8501", "--server.address=0.0.0.0"]