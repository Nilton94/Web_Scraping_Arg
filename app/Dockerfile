# Definindo imagem do python
FROM python:3.9

# Definindo a pasta de trabalho do app
WORKDIR /app

# Adicionando metadata do autor
LABEL org.opencontainers.image.authors="josenilton1878@gmail.com"

# Instalando as dependências do SO
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    firefox-esr \
    && rm -rf /var/lib/apt/lists/*

# Instalando GeckoDriver
RUN apt-get update && apt-get install -y \
    wget \
    && wget -O /tmp/geckodriver.tar.gz https://github.com/mozilla/geckodriver/releases/download/v0.34.0/geckodriver-v0.34.0-linux64.tar.gz \
    && tar -C /usr/local/bin -xzf /tmp/geckodriver.tar.gz \
    && rm /tmp/geckodriver.tar.gz

# # Instalando Chorme
# RUN apt-get update && apt-get install -y wget unzip && \
#     wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
#     apt install -y ./google-chrome-stable_current_amd64.deb && \
#     rm google-chrome-stable_current_amd64.deb && \
#     apt-get clean

# # Copiando repositório
# RUN git clone https://github.com/Nilton94/Web_Scraping_Arg/tree/main/app .

# Copiando do próprio diretório onde está o código
COPY . .

# Instalando dependencias do python
RUN pip install -r requirements.txt

# Expondo porta necessária para rodar o streamlit
EXPOSE 8501

# Checando se a porta está disponível
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app_streamlit.py", "--server.port=8501", "--server.address=0.0.0.0"]