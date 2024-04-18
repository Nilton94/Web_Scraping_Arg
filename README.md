# Web Scraping de Imóveis - Argenprop e Zonaprop


> ## Conteúdo

- [Diagrama](#diagrama)
- [Descrição do Projeto](#descrição-do-projeto)
- [Estrutura](#estrutura)
    - [Data](#data)
    - [Utils](#utils)
- [Como Utilizar o App](#como-utilizar-o-app)
    - [Render](#render)
    - [Docker Image](#docker-image)
- [Conclusão](#conclusão)

> ## Diagrama
<!-- ![Diagrama](.\app\assets\arquitetura_web_scraping_arg.png) -->
<p align="center">
  <img src=.\app\assets\arquitetura_web_scraping_arg.png alt="Diagrama">
</p>

> ## Descrição do Projeto

Visando facilitar a procura por imóveis de aluguel, criei uma aplicação em Python que facilita a busca nos sites de imóveis da minha região. Usando Streamlit como frontend, pude criar um mapa interativo com as coordenadas dos imóveis selecionados e, ao clicar nos imóveis, salvá-los em um dataframe para posterior envio por email ou consulta direta no link.

O app possui diversos filtros para facilitar a escolha dos imóveis com as características mais importantes. Além disso, todo o código foi colocado em uma imagem do Docker para facilitar tanto o deploy no Render quanto uso em outras máquinas.

> ## Estrutura
- ### Data
    - Diretório com os dados tanto geográficos quanto das páginas web retornadas pelo web scraping
    - Cada site tem seu banco de dados no DuckDB salvando os dados brutos e os dados enriquecidos com coordenadas geográficas e dados de distância para alguns pontos.
- ### Utils
    - Diretório com todas as classes e funções usadas para a criação dos crawlers, armazenamento e tratamento dos dados, bem como para a criação do frontend
    - <b> lat_long </b>:
        - Consolida funções para retorno das coordenadas geográficas com base no endereço passado, usando a biblioteca Geopy
    - <b> log_config </b>:
        - Possui as configurações do logger usando a biblioteca logging
    - <b> utils_scraper </b>:
        - Possui as classes para realizar o web scraping de ambos os sites
        - Para o site Zonaprop foi utilizado Selenium
        - Já para o site Argenprop foi utilizado Asyncio + Aiohttp
    - <b> utils_storage </b>:
        - Possui as classes para inserção e gerenciamento dos no DuckDB
    - <b> utils_streamlit </b>:
        - Possui as classes para criação dos objetos no Streamlit, como o mapa interativo usando Folium
> ## Como Utilizar o App
- ### Render:
    - Para visualizar o app, basta acessar o link onde app está hospedado
    - Link: https://web-scraping-argenprop-zonaprop.onrender.com
- ### Docker Image
    - Tendo o Docker instalado e ativo, faço o pull da imagem no Docker Hub
        ```
        $ docker pull niltonandrade/web-scraping-argenprop-zonaprop-jnga:latest
        ```
    - Com a imagem já baixada, basta dar o run. Como o app tem uma sessão de envio de e-mail, basta passar valores para as varíaveis de ambiente EMAIL e EMAIL_PASSWORD, e assim será possível enviar a lista de imóveis selecionados para o e-mail destinatário especificado no frontend.
        ```
        $ docker run -dp 127.0.0.1:8501:8501 --name {nome do container/opcional} -e EMAIL={email remetente/opcional} -e EMAIL_PASSWORD={senha do email remetente/opcional} niltonandrade/web-scraping-argenprop-zonaprop-jnga:latest
        ```