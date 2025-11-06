## Resumo geral:

- O código faz uma pipeline automatizada para coletar dados do site brasil.io, salva em formato jon e depois converte para parquet organizado por mês e ano.

## Configuração inicial

- Bibliotecas necessárias: os, json, requests, pandas e pathlib;
- Código cria estrutura de pastas raw e bronze(mkdir...);
- É necessário ter o token de API.


## Funções:

### coletar_dados(): 
- Começa da página 1 e vai até a pagina 1000, pois foi a pagina definida como limite;
- verifica se o arquivo já existe, se sim já vai para a próxima página;
- Faz a requisição a API com o token;
- Se cair no ERRO 429 (muitas requisições) o programa faz esperar 10 segundos para voltar a tentar;
- Baixando as páginas salva o conteúdo em json na pasta raw

### gerar_parquet():
- Lê os aqruivos json da pasta raw;
- Cria um DF com os registros;
- Converte a coluna data para date time para poder extrair o ano e o mês;
- Junta tudo em um único DF;
- Por fim separa os dados por ano e mês e coloca na pasta datase/bronze/ano/mes.

### main():
- Função que executa as funções citadas anteriormente
