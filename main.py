import os
import json
import time
import requests
import pandas as pd
from pathlib import Path

# CONFIGURAÇÃO INICIAL
API_URL = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data"
TOKEN = "da3c4e1a384a7e1cd9bd0528ff2cc4452bec92b9"
HEADERS = {"Authorization": f"Token {TOKEN}"}

# ESQUEMA DE PASTAS
BASE_PATH = Path("dataset")
RAW_PATH = BASE_PATH / "raw"
BRONZE_PATH = BASE_PATH / "bronze"

RAW_PATH.mkdir(parents=True, exist_ok=True)
BRONZE_PATH.mkdir(parents=True, exist_ok=True)


# FUNÇÃO PARA BAIXAR OS DADOS - LIMITADO A 1000 PÁGINAS
def coletar_dados(max_paginas=1000):
    pagina_atual = 1
    proxima_url = API_URL

    while proxima_url and pagina_atual <= max_paginas:
        nome_arquivo = RAW_PATH / f"pagina_{pagina_atual:03}.json"

        if nome_arquivo.exists(): # VERIFCA SE A PÁGINA JÁ EXISTE (ESTÁ BAIXADA)
            print(f"⏭️ Página {pagina_atual} já baixada, pulando...")
            pagina_atual += 1
            proxima_url = f"{API_URL}/?page={pagina_atual}"
            continue

        print(f"⬇️ Coletando página {pagina_atual}...")
        resposta = requests.get(proxima_url, headers=HEADERS)

        if resposta.status_code == 429: # ERRO 429: O SITE NÃO DEIXA MAIS FAZER REQUISIÇÕES, DEVE-SE ESPERAR PARA PODER CONTINUAR
            print("⚠️ Muitas requisições! Aguardando 10 segundos...")
            time.sleep(10)
            continue

        resposta.raise_for_status()
        conteudo = resposta.json()
        # GARANTE QUE OS DADOS BRUTOS SEJAM SALVOS DE FORMA LEGÍVEL (pagina_x.json)
        with open(nome_arquivo, "w", encoding="utf-8") as arquivo:
            json.dump(conteudo["results"], arquivo, ensure_ascii=False, indent=2)

        proxima_url = conteudo["next"]
        pagina_atual += 1
        

    print(f"✅ Coleta concluída! ({pagina_atual - 1} páginas processadas)")


# FUNÇÃO PARA CONVERSÃO PARA PARQUET, FAZ A SEPARAÇÃO POR ANO E MÊS
def gerar_parquet():
    print("🔄 Convertendo JSONs em arquivos Parquet...")
    tabelas = []

    for arquivo_json in RAW_PATH.glob("*.json"):
        with open(arquivo_json, "r", encoding="utf-8") as f:
            registros = json.load(f)
        tabela = pd.DataFrame(registros)

        if "data" in tabela.columns:
            tabela["data"] = pd.to_datetime(tabela["data"], errors="coerce")
            tabela["ano"] = tabela["data"].dt.year
            tabela["mes"] = tabela["data"].dt.month

        tabelas.append(tabela)

    completo = pd.concat(tabelas, ignore_index=True)

    for (ano, mes), dados_mes in completo.groupby(["ano", "mes"]):
        destino = BRONZE_PATH / f"ano={ano}" / f"mes={mes:02}"
        destino.mkdir(parents=True, exist_ok=True)
        dados_mes.to_parquet(destino / "dados.parquet", index=False)

    print("🏁 Conversão finalizada! Dados disponíveis em 'dataset/bronze'.")


# EXECUÇÃO AUTOMÁTICA
def main():
    print("🚀 Iniciando pipeline completa do Brasil.IO...")
    try:
        coletar_dados(max_paginas=1000)
        gerar_parquet()
        print("🎉 Pipeline executada com sucesso!")
    except Exception as erro:
        print(f"❌ Falha na execução: {erro}")


if __name__ == "__main__":
    main()