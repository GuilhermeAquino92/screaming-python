# status_checker.py

import requests
from tqdm import tqdm
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

HEADERS_PADRAO = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

def checar_status_http(url, timeout=10):
    try:
        response = requests.head(url, allow_redirects=True, timeout=timeout, headers=HEADERS_PADRAO, verify=False)
        if response.status_code == 405:
            response = requests.get(url, stream=True, timeout=timeout, headers=HEADERS_PADRAO, verify=False)
        return response.status_code, response.headers.get("Content-Type", "")
    except requests.exceptions.RequestException as e:
        return None, str(e)

def verificar_status_http(lista_urls, max_threads=30):
    resultados = []

    def tarefa(url):
        status_code, tipo_conteudo = checar_status_http(url)
        return {
            "url": url,
            "status_code_http": status_code,
            "tipo_conteudo_http": tipo_conteudo
        }

    print(f"🔄 Verificando com até {max_threads} threads...")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futuros = {executor.submit(tarefa, url): url for url in lista_urls}
        for i, future in enumerate(as_completed(futuros), 1):
            try:
                resultados.append(future.result())
            except Exception as e:
                resultados.append({"url": futuros[future], "status_code_http": None, "tipo_conteudo_http": str(e)})

            if i % 100 == 0 or i == len(lista_urls):
                print(f"⏱️ {i}/{len(lista_urls)} URLs verificadas...")

    return resultados
