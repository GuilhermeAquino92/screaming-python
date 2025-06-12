# metatags.py

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9"
}

def extrair_metadados(url):
    try:
        response = requests.get(url, timeout=10, headers=HEADERS, verify=False)
        soup = BeautifulSoup(response.text, "lxml")

        title = soup.title.string.strip() if soup.title else ""
        description = soup.find("meta", attrs={"name": "description"})
        canonical = soup.find("link", rel="canonical")

        return {
            "url": url,
            "title": title,
            "description": description["content"].strip() if description and "content" in description.attrs else "",
            "canonical": canonical["href"].strip() if canonical and "href" in canonical.attrs else ""
        }

    except Exception as e:
        return {
            "url": url,
            "title": "",
            "description": "",
            "canonical": f"Erro: {str(e)}"
        }

def extrair_metatags(lista_urls, max_threads=30):
    resultados = []

    print(f"🔄 Extraindo metatags com até {max_threads} threads...")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futuros = {executor.submit(extrair_metadados, url): url for url in lista_urls}
        for i, future in enumerate(as_completed(futuros), 1):
            try:
                resultados.append(future.result())
            except Exception as e:
                resultados.append({
                    "url": futuros[future],
                    "title": "",
                    "description": "",
                    "canonical": f"Erro: {str(e)}"
                })

            if i % 100 == 0 or i == len(lista_urls):
                print(f"⏱️ {i}/{len(lista_urls)} metatags extraídas...")

    return resultados
