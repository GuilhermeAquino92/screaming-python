# http_inseguro.py

import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

def analisar_http_inseguro(url):
    resultados = []

    try:
        response = requests.get(url, timeout=10, verify=False)
        html = response.text
        soup = BeautifulSoup(html, "lxml")

        # 1. HTML bruto
        if "http://" in html:
            ocorrencias_html = re.findall(r'(http://[^"\s\)]+)', html)
            for ocorrencia in set(ocorrencias_html):
                resultados.append({
                    "url": url,
                    "tipo": "html",
                    "trecho": ocorrencia
                })

        # 2. CSS inline
        for tag in soup.find_all(True):
            style = tag.get("style")
            if style and "http://" in style:
                links_css = re.findall(r'(http://[^"\s\)]+)', style)
                for link in links_css:
                    resultados.append({
                        "url": url,
                        "tipo": "css_inline",
                        "trecho": link
                    })

        # 3. CSS em <style>
        for style_tag in soup.find_all("style"):
            css = style_tag.get_text()
            if "http://" in css:
                links_style = re.findall(r'(http://[^"\s\)]+)', css)
                for link in links_style:
                    resultados.append({
                        "url": url,
                        "tipo": "style_tag",
                        "trecho": link
                    })

    except Exception as e:
        resultados.append({
            "url": url,
            "tipo": "erro",
            "trecho": str(e)
        })

    return resultados

def extrair_http_inseguros(urls, max_threads=30):
    resultados = []

    print(f"🔄 Buscando http:// com até {max_threads} threads...")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futuros = {executor.submit(analisar_http_inseguro, url): url for url in urls}
        for i, future in enumerate(as_completed(futuros), 1):
            try:
                resultados.extend(future.result())
            except Exception as e:
                resultados.append({
                    "url": futuros[future],
                    "tipo": "erro",
                    "trecho": str(e)
                })

            if i % 100 == 0 or i == len(urls):
                print(f"⏱️ {i}/{len(urls)} URLs processadas...")

    return resultados
