# crawler_selenium.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urljoin, urlparse
from tqdm import tqdm
import os
import pickle
import time

# ========================
# Utilitários de Cache
# ========================

def salvar_cache(nome_arquivo, dados):
    with open(nome_arquivo, 'wb') as f:
        pickle.dump(dados, f)

def carregar_cache(nome_arquivo):
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, 'rb') as f:
            return pickle.load(f)
    return None

def excluir_cache(nome_arquivo):
    if os.path.exists(nome_arquivo):
        os.remove(nome_arquivo)
        print(f"🧹 Cache antigo excluído: {nome_arquivo}")

# ========================
# Lógica de link
# ========================

def link_eh_util(href):
    if not href or href.startswith(('mailto:', 'tel:', 'javascript:', '#')):
        return False
    if any(href.lower().endswith(ext) for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.zip']):
        return False
    return True

# ========================
# Função principal
# ========================

def rastrear_selenium_profundo(url_inicial, max_urls=1000, max_depth=3, delay=2, forcar_reindexacao=False):
    cache_path = f".cache_{urlparse(url_inicial).netloc.replace('.', '_')}_selenium.pkl"

    if forcar_reindexacao:
        excluir_cache(cache_path)

    if not forcar_reindexacao:
        cache = carregar_cache(cache_path)
        if cache:
            print(f"♻️ Cache Selenium encontrado: {cache_path}")
            return cache

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    dominio_base = urlparse(url_inicial).netloc
    fila = [(url_inicial, 0)]
    visitadas = set()
    resultados = []

    with tqdm(total=max_urls, desc="🕷️ Rastreamento Selenium") as pbar:
        try:
            while fila and len(resultados) < max_urls:
                url_atual, nivel = fila.pop(0)

                if url_atual in visitadas or nivel > max_depth:
                    continue

                try:
                    driver.get(url_atual)
                    time.sleep(delay)
                    status_code = 200
                    tipo_conteudo = "text/html"

                    resultados.append({
                        "url": url_atual,
                        "nivel": nivel,
                        "status_code": status_code,
                        "tipo_conteudo": tipo_conteudo
                    })
                    pbar.update(1)
                    visitadas.add(url_atual)

                    elementos = driver.find_elements(By.TAG_NAME, "a")
                    for el in elementos:
                        href = el.get_attribute("href")
                        if link_eh_util(href):
                            href = href.split("#")[0].rstrip("/")
                            if urlparse(href).netloc == dominio_base and href not in visitadas:
                                fila.append((href, nivel + 1))

                except Exception as e:
                    resultados.append({
                        "url": url_atual,
                        "nivel": nivel,
                        "status_code": None,
                        "tipo_conteudo": f"Erro: {e}"
                    })
                    pbar.update(1)

        finally:
            driver.quit()

    salvar_cache(cache_path, resultados)
    return resultados
