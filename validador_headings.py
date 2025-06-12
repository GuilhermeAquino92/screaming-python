# validador_headings.py

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import warnings
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings("ignore")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9"
}

def validar_headings_em_url(url):
    try:
        response = requests.get(url, timeout=10, headers=HEADERS, verify=False)
        soup = BeautifulSoup(response.text, "lxml")

        headings = {f"h{i}": [] for i in range(1, 7)}
        for i in range(1, 7):
            for tag in soup.find_all(f"h{i}"):
                texto = tag.get_text(strip=True)
                if texto:
                    headings[f"h{i}"].append(texto)

        erros = []

        if not headings["h1"]:
            erros.append("H1 ausente")
        elif len(headings["h1"]) > 1:
            erros.append("H1 duplicado")

        if not headings["h2"]:
            erros.append("H2 ausente")

        ordem = []
        for i in range(1, 7):
            if headings[f"h{i}"]:
                ordem.append(i)

        if ordem != sorted(ordem):
            erros.append("Hierarquia invertida")

        return {
            "url": url,
            "h1": len(headings["h1"]),
            "h2": len(headings["h2"]),
            "h3": len(headings["h3"]),
            "h4": len(headings["h4"]),
            "h5": len(headings["h5"]),
            "h6": len(headings["h6"]),
            "problemas": "; ".join(erros) if erros else "OK"
        }

    except Exception as e:
        return {
            "url": url,
            "h1": 0,
            "h2": 0,
            "h3": 0,
            "h4": 0,
            "h5": 0,
            "h6": 0,
            "problemas": f"Erro: {str(e)}"
        }

def validar_headings(lista_urls, max_threads=30):
    print(f"🔄 Validando headings com até {max_threads} threads...")
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        resultados = list(tqdm(executor.map(validar_headings_em_url, lista_urls), total=len(lista_urls), desc="🧠 Headings"))
    return resultados
