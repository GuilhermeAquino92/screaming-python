# cache_manager.py

import os
import json
import hashlib
from datetime import datetime, timedelta

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _get_cache_file(domain):
    return os.path.join(CACHE_DIR, f"{domain}_cache.json")

def _calcular_hash_html(html):
    return hashlib.md5(html.encode("utf-8")).hexdigest()

def carregar_cache(domain):
    path = _get_cache_file(domain)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def salvar_cache(domain, cache_dict):
    path = _get_cache_file(domain)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache_dict, f, ensure_ascii=False, indent=2)

def precisa_reprocessar(url, html, cache_dict, expiracao_padrao=7):
    hash_atual = _calcular_hash_html(html)
    registro = cache_dict.get(url)

    if not registro:
        return True  # URL nova

    expiracao = 2 if any(p in url for p in ["/blog", "/noticia"]) else expiracao_padrao
    data_ultima = datetime.fromisoformat(registro["last_checked"])
    expirado = datetime.now() - data_ultima > timedelta(days=expiracao)
    conteudo_mudou = registro["hash"] != hash_atual

    return expirado or conteudo_mudou

def atualizar_cache(cache_dict, url, html):
    cache_dict[url] = {
        "hash": _calcular_hash_html(html),
        "last_checked": datetime.now().isoformat()
    }
