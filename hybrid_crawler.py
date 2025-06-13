# hybrid_crawler.py - Sistema Híbrido Simples e Funcional

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import warnings
import os
import pickle

warnings.filterwarnings("ignore")

# 🎯 Detecta se Playwright está disponível
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
    print("✅ Playwright detectado - Modo híbrido ativo")
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️ Playwright não instalado - Modo Requests apenas")

# ========================
# 🚀 Sessão Otimizada (Requests)
# ========================
class FastSession:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        })
    
    def get(self, url, **kwargs):
        kwargs.setdefault('timeout', 10)
        kwargs.setdefault('verify', False)
        return self.session.get(url, **kwargs)

fast_session = FastSession()

# ========================
# 🧠 Detecção Inteligente de JS
# ========================
def detectar_precisa_js(url):
    """🔍 Testa se URL precisa de JavaScript para renderização completa"""
    try:
        # 1. Pega versão sem JS (Requests)
        response = fast_session.get(url)
        soup_requests = BeautifulSoup(response.text, 'lxml')
        
        title_requests = soup_requests.title.string.strip() if soup_requests.title else ""
        h1_requests = len(soup_requests.find_all('h1'))
        headings_requests = len(soup_requests.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']))
        
        # 2. Detecta padrões que indicam JS necessário
        html_content = response.text.lower()
        
        # Framework indicators
        js_frameworks = ['react', 'angular', 'vue', 'next.js', 'nuxt']
        has_framework = any(fw in html_content for fw in js_frameworks)
        
        # SPA indicators
        spa_indicators = ['ng-app', 'data-reactroot', 'v-app', '__next', '__nuxt']
        has_spa = any(indicator in html_content for indicator in spa_indicators)
        
        # Empty title/content indicators
        has_empty_title = title_requests in ['', 'Loading...', 'App', 'React App', 'Vue App']
        has_few_headings = headings_requests < 2
        
        # 3. Score de necessidade de JS
        js_score = 0
        reasons = []
        
        if has_framework:
            js_score += 30
            reasons.append("Framework JS detectado")
        
        if has_spa:
            js_score += 25
            reasons.append("SPA detectado")
        
        if has_empty_title:
            js_score += 20
            reasons.append("Title suspeito")
        
        if has_few_headings:
            js_score += 15
            reasons.append("Poucos headings")
        
        # Se há muitos scripts
        script_count = len(soup_requests.find_all('script'))
        if script_count > 10:
            js_score += 10
            reasons.append(f"{script_count} scripts")
        
        # 4. Decisão
        needs_js = js_score >= 30  # Threshold
        reason = " | ".join(reasons) if reasons else "Site estático"
        
        return needs_js, js_score, reason
        
    except Exception as e:
        return True, 100, f"Erro na detecção: {str(e)}"

# ========================
# 🎭 Processador Playwright Simples
# ========================
def processar_url_playwright(url, nivel, dominio_base):
    """🎭 Processa URL com Playwright (renderização JS)"""
    if not PLAYWRIGHT_AVAILABLE:
        raise Exception("Playwright não disponível")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Configurações básicas
            page.set_viewport_size({"width": 1366, "height": 768})
            page.set_extra_http_headers({
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
            })
            
            start_time = time.time()
            
            # Navega e aguarda
            response = page.goto(url, wait_until='domcontentloaded', timeout=20000)
            page.wait_for_load_state('networkidle', timeout=10000)
            
            response_time = (time.time() - start_time) * 1000
            
            # Extrai dados SEO
            title = page.title()
            
            # Meta description
            description_elem = page.query_selector('meta[name="description"]')
            description = description_elem.get_attribute('content') if description_elem else ""
            
            # Headings
            headings = {}
            for i in range(1, 7):
                count = len(page.query_selector_all(f'h{i}'))
                headings[f'h{i}'] = count
            
            # Links internos
            links = []
            link_elements = page.query_selector_all('a[href]')
            for link in link_elements:
                href = link.get_attribute('href')
                if href and not href.startswith(('mailto:', 'tel:', 'javascript:', '#')):
                    full_url = urljoin(url, href)
                    parsed = urlparse(full_url)
                    if parsed.netloc == dominio_base:
                        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                        if parsed.query:
                            clean_url += f"?{parsed.query}"
                        links.append(clean_url.rstrip('/'))
            
            browser.close()
            
            return {
                "url": url,
                "nivel": nivel,
                "status_code": response.status if response else None,
                "tipo_conteudo": response.headers.get("content-type", "unknown") if response else "unknown",
                "title": title,
                "description": description,
                **headings,
                "response_time": round(response_time, 2),
                "links_encontrados": list(set(links)),
                "crawler_method": "playwright"
            }
    
    except Exception as e:
        return {
            "url": url,
            "nivel": nivel,
            "status_code": None,
            "tipo_conteudo": f"Erro Playwright: {str(e)}",
            "crawler_method": "playwright_error"
        }

# ========================
# ⚡ Processador Requests Simples
# ========================
def processar_url_requests(url, nivel, dominio_base):
    """⚡ Processa URL com Requests (rápido)"""
    try:
        start_time = time.time()
        response = fast_session.get(url)
        response_time = (time.time() - start_time) * 1000
        
        # Extrai dados básicos
        soup = BeautifulSoup(response.text, 'lxml')
        
        title = soup.title.string.strip() if soup.title else ""
        
        description_elem = soup.find("meta", attrs={"name": "description"})
        description = description_elem["content"].strip() if description_elem and "content" in description_elem.attrs else ""
        
        # Headings
        headings = {}
        for i in range(1, 7):
            headings[f'h{i}'] = len(soup.find_all(f'h{i}'))
        
        # Links internos
        links = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            if not href.startswith(('mailto:', 'tel:', 'javascript:', '#')):
                full_url = urljoin(url, href)
                parsed = urlparse(full_url)
                if parsed.netloc == dominio_base:
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    if parsed.query:
                        clean_url += f"?{parsed.query}"
                    links.append(clean_url.rstrip('/'))
        
        return {
            "url": url,
            "nivel": nivel,
            "status_code": response.status_code,
            "tipo_conteudo": response.headers.get("Content-Type", "unknown"),
            "title": title,
            "description": description,
            **headings,
            "response_time": round(response_time, 2),
            "links_encontrados": list(set(links)),
            "crawler_method": "requests"
        }
        
    except Exception as e:
        return {
            "url": url,
            "nivel": nivel,
            "status_code": None,
            "tipo_conteudo": f"Erro Requests: {str(e)}",
            "crawler_method": "requests_error"
        }

# ========================
# 🎯 Sistema Híbrido Principal
# ========================
def rastrear_hibrido_inteligente(
    url_inicial, 
    max_urls=1000, 
    max_depth=3, 
    forcar_reindexacao=False,
    modo="auto"  # auto, requests, playwright
):
    """
    🎯 Crawler híbrido inteligente
    
    modo:
    - 'auto': Detecta automaticamente se precisa JS
    - 'requests': Força uso do Requests (rápido)
    - 'playwright': Força uso do Playwright (completo)
    """
    
    # Cache
    dominio = urlparse(url_inicial).netloc.replace('.', '_')
    cache_path = f".cache_{dominio}_hybrid.pkl"
    
    if forcar_reindexacao and os.path.exists(cache_path):
        os.remove(cache_path)
        print(f"🧹 Cache híbrido removido")
    
    if not forcar_reindexacao and os.path.exists(cache_path):
        with open(cache_path, 'rb') as f:
            cache = pickle.load(f)
        print(f"♻️ Cache híbrido carregado: {len(cache)} URLs")
        return cache
    
    print(f"🚀 Crawler Híbrido Inteligente iniciado!")
    print(f"📊 Configuração: {max_urls} URLs, profundidade {max_depth}, modo: {modo}")
    
    # Estruturas
    visitadas = set()
    fila = [(url_inicial, 0)]
    resultados = []
    dominio_base = urlparse(url_inicial).netloc
    
    # Modo automático: detecta necessidade de JS
    if modo == "auto" and PLAYWRIGHT_AVAILABLE:
        print(f"🧠 Detectando se site precisa de JavaScript...")
        needs_js, js_score, reason = detectar_precisa_js(url_inicial)
        if needs_js:
            print(f"🎭 DECISÃO: Usar Playwright (Score: {js_score}/100)")
            print(f"📝 Razão: {reason}")
            modo_final = "playwright"
        else:
            print(f"⚡ DECISÃO: Usar Requests (Score: {js_score}/100)")
            print(f"📝 Razão: {reason}")
            modo_final = "requests"
    elif modo == "auto" and not PLAYWRIGHT_AVAILABLE:
        print(f"⚡ Playwright não disponível - usando Requests")
        modo_final = "requests"
    else:
        modo_final = modo
    
    # Escolhe processador
    if modo_final == "playwright" and PLAYWRIGHT_AVAILABLE:
        processar_func = processar_url_playwright
        print(f"🎭 Usando Playwright para renderização completa")
    else:
        processar_func = processar_url_requests
        print(f"⚡ Usando Requests para máxima velocidade")
    
    # Crawling
    with tqdm(total=max_urls, desc=f"🔍 Crawling ({modo_final})") as pbar:
        while fila and len(resultados) < max_urls:
            url_atual, nivel = fila.pop(0)
            
            if url_atual not in visitadas and nivel <= max_depth:
                visitadas.add(url_atual)
                
                # Processa URL
                resultado = processar_func(url_atual, nivel, dominio_base)
                resultados.append(resultado)
                
                # Adiciona links à fila
                if resultado.get("links_encontrados") and nivel < max_depth:
                    for link in resultado["links_encontrados"]:
                        if link not in visitadas and len(visitadas) + len(fila) < max_urls:
                            fila.append((link, nivel + 1))
                
                pbar.update(1)
    
    # Salva cache
    with open(cache_path, 'wb') as f:
        pickle.dump(resultados, f)
    
    # Estatísticas
    playwright_count = len([r for r in resultados if r.get("crawler_method") == "playwright"])
    requests_count = len([r for r in resultados if r.get("crawler_method") == "requests"])
    
    print(f"\n✅ Crawling híbrido concluído!")
    print(f"📊 {len(resultados)} URLs processadas")
    print(f"🎭 Playwright: {playwright_count} URLs")
    print(f"⚡ Requests: {requests_count} URLs")
    
    return resultados

# ========================
# 🧪 Função de Teste
# ========================
def testar_sistema_hibrido():
    """🧪 Testa o sistema híbrido"""
    urls_teste = [
        "https://example.com",
        "https://gndisul.com.br"
    ]
    
    for url in urls_teste:
        print(f"\n🔍 Testando: {url}")
        
        # Teste de detecção
        needs_js, score, reason = detectar_precisa_js(url)
        print(f"   🧠 Precisa JS: {needs_js} (Score: {score}/100)")
        print(f"   📝 Razão: {reason}")
        
        # Teste de crawling
        resultado = rastrear_hibrido_inteligente(url, max_urls=5, max_depth=1)
        print(f"   📊 URLs coletadas: {len(resultado)}")
        
        if resultado:
            print(f"   📋 Primeiro resultado:")
            primeiro = resultado[0]
            print(f"      Title: {primeiro.get('title', 'N/A')}")
            print(f"      Status: {primeiro.get('status_code', 'N/A')}")
            print(f"      Método: {primeiro.get('crawler_method', 'N/A')}")

if __name__ == "__main__":
    testar_sistema_hibrido()