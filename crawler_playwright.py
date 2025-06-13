# crawler_playwright.py - Enterprise SEO Crawler com Playwright

import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from urllib.parse import urljoin, urlparse
from tqdm.asyncio import tqdm
import time
import os
import pickle
import warnings
from concurrent.futures import ThreadPoolExecutor
import json
from typing import List, Dict, Optional, Tuple
import logging

warnings.filterwarnings("ignore")

# ========================
# 🎯 Configurações Enterprise
# ========================

class PlaywrightConfig:
    """🔧 Configurações otimizadas para SEO enterprise"""
    
    # 🚀 Performance Settings
    BROWSER_POOL_SIZE = 3  # Número de browsers paralelos
    MAX_CONCURRENT_PAGES = 10  # Páginas simultâneas por browser
    PAGE_TIMEOUT = 30000  # 30s timeout (sites complexos)
    NAVIGATION_TIMEOUT = 20000  # 20s para navegação
    
    # 🎯 SEO Settings
    USER_AGENT = "Mozilla/5.0 (compatible; SEO-Analyzer/1.0; +https://seo-analyzer.com/bot)"
    VIEWPORT = {"width": 1366, "height": 768}  # Desktop padrão para SEO
    
    # 📊 Content Settings
    WAIT_FOR_NETWORK_IDLE = True  # Aguarda AJAX/lazy loading
    WAIT_FOR_FONTS = True  # Aguarda fontes (afeta rendering)
    COLLECT_CONSOLE_ERRORS = True  # Debug JS errors
    
    # 🔍 SEO Detection
    JS_FRAMEWORKS_PATTERNS = [
        'react', 'vue', 'angular', 'next.js', 'nuxt', 'gatsby',
        'spa', 'ajax', 'dynamic', 'client-side'
    ]

# ========================
# 🧠 Detecção Inteligente de Sites
# ========================

class SiteAnalyzer:
    """🔍 Detecta se site precisa de JavaScript para SEO"""
    
    @staticmethod
    async def needs_javascript(page: Page, url: str) -> Tuple[bool, str]:
        """
        🎯 Detecta se site precisa de JS para renderização completa
        
        Returns:
            (needs_js: bool, reason: str)
        """
        try:
            reasons = []
            
            # 1. 🔍 Detecta frameworks JS no HTML
            html_content = await page.content()
            
            # Patterns de frameworks
            js_patterns = [
                ('react', 'data-reactroot'),
                ('vue', 'v-'),
                ('angular', 'ng-'),
                ('next.js', '__NEXT_DATA__'),
                ('nuxt', '__NUXT__'),
                ('gatsby', '___gatsby'),
            ]
            
            for framework, pattern in js_patterns:
                if pattern in html_content.lower():
                    reasons.append(f"Framework {framework} detectado")
            
            # 2. 🎯 Verifica se title/meta são dinâmicos
            initial_title = await page.title()
            
            # Aguarda JS executar
            await page.wait_for_timeout(2000)
            await page.wait_for_load_state('networkidle', timeout=10000)
            
            final_title = await page.title()
            
            if initial_title != final_title:
                reasons.append(f"Title dinâmico: '{initial_title}' → '{final_title}'")
            
            # 3. 🔍 Detecta lazy loading / AJAX
            script_tags = await page.query_selector_all('script')
            has_ajax = False
            
            for script in script_tags:
                script_content = await script.get_attribute('src') or ''
                if any(term in script_content.lower() for term in ['ajax', 'fetch', 'axios', 'xhr']):
                    has_ajax = True
                    break
            
            if has_ajax:
                reasons.append("AJAX/Fetch detectado")
            
            # 4. 🎯 Verifica se há mais headings após JS
            initial_headings = await page.query_selector_all('h1, h2, h3, h4, h5, h6')
            
            # Simula scroll para trigger lazy loading
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
            
            final_headings = await page.query_selector_all('h1, h2, h3, h4, h5, h6')
            
            if len(final_headings) > len(initial_headings):
                reasons.append(f"Headings dinâmicos: {len(initial_headings)} → {len(final_headings)}")
            
            # 🎯 Decisão final
            needs_js = len(reasons) > 0
            reason_text = " | ".join(reasons) if reasons else "Site estático detectado"
            
            return needs_js, reason_text
            
        except Exception as e:
            return True, f"Erro na detecção, usando JS por segurança: {str(e)}"

# ========================
# 🚀 Browser Pool Manager
# ========================

class BrowserPool:
    """🎯 Gerencia pool de browsers para performance máxima"""
    
    def __init__(self, pool_size: int = 3):
        self.pool_size = pool_size
        self.browsers: List[Browser] = []
        self.contexts: List[BrowserContext] = []
        self.semaphore = asyncio.Semaphore(pool_size * PlaywrightConfig.MAX_CONCURRENT_PAGES)
        
    async def initialize(self, playwright):
        """🚀 Inicializa pool de browsers"""
        print(f"🚀 Inicializando pool de {self.pool_size} browsers...")
        
        for i in range(self.pool_size):
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-images',  # 🚀 Otimização: não carrega imagens
                    '--disable-javascript-harmony-shipping',
                    '--disable-background-timer-throttling',
                    '--disable-renderer-backgrounding',
                    '--disable-backgrounding-occluded-windows',
                ]
            )
            
            context = await browser.new_context(
                user_agent=PlaywrightConfig.USER_AGENT,
                viewport=PlaywrightConfig.VIEWPORT,
                ignore_https_errors=True,
                java_script_enabled=True,
                accept_downloads=False,
            )
            
            self.browsers.append(browser)
            self.contexts.append(context)
        
        print(f"✅ Pool de browsers inicializado com sucesso!")
    
    async def get_page(self) -> Tuple[Page, int]:
        """🎯 Obtém página do pool com load balancing"""
        await self.semaphore.acquire()
        
        # Round-robin simples
        context_index = len([c for c in self.contexts if len(c.pages) > 0]) % len(self.contexts)
        context = self.contexts[context_index]
        
        page = await context.new_page()
        
        # 🎯 Configurações SEO específicas
        await page.set_extra_http_headers({
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        
        return page, context_index
    
    async def release_page(self, page: Page):
        """📤 Libera página de volta ao pool"""
        try:
            await page.close()
        except:
            pass
        finally:
            self.semaphore.release()
    
    async def close_all(self):
        """🔚 Fecha todos os browsers do pool"""
        print("🔚 Fechando pool de browsers...")
        for browser in self.browsers:
            try:
                await browser.close()
            except:
                pass

# ========================
# 🎯 Processador de URL Individual
# ========================

async def processar_url_playwright(
    url: str, 
    nivel: int, 
    dominio_base: str, 
    browser_pool: BrowserPool,
    config: PlaywrightConfig = PlaywrightConfig()
) -> Dict:
    """🎯 Processa uma URL com Playwright (enterprise-grade)"""
    
    page = None
    start_time = time.time()
    
    try:
        # 🚀 Obtém página do pool
        page, browser_index = await browser_pool.get_page()
        
        # 📊 Configurações de timeout
        page.set_default_timeout(config.PAGE_TIMEOUT)
        page.set_default_navigation_timeout(config.NAVIGATION_TIMEOUT)
        
        # 🎯 Coleta erros de console (debugging)
        console_errors = []
        if config.COLLECT_CONSOLE_ERRORS:
            page.on("console", lambda msg: 
                console_errors.append(msg.text) if msg.type == "error" else None
            )
        
        # 🚀 Navegação com otimizações
        response = await page.goto(
            url,
            wait_until='domcontentloaded',  # Mais rápido que 'load'
            timeout=config.NAVIGATION_TIMEOUT
        )
        
        # ⏱️ Aguarda estabilização (crítico para SEO)
        if config.WAIT_FOR_NETWORK_IDLE:
            try:
                await page.wait_for_load_state('networkidle', timeout=10000)
            except:
                pass  # Não bloqueia se timeout
        
        # 🎯 Aguarda fontes carregarem (afeta rendering)
        if config.WAIT_FOR_FONTS:
            try:
                await page.wait_for_function(
                    "document.fonts.status === 'loaded'",
                    timeout=5000
                )
            except:
                pass
        
        # 📊 Coleta dados SEO completos
        resultado = await extrair_dados_seo_completos(page, url, nivel)
        
        # 🔍 Detecta se site precisa de JS
        needs_js, js_reason = await SiteAnalyzer.needs_javascript(page, url)
        
        # ⚡ Performance metrics
        response_time = (time.time() - start_time) * 1000
        
        # 🎯 Dados de retorno (compatível com estrutura original)
        resultado.update({
            # 📊 Dados originais mantidos
            "url": url,
            "nivel": nivel,
            "status_code": response.status if response else None,
            "tipo_conteudo": response.headers.get("content-type", "unknown") if response else "unknown",
            
            # 🆕 Dados enterprise
            "response_time": round(response_time, 2),
            "browser_index": browser_index,
            "needs_javascript": needs_js,
            "js_detection_reason": js_reason,
            "console_errors": console_errors[:5],  # Máximo 5 erros
            "final_url": page.url,  # URL após redirects
            "redirected": page.url != url,
            
            # 🔍 Links encontrados
            "links_encontrados": await extrair_links_internos(page, dominio_base)
        })
        
        return resultado
        
    except Exception as e:
        # 📊 Estrutura de erro compatível
        return {
            "url": url,
            "nivel": nivel,
            "status_code": None,
            "tipo_conteudo": f"Erro Playwright: {str(e)}",
            "response_time": (time.time() - start_time) * 1000,
            "needs_javascript": True,
            "js_detection_reason": f"Erro: {str(e)}",
            "error_details": str(e)
        }
    
    finally:
        if page:
            await browser_pool.release_page(page)

# ========================
# 🎯 Extração de Dados SEO
# ========================

async def extrair_dados_seo_completos(page: Page, url: str, nivel: int) -> Dict:
    """🎯 Extrai todos os dados SEO da página renderizada"""
    
    try:
        # 🎯 Title (pós-JS)
        title = await page.title()
        
        # 📋 Meta description
        description_elem = await page.query_selector('meta[name="description"], meta[property="og:description"]')
        description = ""
        if description_elem:
            description = await description_elem.get_attribute('content') or ""
        
        # 🔗 Canonical
        canonical_elem = await page.query_selector('link[rel="canonical"]')
        canonical = ""
        if canonical_elem:
            canonical = await canonical_elem.get_attribute('href') or ""
        
        # 🏷️ Headings (pós-JS)
        headings_data = {}
        for i in range(1, 7):
            headings = await page.query_selector_all(f'h{i}')
            headings_texts = []
            
            for heading in headings:
                text = await heading.inner_text()
                if text.strip():
                    headings_texts.append(text.strip())
            
            headings_data[f'h{i}'] = len(headings_texts)
            headings_data[f'h{i}_texts'] = headings_texts
        
        # 🎯 Meta tags adicionais
        og_title_elem = await page.query_selector('meta[property="og:title"]')
        og_title = ""
        if og_title_elem:
            og_title = await og_title_elem.get_attribute('content') or ""
        
        twitter_title_elem = await page.query_selector('meta[name="twitter:title"]')
        twitter_title = ""
        if twitter_title_elem:
            twitter_title = await twitter_title_elem.get_attribute('content') or ""
        
        # 📊 Structured Data (JSON-LD)
        structured_data = []
        json_ld_scripts = await page.query_selector_all('script[type="application/ld+json"]')
        for script in json_ld_scripts:
            try:
                content = await script.inner_text()
                structured_data.append(json.loads(content))
            except:
                pass
        
        return {
            # 📋 Dados básicos (compatibilidade)
            "title": title,
            "description": description,
            "canonical": canonical,
            
            # 🏷️ Headings
            **headings_data,
            
            # 🆕 Meta tags avançadas
            "og_title": og_title,
            "twitter_title": twitter_title,
            
            # 📊 Dados estruturados
            "structured_data_count": len(structured_data),
            "has_structured_data": len(structured_data) > 0,
            
            # 🎯 Dados para validação de headings
            "h1_texts": headings_data.get('h1_texts', []),
            "h2_texts": headings_data.get('h2_texts', []),
            "h1_ausente": headings_data.get('h1', 0) == 0,
            "h2_ausente": headings_data.get('h2', 0) == 0,
        }
        
    except Exception as e:
        return {
            "title": "",
            "description": "",
            "canonical": f"Erro extraindo dados: {str(e)}",
            "h1": 0, "h2": 0, "h3": 0, "h4": 0, "h5": 0, "h6": 0,
            "h1_texts": [], "h2_texts": [],
            "h1_ausente": True, "h2_ausente": True
        }

# ========================
# 🔗 Extração de Links
# ========================

async def extrair_links_internos(page: Page, dominio_base: str) -> List[str]:
    """🔗 Extrai links internos da página (pós-JS)"""
    
    try:
        links = []
        
        # 🔍 Busca todos os links
        link_elements = await page.query_selector_all('a[href]')
        
        for link_elem in link_elements:
            href = await link_elem.get_attribute('href')
            
            if href and not href.startswith(('mailto:', 'tel:', 'javascript:', '#')):
                # Resolve URL relativa
                full_url = urljoin(page.url, href)
                parsed = urlparse(full_url)
                
                # Só links internos
                if parsed.netloc == dominio_base:
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    if parsed.query:
                        clean_url += f"?{parsed.query}"
                    
                    links.append(clean_url.rstrip('/'))
        
        return list(set(links))  # Remove duplicatas
        
    except Exception as e:
        return []

# ========================
# 💾 Cache System
# ========================

def salvar_cache_playwright(nome_arquivo: str, dados: List[Dict]):
    """💾 Salva cache compatível"""
    with open(nome_arquivo, 'wb') as f:
        pickle.dump(dados, f)

def carregar_cache_playwright(nome_arquivo: str) -> Optional[List[Dict]]:
    """📂 Carrega cache existente"""
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, 'rb') as f:
            return pickle.load(f)
    return None

def excluir_cache_playwright(nome_arquivo: str):
    """🗑️ Remove cache antigo"""
    if os.path.exists(nome_arquivo):
        os.remove(nome_arquivo)
        print(f"🧹 Cache Playwright removido: {nome_arquivo}")

# ========================
# 🚀 Função Principal Enterprise
# ========================

async def rastrear_playwright_profundo(
    url_inicial: str,
    max_urls: int = 1000,
    max_depth: int = 3,
    forcar_reindexacao: bool = False,
    browser_pool_size: int = 3
) -> List[Dict]:
    """
    🚀 Crawler Playwright Enterprise para SEO
    
    Args:
        url_inicial: URL inicial para crawling
        max_urls: Máximo de URLs para processar
        max_depth: Profundidade máxima
        forcar_reindexacao: Ignora cache existente
        browser_pool_size: Número de browsers no pool
    
    Returns:
        Lista de dicionários com dados SEO completos
    """
    
    # 💾 Sistema de cache
    cache_path = f".cache_{urlparse(url_inicial).netloc.replace('.', '_')}_playwright.pkl"
    
    if forcar_reindexacao:
        excluir_cache_playwright(cache_path)
    
    if not forcar_reindexacao:
        cache = carregar_cache_playwright(cache_path)
        if cache:
            print(f"♻️ Cache Playwright encontrado: {cache_path}")
            return cache
    
    print(f"🚀 Crawler Playwright Enterprise iniciado!")
    print(f"📊 Configuração: {max_urls} URLs, profundidade {max_depth}, {browser_pool_size} browsers")
    print(f"🎯 Modo: Renderização completa com JavaScript")
    
    resultados = []
    dominio_base = urlparse(url_inicial).netloc
    visitadas = set()
    fila = [(url_inicial, 0)]
    
    async with async_playwright() as playwright:
        # 🚀 Inicializa pool de browsers
        browser_pool = BrowserPool(browser_pool_size)
        await browser_pool.initialize(playwright)
        
        try:
            # 📊 Progress bar assíncrona
            with tqdm(total=max_urls, desc="🎯 Playwright SEO Crawling") as pbar:
                
                # 🔄 Processa URLs em lotes
                while fila and len(resultados) < max_urls:
                    # 📦 Cria lote de URLs para processar
                    lote_atual = []
                    tasks = []
                    
                    while (fila and 
                           len(lote_atual) < PlaywrightConfig.MAX_CONCURRENT_PAGES and 
                           len(resultados) + len(lote_atual) < max_urls):
                        
                        url_atual, nivel = fila.pop(0)
                        
                        if url_atual not in visitadas and nivel <= max_depth:
                            lote_atual.append((url_atual, nivel))
                            visitadas.add(url_atual)
                            
                            # 🚀 Cria task assíncrona
                            task = processar_url_playwright(
                                url_atual, nivel, dominio_base, browser_pool
                            )
                            tasks.append(task)
                    
                    # ⚡ Executa lote em paralelo
                    if tasks:
                        lote_resultados = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        for resultado in lote_resultados:
                            if isinstance(resultado, dict):
                                resultados.append(resultado)
                                
                                # 🔗 Adiciona links encontrados à fila
                                if (resultado.get("links_encontrados") and 
                                    resultado["nivel"] < max_depth):
                                    for link in resultado["links_encontrados"]:
                                        if (link not in visitadas and 
                                            len(visitadas) + len(fila) < max_urls):
                                            fila.append((link, resultado["nivel"] + 1))
                                
                                pbar.update(1)
                            else:
                                pbar.update(1)
        
        finally:
            # 🔚 Cleanup
            await browser_pool.close_all()
    
    # 💾 Salva cache
    salvar_cache_playwright(cache_path, resultados)
    
    # 📊 Estatísticas finais
    js_sites = len([r for r in resultados if r.get("needs_javascript", False)])
    print(f"\n✅ Crawling Playwright concluído!")
    print(f"📊 {len(resultados)} URLs processadas")
    print(f"🤖 {js_sites} sites precisaram de JavaScript ({js_sites/len(resultados)*100:.1f}%)")
    
    return resultados

# ========================
# 🧪 Função de Teste
# ========================

async def testar_playwright_crawler():
    """🧪 Testa o crawler Playwright"""
    print("🧪 Testando Crawler Playwright...")
    
    # URLs de teste
    urls_teste = [
        "https://example.com",  # Site simples
        # Adicione URLs de teste aqui
    ]
    
    for url in urls_teste:
        print(f"\n🔍 Testando: {url}")
        resultado = await rastrear_playwright_profundo(url, max_urls=5, max_depth=1)
        
        for item in resultado:
            print(f"  ✅ {item['url']}")
            print(f"     Title: {item.get('title', 'N/A')}")
            print(f"     Status: {item.get('status_code', 'N/A')}")
            print(f"     JS Needed: {item.get('needs_javascript', 'N/A')}")

if __name__ == "__main__":
    # 🚀 Execução de teste
    asyncio.run(testar_playwright_crawler())