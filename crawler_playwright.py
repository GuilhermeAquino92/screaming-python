# crawler_playwright.py - Pipeline LEAN: Title V5 Hardened + Browser Pool Inteligente

import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
from urllib.parse import urljoin, urlparse
from tqdm.asyncio import tqdm
import time
import os
import pickle
import warnings
from typing import List, Dict, Optional, Tuple

warnings.filterwarnings("ignore")

# 🎯 CONFIGURAÇÕES SIMPLES E EFICAZES
TITLE_TIMEOUT = 15000  # 15s - HARDENED para sites como GNDI
FALLBACK_TIMEOUT = 3000  # 3s adicional para JS assíncrono
PAGE_TIMEOUT = 30000
NAV_TIMEOUT = 20000
BROWSER_POOL_SIZE = 3

# ========================
# 🎭 BROWSER POOL SIMPLES E EFICAZ
# ========================

class BrowserPool:
    """🎭 Pool de browsers - simples e funcional"""
    
    def __init__(self, size: int = BROWSER_POOL_SIZE):
        self.size = size
        self.browsers: List[Browser] = []
        self.contexts: List[BrowserContext] = []
        self.semaphore = asyncio.Semaphore(size * 10)  # 10 páginas por browser
    
    async def initialize(self, playwright):
        """🚀 Inicializa pool"""
        print(f"🎭 Inicializando {self.size} browsers...")
        
        for i in range(self.size):
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-images',  # Performance
                    '--disable-web-security'
                ]
            )
            
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (compatible; SEO-Analyzer/1.0)",
                viewport={"width": 1366, "height": 768},
                ignore_https_errors=True
            )
            
            self.browsers.append(browser)
            self.contexts.append(context)
        
        print(f"✅ Pool inicializado: {self.size} browsers")
    
    async def get_page(self) -> Tuple[Page, int]:
        """🎯 Obtém página com load balancing"""
        await self.semaphore.acquire()
        
        # Load balancing simples
        context_loads = [len(ctx.pages) for ctx in self.contexts]
        ctx_index = context_loads.index(min(context_loads))
        
        page = await self.contexts[ctx_index].new_page()
        page.set_default_timeout(PAGE_TIMEOUT)
        page.set_default_navigation_timeout(NAV_TIMEOUT)
        
        return page, ctx_index
    
    async def release_page(self, page: Page):
        """📤 Libera página"""
        try:
            await page.close()
        except:
            pass
        finally:
            self.semaphore.release()
    
    async def close_all(self):
        """🔚 Fecha todos os browsers"""
        for browser in self.browsers:
            try:
                await browser.close()
            except:
                pass

# ========================
# 📝 TITLE EXTRACTOR V5 HARDENED
# ========================

async def extract_title_hardened(page: Page, url: str) -> str:
    """📝 Title V5 Hardened - resolve problemas de JS assíncrono"""
    
    try:
        # ESTRATÉGIA 1: Wait for function com timeout aumentado
        try:
            await page.wait_for_function(
                "document.title && document.title.trim().length > 0", 
                timeout=TITLE_TIMEOUT
            )
        except PlaywrightTimeoutError:
            pass
        
        # ESTRATÉGIA 2: Title API
        title = await page.title()
        
        # ESTRATÉGIA 3: DOM direto se vazio
        if not title or title.strip() == "":
            try:
                title_elem = await page.query_selector('title')
                if title_elem:
                    title = await title_elem.inner_text()
            except:
                pass
        
        # ESTRATÉGIA 4: Fallback agressivo para SPAs
        if not title or title.strip() == "" or title.lower() in ['loading', 'carregando', 'app']:
            await page.wait_for_timeout(FALLBACK_TIMEOUT)
            title = await page.title()
            
            if not title or title.strip() == "":
                title = await page.evaluate("document.title")
        
        # ESTRATÉGIA 5: Title alternativo via H1
        if not title or title.strip() == "":
            try:
                h1_elem = await page.query_selector('h1')
                if h1_elem:
                    h1_text = await h1_elem.inner_text()
                    if h1_text and len(h1_text.strip()) > 3:
                        title = h1_text.strip()
            except:
                pass
        
        # Limpa title
        title = title.strip() if title else ""
        
        # Blacklist básica
        blacklist = ['loading', 'carregando', 'app', 'react app', 'vue app', 'angular app']
        if title.lower() in blacklist:
            title = ""
        
        return title
        
    except Exception as e:
        print(f"   ❌ Erro extraindo title de {url}: {e}")
        return ""

# ========================
# 📊 SEO DATA EXTRACTOR SIMPLES
# ========================

async def extract_seo_data(page: Page, url: str) -> Dict:
    """📊 Extrai dados SEO essenciais - sem over-engineering"""
    
    try:
        # Meta description
        desc_elem = await page.query_selector('meta[name="description"]')
        description = ""
        if desc_elem:
            description = await desc_elem.get_attribute('content') or ""
        
        # Canonical
        canonical_elem = await page.query_selector('link[rel="canonical"]')
        canonical = ""
        if canonical_elem:
            canonical = await canonical_elem.get_attribute('href') or ""
        
        # Headings (simples e eficaz)
        headings_data = {}
        for i in range(1, 7):
            headings = await page.query_selector_all(f'h{i}')
            texts = []
            for h in headings:
                text = await h.inner_text()
                if text.strip():
                    texts.append(text.strip())
            
            headings_data[f'h{i}'] = len(texts)
            headings_data[f'h{i}_texts'] = texts
        
        # Open Graph básico
        og_title_elem = await page.query_selector('meta[property="og:title"]')
        og_title = ""
        if og_title_elem:
            og_title = await og_title_elem.get_attribute('content') or ""
        
        return {
            'description': description.strip(),
            'canonical': canonical.strip(),
            'og_title': og_title.strip(),
            **headings_data,
            'h1_ausente': headings_data.get('h1', 0) == 0,
            'h2_ausente': headings_data.get('h2', 0) == 0
        }
        
    except Exception as e:
        print(f"   ❌ Erro extraindo SEO data de {url}: {e}")
        return {'description': '', 'canonical': '', 'og_title': '', 'h1_ausente': True, 'h2_ausente': True}

# ========================
# 🔗 LINK EXTRACTOR SIMPLES
# ========================

async def extract_links(page: Page, domain: str) -> List[str]:
    """🔗 Extrai links internos - simples e eficaz"""
    
    try:
        links = await page.evaluate(f"""
            () => {{
                const links = [];
                document.querySelectorAll('a[href]').forEach(link => {{
                    const href = link.getAttribute('href');
                    if (href && !href.startsWith('#') && !href.startsWith('mailto:') && !href.startsWith('tel:')) {{
                        let fullUrl;
                        if (href.startsWith('/')) {{
                            fullUrl = window.location.origin + href;
                        }} else if (href.startsWith('http')) {{
                            fullUrl = href;
                        }} else {{
                            fullUrl = new URL(href, window.location.href).href;
                        }}
                        
                        if (fullUrl.includes('{domain}')) {{
                            links.push(fullUrl.split('#')[0].split('?')[0]);
                        }}
                    }}
                }});
                return [...new Set(links)];
            }}
        """)
        
        return links
        
    except Exception as e:
        return []

# ========================
# 🧠 SITE ANALYZER SIMPLES
# ========================

async def analyze_site_simple(page: Page, url: str) -> Dict:
    """🧠 Análise simples: só detecta se precisa de JS"""
    
    try:
        # Detecta frameworks JS básicos
        html_content = await page.content()
        html_lower = html_content.lower()
        
        js_frameworks = ['react', 'vue', 'angular', 'next.js']
        detected_framework = None
        
        for fw in js_frameworks:
            if fw in html_lower:
                detected_framework = fw
                break
        
        # Title dinâmico?
        initial_title = await page.title()
        await page.wait_for_timeout(1000)
        final_title = await page.title()
        
        title_dynamic = initial_title != final_title
        needs_js = bool(detected_framework) or title_dynamic
        
        reason = ""
        if detected_framework:
            reason = f"Framework {detected_framework} detectado"
        elif title_dynamic:
            reason = "Title renderizado dinamicamente"
        else:
            reason = "Site estático"
        
        return {
            'needs_javascript': needs_js,
            'js_reason': reason,
            'framework_detected': detected_framework or 'none'
        }
        
    except Exception as e:
        return {
            'needs_javascript': True,
            'js_reason': f"Erro na análise: {str(e)}",
            'framework_detected': 'unknown'
        }

# ========================
# 🎯 PROCESSADOR PRINCIPAL
# ========================

async def process_url_lean(url: str, nivel: int, domain: str, browser_pool: BrowserPool) -> Dict:
    """🎯 Processa URL com pipeline limpo"""
    
    page = None
    start_time = time.time()
    
    try:
        # 1. Obtém página
        page, browser_index = await browser_pool.get_page()
        
        # 2. Navega
        response = await page.goto(url, wait_until='domcontentloaded', timeout=NAV_TIMEOUT)
        
        # 3. Aguarda estabilização
        try:
            await page.wait_for_load_state('networkidle', timeout=8000)
        except:
            pass
        
        # 4. PIPELINE DE EXTRAÇÃO
        title = await extract_title_hardened(page, url)
        seo_data = await extract_seo_data(page, url)
        site_analysis = await analyze_site_simple(page, url)
        links = await extract_links(page, domain)
        
        # 5. Resultado consolidado
        processing_time = round((time.time() - start_time) * 1000, 2)
        
        return {
            # Dados básicos
            'url': url,
            'nivel': nivel,
            'status_code_http': response.status if response else None,
            'tipo_conteudo': response.headers.get('content-type', 'unknown') if response else 'unknown',
            'final_url': page.url,
            'redirected': page.url != url,
            
            # Title V5 Hardened
            'title': title,
            
            # SEO Data
            **seo_data,
            
            # Site Analysis
            'needs_javascript': site_analysis['needs_javascript'],
            'js_detection_reason': site_analysis['js_reason'],
            'framework_detected': site_analysis['framework_detected'],
            
            # Links e performance
            'links_encontrados': links,
            'response_time': processing_time,
            'browser_index': browser_index,
            
            # Metadados
            'crawler_version': 'lean_v1.0',
            'extraction_timestamp': time.time()
        }
        
    except Exception as e:
        return {
            'url': url,
            'nivel': nivel,
            'status_code_http': None,
            'tipo_conteudo': f'Erro: {str(e)}',
            'title': '',
            'error': str(e),
            'response_time': round((time.time() - start_time) * 1000, 2)
        }
    
    finally:
        if page:
            await browser_pool.release_page(page)

# ========================
# 📦 URL MANAGER SIMPLES
# ========================

class SimpleURLManager:
    """📦 Gerenciador de URLs - simples e eficaz"""
    
    def __init__(self, domain: str, max_urls: int = 1000):
        self.domain = domain
        self.max_urls = max_urls
        self.visited = set()
        self.queue = [(0, "")]  # (nivel, url) - será preenchido na inicialização
        self.discovered = 0
    
    def add_url(self, url: str, nivel: int):
        """➕ Adiciona URL se válida"""
        if (url not in self.visited and 
            len(self.visited) + len(self.queue) < self.max_urls and
            self.domain in url):
            self.queue.append((nivel, url))
            return True
        return False
    
    def add_urls_batch(self, urls: List[str], nivel: int):
        """📦 Adiciona lote de URLs"""
        added = 0
        for url in urls:
            if self.add_url(url, nivel):
                added += 1
        self.discovered += added
        return added
    
    def get_next_url(self) -> Optional[Tuple[str, int]]:
        """🔄 Obtém próxima URL"""
        if self.queue:
            nivel, url = self.queue.pop(0)
            if url:  # Primeira iteração tem URL vazia
                self.visited.add(url)
                return url, nivel
        return None
    
    def get_stats(self) -> Dict:
        """📊 Estatísticas simples"""
        return {
            'visited': len(self.visited),
            'queue': len(self.queue),
            'discovered': self.discovered
        }

# ========================
# 💾 CACHE SIMPLES
# ========================

def save_cache(filepath: str, data: List[Dict]):
    """💾 Salva cache"""
    with open(filepath, 'wb') as f:
        pickle.dump(data, f)

def load_cache(filepath: str) -> Optional[List[Dict]]:
    """📂 Carrega cache"""
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)
    return None

def delete_cache(filepath: str):
    """🗑️ Remove cache"""
    if os.path.exists(filepath):
        os.remove(filepath)

# ========================
# 🚀 FUNÇÃO PRINCIPAL LEAN
# ========================

async def rastrear_playwright_profundo(
    url_inicial: str,
    max_urls: int = 1000,
    max_depth: int = 3,
    forcar_reindexacao: bool = False,
    browser_pool_size: int = BROWSER_POOL_SIZE,
    perfil_seo: str = 'blog'
) -> List[Dict]:
    """🚀 Crawler Playwright LEAN - Title V5 Hardened + Pipeline Simples"""
    
    # Cache
    domain = urlparse(url_inicial).netloc.replace('.', '_')
    cache_path = f".cache_{domain}_playwright_lean.pkl"
    
    if forcar_reindexacao:
        delete_cache(cache_path)
    
    if not forcar_reindexacao:
        cached = load_cache(cache_path)
        if cached:
            print(f"♻️ Cache encontrado: {len(cached)} URLs")
            return cached
    
    print(f"🚀 Crawler Playwright LEAN iniciado!")
    print(f"📊 Config: {max_urls} URLs, profundidade {max_depth}, {browser_pool_size} browsers")
    print(f"🔧 Title V5 Hardened: timeout {TITLE_TIMEOUT/1000}s + fallback {FALLBACK_TIMEOUT/1000}s")
    
    # Inicialização
    domain_clean = urlparse(url_inicial).netloc
    url_manager = SimpleURLManager(domain_clean, max_urls)
    url_manager.add_url(url_inicial, 0)
    
    results = []
    
    async with async_playwright() as playwright:
        browser_pool = BrowserPool(browser_pool_size)
        await browser_pool.initialize(playwright)
        
        try:
            with tqdm(total=max_urls, desc="🎯 Playwright LEAN Crawling") as pbar:
                
                while len(results) < max_urls:
                    # Obtém próxima URL
                    next_url = url_manager.get_next_url()
                    if not next_url:
                        break
                    
                    url, nivel = next_url
                    
                    # Processa URL
                    result = await process_url_lean(url, nivel, domain_clean, browser_pool)
                    
                    if result:
                        results.append(result)
                        pbar.update(1)
                        
                        # Adiciona links encontrados
                        if result.get('links_encontrados') and nivel < max_depth:
                            added = url_manager.add_urls_batch(
                                result['links_encontrados'], 
                                nivel + 1
                            )
                            
                            if added > 0 and len(results) % 50 == 0:
                                print(f"   🔗 {added} URLs adicionadas")
                    
                    # Log periódico
                    if len(results) % 50 == 0:
                        stats = url_manager.get_stats()
                        print(f"   📊 Progresso: {len(results)} URLs | Fila: {stats['queue']}")
        
        finally:
            await browser_pool.close_all()
    
    # Salva cache
    save_cache(cache_path, results)
    
    # Relatório final
    titles_captured = len([r for r in results if r.get('title', '').strip()])
    js_sites = len([r for r in results if r.get('needs_javascript', False)])
    
    print(f"\n📊 RELATÓRIO FINAL LEAN:")
    print(f"   URLs processadas: {len(results)}")
    print(f"   Titles capturados: {titles_captured}/{len(results)} ({titles_captured/len(results)*100:.1f}%)")
    print(f"   Sites com JS: {js_sites} ({js_sites/len(results)*100:.1f}%)")
    print(f"   Cache salvo: {cache_path}")
    
    return results

# ========================
# 🧪 TESTE RÁPIDO
# ========================

async def test_lean_crawler():
    """🧪 Teste do crawler lean"""
    
    print("🧪 Testando Crawler LEAN...")
    
    results = await rastrear_playwright_profundo(
        "https://gndisul.com.br",
        max_urls=10,
        max_depth=2
    )
    
    print(f"\n📊 Resultados: {len(results)} URLs")
    
    for i, result in enumerate(results[:3]):
        title = result.get('title', 'N/A')
        status = "✅" if title and title != 'N/A' else "❌"
        
        print(f"{i+1}. {status} {result['url']}")
        print(f"   Title: '{title[:50]}...'")
        print(f"   Status: {result.get('status_code_http', 'N/A')}")
        print(f"   JS: {result.get('needs_javascript', False)}")

if __name__ == "__main__":
    print("🚀 Crawler Playwright LEAN - Title V5 Hardened + Pipeline Simples")
    asyncio.run(test_lean_crawler())