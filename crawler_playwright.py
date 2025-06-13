# crawler_playwright.py - Enterprise SEO Crawler com URLManager SEO Integrado

import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
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

# 🆕 IMPORT DO URL MANAGER SEO
from url_manager_seo import URLManagerSEO

warnings.filterwarnings("ignore")

# ========================
# 🎯 Configurações Enterprise (MANTÉM SEU CÓDIGO ORIGINAL)
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
# 🧠 Detecção Inteligente de Sites (MANTÉM SEU CÓDIGO)
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
# 🚀 Browser Pool Manager (MANTÉM SEU CÓDIGO ORIGINAL)
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
# 🎯 Processador de URL Individual (SEU CÓDIGO + TITLE V5)
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
        try:
            response = await page.goto(
                url,
                wait_until='domcontentloaded',  # Mais rápido que 'load'
                timeout=config.NAVIGATION_TIMEOUT
            )
        except PlaywrightTimeoutError as e:
            print(f"⚠️ Timeout na navegação: {url}")
            response = None
        
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
            "status_code_http": response.status if response else None,
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
            "status_code_http": None,
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
# 🎯 Extração de Dados SEO (COM TITLE V5 INTEGRADO)
# ========================

async def extrair_dados_seo_completos(page: Page, url: str, nivel: int) -> Dict:
    """🎯 Extrai todos os dados SEO da página renderizada com Title V5"""
    
    try:
        # 🚀 TITLE V5 - Sistema semântico avançado
        title = await capturar_title_robusto_v5(page)
        
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
        print(f"❌ Erro extraindo dados SEO de {url}: {e}")
        return {
            "title": "",
            "description": "",
            "canonical": f"Erro extraindo dados: {str(e)}",
            "h1": 0, "h2": 0, "h3": 0, "h4": 0, "h5": 0, "h6": 0,
            "h1_texts": [], "h2_texts": [],
            "h1_ausente": True, "h2_ausente": True
        }

# ========================
# 🚀 TITLE V5 - Sistema Semântico Completo
# ========================

async def capturar_title_robusto_v5(page: Page) -> str:
    """🎯 Title Capture V5 - Sistema Semântico com Blacklist Inteligente"""
    
    try:
        # 1️⃣ CAPTURA INICIAL ROBUSTA
        title = await _capturar_title_basico(page)
        
        # 2️⃣ FILTRO SEMÂNTICO V5 - Blacklist de titles inúteis
        title_processado = _aplicar_filtro_semantico_v5(title)
        
        # 3️⃣ FALLBACK INTELIGENTE se title for inútil
        if _title_e_inutel(title_processado):
            title_alternativo = await _buscar_title_alternativo(page)
            if title_alternativo and not _title_e_inutel(title_alternativo):
                print(f"   🔄 Title alternativo usado: '{title_alternativo[:50]}...'")
                return title_alternativo
        
        # 🔍 LOG FINAL
        if title_processado:
            print(f"   ✅ Title V5 capturado: '{title_processado[:50]}{'...' if len(title_processado) > 50 else ''}'")
        else:
            print("   ❌ Title permanece vazio após V5")
        
        return title_processado
        
    except Exception as e:
        print(f"   ❌ Erro crítico Title V5: {e}")
        return ""

async def _capturar_title_basico(page: Page) -> str:
    """🎯 Captura básica robusta"""
    
    # Estratégia 1: Wait for function
    try:
        await page.wait_for_function(
            "document.title && document.title.trim().length > 0", 
            timeout=5000
        )
    except:
        pass
    
    # Estratégia 2: Title API
    title = await page.title()
    
    # Estratégia 3: DOM direto
    if not title or title.strip() == "":
        try:
            title_element = await page.query_selector('title')
            if title_element:
                title = await title_element.inner_text()
        except:
            pass
    
    # Estratégia 4: SPA wait + evaluate
    if not title or title.strip() == "" or title.lower() in ['loading', 'carregando', 'app']:
        try:
            await page.wait_for_timeout(2000)
            title = await page.title()
            
            if not title or title.strip() == "":
                title = await page.evaluate("document.title")
        except:
            pass
    
    return title.strip() if title else ""

def _aplicar_filtro_semantico_v5(title: str) -> str:
    """🎯 Filtro Semântico V5 - Remove titles inúteis renderizados"""
    
    if not title:
        return ""
    
    title_clean = title.strip()
    title_lower = title_clean.lower()
    
    # 🚫 BLACKLIST V5 - Titles inúteis pós-renderização
    blacklist_titles = {
        # Estados de loading
        'loading', 'carregando', 'loading...', 'carregando...',
        'aguarde', 'wait', 'please wait', 'por favor aguarde',
        
        # Apps genéricos
        'app', 'react app', 'vue app', 'angular app', 'next app',
        'nextjs app', 'nuxt app', 'gatsby app',
        
        # Placeholders técnicos
        'apisanitizer', 'sanitizer', 'api', 'index', 'main',
        'default', 'untitled', 'document', 'new document',
        
        # Estados de erro
        'error', 'erro', 'not found', 'página não encontrada',
        '404', '500', 'server error', 'erro do servidor',
        
        # CMS defaults
        'wordpress', 'wp', 'drupal', 'joomla', 'cms',
        'admin', 'dashboard', 'painel',
        
        # Desenvolvimento
        'localhost', 'development', 'dev', 'test', 'teste',
        'staging', 'beta', 'alpha', 'demo',
        
        # Genéricos extremos
        'page', 'página', 'site', 'website', 'web site',
        'home', 'início', 'inicial', 'principal'
    }
    
    # Verifica blacklist exata
    if title_lower in blacklist_titles:
        print(f"   🚫 Title blacklisted: '{title_clean}'")
        return ""
    
    # Verifica padrões suspeitos
    if len(title_clean) < 3:
        print(f"   🚫 Title muito curto: '{title_clean}'")
        return ""
    
    if not any(c.isalpha() for c in title_clean):
        print(f"   🚫 Title sem letras: '{title_clean}'")
        return ""
    
    palavras_suspeitas = ['undefined', 'null', 'nan', '[object object]', 'typeof']
    if any(palavra in title_lower for palavra in palavras_suspeitas):
        print(f"   🚫 Title com JS artifacts: '{title_clean}'")
        return ""
    
    if any(char in title_clean for char in ['/', '\\', 'http://', 'https://']):
        print(f"   🚫 Title parece URL: '{title_clean}'")
        return ""
    
    return title_clean

def _title_e_inutel(title: str) -> bool:
    """🔍 Verifica se title é inútil"""
    if not title or len(title.strip()) < 3:
        return True
    
    title_lower = title.lower().strip()
    
    titles_inuteis = [
        'loading', 'app', 'react app', 'vue app', 'angular app',
        'apisanitizer', 'sanitizer', 'default', 'untitled',
        'document', 'page', 'site', 'website', 'home'
    ]
    
    return title_lower in titles_inuteis

async def _buscar_title_alternativo(page: Page) -> str:
    """🔍 Busca titles alternativos quando o principal é inútil"""
    
    try:
        alternativas = []
        
        # 1. H1 principal como title
        h1_elem = await page.query_selector('h1')
        if h1_elem:
            h1_text = await h1_elem.inner_text()
            if h1_text and len(h1_text.strip()) > 3:
                alternativas.append(f"H1: {h1_text.strip()}")
        
        # 2. Meta og:title
        og_title_elem = await page.query_selector('meta[property="og:title"]')
        if og_title_elem:
            og_title = await og_title_elem.get_attribute('content')
            if og_title and len(og_title.strip()) > 3:
                alternativas.append(f"OG: {og_title.strip()}")
        
        # 3. Meta twitter:title
        twitter_title_elem = await page.query_selector('meta[name="twitter:title"]')
        if twitter_title_elem:
            twitter_title = await twitter_title_elem.get_attribute('content')
            if twitter_title and len(twitter_title.strip()) > 3:
                alternativas.append(f"Twitter: {twitter_title.strip()}")
        
        # 4. Primeiro heading não-vazio
        for i in range(2, 7):  # h2 até h6
            heading_elem = await page.query_selector(f'h{i}')
            if heading_elem:
                heading_text = await heading_elem.inner_text()
                if heading_text and len(heading_text.strip()) > 3:
                    alternativas.append(f"H{i}: {heading_text.strip()}")
                    break
        
        # 5. Retorna a melhor alternativa
        if alternativas:
            melhor = alternativas[0]
            tipo, texto = melhor.split(': ', 1)
            print(f"   🔄 Title alternativo encontrado via {tipo}")
            return texto
        
        return ""
        
    except Exception as e:
        print(f"   ⚠️ Erro buscando title alternativo: {e}")
        return ""

# ========================
# 🔗 Extração de Links (MANTÉM SEU CÓDIGO)
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
# 💾 Cache System (MANTÉM SEU CÓDIGO)
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
# 🚀 FUNÇÃO PRINCIPAL MODIFICADA COM URL MANAGER SEO
# ========================

async def rastrear_playwright_profundo(
    url_inicial: str,
    max_urls: int = 1000,
    max_depth: int = 3,
    forcar_reindexacao: bool = False,
    browser_pool_size: int = 3,
    perfil_seo: str = 'blog'  # 🆕 NOVO PARÂMETRO
) -> List[Dict]:
    """🚀 Crawler Playwright Enterprise com URLManager SEO"""
    
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
    print(f"🎯 Perfil SEO: {perfil_seo}")
    print(f"🎯 Modo: Renderização completa com JavaScript + URLManager SEO")
    
    # 🆕 INICIALIZA URL MANAGER SEO
    dominio_base = urlparse(url_inicial).netloc
    url_manager = URLManagerSEO(dominio_base, max_urls, perfil_seo)
    url_manager.adicionar_url(url_inicial, 0)
    
    resultados = []
    
    async with async_playwright() as playwright:
        # 🚀 Inicializa pool de browsers
        browser_pool = BrowserPool(browser_pool_size)
        await browser_pool.initialize(playwright)
        
        try:
            # 📊 Progress bar assíncrona
            with tqdm(total=max_urls, desc="🎯 Playwright SEO Crawling") as pbar:
                
                # 🔄 NOVA LÓGICA COM URL MANAGER SEO
                while True:
                    # Obtém próxima URL do manager
                    proxima = url_manager.obter_proxima_url()
                    if not proxima:
                        break
                    
                    url_atual, nivel = proxima
                    
                    # 📦 Cria lote de URLs para processar em paralelo
                    lote_atual = [(url_atual, nivel)]
                    tasks = []
                    
                    # Adiciona mais URLs ao lote se disponível
                    for _ in range(PlaywrightConfig.MAX_CONCURRENT_PAGES - 1):
                        proxima_extra = url_manager.obter_proxima_url()
                        if proxima_extra and len(resultados) + len(lote_atual) < max_urls:
                            lote_atual.append(proxima_extra)
                        else:
                            break
                    
                    # Cria tasks para o lote
                    for url_lote, nivel_lote in lote_atual:
                        task = processar_url_playwright(
                            url_lote, nivel_lote, dominio_base, browser_pool
                        )
                        tasks.append(task)
                    
                    # ⚡ Executa lote em paralelo
                    if tasks:
                        lote_resultados = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        for resultado in lote_resultados:
                            if isinstance(resultado, dict):
                                resultados.append(resultado)
                                
                                # 🔗 Adiciona links encontrados ao manager
                                if (resultado.get("links_encontrados") and 
                                    resultado["nivel"] < max_depth):
                                    adicionadas = url_manager.adicionar_lote_urls_seo(
                                        resultado["links_encontrados"], 
                                        resultado["nivel"] + 1, 
                                        resultado["url"]
                                    )
                                    
                                    if adicionadas > 0 and len(resultados) % 50 == 0:
                                        print(f"   🔗 {adicionadas} URLs adicionadas do nível {resultado['nivel']+1}")
                                
                                pbar.update(1)
                            else:
                                pbar.update(1)
                    
                    # Log periódico com estatísticas SEO
                    if len(resultados) % 50 == 0:
                        relatorio = url_manager.obter_relatorio_seo()
                        print(f"   📊 Progresso: {len(resultados)} URLs | Fila: {relatorio['urls_na_fila']}")
                        print(f"      Tipos: {relatorio['distribuicao_por_tipo']}")
        
        finally:
            # 🔚 Cleanup
            await browser_pool.close_all()
    
    # 💾 Salva cache
    salvar_cache_playwright(cache_path, resultados)
    
    # 📊 Relatório final SEO
    relatorio_final = url_manager.obter_relatorio_seo()
    js_sites = len([r for r in resultados if r.get("needs_javascript", False)])
    
    print(f"\n📊 RELATÓRIO PLAYWRIGHT SEO FINAL:")
    print(f"   Perfil usado: {relatorio_final['perfil_seo']}")
    print(f"   URLs processadas: {relatorio_final['urls_visitadas']}")
    print(f"   URLs descobertas: {relatorio_final['total_descobertas']}")
    print(f"   Distribuição: {relatorio_final['distribuicao_por_tipo']}")
    print(f"   Cobertura: {relatorio_final['cobertura_por_tipo']}")
    print(f"   🤖 Sites com JS: {js_sites} ({js_sites/len(resultados)*100:.1f}%)")
    
    return resultados

# ========================
# 🧪 Função de Teste
# ========================

async def testar_playwright_crawler():
    """🧪 Testa o crawler Playwright com URLManager SEO"""
    print("🧪 Testando Crawler Playwright Enterprise com URLManager SEO...")
    
    # URLs de teste
    urls_teste = [
        "https://example.com",  # Site simples
        "https://gndisul.com.br",  # Site real para teste
    ]
    
    for url in urls_teste:
        print(f"\n🔍 Testando: {url}")
        resultado = await rastrear_playwright_profundo(
            url, 
            max_urls=10, 
            max_depth=2, 
            perfil_seo='blog'
        )
        
        print(f"📊 Resultados obtidos: {len(resultado)}")
        
        if resultado:
            for i, item in enumerate(resultado[:3]):  # Mostra primeiros 3
                print(f"  {i+1}. ✅ {item['url']}")
                print(f"       Title: {item.get('title', 'N/A')[:60]}...")
                print(f"       Status: {item.get('status_code_http', 'N/A')}")
                print(f"       JS Needed: {item.get('needs_javascript', 'N/A')}")
                print(f"       Links: {len(item.get('links_encontrados', []))}")

# ========================
# 🎯 EXECUÇÃO COMPATÍVEL
# ========================

if __name__ == "__main__":
    print("🚀 Testando Crawler Playwright Enterprise v3.0 com URLManager SEO")
    print("🎯 Funcionalidades:")
    print("   ✅ Browser Pool otimizado")
    print("   ✅ SiteAnalyzer inteligente") 
    print("   ✅ Title V5 com blacklist semântica")
    print("   ✅ URLManager SEO com priorização")
    print("   ✅ Relatórios detalhados por tipo")
    
    # 🚀 Execução de teste
    asyncio.run(testar_playwright_crawler())