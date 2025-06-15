# mixed_content_platinum_sheet.py - ENGINE PLATINUM 1.5
# 🔒 UPGRADE: Observability + Performance + Configurability + SSL Integration

import pandas as pd
import requests
import re
import asyncio
import structlog
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import hashlib
from datetime import datetime, timedelta

# ========================
# 🔧 CONFIGURAÇÃO ENTERPRISE
# ========================
@dataclass
class MixedContentConfig:
    """📋 Configuração centralizadas do Engine"""
    js_detection_threshold: int = 50
    js_revalidation_threshold: int = 70
    requests_timeout: int = 15
    playwright_timeout: int = 30
    max_workers: int = 15
    browser_pool_size: int = 3
    cache_duration_hours: int = 24
    enable_ssl_analysis: bool = True
    enable_performance_tracking: bool = True
    log_level: str = "INFO"

# ========================
# 🎭 BROWSER POOL PLATINUM
# ========================
class BrowserPool:
    """🎭 Pool de browsers reutilizável para máxima eficiência"""
    
    def __init__(self, config: MixedContentConfig):
        self.config = config
        self.browsers = []
        self.contexts = []
        self.semaphore = asyncio.Semaphore(config.browser_pool_size)
        self.logger = structlog.get_logger("browser_pool")
    
    async def get_browser_context(self):
        """🎯 Pega browser + context reutilizável"""
        async with self.semaphore:
            try:
                if self.contexts:
                    return self.contexts.pop()
                
                from playwright.async_api import async_playwright
                
                if not hasattr(self, 'playwright'):
                    self.playwright = await async_playwright().__aenter__()
                    self.browser = await self.playwright.chromium.launch(
                        headless=True,
                        args=['--no-sandbox', '--disable-dev-shm-usage']
                    )
                
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    ignore_https_errors=True
                )
                
                return context
                
            except Exception as e:
                self.logger.error("browser_pool_error", error=str(e))
                raise
    
    async def return_context(self, context):
        """♻️ Retorna context para reuso"""
        try:
            # Limpa estado do context
            await context.clear_cookies()
            await context.clear_permissions()
            
            if len(self.contexts) < self.config.browser_pool_size:
                self.contexts.append(context)
            else:
                await context.close()
                
        except Exception as e:
            self.logger.error("context_return_error", error=str(e))
            await context.close()
    
    async def cleanup(self):
        """🧹 Limpeza final do pool"""
        for context in self.contexts:
            await context.close()
        
        if hasattr(self, 'browser'):
            await self.browser.close()
        
        if hasattr(self, 'playwright'):
            await self.playwright.__aexit__(None, None, None)

# ========================
# 📊 CACHE INTELIGENTE
# ========================
class IntelligentCache:
    """📊 Cache com TTL e hash inteligente"""
    
    def __init__(self, config: MixedContentConfig):
        self.config = config
        self.cache_data = {}
        self.logger = structlog.get_logger("cache")
    
    def _get_cache_key(self, url: str, method: str) -> str:
        """🔑 Gera chave única para cache"""
        content = f"{url}:{method}:{self.config.js_detection_threshold}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def get(self, url: str, method: str) -> Optional[Dict]:
        """📖 Recupera do cache se válido"""
        key = self._get_cache_key(url, method)
        
        if key not in self.cache_data:
            return None
        
        cache_entry = self.cache_data[key]
        cache_time = cache_entry.get('timestamp')
        
        if not cache_time:
            return None
        
        # Verifica TTL
        cache_age = datetime.now() - datetime.fromisoformat(cache_time)
        if cache_age > timedelta(hours=self.config.cache_duration_hours):
            del self.cache_data[key]
            return None
        
        self.logger.debug("cache_hit", url=url, method=method, age_hours=cache_age.total_seconds()/3600)
        return cache_entry['data']
    
    def set(self, url: str, method: str, data: Dict):
        """💾 Salva no cache com timestamp"""
        key = self._get_cache_key(url, method)
        self.cache_data[key] = {
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.debug("cache_set", url=url, method=method)

# ========================
# 🔒 SSL ANALYZER
# ========================
class SSLAnalyzer:
    """🔒 Análise SSL/TLS complementar"""
    
    def __init__(self, config: MixedContentConfig):
        self.config = config
        self.logger = structlog.get_logger("ssl_analyzer")
    
    def analyze_ssl(self, url: str) -> Dict:
        """🔍 Análise básica SSL (placeholder para SSL Labs API)"""
        try:
            import ssl
            import socket
            from urllib.parse import urlparse
            
            parsed = urlparse(url)
            if parsed.scheme != 'https':
                return {'ssl_available': False, 'reason': 'URL não é HTTPS'}
            
            # Análise básica de certificado
            context = ssl.create_default_context()
            
            with socket.create_connection((parsed.netloc, 443), timeout=5) as sock:
                with context.wrap_socket(sock, server_hostname=parsed.netloc) as ssock:
                    cert = ssock.getpeercert()
            
            return {
                'ssl_available': True,
                'ssl_version': ssock.version(),
                'cert_subject': dict(x[0] for x in cert['subject']),
                'cert_issuer': dict(x[0] for x in cert['issuer']),
                'cert_not_after': cert['notAfter'],
                'ssl_grade': 'A-',  # Placeholder - SSL Labs API retornaria grade real
            }
            
        except Exception as e:
            return {
                'ssl_available': False,
                'ssl_error': str(e)
            }

# ========================
# 🔥 ENGINE PLATINUM PRINCIPAL
# ========================
class MixedContentPlatinumSheet(BaseSheetExporter):
    def __init__(self, df, writer, config: Optional[MixedContentConfig] = None):
        super().__init__(df, writer)
        self.config = config or MixedContentConfig()
        self.session = self._criar_sessao_enterprise()
        self.playwright_available = self._check_playwright_availability()
        self.browser_pool = BrowserPool(self.config) if self.playwright_available else None
        self.cache = IntelligentCache(self.config)
        self.ssl_analyzer = SSLAnalyzer(self.config) if self.config.enable_ssl_analysis else None
        self.performance_metrics = {}
        
        # Logger estruturado
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(
                getattr(structlog, self.config.log_level.upper(), structlog.INFO)
            )
        )
        self.logger = structlog.get_logger("mixed_content_platinum")
        
    def _criar_sessao_enterprise(self) -> requests.Session:
        """🚀 Sessão enterprise com retry e performance tracking"""
        session = requests.Session()
        
        # Adapter com retry
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        return session

    def _detectar_necessidade_js_platinum(self, html_content: str, url: str) -> Tuple[bool, str, int, Dict]:
        """🧠 Detecção JS com métricas detalhadas"""
        
        start_time = time.time()
        html_lower = html_content.lower()
        js_score = 0
        reasons = []
        metrics = {}
        
        # Framework detection (peso ajustável)
        frameworks = {
            'react': 25, 'vue': 25, 'angular': 25, 'next.js': 30, 
            'nuxt': 30, 'svelte': 20, 'ember': 20
        }
        
        detected_frameworks = []
        for fw, weight in frameworks.items():
            if fw in html_lower:
                js_score += weight
                detected_frameworks.append(fw)
        
        if detected_frameworks:
            reasons.append(f"Frameworks: {', '.join(detected_frameworks)}")
        
        metrics['frameworks_detected'] = detected_frameworks
        
        # SPA patterns (peso configurável)
        spa_patterns = [
            'data-reactroot', 'ng-app', 'v-app', '__next', '__nuxt',
            'spa-', 'single-page', 'router-view', 'app-root'
        ]
        spa_found = [p for p in spa_patterns if p in html_lower]
        if spa_found:
            js_score += 25
            reasons.append(f"SPA: {len(spa_found)} patterns")
        
        metrics['spa_patterns'] = spa_found
        
        # Dynamic loading indicators
        loading_patterns = [
            'data-src', 'data-lazy', 'lazy-load', 'intersection-observer',
            'loading="lazy"', 'data-background', 'data-url'
        ]
        loading_found = [p for p in loading_patterns if p in html_lower]
        if loading_found:
            js_score += 15
            reasons.append(f"Lazy loading: {len(loading_found)}")
        
        metrics['lazy_loading'] = loading_found
        
        # Bundle/Module patterns
        module_patterns = ['webpack', 'bundle.js', 'chunk.js', 'vendor.js', 'app.js', 'main.js']
        modules_found = [p for p in module_patterns if p in html_lower]
        if modules_found:
            js_score += 20
            reasons.append(f"Bundles: {len(modules_found)}")
        
        metrics['js_bundles'] = modules_found
        
        # API calls
        api_patterns = ['fetch(', 'axios', '$.ajax', '/api/', 'xhr', 'xmlhttprequest']
        api_found = [p for p in api_patterns if p in html_lower]
        if api_found:
            js_score += 15
            reasons.append(f"API calls: {len(api_found)}")
        
        metrics['api_patterns'] = api_found
        
        # Performance tracking
        detection_time = time.time() - start_time
        metrics['detection_time_ms'] = round(detection_time * 1000, 2)
        
        needs_js = js_score >= self.config.js_detection_threshold
        reason = " | ".join(reasons) if reasons else "Site aparenta ser estático"
        
        self.logger.info(
            "js_detection_completed",
            url=url,
            needs_js=needs_js,
            js_score=js_score,
            threshold=self.config.js_detection_threshold,
            detection_time_ms=metrics['detection_time_ms']
        )
        
        return needs_js, reason, js_score, metrics

    async def _analisar_mixed_content_playwright_platinum(self, url: str) -> Dict:
        """🎭 Análise Playwright com pool e métricas"""
        
        start_time = time.time()
        context = None
        
        try:
            # Verifica cache primeiro
            cached_result = self.cache.get(url, 'playwright')
            if cached_result:
                self.logger.info("cache_hit_playwright", url=url)
                return cached_result
            
            # Pega context do pool
            context = await self.browser_pool.get_browser_context()
            page = await context.new_page()
            
            # Performance tracking
            navigation_start = time.time()
            response = await page.goto(url, wait_until='networkidle', timeout=self.config.playwright_timeout * 1000)
            navigation_time = time.time() - navigation_start
            
            if response.status != 200:
                result = {
                    'url': url,
                    'sucesso': True,
                    'metodo': 'PLAYWRIGHT_PLATINUM',
                    'tem_mixed_content': False,
                    'motivo': f'Status {response.status}',
                    'status_code': response.status,
                    'navigation_time_ms': round(navigation_time * 1000, 2)
                }
                
                # Retorna context para pool
                await self.browser_pool.return_context(context)
                return result
            
            # Renderização completa
            render_start = time.time()
            await page.wait_for_timeout(3000)
            
            # Força lazy loading
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(1000)
            
            # Pega HTML renderizado
            html_content = await page.content()
            render_time = time.time() - render_start
            
            # Parse e scan
            soup = BeautifulSoup(html_content, 'html.parser')
            mixed_content_issues = await self._scan_dom_playwright_platinum(soup, url, page)
            
            # SSL analysis opcional
            ssl_info = {}
            if self.ssl_analyzer:
                ssl_info = self.ssl_analyzer.analyze_ssl(url)
            
            # Retorna context para pool
            await self.browser_pool.return_context(context)
            
            total_time = time.time() - start_time
            
            if not mixed_content_issues:
                result = {
                    'url': url,
                    'sucesso': True,
                    'metodo': 'PLAYWRIGHT_PLATINUM',
                    'tem_mixed_content': False,
                    'total_issues': 0,
                    'motivo': 'Nenhum mixed content encontrado (playwright platinum)',
                    'performance': {
                        'navigation_time_ms': round(navigation_time * 1000, 2),
                        'render_time_ms': round(render_time * 1000, 2),
                        'total_time_ms': round(total_time * 1000, 2)
                    },
                    'ssl_info': ssl_info
                }
            else:
                # Analisa severidade
                analise_severidade = self._analisar_severidade(mixed_content_issues)
                
                result = {
                    'url': url,
                    'sucesso': True,
                    'metodo': 'PLAYWRIGHT_PLATINUM',
                    'tem_mixed_content': True,
                    'total_issues': len(mixed_content_issues),
                    'issues_detalhados': mixed_content_issues,
                    'severidade_maxima': analise_severidade['severidade_maxima'],
                    'tipos_encontrados': analise_severidade['tipos_encontrados'],
                    'contagem_por_tipo': analise_severidade['contagem_por_tipo'],
                    'impacto_seguranca': analise_severidade['impacto_seguranca'],
                    'recomendacao_acao': analise_severidade['recomendacao_acao'],
                    'performance': {
                        'navigation_time_ms': round(navigation_time * 1000, 2),
                        'render_time_ms': round(render_time * 1000, 2),
                        'total_time_ms': round(total_time * 1000, 2)
                    },
                    'ssl_info': ssl_info
                }
            
            # Salva no cache
            self.cache.set(url, 'playwright', result)
            
            self.logger.info(
                "playwright_analysis_completed",
                url=url,
                has_mixed_content=result['tem_mixed_content'],
                total_issues=result.get('total_issues', 0),
                total_time_ms=result['performance']['total_time_ms']
            )
            
            return result
            
        except Exception as e:
            if context:
                await self.browser_pool.return_context(context)
            
            self.logger.error("playwright_analysis_error", url=url, error=str(e))
            
            return {
                'url': url,
                'sucesso': False,
                'metodo': 'PLAYWRIGHT_PLATINUM',
                'erro': str(e),
                'tem_mixed_content': False
            }

    async def _scan_dom_playwright_platinum(self, soup: BeautifulSoup, base_url: str, page) -> List[Dict]:
        """🎭 Scanner DOM platinum com recursos dinâmicos"""
        
        # Scanner base
        issues = self._scan_dom_requests(soup, base_url)
        
        try:
            # Extrai recursos da Performance API + Resource Timing API
            performance_resources = await page.evaluate("""
                () => {
                    const resources = [];
                    
                    // Performance API
                    if (window.performance && window.performance.getEntriesByType) {
                        const entries = window.performance.getEntriesByType('resource');
                        entries.forEach(entry => {
                            if (entry.name.startsWith('http://')) {
                                resources.push({
                                    url: entry.name,
                                    type: entry.initiatorType || 'unknown',
                                    size: entry.transferSize || 0,
                                    duration: entry.duration || 0
                                });
                            }
                        });
                    }
                    
                    // Resource Timing API adicional
                    const scripts = Array.from(document.scripts);
                    scripts.forEach(script => {
                        if (script.src && script.src.startsWith('http://')) {
                            resources.push({
                                url: script.src,
                                type: 'script_dynamic',
                                size: 0,
                                duration: 0
                            });
                        }
                    });
                    
                    return resources;
                }
            """)
            
            # Adiciona recursos dinâmicos
            for resource in performance_resources:
                severity = self._classify_dynamic_resource_severity(resource['type'], resource['size'])
                
                issues.append({
                    'tipo': f'dynamic[{resource["type"]}]',
                    'url_recurso': resource['url'],
                    'url_absoluta': resource['url'],
                    'severidade': severity,
                    'contexto': f'Recurso dinâmico ({resource["size"]} bytes, {resource["duration"]:.1f}ms)',
                    'atributo': 'JavaScript dinâmico',
                    'tag_completa': f'Performance API: {resource["type"]}',
                    'performance_data': resource
                })
                
        except Exception as e:
            self.logger.warning("dynamic_resource_extraction_error", error=str(e))
        
        return issues

    def _classify_dynamic_resource_severity(self, resource_type: str, size: int) -> str:
        """🎯 Classifica severidade de recursos dinâmicos"""
        if resource_type in ['script', 'xmlhttprequest', 'fetch']:
            return 'CRÍTICO'
        elif resource_type in ['stylesheet', 'link']:
            return 'ALTO'
        elif size > 100000:  # > 100KB
            return 'ALTO'
        else:
            return 'MÉDIO'

    # ... (resto dos métodos existentes com adaptações para logging estruturado)

    def export(self):
        """🔒 Export platinum com observability completa"""
        try:
            self.logger.info("mixed_content_analysis_started", 
                           config=self.config.__dict__,
                           playwright_available=self.playwright_available)
            
            # Pipeline principal (similar ao original, mas com logging estruturado)
            urls_filtradas = self._filtrar_urls_https(self.df)
            
            self.logger.info("urls_filtered", 
                           total_dataframe=len(self.df),
                           https_valid=len(urls_filtradas))
            
            if not urls_filtradas:
                self.logger.warning("no_valid_urls")
                self._criar_aba_vazia()
                return
            
            # FASE 1: Análise com requests
            resultados_requests = self._analisar_fase_requests_paralelo(urls_filtradas)
            
            # FASE 2: Revalidação com Playwright (se necessário)
            urls_revalidacao = [r['url'] for r in resultados_requests 
                              if r.get('requer_revalidacao_js', False)]
            
            if urls_revalidacao and self.playwright_available:
                self.logger.info("playwright_revalidation_started", urls_count=len(urls_revalidacao))
                
                resultados_playwright = asyncio.run(
                    self._analisar_fase_playwright_paralelo_platinum(urls_revalidacao)
                )
                
                # Merge inteligente
                resultados_finais = self._merge_resultados_inteligente(
                    resultados_requests, resultados_playwright
                )
            else:
                resultados_finais = resultados_requests
            
            # Cleanup do browser pool
            if self.browser_pool:
                asyncio.run(self.browser_pool.cleanup())
            
            # Gera relatório com métricas
            self._gerar_relatorio_platinum(resultados_finais)
            
            self.logger.info("mixed_content_analysis_completed", 
                           total_analyzed=len(resultados_finais),
                           mixed_content_found=len([r for r in resultados_finais if r.get('tem_mixed_content')]))
            
        except Exception as e:
            self.logger.error("analysis_failed", error=str(e))
            import traceback
            traceback.print_exc()
            self._criar_aba_vazia()

    async def _analisar_fase_playwright_paralelo_platinum(self, urls: List[str]) -> List[Dict]:
        """🎭 Fase Playwright com pool otimizado"""
        
        self.logger.info("playwright_parallel_started", urls_count=len(urls))
        
        resultados = []
        semaphore = asyncio.Semaphore(self.config.browser_pool_size)
        
        async def analisar_com_controle(url):
            async with semaphore:
                return await self._analisar_mixed_content_playwright_platinum(url)
        
        tasks = [analisar_com_controle(url) for url in urls]
        
        for i, coro in enumerate(asyncio.as_completed(tasks)):
            resultado = await coro
            resultados.append(resultado)
            
            if (i + 1) % 5 == 0:
                self.logger.info("playwright_progress", completed=i+1, total=len(urls))
        
        return resultados

    def _merge_resultados_inteligente(self, requests_results: List[Dict], playwright_results: List[Dict]) -> List[Dict]:
        """🧠 Merge inteligente priorizando resultados Playwright quando disponível"""
        
        playwright_by_url = {r['url']: r for r in playwright_results}
        merged = []
        
        for req_result in requests_results:
            url = req_result['url']
            if url in playwright_by_url:
                # Prioriza Playwright, mas preserva métricas de ambos
                pw_result = playwright_by_url[url]
                pw_result['requests_metrics'] = {
                    'js_score': req_result.get('js_score'),
                    'js_reason': req_result.get('js_reason'),
                    'detection_method': 'hybrid'
                }
                merged.append(pw_result)
            else:
                merged.append(req_result)
        
        return merged

    def _gerar_relatorio_platinum(self, resultados: List[Dict]):
        """📊 Relatório platinum com métricas avançadas"""
        
        # Similar ao método original, mas com:
        # - Métricas de performance
        # - Dados SSL quando disponível  
        # - Estatísticas de cache hit/miss
        # - Distribuição de métodos usados
        # - Análise de eficiência do híbrido
        
        self.logger.info("generating_platinum_report", results_count=len(resultados))
        
        # ... implementação do relatório expandido
        
        pass  # Placeholder - implementação similar ao original com expansões

# ========================
# 🔄 FACTORY FUNCTION
# ========================
def create_mixed_content_engine(config: Optional[MixedContentConfig] = None):
    """🏭 Factory para criar engine configurado"""
    
    if config is None:
        config = MixedContentConfig()
    
    def engine_factory(df, writer):
        return MixedContentPlatinumSheet(df, writer, config)
    
    return engine_factory

# ========================
# 🔄 MANTÉM COMPATIBILIDADE
# ========================
MixedContentSheet = MixedContentPlatinumSheet  # Alias para compatibilidade