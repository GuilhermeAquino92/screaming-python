# config.py - Configurações Centralizadas do Crawler Modular

import os
from typing import Dict, Any

class CrawlerGlobalConfig:
    """🔧 Configurações globais do sistema de crawler"""
    
    # Versão
    VERSION = "2.0.0-modular"
    
    # Timeouts globais (em ms)
    DEFAULT_TITLE_TIMEOUT = 15000  # 15s - HARDENED
    DEFAULT_PAGE_TIMEOUT = 30000   # 30s
    DEFAULT_NAVIGATION_TIMEOUT = 20000  # 20s
    
    # Performance
    DEFAULT_BROWSER_POOL_SIZE = 3
    DEFAULT_MAX_CONCURRENT_PAGES = 10
    DEFAULT_BATCH_SIZE = 5
    
    # Limites
    DEFAULT_MAX_URLS = 1000
    DEFAULT_MAX_DEPTH = 3
    
    # Cache
    CACHE_ENABLED = True
    CACHE_DIR = ".cache"
    
    # Logs
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    PROGRESS_LOG_INTERVAL = 50
    
    # SEO
    AVAILABLE_SEO_PROFILES = ['blog', 'ecommerce', 'saas', 'portal', 'institucional']
    DEFAULT_SEO_PROFILE = 'blog'
    
    # User Agent
    USER_AGENT = "Mozilla/5.0 (compatible; SEO-Analyzer-Modular/2.0; +https://seo-analyzer.com/bot)"
    
    # Headers padrão
    DEFAULT_HEADERS = {
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }
    
    # Browser args para performance
    BROWSER_ARGS = [
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
        '--disable-web-security',
        '--disable-images',  # Otimização: não carrega imagens
        '--disable-javascript-harmony-shipping',
        '--disable-background-timer-throttling',
        '--disable-renderer-backgrounding',
        '--disable-backgrounding-occluded-windows',
    ]
    
    @classmethod
    def get_config_dict(cls) -> Dict[str, Any]:
        """📋 Retorna configurações como dicionário"""
        return {
            'version': cls.VERSION,
            'timeouts': {
                'title': cls.DEFAULT_TITLE_TIMEOUT,
                'page': cls.DEFAULT_PAGE_TIMEOUT,
                'navigation': cls.DEFAULT_NAVIGATION_TIMEOUT
            },
            'performance': {
                'browser_pool_size': cls.DEFAULT_BROWSER_POOL_SIZE,
                'max_concurrent_pages': cls.DEFAULT_MAX_CONCURRENT_PAGES,
                'batch_size': cls.DEFAULT_BATCH_SIZE
            },
            'limits': {
                'max_urls': cls.DEFAULT_MAX_URLS,
                'max_depth': cls.DEFAULT_MAX_DEPTH
            },
            'cache': {
                'enabled': cls.CACHE_ENABLED,
                'dir': cls.CACHE_DIR
            },
            'seo': {
                'profiles': cls.AVAILABLE_SEO_PROFILES,
                'default_profile': cls.DEFAULT_SEO_PROFILE
            }
        }
    
    @classmethod
    def setup_logging(cls):
        """📝 Configura logging global"""
        import logging
        
        logging.basicConfig(
            level=getattr(logging, cls.LOG_LEVEL),
            format=cls.LOG_FORMAT
        )
        
        # Reduz verbosidade de bibliotecas externas
        logging.getLogger('playwright').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    @classmethod
    def create_cache_dir(cls):
        """📁 Cria diretório de cache se não existir"""
        if cls.CACHE_ENABLED:
            os.makedirs(cls.CACHE_DIR, exist_ok=True)

# Inicialização automática
CrawlerGlobalConfig.setup_logging()
CrawlerGlobalConfig.create_cache_dir()