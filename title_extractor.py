# title_extractor.py - Title V5 Hardened Standalone

from playwright.async_api import Page
from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class TitleExtractorV5:
    """📝 Title Extractor V5 Hardened - Standalone e focado"""
    
    def __init__(self, timeout: int = 15000, fallback_timeout: int = 3000):
        self.timeout = timeout
        self.fallback_timeout = fallback_timeout
        
        # Blacklist simples
        self.blacklist = [
            'loading', 'carregando', 'loading...', 'carregando...',
            'app', 'react app', 'vue app', 'angular app', 'next app',
            'apisanitizer', 'sanitizer', 'default', 'untitled',
            'document', 'page', 'site', 'website', 'home'
        ]
        
        # Estatísticas
        self.stats = {
            'total_extractions': 0,
            'successful_extractions': 0,
            'strategy_used': {},
            'blacklisted_count': 0
        }
    
    async def extract(self, page: Page, url: str) -> Dict[str, any]:
        """🎯 Extrai title com estratégias hardened"""
        
        self.stats['total_extractions'] += 1
        
        try:
            # ESTRATÉGIA 1: Wait for function (timeout aumentado)
            title, strategy = await self._strategy_wait_for_function(page)
            
            # ESTRATÉGIA 2: Title API direto
            if not title:
                title, strategy = await self._strategy_title_api(page)
            
            # ESTRATÉGIA 3: DOM direto
            if not title:
                title, strategy = await self._strategy_dom_direct(page)
            
            # ESTRATÉGIA 4: Fallback agressivo (SPAs)
            if not title or self._is_loading_state(title):
                title, strategy = await self._strategy_spa_fallback(page)
            
            # ESTRATÉGIA 5: Title alternativo (H1, OG)
            if not title or self._is_blacklisted(title):
                title, strategy = await self._strategy_alternative_sources(page)
            
            # Limpa e valida
            title_clean = self._clean_title(title)
            
            # Atualiza estatísticas
            if title_clean:
                self.stats['successful_extractions'] += 1
                
            if strategy not in self.stats['strategy_used']:
                self.stats['strategy_used'][strategy] = 0
            self.stats['strategy_used'][strategy] += 1
            
            if self._is_blacklisted(title):
                self.stats['blacklisted_count'] += 1
            
            return {
                'title': title_clean,
                'strategy_used': strategy,
                'original_title': title,
                'is_blacklisted': self._is_blacklisted(title),
                'extraction_success': bool(title_clean),
                'url': url
            }
            
        except Exception as e:
            logger.error(f"Erro extraindo title de {url}: {e}")
            return {
                'title': '',
                'strategy_used': 'error',
                'original_title': '',
                'is_blacklisted': False,
                'extraction_success': False,
                'error': str(e),
                'url': url
            }
    
    async def _strategy_wait_for_function(self, page: Page) -> tuple[str, str]:
        """🔧 Estratégia 1: Wait for function com timeout aumentado"""
        try:
            await page.wait_for_function(
                "document.title && document.title.trim().length > 0", 
                timeout=self.timeout
            )
            title = await page.title()
            return title.strip() if title else "", "wait_for_function"
        except PlaywrightTimeoutError:
            return "", "wait_for_function_timeout"
        except Exception:
            return "", "wait_for_function_error"
    
    async def _strategy_title_api(self, page: Page) -> tuple[str, str]:
        """🔧 Estratégia 2: Title API direto"""
        try:
            title = await page.title()
            return title.strip() if title else "", "title_api"
        except Exception:
            return "", "title_api_error"
    
    async def _strategy_dom_direct(self, page: Page) -> tuple[str, str]:
        """🔧 Estratégia 3: DOM direto"""
        try:
            title_elem = await page.query_selector('title')
            if title_elem:
                title = await title_elem.inner_text()
                return title.strip() if title else "", "dom_direct"
            return "", "dom_direct_not_found"
        except Exception:
            return "", "dom_direct_error"
    
    async def _strategy_spa_fallback(self, page: Page) -> tuple[str, str]:
        """🔧 Estratégia 4: Fallback agressivo para SPAs"""
        try:
            # Wait adicional para JS assíncrono
            await page.wait_for_timeout(self.fallback_timeout)
            
            # Nova tentativa
            title = await page.title()
            if not title or title.strip() == "":
                title = await page.evaluate("document.title")
            
            return title.strip() if title else "", "spa_fallback"
        except Exception:
            return "", "spa_fallback_error"
    
    async def _strategy_alternative_sources(self, page: Page) -> tuple[str, str]:
        """🔧 Estratégia 5: Fontes alternativas"""
        try:
            # H1 principal
            h1_elem = await page.query_selector('h1')
            if h1_elem:
                h1_text = await h1_elem.inner_text()
                if h1_text and len(h1_text.strip()) > 3:
                    return h1_text.strip(), "h1_alternative"
            
            # Meta og:title
            og_title_elem = await page.query_selector('meta[property="og:title"]')
            if og_title_elem:
                og_title = await og_title_elem.get_attribute('content')
                if og_title and len(og_title.strip()) > 3:
                    return og_title.strip(), "og_title_alternative"
            
            # Meta twitter:title
            twitter_title_elem = await page.query_selector('meta[name="twitter:title"]')
            if twitter_title_elem:
                twitter_title = await twitter_title_elem.get_attribute('content')
                if twitter_title and len(twitter_title.strip()) > 3:
                    return twitter_title.strip(), "twitter_title_alternative"
            
            return "", "no_alternative_found"
            
        except Exception:
            return "", "alternative_sources_error"
    
    def _is_loading_state(self, title: str) -> bool:
        """🔍 Verifica se title está em estado de loading"""
        if not title:
            return True
        
        loading_states = ['loading', 'carregando', 'app', 'react app', 'vue app']
        return title.lower().strip() in loading_states
    
    def _is_blacklisted(self, title: str) -> bool:
        """🚫 Verifica se title está na blacklist"""
        if not title:
            return True
        
        return title.lower().strip() in self.blacklist
    
    def _clean_title(self, title: str) -> str:
        """🧹 Limpa e valida title"""
        if not title:
            return ""
        
        title_clean = title.strip()
        
        # Remove se for blacklisted
        if self._is_blacklisted(title_clean):
            return ""
        
        # Validações básicas
        if len(title_clean) < 3:
            return ""
        
        if not any(c.isalpha() for c in title_clean):
            return ""
        
        return title_clean
    
    def get_stats(self) -> Dict:
        """📊 Retorna estatísticas"""
        success_rate = 0
        if self.stats['total_extractions'] > 0:
            success_rate = (self.stats['successful_extractions'] / self.stats['total_extractions']) * 100
        
        return {
            'total_extractions': self.stats['total_extractions'],
            'successful_extractions': self.stats['successful_extractions'],
            'success_rate_percent': round(success_rate, 2),
            'blacklisted_count': self.stats['blacklisted_count'],
            'strategy_usage': self.stats['strategy_used'].copy(),
            'most_used_strategy': max(self.stats['strategy_used'], key=self.stats['strategy_used'].get) if self.stats['strategy_used'] else None
        }
    
    def reset_stats(self):
        """🔄 Reseta estatísticas"""
        self.stats = {
            'total_extractions': 0,
            'successful_extractions': 0,
            'strategy_used': {},
            'blacklisted_count': 0
        }

# ========================
# 🧪 FUNÇÃO DE TESTE STANDALONE
# ========================

async def test_title_extractor():
    """🧪 Testa o extrator de title"""
    from playwright.async_api import async_playwright
    
    extractor = TitleExtractorV5(timeout=15000, fallback_timeout=3000)
    
    test_urls = [
        "https://example.com",
        "https://gndisul.com.br"
    ]
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        
        for url in test_urls:
            print(f"🔍 Testando: {url}")
            
            try:
                await page.goto(url, timeout=20000)
                result = await extractor.extract(page, url)
                
                print(f"   Title: '{result['title']}'")
                print(f"   Strategy: {result['strategy_used']}")
                print(f"   Success: {result['extraction_success']}")
                print(f"   Blacklisted: {result['is_blacklisted']}")
                
            except Exception as e:
                print(f"   ❌ Erro: {e}")
        
        await browser.close()
    
    # Estatísticas finais
    stats = extractor.get_stats()
    print(f"\n📊 Estatísticas:")
    print(f"   Success rate: {stats['success_rate_percent']:.1f}%")
    print(f"   Strategy usage: {stats['strategy_usage']}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_title_extractor())