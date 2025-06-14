# seo_extractor.py - Extrator SEO Simples e Focado

from playwright.async_api import Page
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class SEOExtractor:
    """📊 Extrator SEO - Simples, Focado e Eficaz"""
    
    def __init__(self):
        self.stats = {
            'extractions_performed': 0,
            'successful_extractions': 0,
            'meta_tags_found': 0,
            'headings_found': 0,
            'structured_data_found': 0
        }
    
    async def extract(self, page: Page, url: str) -> Dict[str, Any]:
        """📊 Extrai dados SEO essenciais"""
        
        self.stats['extractions_performed'] += 1
        
        try:
            # Extração em paralelo dos dados principais
            meta_data = await self._extract_meta_tags(page)
            headings_data = await self._extract_headings(page)
            og_data = await self._extract_open_graph(page)
            structured_data = await self._extract_structured_data(page)
            
            # Resultado consolidado
            seo_data = {
                # Meta tags básicas
                'description': meta_data.get('description', ''),
                'keywords': meta_data.get('keywords', ''),
                'robots': meta_data.get('robots', ''),
                'canonical': meta_data.get('canonical', ''),
                'viewport': meta_data.get('viewport', ''),
                
                # Headings
                **headings_data,
                
                # Open Graph
                'og_title': og_data.get('title', ''),
                'og_description': og_data.get('description', ''),
                'og_image': og_data.get('image', ''),
                'og_url': og_data.get('url', ''),
                'og_type': og_data.get('type', ''),
                
                # Structured Data
                'structured_data_count': len(structured_data),
                'has_structured_data': len(structured_data) > 0,
                'structured_data_types': list(set([item.get('type', '') for item in structured_data])),
                
                # URL info
                'url': url,
                'final_url': page.url,
                'is_redirected': page.url != url,
                
                # Score SEO básico
                'seo_score': self._calculate_basic_seo_score(meta_data, headings_data, og_data, structured_data)
            }
            
            self.stats['successful_extractions'] += 1
            return seo_data
            
        except Exception as e:
            logger.error(f"Erro extraindo SEO data de {url}: {e}")
            return {
                'url': url,
                'error': str(e),
                'seo_score': 0
            }
    
    async def _extract_meta_tags(self, page: Page) -> Dict[str, str]:
        """📋 Extrai meta tags importantes"""
        
        meta_data = {}
        
        try:
            # Meta description
            desc_elem = await page.query_selector('meta[name="description"]')
            if desc_elem:
                meta_data['description'] = await desc_elem.get_attribute('content') or ""
                self.stats['meta_tags_found'] += 1
            
            # Meta keywords
            keywords_elem = await page.query_selector('meta[name="keywords"]')
            if keywords_elem:
                meta_data['keywords'] = await keywords_elem.get_attribute('content') or ""
            
            # Meta robots
            robots_elem = await page.query_selector('meta[name="robots"]')
            if robots_elem:
                meta_data['robots'] = await robots_elem.get_attribute('content') or ""
            
            # Canonical
            canonical_elem = await page.query_selector('link[rel="canonical"]')
            if canonical_elem:
                meta_data['canonical'] = await canonical_elem.get_attribute('href') or ""
            
            # Viewport
            viewport_elem = await page.query_selector('meta[name="viewport"]')
            if viewport_elem:
                meta_data['viewport'] = await viewport_elem.get_attribute('content') or ""
        
        except Exception as e:
            logger.debug(f"Erro extraindo meta tags: {e}")
        
        return meta_data
    
    async def _extract_headings(self, page: Page) -> Dict[str, Any]:
        """🏷️ Extrai headings de forma simples e eficaz"""
        
        headings_data = {}
        
        try:
            for level in range(1, 7):  # h1 até h6
                headings = await page.query_selector_all(f'h{level}')
                texts = []
                
                for heading in headings:
                    try:
                        text = await heading.inner_text()
                        if text.strip():
                            texts.append(text.strip())
                    except:
                        continue
                
                headings_data[f'h{level}'] = len(texts)
                headings_data[f'h{level}_texts'] = texts
                
                if texts:
                    self.stats['headings_found'] += len(texts)
            
            # Análise básica de estrutura
            headings_data['h1_ausente'] = headings_data.get('h1', 0) == 0
            headings_data['h1_multiple'] = headings_data.get('h1', 0) > 1
            headings_data['h2_ausente'] = headings_data.get('h2', 0) == 0
            
            # Hierarquia básica
            has_h1 = headings_data.get('h1', 0) > 0
            has_h2 = headings_data.get('h2', 0) > 0
            has_h3 = headings_data.get('h3', 0) > 0
            
            hierarchy_issues = []
            if not has_h1 and (has_h2 or has_h3):
                hierarchy_issues.append('h1_missing_with_lower_levels')
            if has_h1 and not has_h2 and has_h3:
                hierarchy_issues.append('h2_skipped')
            
            headings_data['hierarchy_issues'] = hierarchy_issues
            headings_data['has_hierarchy_issues'] = len(hierarchy_issues) > 0
        
        except Exception as e:
            logger.debug(f"Erro extraindo headings: {e}")
            # Retorna estrutura vazia se houver erro
            for level in range(1, 7):
                headings_data[f'h{level}'] = 0
                headings_data[f'h{level}_texts'] = []
        
        return headings_data
    
    async def _extract_open_graph(self, page: Page) -> Dict[str, str]:
        """🌐 Extrai dados Open Graph"""
        
        og_data = {}
        
        try:
            og_tags = {
                'title': 'meta[property="og:title"]',
                'description': 'meta[property="og:description"]',
                'image': 'meta[property="og:image"]',
                'url': 'meta[property="og:url"]',
                'type': 'meta[property="og:type"]',
                'site_name': 'meta[property="og:site_name"]'
            }
            
            for key, selector in og_tags.items():
                elem = await page.query_selector(selector)
                if elem:
                    content = await elem.get_attribute('content')
                    og_data[key] = content.strip() if content else ""
        
        except Exception as e:
            logger.debug(f"Erro extraindo Open Graph: {e}")
        
        return og_data
    
    async def _extract_structured_data(self, page: Page) -> List[Dict]:
        """📊 Extrai dados estruturados (JSON-LD)"""
        
        structured_data = []
        
        try:
            # JSON-LD
            json_scripts = await page.query_selector_all('script[type="application/ld+json"]')
            
            for script in json_scripts:
                try:
                    content = await script.inner_text()
                    if content.strip():
                        import json
                        data = json.loads(content)
                        
                        # Identifica tipo
                        schema_type = "Unknown"
                        if isinstance(data, dict):
                            schema_type = data.get('@type', 'Unknown')
                        elif isinstance(data, list) and data:
                            schema_type = data[0].get('@type', 'Unknown') if isinstance(data[0], dict) else 'Array'
                        
                        structured_data.append({
                            'type': schema_type,
                            'format': 'json-ld',
                            'data': data
                        })
                        
                        self.stats['structured_data_found'] += 1
                        
                except (json.JSONDecodeError, AttributeError):
                    continue
                except Exception as e:
                    logger.debug(f"Erro processando JSON-LD: {e}")
        
        except Exception as e:
            logger.debug(f"Erro extraindo structured data: {e}")
        
        return structured_data
    
    def _calculate_basic_seo_score(self, meta_data: Dict, headings_data: Dict, og_data: Dict, structured_data: List) -> int:
        """📊 Calcula score SEO básico (0-100)"""
        
        score = 0
        
        try:
            # Meta tags (30 pontos)
            if meta_data.get('description'):
                score += 15
            if meta_data.get('canonical'):
                score += 10
            if meta_data.get('robots'):
                score += 5
            
            # Headings (30 pontos)
            if not headings_data.get('h1_ausente', True):
                score += 15
            if not headings_data.get('h1_multiple', False):
                score += 5
            if not headings_data.get('h2_ausente', True):
                score += 10
            
            # Open Graph (20 pontos)
            if og_data.get('title'):
                score += 5
            if og_data.get('description'):
                score += 5
            if og_data.get('image'):
                score += 5
            if og_data.get('url'):
                score += 5
            
            # Structured Data (10 pontos)
            if structured_data:
                score += min(10, len(structured_data) * 3)
            
            # Hierarquia (10 pontos)
            if not headings_data.get('has_hierarchy_issues', True):
                score += 10
        
        except Exception as e:
            logger.debug(f"Erro calculando SEO score: {e}")
        
        return min(score, 100)
    
    def get_stats(self) -> Dict[str, Any]:
        """📊 Retorna estatísticas do extrator"""
        
        success_rate = 0
        if self.stats['extractions_performed'] > 0:
            success_rate = (self.stats['successful_extractions'] / self.stats['extractions_performed']) * 100
        
        return {
            'extractions_performed': self.stats['extractions_performed'],
            'successful_extractions': self.stats['successful_extractions'],
            'success_rate_percent': round(success_rate, 2),
            'meta_tags_found': self.stats['meta_tags_found'],
            'headings_found': self.stats['headings_found'],
            'structured_data_found': self.stats['structured_data_found']
        }
    
    def reset_stats(self):
        """🔄 Reseta estatísticas"""
        self.stats = {
            'extractions_performed': 0,
            'successful_extractions': 0,
            'meta_tags_found': 0,
            'headings_found': 0,
            'structured_data_found': 0
        }

# ========================
# 🧪 TESTE STANDALONE
# ========================

async def test_seo_extractor():
    """🧪 Testa o extrator SEO"""
    from playwright.async_api import async_playwright
    
    extractor = SEOExtractor()
    
    test_urls = [
        "https://example.com",
        "https://gndisul.com.br"
    ]
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        
        for url in test_urls:
            print(f"🔍 Testando SEO: {url}")
            
            try:
                await page.goto(url, timeout=20000)
                result = await extractor.extract(page, url)
                
                print(f"   Description: {'✅' if result.get('description') else '❌'}")
                print(f"   H1: {result.get('h1', 0)} | H2: {result.get('h2', 0)}")
                print(f"   OG Tags: {'✅' if result.get('og_title') else '❌'}")
                print(f"   Structured Data: {result.get('structured_data_count', 0)}")
                print(f"   SEO Score: {result.get('seo_score', 0)}/100")
                
            except Exception as e:
                print(f"   ❌ Erro: {e}")
        
        await browser.close()
    
    # Estatísticas
    stats = extractor.get_stats()
    print(f"\n📊 Estatísticas SEO:")
    print(f"   Success rate: {stats['success_rate_percent']:.1f}%")
    print(f"   Meta tags found: {stats['meta_tags_found']}")
    print(f"   Headings found: {stats['headings_found']}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_seo_extractor())