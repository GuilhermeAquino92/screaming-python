# seo_extractor.py - CORREÇÃO COMPLETA para Lazy Loading

from playwright.async_api import Page
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class SEOExtractor:
    """📊 Extrator SEO - CORRIGIDO para capturar headings com lazy loading"""
    
    def __init__(self):
        self.stats = {
            'extractions_performed': 0,
            'successful_extractions': 0,
            'meta_tags_found': 0,
            'headings_found': 0,
            'structured_data_found': 0
        }
    
    async def extract(self, page: Page, url: str) -> Dict[str, Any]:
        """📊 Extrai dados SEO essenciais - COM CORREÇÃO DE LAZY LOADING"""
        
        self.stats['extractions_performed'] += 1
        
        try:
            # 🚀 CORREÇÃO 1: SCROLL FORÇADO ANTES DE QUALQUER EXTRAÇÃO
            await self._force_lazy_loading(page)
            
            # Extração em paralelo dos dados principais
            meta_data = await self._extract_meta_tags(page)
            headings_data = await self._extract_headings_corrigido(page)  # VERSÃO CORRIGIDA
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
                
                # Headings CORRIGIDOS
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
    
    async def _force_lazy_loading(self, page: Page):
        """🚀 FORÇA LAZY LOADING - Scroll + Espera"""
        try:
            # 1. Scroll para baixo (força lazy loading)
            await page.evaluate("""
                window.scrollTo(0, document.body.scrollHeight);
            """)
            
            # 2. Espera para renderização
            await page.wait_for_timeout(1500)
            
            # 3. Scroll para cima (garante que tudo foi processado)
            await page.evaluate("""
                window.scrollTo(0, 0);
            """)
            
            # 4. Espera adicional para estabilização
            await page.wait_for_timeout(500)
            
            # 5. Aguarda network idle (se possível)
            try:
                await page.wait_for_load_state('networkidle', timeout=3000)
            except:
                pass  # Ignora timeout se demorar muito
                
        except Exception as e:
            logger.debug(f"Erro no force lazy loading: {e}")
    
    async def _extract_headings_corrigido(self, page: Page) -> Dict[str, Any]:
        """🏷️ VERSÃO CORRIGIDA - Extrai headings com força total"""
        
        headings_data = {}
        
        try:
            # 🔥 ESTRATÉGIA DUPLA DE EXTRAÇÃO
            
            # MÉTODO 1: Query Selector All (padrão)
            for level in range(1, 7):  # h1 até h6
                headings = await page.query_selector_all(f'h{level}')
                texts = []
                
                for heading in headings:
                    try:
                        # Tenta inner_text primeiro
                        text = await heading.inner_text()
                        if not text or not text.strip():
                            # Fallback: textContent
                            text = await heading.evaluate('el => el.textContent')
                        
                        if text and text.strip():
                            texts.append(text.strip())
                            
                    except Exception as e:
                        logger.debug(f"Erro extraindo texto do h{level}: {e}")
                        continue
                
                headings_data[f'h{level}'] = len(texts)
                headings_data[f'h{level}_texts'] = texts
                
                if texts:
                    self.stats['headings_found'] += len(texts)
            
            # 🔥 MÉTODO 2: Evaluate JavaScript (para casos extremos)
            try:
                js_headings = await page.evaluate("""
                    () => {
                        const headings = {};
                        for (let i = 1; i <= 6; i++) {
                            const elements = document.querySelectorAll(`h${i}`);
                            const texts = [];
                            elements.forEach(el => {
                                const text = el.innerText || el.textContent || '';
                                if (text.trim()) {
                                    texts.push(text.trim());
                                }
                            });
                            headings[`h${i}`] = texts.length;
                            headings[`h${i}_texts`] = texts;
                        }
                        return headings;
                    }
                """)
                
                # 🔥 MERGE: Usa o método que encontrou mais headings
                for level in range(1, 7):
                    key = f'h{level}'
                    key_texts = f'h{level}_texts'
                    
                    # Se JS encontrou mais headings, usa JS
                    if js_headings.get(key, 0) > headings_data.get(key, 0):
                        headings_data[key] = js_headings[key]
                        headings_data[key_texts] = js_headings[key_texts]
                        logger.debug(f"JS method found more {key}: {js_headings[key]} vs {headings_data.get(key, 0)}")
                
            except Exception as e:
                logger.debug(f"Erro no método JS de headings: {e}")
            
            # 🔥 DEBUG: Log dos resultados
            total_headings = sum(headings_data.get(f'h{i}', 0) for i in range(1, 7))
            if total_headings > 0:
                logger.debug(f"Headings encontrados: H1:{headings_data.get('h1', 0)} H2:{headings_data.get('h2', 0)} H3:{headings_data.get('h3', 0)}")
            else:
                logger.warning(f"ZERO headings encontrados - possível problema de lazy loading")
            
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
            logger.error(f"Erro crítico extraindo headings: {e}")
            # Retorna estrutura vazia se houver erro
            for level in range(1, 7):
                headings_data[f'h{level}'] = 0
                headings_data[f'h{level}_texts'] = []
        
        return headings_data
    
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
    
    async def _extract_open_graph(self, page: Page) -> Dict[str, str]:
        """🌐 Extrai dados Open Graph"""
        
        og_data = {}
        
        try:
            og_tags = {
                'title': 'meta[property="og:title"]',
                'description': 'meta[property="og:description"]',
                'image': 'meta[property="og:image"]',
                'url': 'meta[property="og:url"]',
                'type': 'meta[property="og:type"]'
            }
            
            for key, selector in og_tags.items():
                elem = await page.query_selector(selector)
                if elem:
                    content = await elem.get_attribute('content')
                    if content:
                        og_data[key] = content.strip()
        
        except Exception as e:
            logger.debug(f"Erro extraindo Open Graph: {e}")
        
        return og_data
    
    async def _extract_structured_data(self, page: Page) -> List[Dict]:
        """📊 Extrai dados estruturados (JSON-LD)"""
        
        try:
            structured_data = await page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                    const data = [];
                    
                    scripts.forEach(script => {
                        try {
                            const json = JSON.parse(script.textContent);
                            data.push(json);
                        } catch (e) {
                            // Ignora JSON inválido
                        }
                    });
                    
                    return data;
                }
            """)
            
            if structured_data:
                self.stats['structured_data_found'] += len(structured_data)
            
            return structured_data
        
        except Exception as e:
            logger.debug(f"Erro extraindo structured data: {e}")
            return []
    
    def _calculate_basic_seo_score(self, meta_data: Dict, headings_data: Dict, og_data: Dict, structured_data: List) -> int:
        """📊 Calcula score SEO básico (0-100)"""
        
        score = 0
        
        # Meta description (20 pontos)
        if meta_data.get('description'):
            desc_len = len(meta_data['description'])
            if 120 <= desc_len <= 160:
                score += 20
            elif 80 <= desc_len <= 200:
                score += 15
            elif desc_len > 0:
                score += 10
        
        # H1 (20 pontos)
        h1_count = headings_data.get('h1', 0)
        if h1_count == 1:
            score += 20
        elif h1_count > 1:
            score += 10
        
        # H2 (15 pontos)
        if headings_data.get('h2', 0) > 0:
            score += 15
        
        # Estrutura de headings (15 pontos)
        if not headings_data.get('has_hierarchy_issues', True):
            score += 15
        
        # Open Graph (15 pontos)
        og_score = 0
        if og_data.get('title'):
            og_score += 5
        if og_data.get('description'):
            og_score += 5
        if og_data.get('image'):
            og_score += 5
        score += og_score
        
        # Dados estruturados (10 pontos)
        if structured_data:
            score += 10
        
        # Canonical (5 pontos)
        if meta_data.get('canonical'):
            score += 5
        
        return min(score, 100)
    
    def get_stats(self) -> Dict[str, Any]:
        """📊 Retorna estatísticas do extrator"""
        
        if self.stats['extractions_performed'] > 0:
            success_rate = (self.stats['successful_extractions'] / self.stats['extractions_performed']) * 100
        else:
            success_rate = 0
        
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