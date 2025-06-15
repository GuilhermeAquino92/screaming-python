# exporters/sheets/metatags_sheet.py - COM ENGINE CIRÚRGICA
# 🏷️ ENGINE CIRÚRGICA: Extração e análise completa de metatags SEO

import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter
import warnings
warnings.filterwarnings("ignore")

class MetatagsSheet(BaseSheetExporter):
    def export(self):
        """🏷️ Gera aba de metatags com engine cirúrgica completa"""
        
        print("🏷️ Executando ENGINE CIRÚRGICA - Metatags SEO...")
        print("   🎯 DOM parsing real para metatags")
        print("   📊 Análise de qualidade SEO")
        print("   🔍 Detecção de problemas estruturais")
        
        # Extrai URLs únicas do DataFrame
        urls = self.df['url'].dropna().unique().tolist()
        
        if not urls:
            print("   ⚠️ Nenhuma URL encontrada")
            pd.DataFrame({
                'URL': [],
                'Title_Length': [],
                'Title_SEO_Score': [],
                'Title': [],
                'Description_Length': [],
                'Description_SEO_Score': [], 
                'Description': [],
                'Canonical_Status': [],
                'Canonical': [],
                'Robots': [],
                'Viewport': [],
                'Keywords': [],
                'Problemas_SEO': []
            }).to_excel(self.writer, index=False, sheet_name="Metatags")
            return
        
        print(f"   🌐 Processando {len(urls)} URLs...")
        
        # Processa URLs em paralelo
        resultados = self._processar_urls_paralelo(urls)
        
        # Cria DataFrame final
        df_final = pd.DataFrame(resultados)
        
        # Ordena por problemas SEO primeiro, depois por URL
        if not df_final.empty:
            # Prioriza URLs com mais problemas
            df_final['score_problemas'] = df_final['Problemas_SEO'].str.len()
            df_final = df_final.sort_values(['score_problemas', 'URL'], ascending=[False, True])
            df_final = df_final.drop('score_problemas', axis=1)
        
        # Exporta para Excel
        df_final.to_excel(self.writer, index=False, sheet_name="Metatags")
        
        # Estatísticas
        total_problemas = sum(len(row.get('Problemas_SEO', [])) for row in resultados)
        print(f"   ✅ {len(resultados)} metatags analisadas")
        print(f"   🎯 {total_problemas} problemas SEO detectados")
    
    def _processar_urls_paralelo(self, urls):
        """⚡ Processa URLs em paralelo com otimização"""
        
        HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9"
        }
        
        def extrair_metatags_url(url):
            """🎯 Extrai metatags de uma URL específica"""
            try:
                response = requests.get(url, timeout=10, headers=HEADERS, verify=False)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extrai dados básicos
                title_tag = soup.find('title')
                title_text = title_tag.get_text().strip() if title_tag else ""
                
                description_tag = soup.find('meta', attrs={'name': 'description'})
                description_text = description_tag.get('content', '').strip() if description_tag else ""
                
                canonical_tag = soup.find('link', rel='canonical')
                canonical_url = canonical_tag.get('href', '').strip() if canonical_tag else ""
                
                robots_tag = soup.find('meta', attrs={'name': 'robots'})
                robots_text = robots_tag.get('content', '').strip() if robots_tag else ""
                
                viewport_tag = soup.find('meta', attrs={'name': 'viewport'})
                viewport_text = viewport_tag.get('content', '').strip() if viewport_tag else ""
                
                keywords_tag = soup.find('meta', attrs={'name': 'keywords'})
                keywords_text = keywords_tag.get('content', '').strip() if keywords_tag else ""
                
                # Análise SEO
                title_length = len(title_text)
                description_length = len(description_text)
                
                # Score de qualidade SEO
                title_score = self._calcular_score_title(title_text, title_length)
                description_score = self._calcular_score_description(description_text, description_length)
                canonical_status = self._analisar_canonical(canonical_url, url)
                
                # Detecta problemas SEO
                problemas = self._detectar_problemas_seo(
                    title_text, title_length,
                    description_text, description_length,
                    canonical_url, robots_text, viewport_text
                )
                
                return {
                    'URL': url,
                    'Title_Length': title_length,
                    'Title_SEO_Score': title_score,
                    'Title': title_text,
                    'Description_Length': description_length,
                    'Description_SEO_Score': description_score,
                    'Description': description_text,
                    'Canonical_Status': canonical_status,
                    'Canonical': canonical_url,
                    'Robots': robots_text,
                    'Viewport': viewport_text,
                    'Keywords': keywords_text,
                    'Problemas_SEO': problemas
                }
                
            except Exception as e:
                return {
                    'URL': url,
                    'Title_Length': 0,
                    'Title_SEO_Score': 'ERRO',
                    'Title': f"ERRO: {str(e)}",
                    'Description_Length': 0,
                    'Description_SEO_Score': 'ERRO',
                    'Description': "",
                    'Canonical_Status': 'ERRO',
                    'Canonical': "",
                    'Robots': "",
                    'Viewport': "",
                    'Keywords': "",
                    'Problemas_SEO': [f"Erro de acesso: {str(e)}"]
                }
        
        # Execução paralela
        resultados = []
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            future_to_url = {executor.submit(extrair_metatags_url, url): url for url in urls}
            
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress indicator a cada 50 URLs
                if len(resultados) % 50 == 0:
                    print(f"      📊 {len(resultados)}/{len(urls)} metatags processadas...")
        
        return resultados
    
    def _calcular_score_title(self, title, length):
        """📏 Calcula score de qualidade do title para SEO"""
        if not title:
            return "AUSENTE"
        
        if length < 10:
            return "MUITO_CURTO"
        elif length < 30:
            return "CURTO"
        elif length <= 60:
            return "IDEAL"
        elif length <= 70:
            return "BOM"
        else:
            return "MUITO_LONGO"
    
    def _calcular_score_description(self, description, length):
        """📝 Calcula score de qualidade da description para SEO"""
        if not description:
            return "AUSENTE"
        
        if length < 50:
            return "MUITO_CURTA"
        elif length < 120:
            return "CURTA"
        elif length <= 160:
            return "IDEAL"
        elif length <= 180:
            return "BOA"
        else:
            return "MUITO_LONGA"
    
    def _analisar_canonical(self, canonical_url, original_url):
        """🔗 Analisa status do canonical"""
        if not canonical_url:
            return "AUSENTE"
        
        # Normaliza URLs para comparação
        canonical_limpo = canonical_url.lower().rstrip('/')
        original_limpo = original_url.lower().rstrip('/')
        
        if canonical_limpo == original_limpo:
            return "AUTO_REFERENCIA"
        elif canonical_url.startswith('http'):
            return "EXTERNO"
        else:
            return "RELATIVO"
    
    def _detectar_problemas_seo(self, title, title_len, description, desc_len, canonical, robots, viewport):
        """🚨 Detecta problemas específicos de SEO"""
        problemas = []
        
        # Problemas de Title
        if not title:
            problemas.append("Title ausente")
        elif title_len > 70:
            problemas.append("Title muito longo (>70 chars)")
        elif title_len < 10:
            problemas.append("Title muito curto (<10 chars)")
        elif title.lower() in ['home', 'página inicial', 'untitled', 'document']:
            problemas.append("Title genérico")
        
        # Problemas de Description
        if not description:
            problemas.append("Description ausente")
        elif desc_len > 180:
            problemas.append("Description muito longa (>180 chars)")
        elif desc_len < 50:
            problemas.append("Description muito curta (<50 chars)")
        
        # Problemas de Canonical
        if not canonical:
            problemas.append("Canonical ausente")
        
        # Problemas de Robots
        if robots and any(directive in robots.lower() for directive in ['noindex', 'nofollow']):
            problemas.append(f"Robots restritivo: {robots}")
        
        # Problemas de Viewport
        if not viewport:
            problemas.append("Viewport ausente")
        elif 'width=device-width' not in viewport:
            problemas.append("Viewport não responsivo")
        
        # Título e descrição idênticos (problema comum)
        if title and description and title.strip() == description.strip():
            problemas.append("Title = Description (duplicação)")
        
        return problemas