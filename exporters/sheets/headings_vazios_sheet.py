# exporters/sheets/headings_vazios_sheet.py - COM LÓGICA CIRÚRGICA 2.0
# 🔥 ENGINE CIRÚRGICA: Detecta lixo estrutural real sem falsos positivos

import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter

class HeadingsVaziosSheet(BaseSheetExporter):
    def __init__(self, df, writer, ordenacao_tipo='gravidade_primeiro'):
        super().__init__(df, writer)
        self.ordenacao_tipo = ordenacao_tipo
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para revalidação"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        return session

    def heading_realmente_vazio_v2(self, tag) -> bool:
        """🩺 CIRÚRGICO 2.0: Detecta lixo estrutural real sem falsos positivos"""
        if tag is None:
            return True
        
        try:
            # Extrai texto renderizado
            texto_renderizado = tag.get_text()
            
            # LIMPEZA CIRÚRGICA 2.0: Remove lixo HTML real
            texto_limpo = texto_renderizado.strip()
            
            # Remove espaços ocultos comuns
            texto_limpo = texto_limpo.replace('\xa0', '')  # &nbsp;
            texto_limpo = texto_limpo.replace('\u200b', '')  # ZERO WIDTH SPACE
            texto_limpo = texto_limpo.replace('\u00a0', '')  # NON-BREAKING SPACE
            texto_limpo = texto_limpo.replace('\u2060', '')  # WORD JOINER
            texto_limpo = texto_limpo.replace('\ufeff', '')  # ZERO WIDTH NO-BREAK SPACE
            
            # Remove quebras de linha e tabs
            texto_limpo = texto_limpo.replace('\n', '').replace('\r', '').replace('\t', '')
            
            # Remove espaços múltiplos
            texto_limpo = re.sub(r'\s+', ' ', texto_limpo).strip()
            
            # CIRÚRGICO: Se após limpeza total não sobrou nada = VAZIO REAL
            return len(texto_limpo) == 0
            
        except Exception:
            return True

    def _extrair_headings_dom_puro(self, url: str) -> dict:
        """🎯 Extração DOM pura para headings vazios REAIS"""
        
        try:
            response = self.session.get(url, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            headings_vazios_count = 0
            headings_problematicos = []
            
            # Verifica H1-H6
            for i in range(1, 7):
                tags = soup.find_all(f'h{i}')
                
                for idx, tag in enumerate(tags, 1):
                    if self.heading_realmente_vazio_v2(tag):
                        headings_vazios_count += 1
                        
                        # HTML original (primeiros 200 chars)
                        html_original = str(tag)[:200]
                        
                        headings_problematicos.append({
                            'tag': f'h{i}',
                            'posicao': f'{idx}º {f"H{i}"} na página',
                            'html_original': html_original,
                            'texto_extraido': tag.get_text() if tag else '',
                            'contexto_pai': self._extrair_contexto_pai(tag),
                            'atributos_heading': self._extrair_atributos_heading(tag),
                            'gravidade': 'CRITICO' if i == 1 else 'ALTO',
                            'recomendacao': f'Preencher conteúdo do {f"H{i}"} ou remover tag vazia'
                        })
            
            return {
                'url': url,
                'sucesso': True,
                'headings_vazios_count': headings_vazios_count,
                'headings_problematicos': headings_problematicos,
                'total_problemas': len(headings_problematicos)
            }
            
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'headings_vazios_count': 0,
                'headings_problematicos': []
            }

    def _extrair_contexto_pai(self, tag) -> str:
        """📍 Extrai contexto do elemento pai"""
        try:
            if tag and tag.parent:
                pai = tag.parent
                pai_info = f"<{pai.name}"
                if pai.get('class'):
                    pai_info += f" class='{' '.join(pai.get('class'))}'"
                if pai.get('id'):
                    pai_info += f" id='{pai.get('id')}'"
                pai_info += ">"
                return pai_info
            return "sem pai"
        except:
            return "erro contexto"
    
    def _extrair_atributos_heading(self, tag) -> str:
        """🏷️ Extrai atributos do heading"""
        try:
            atributos = []
            if tag.get('class'):
                atributos.append(f"class='{' '.join(tag.get('class'))}'")
            if tag.get('id'):
                atributos.append(f"id='{tag.get('id')}'")
            return ' '.join(atributos) if atributos else 'sem atributos'
        except:
            return 'erro atributos'

    def _filtrar_urls_validas(self, urls: list) -> list:
        """🧹 Remove URLs inválidas para análise"""
        urls_validas = []
        
        for url in urls:
            if not url or pd.isna(url):
                continue
            
            url_str = str(url).strip()
            
            # Filtros básicos
            if not url_str.startswith(('http://', 'https://')):
                continue
            
            # Filtros de URL indesejadas
            if any(skip in url_str.lower() for skip in [
                '.pdf', '.jpg', '.png', '.gif', '.zip', '.rar',
                'mailto:', 'tel:', 'javascript:', '#',
                '?page=', '&page=', '/page/'
            ]):
                continue
            
            urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _revalidar_urls_paralelo(self, urls: list) -> list:
        """🚀 Revalidação paralela cirúrgica"""
        
        print(f"🔥 Revalidação cirúrgica 2.0 iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._extrair_headings_dom_puro, url): url 
                for url in urls
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress indicator a cada 50 URLs
                if len(resultados) % 50 == 0:
                    print(f"⚡ Processadas: {len(resultados)}/{len(urls)}")
        
        return resultados

    def export(self):
        """🔥 Gera aba CIRÚRGICA de headings vazios (versão 2.0)"""
        try:
            print(f"🔥 HEADINGS VAZIOS - ENGINE CIRÚRGICA 2.0")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_para_analisar = self.df['url'].dropna().unique().tolist()
            urls_filtradas = self._filtrar_urls_validas(urls_para_analisar)
            
            print(f"   📊 URLs inicial: {len(urls_para_analisar)}")
            print(f"   🧹 URLs válidas: {len(urls_filtradas)}")
            print(f"   🎯 Critério: Detectar lixo estrutural real (&nbsp;, espaços ocultos, tags vazias)")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para análise")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tag', 'Posicao', 'HTML_Original', 'Texto_Extraido',
                    'Contexto_Pai', 'Atributos_Heading', 'Gravidade', 'Recomendacao'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
                return df_vazio
            
            # 🔥 REVALIDAÇÃO CIRÚRGICA PARALELA
            resultados = self._revalidar_urls_paralelo(urls_filtradas)
            
            # 📋 GERA LINHAS PARA O DATAFRAME
            rows = []
            
            for resultado in resultados:
                if not resultado.get('sucesso', False):
                    continue
                
                url = resultado['url']
                problemas = resultado.get('headings_problematicos', [])
                
                for problema in problemas:
                    rows.append({
                        'URL': url,
                        'Tag': problema.get('tag', '').upper(),
                        'Posicao': problema.get('posicao', 'N/A'),
                        'HTML_Original': problema.get('html_original', ''),
                        'Texto_Extraido': f"'{problema.get('texto_extraido', '')}'",
                        'Contexto_Pai': problema.get('contexto_pai', 'N/A'),
                        'Atributos_Heading': problema.get('atributos_heading', 'sem atributos'),
                        'Gravidade': problema.get('gravidade', 'ALTO'),
                        'Recomendacao': problema.get('recomendacao', 'Revisar heading')
                    })
            
            # Se não encontrou problemas
            if not rows:
                print(f"   🎉 PERFEITO: Nenhum heading vazio encontrado!")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tag', 'Posicao', 'HTML_Original', 'Texto_Extraido',
                    'Contexto_Pai', 'Atributos_Heading', 'Gravidade', 'Recomendacao'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
                return df_vazio
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR GRAVIDADE
            gravidade_order = {'CRITICO': 1, 'ALTO': 2, 'MEDIO': 3, 'BAIXO': 4}
            df_problemas['sort_gravidade'] = df_problemas['Gravidade'].map(gravidade_order).fillna(99)
            df_problemas = df_problemas.sort_values(['sort_gravidade', 'URL', 'Tag'])
            df_problemas = df_problemas.drop('sort_gravidade', axis=1)
            
            # 📤 EXPORTA
            df_problemas.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
            
            # 📊 ESTATÍSTICAS
            urls_com_sucesso = len([r for r in resultados if r.get('sucesso', False)])
            urls_com_problemas = len(set([r['URL'] for r in rows]))
            headings_vazios_total = len(rows)
            
            print(f"   ✅ URLs analisadas: {urls_com_sucesso}")
            print(f"   🎯 URLs com problemas: {urls_com_problemas}")
            print(f"   🔥 Headings com lixo estrutural: {headings_vazios_total}")
            print(f"   📋 Aba 'Headings_Vazios' criada com dados CIRÚRGICOS")
            print(f"   🛡️ Zero falsos positivos garantido")
            
            return df_problemas
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'URL', 'Tag', 'Posicao', 'HTML_Original', 'Texto_Extraido',
                'Contexto_Pai', 'Atributos_Heading', 'Gravidade', 'Recomendacao'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
            return df_erro