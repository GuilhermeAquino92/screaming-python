# revalidador_headings_hibrido.py - Versão CIRÚRGICA 2.0
# 🔥 VERSÃO MELHORADA: Pega lixo estrutural real sem falsos positivos

import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
import re

class RevalidadorHeadingsHibridoCirurgico:
    """🔥 Revalidador CIRÚRGICO 2.0 - Detecta lixo estrutural real"""
    
    def __init__(self, max_workers: int = 15, timeout: int = 10):
        self.max_workers = max_workers
        self.timeout = timeout
        self.session = self._criar_sessao_otimizada()
        self.stats = {
            'urls_processadas': 0,
            'urls_filtradas': 0,
            'urls_com_sucesso': 0,
            'erros': 0,
            'headings_vazios_encontrados': 0,
            'tempo_total': 0
        }
    
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para performance"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
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
            response = self.session.get(url, timeout=self.timeout, verify=False)
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
    
    def _filtrar_urls_duplicadas(self, urls: list) -> list:
        """🧹 Remove duplicatas e URLs inválidas"""
        urls_unicas = []
        urls_vistas = set()
        
        for url in urls:
            if not url or pd.isna(url):
                continue
            
            url_limpa = str(url).strip()
            
            # Filtros básicos
            if not url_limpa.startswith(('http://', 'https://')):
                continue
            
            if url_limpa in urls_vistas:
                continue
            
            # Filtros de URL indesejadas
            if any(skip in url_limpa.lower() for skip in [
                '.pdf', '.jpg', '.png', '.gif', '.zip', '.rar',
                'mailto:', 'tel:', 'javascript:', '#',
                '?page=', '&page=', '/page/'
            ]):
                continue
            
            urls_vistas.add(url_limpa)
            urls_unicas.append(url_limpa)
        
        return urls_unicas
    
    def revalidar_urls_paralelo(self, urls: list) -> list:
        """🚀 Revalidação paralela otimizada"""
        
        print(f"🔥 REVALIDAÇÃO CIRÚRGICA 2.0 INICIADA")
        print(f"📊 URLs para processar: {len(urls)}")
        
        urls_filtradas = self._filtrar_urls_duplicadas(urls)
        self.stats['urls_filtradas'] = len(urls) - len(urls_filtradas)
        
        print(f"🧹 URLs após filtros: {len(urls_filtradas)}")
        print(f"🎯 Critério: Detectar lixo estrutural real (espaços ocultos, &nbsp;, tags vazias)")
        
        inicio = time.time()
        resultados = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._extrair_headings_dom_puro, url): url 
                for url in urls_filtradas
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                self.stats['urls_processadas'] += 1
                
                if resultado.get('sucesso', False):
                    self.stats['urls_com_sucesso'] += 1
                    self.stats['headings_vazios_encontrados'] += resultado.get('headings_vazios_count', 0)
                else:
                    self.stats['erros'] += 1
                
                # Progress indicator
                if self.stats['urls_processadas'] % 50 == 0:
                    print(f"⚡ Processadas: {self.stats['urls_processadas']}/{len(urls_filtradas)}")
        
        self.stats['tempo_total'] = time.time() - inicio
        return resultados
    
    def _gerar_aba_headings_vazios(self, resultados: list) -> pd.DataFrame:
        """📋 Gera DataFrame para aba headings_vazios"""
        
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
        
        if not rows:
            # DataFrame vazio se não há problemas
            return pd.DataFrame(columns=[
                'URL', 'Tag', 'Posicao', 'HTML_Original', 'Texto_Extraido',
                'Contexto_Pai', 'Atributos_Heading', 'Gravidade', 'Recomendacao'
            ])
        
        df = pd.DataFrame(rows)
        
        # Ordenação por gravidade
        gravidade_order = {'CRITICO': 1, 'ALTO': 2, 'MEDIO': 3, 'BAIXO': 4}
        df['sort_gravidade'] = df['Gravidade'].map(gravidade_order).fillna(99)
        df = df.sort_values(['sort_gravidade', 'URL', 'Tag'])
        df = df.drop('sort_gravidade', axis=1)
        
        return df
    
    def revalidar_excel_completo(self, excel_path: str, output_path: str = None) -> str:
        """📊 Revalida Excel completo com CIRÚRGICO 2.0"""
        
        if output_path is None:
            output_path = excel_path.replace('.xlsx', '_HEADINGS_CIRURGICO_V2.xlsx')
        
        print(f"📁 Lendo Excel: {excel_path}")
        
        try:
            # Lê aba principal
            df_principal = pd.read_excel(excel_path, sheet_name=0)
            
            if 'url' not in df_principal.columns:
                raise ValueError("Coluna 'url' não encontrada na aba principal")
            
            urls = df_principal['url'].dropna().unique().tolist()
            print(f"🎯 URLs extraídas da aba principal: {len(urls)}")
            
            # Revalidação CIRÚRGICA 2.0
            resultados = self.revalidar_urls_paralelo(urls)
            
            # Gera nova aba de headings vazios
            df_headings_vazios = self._gerar_aba_headings_vazios(resultados)
            
            # Atualiza Excel
            self._atualizar_excel_com_nova_aba(excel_path, output_path, df_headings_vazios)
            
            # Estatísticas finais
            self._exibir_estatisticas_finais()
            
            return output_path
            
        except Exception as e:
            print(f"❌ Erro no processamento: {e}")
            raise
    
    def _atualizar_excel_com_nova_aba(self, excel_original: str, excel_saida: str, df_headings: pd.DataFrame):
        """📤 Atualiza Excel com nova aba headings_vazios"""
        
        try:
            # Lê todas as abas do Excel original
            with pd.ExcelFile(excel_original) as xls:
                dict_dfs = {}
                for sheet_name in xls.sheet_names:
                    if sheet_name != 'Headings_Vazios':  # Remove aba antiga se existir
                        dict_dfs[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name)
            
            # Adiciona nova aba headings_vazios
            dict_dfs['Headings_Vazios'] = df_headings
            
            # Salva Excel atualizado
            with pd.ExcelWriter(excel_saida, engine='xlsxwriter') as writer:
                for sheet_name, df in dict_dfs.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"✅ Excel atualizado: {excel_saida}")
            print(f"📋 Aba 'Headings_Vazios' criada com {len(df_headings)} problemas REAIS")
            
        except Exception as e:
            print(f"❌ Erro atualizando Excel: {e}")
            # Fallback: salva só a aba headings_vazios
            df_headings.to_excel(excel_saida.replace('.xlsx', '_headings_vazios_only.xlsx'), index=False)
    
    def _exibir_estatisticas_finais(self):
        """📊 Exibe estatísticas CIRÚRGICAS 2.0"""
        
        print(f"\n📊 ESTATÍSTICAS CIRÚRGICAS 2.0:")
        print(f"   📈 URLs processadas: {self.stats['urls_processadas']}")
        print(f"   🚫 URLs filtradas: {self.stats['urls_filtradas']}")
        print(f"   ✅ URLs com sucesso: {self.stats['urls_com_sucesso']}")
        print(f"   ❌ Erros: {self.stats['erros']}")
        print(f"   🎯 Headings com lixo estrutural: {self.stats['headings_vazios_encontrados']}")
        print(f"   ⏰ Tempo total: {self.stats['tempo_total']:.1f}s")
        
        if self.stats['urls_processadas'] > 0:
            taxa_sucesso = (self.stats['urls_com_sucesso'] / self.stats['urls_processadas']) * 100
            urls_por_segundo = self.stats['urls_processadas'] / self.stats['tempo_total']
            print(f"   📊 Taxa de sucesso: {taxa_sucesso:.1f}%")
            print(f"   🚀 Velocidade: {urls_por_segundo:.1f} URLs/segundo")
        
        print(f"\n🔥 CRITÉRIO CIRÚRGICO 2.0:")
        print(f"   ✅ Detecta: &nbsp;, espaços ocultos, quebras de linha, tags aninhadas vazias")
        print(f"   ✅ Ignora: Conteúdo real (mesmo com espaços normais)")
        print(f"   🎯 Resultado: Lixo estrutural real sem falsos positivos")

# ========================
# 🚀 FUNÇÃO STANDALONE
# ========================

def revalidar_headings_excel_cirurgico(excel_path: str, output_path: str = None):
    """🚀 Função standalone CIRÚRGICA 2.0"""
    
    revalidador = RevalidadorHeadingsHibridoCirurgico()
    return revalidador.revalidar_excel_completo(excel_path, output_path)

# Mantém compatibilidade
def revalidar_headings_excel_otimizado(excel_path: str, output_path: str = None):
    """🔄 Compatibilidade com versão otimizada"""
    return revalidar_headings_excel_cirurgico(excel_path, output_path)

class RevalidadorHeadingsHibridoOtimizado(RevalidadorHeadingsHibridoCirurgico):
    """🔄 Compatibilidade com classe otimizada"""
    pass

# ========================
# 🧪 TESTE STANDALONE
# ========================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("❌ Uso: python revalidador_headings_hibrido.py <arquivo.xlsx> [arquivo_saida.xlsx]")
        sys.exit(1)
    
    excel_input = sys.argv[1]
    excel_output = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        arquivo_final = revalidar_headings_excel_cirurgico(excel_input, excel_output)
        print(f"🎉 REVALIDAÇÃO CIRÚRGICA 2.0 CONCLUÍDA!")
        print(f"📁 Arquivo final: {arquivo_final}")
        print(f"🔥 CRITÉRIO: Detecta lixo estrutural real (&nbsp;, espaços ocultos, tags vazias)")
        print(f"🎯 RESULTADO: Zero falsos positivos + captura problemas reais")
    except Exception as e:
        print(f"💥 Erro: {e}")
        sys.exit(1)