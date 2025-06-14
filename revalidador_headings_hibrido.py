# revalidador_headings_hibrido.py - VERSÃO CIRÚRGICA FINAL

import requests
import pandas as pd
from bs4 import BeautifulSoup
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

# 🔧 CONFIGURAÇÕES OTIMIZADAS
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
}

def calcular_threads_auto():
    """🧠 Calcula threads automaticamente baseado no hardware"""
    num_cores = multiprocessing.cpu_count()
    threads_otimo = min(10 * num_cores, 100)
    print(f"🧠 Hardware detectado: {num_cores} cores → {threads_otimo} threads")
    return threads_otimo

def heading_realmente_vazio(tag):
    """🎯 CRITÉRIO CIRÚRGICO À PROVA DE BOMBA: Tem qualquer caractere renderizável? NÃO É vazio."""
    if tag is None:
        return True
    
    try:
        # Pega o texto renderizável (sem tags HTML)
        texto_renderizado = tag.get_text()
        
        # Remove apenas espaços em branco (mantém hífen, emoji, pontuação, etc.)
        texto_limpo = texto_renderizado.strip()
        
        # Se sobrou qualquer coisa = NÃO É VAZIO
        return len(texto_limpo) == 0
        
    except:
        # Se falhou no parsing, considera vazio (proteção à prova de bomba)
        return True

class RevalidadorHeadingsHibridoCirurgico:
    """🎯 Revalidador CIRÚRGICO - Critério exato sem falsos positivos"""
    
    def __init__(self):
        self.stats = {
            'urls_processadas': 0,
            'urls_com_sucesso': 0,
            'urls_filtradas': 0,
            'headings_vazios_encontrados': 0,
            'erros': 0,
            'tempo_total': 0
        }
    
    def revalidar_excel_completo(self, excel_path: str, output_path: str = None):
        """🔄 Revalida Excel - VERSÃO CIRÚRGICA"""
        
        print(f"🔄 REVALIDADOR CIRÚRGICO - Iniciando...")
        print(f"📁 Arquivo: {excel_path}")
        
        start_time = time.time()
        
        try:
            # Lê Excel existente
            df = pd.read_excel(excel_path, sheet_name='Resumo')
            print(f"📊 URLs carregadas: {len(df)}")
            
            # Filtra URLs válidas
            urls_validas = self._filtrar_urls_validas(df)
            print(f"📋 URLs para revalidação: {len(urls_validas)} (filtradas: {len(df) - len(urls_validas)})")
            
            # Calcula threads otimizados
            max_threads = calcular_threads_auto()
            
            # Revalida headings em paralelo
            resultados_headings = self._revalidar_headings_paralelo(urls_validas, max_threads)
            
            # Gera aba headings_vazios
            aba_headings_vazios = self._gerar_aba_headings_vazios(resultados_headings)
            
            # Salva Excel atualizado
            output_final = output_path or excel_path.replace('.xlsx', '_CIRURGICO.xlsx')
            self._atualizar_excel_com_nova_aba(excel_path, output_final, aba_headings_vazios)
            
            # Calcula estatísticas finais
            self.stats['tempo_total'] = time.time() - start_time
            
            # Exibe estatísticas
            self._exibir_estatisticas_finais()
            
            return output_final
            
        except Exception as e:
            print(f"❌ Erro no revalidador: {e}")
            raise e
    
    def _filtrar_urls_validas(self, df: pd.DataFrame) -> list:
        """🚫 Filtro básico - Remove só o que realmente não vale a pena"""
        
        urls_validas = []
        
        for _, row in df.iterrows():
            url = row.get('url', '')
            
            # Filtros básicos
            if not url or not isinstance(url, str):
                self.stats['urls_filtradas'] += 1
                continue
            
            # Remove extensões não-HTML
            extensoes_bloqueadas = (
                '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.doc', '.xls', '.zip', '.js', '.css'
            )
            if url.lower().endswith(extensoes_bloqueadas):
                self.stats['urls_filtradas'] += 1
                continue
            
            # Remove paginações
            if '?page=' in url or '&page=' in url:
                self.stats['urls_filtradas'] += 1
                continue
            
            # Só URLs 200
            status = row.get('status_code_http', row.get('status_code', None))
            if status is not None:
                try:
                    status_num = int(float(status))
                    if status_num != 200:
                        self.stats['urls_filtradas'] += 1
                        continue
                except:
                    self.stats['urls_filtradas'] += 1
                    continue
            
            urls_validas.append(url)
        
        return urls_validas
    
    def _revalidar_headings_paralelo(self, urls: list, max_threads: int) -> list:
        """🔄 Revalidação em paralelo"""
        
        print(f"🔄 Revalidando com {max_threads} threads...")
        
        resultados = []
        
        # Sessão reutilizável
        session = requests.Session()
        session.headers.update(HEADERS)
        
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            # Submete tasks
            future_to_url = {
                executor.submit(self._revalidar_heading_url_cirurgico, url, session): url 
                for url in urls
            }
            
            # Processa resultados com progress bar
            for future in tqdm(as_completed(future_to_url), total=len(urls), desc="🎯 Revalidação Cirúrgica"):
                url = future_to_url[future]
                try:
                    resultado = future.result()
                    resultados.append(resultado)
                    self.stats['urls_processadas'] += 1
                    
                    if resultado.get('sucesso', False):
                        self.stats['urls_com_sucesso'] += 1
                        self.stats['headings_vazios_encontrados'] += resultado.get('headings_vazios_count', 0)
                    else:
                        self.stats['erros'] += 1
                        
                except Exception as e:
                    self.stats['erros'] += 1
                    resultados.append({
                        'url': url,
                        'sucesso': False,
                        'erro': str(e),
                        'headings_vazios_count': 0,
                        'headings_problematicos': []
                    })
        
        session.close()
        return resultados
    
    def _revalidar_heading_url_cirurgico(self, url: str, session: requests.Session) -> dict:
        """🎯 Revalidação CIRÚRGICA de uma URL"""
        
        try:
            # Request
            response = session.get(url, timeout=(5, 15), verify=False, allow_redirects=True)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Analisa headings com critério CIRÚRGICO
            headings_problematicos = []
            headings_vazios_count = 0
            
            # Loop pelos headings
            for i in range(1, 7):  # h1 até h6
                tags_heading = soup.find_all(f'h{i}')
                
                for posicao, tag in enumerate(tags_heading, 1):
                    
                    # 🎯 CRITÉRIO CIRÚRGICO
                    if heading_realmente_vazio(tag):
                        headings_vazios_count += 1
                        
                        # Pega o HTML original para debug
                        html_original = str(tag)
                        
                        # Contexto do pai
                        contexto_pai = self._extrair_contexto_pai(tag)
                        
                        # Atributos
                        atributos = self._extrair_atributos_heading(tag)
                        
                        headings_problematicos.append({
                            'tag': f'h{i}',
                            'posicao': posicao,
                            'contexto_pai': contexto_pai,
                            'atributos_heading': atributos,
                            'html_original': html_original[:200],  # Primeiros 200 chars
                            'texto_extraido': tag.get_text() if tag else '',
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
        """📊 Exibe estatísticas CIRÚRGICAS"""
        
        print(f"\n📊 ESTATÍSTICAS CIRÚRGICAS:")
        print(f"   📈 URLs processadas: {self.stats['urls_processadas']}")
        print(f"   🚫 URLs filtradas: {self.stats['urls_filtradas']}")
        print(f"   ✅ URLs com sucesso: {self.stats['urls_com_sucesso']}")
        print(f"   ❌ Erros: {self.stats['erros']}")
        print(f"   🎯 Headings REALMENTE vazios: {self.stats['headings_vazios_encontrados']}")
        print(f"   ⏰ Tempo total: {self.stats['tempo_total']:.1f}s")
        
        if self.stats['urls_processadas'] > 0:
            taxa_sucesso = (self.stats['urls_com_sucesso'] / self.stats['urls_processadas']) * 100
            urls_por_segundo = self.stats['urls_processadas'] / self.stats['tempo_total']
            print(f"   📊 Taxa de sucesso: {taxa_sucesso:.1f}%")
            print(f"   🚀 Velocidade: {urls_por_segundo:.1f} URLs/segundo")

# ========================
# 🚀 FUNÇÃO STANDALONE
# ========================

def revalidar_headings_excel_cirurgico(excel_path: str, output_path: str = None):
    """🚀 Função standalone CIRÚRGICA"""
    
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
        print(f"🎉 REVALIDAÇÃO CIRÚRGICA CONCLUÍDA!")
        print(f"📁 Arquivo final: {arquivo_final}")
        print(f"🎯 CRITÉRIO: Só headings REALMENTE vazios (sem qualquer caractere renderizável)")
    except Exception as e:
        print(f"💥 Erro: {e}")
        sys.exit(1)