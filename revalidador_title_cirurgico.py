# revalidador_title_cirurgico.py - VALIDADOR CIRÚRGICO DE TITLES

import requests
import pandas as pd
from bs4 import BeautifulSoup
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

# 🔧 CONFIGURAÇÕES
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
}

def calcular_threads_auto():
    """🧠 Calcula threads automaticamente"""
    num_cores = multiprocessing.cpu_count()
    threads_otimo = min(10 * num_cores, 100)
    print(f"🧠 Hardware detectado: {num_cores} cores → {threads_otimo} threads")
    return threads_otimo

def validar_title_cirurgico(soup):
    """🎯 VALIDAÇÃO CIRÚRGICA DO TITLE - Zero heurística, só realidade do DOM"""
    
    try:
        # Procura a tag <title> no DOM
        title_tag = soup.find('title')
        
        if title_tag is None:
            # 🚨 NÃO EXISTE TAG <title> no HTML
            return {
                'tem_problema': True,
                'tipo_problema': 'TAG_AUSENTE',
                'gravidade': 'CRITICO',
                'title_encontrado': None,
                'title_texto': '',
                'html_title_tag': 'TAG NÃO ENCONTRADA',
                'descricao': 'Tag <title> não existe no HTML',
                'recomendacao': 'Adicionar tag <title> no <head> da página'
            }
        
        # Pega o texto da tag
        title_texto = title_tag.get_text().strip()
        
        if not title_texto:
            # 🚨 TAG EXISTE MAS ESTÁ VAZIA
            return {
                'tem_problema': True,
                'tipo_problema': 'TITLE_VAZIO',
                'gravidade': 'CRITICO',
                'title_encontrado': str(title_tag),
                'title_texto': '',
                'html_title_tag': str(title_tag)[:200],
                'descricao': 'Tag <title> existe mas está vazia',
                'recomendacao': 'Preencher conteúdo da tag <title>'
            }
        
        # ✅ TITLE VÁLIDO
        return {
            'tem_problema': False,
            'tipo_problema': 'VALIDO',
            'gravidade': 'OK',
            'title_encontrado': str(title_tag),
            'title_texto': title_texto,
            'html_title_tag': str(title_tag)[:200],
            'descricao': 'Title válido encontrado',
            'recomendacao': 'Nenhuma ação necessária'
        }
        
    except Exception as e:
        # 🛡️ PROTEÇÃO À PROVA DE BOMBA
        return {
            'tem_problema': True,
            'tipo_problema': 'ERRO_PARSING',
            'gravidade': 'MEDIO',
            'title_encontrado': None,
            'title_texto': '',
            'html_title_tag': f'ERRO: {str(e)}',
            'descricao': f'Erro no parsing HTML: {str(e)}',
            'recomendacao': 'Verificar HTML da página'
        }

class RevalidadorTitleCirurgico:
    """🎯 Revalidador CIRÚRGICO de Titles - Só realidade do DOM"""
    
    def __init__(self):
        self.stats = {
            'urls_processadas': 0,
            'urls_com_sucesso': 0,
            'urls_filtradas': 0,
            'tags_ausentes': 0,
            'titles_vazios': 0,
            'erros_parsing': 0,
            'titles_validos': 0,
            'erros': 0,
            'tempo_total': 0
        }
    
    def revalidar_excel_completo(self, excel_path: str, output_path: str = None):
        """🔄 Revalida titles no Excel - VERSÃO CIRÚRGICA"""
        
        print(f"🔄 REVALIDADOR TITLE CIRÚRGICO - Iniciando...")
        print(f"📁 Arquivo: {excel_path}")
        
        start_time = time.time()
        
        try:
            # Lê Excel existente
            df = pd.read_excel(excel_path, sheet_name='Resumo')
            print(f"📊 URLs carregadas: {len(df)}")
            
            # Filtra URLs válidas
            urls_validas = self._filtrar_urls_validas(df)
            print(f"📋 URLs para revalidação: {len(urls_validas)} (filtradas: {len(df) - len(urls_validas)})")
            
            # Calcula threads
            max_threads = calcular_threads_auto()
            
            # Revalida titles em paralelo
            resultados_titles = self._revalidar_titles_paralelo(urls_validas, max_threads)
            
            # Gera aba title_ausente
            aba_title_ausente = self._gerar_aba_title_ausente(resultados_titles)
            
            # Salva Excel atualizado
            output_final = output_path or excel_path.replace('.xlsx', '_TITLE_CIRURGICO.xlsx')
            self._atualizar_excel_com_nova_aba(excel_path, output_final, aba_title_ausente)
            
            # Estatísticas finais
            self.stats['tempo_total'] = time.time() - start_time
            self._exibir_estatisticas_finais()
            
            return output_final
            
        except Exception as e:
            print(f"❌ Erro no revalidador: {e}")
            raise e
    
    def _filtrar_urls_validas(self, df: pd.DataFrame) -> list:
        """🚫 Filtro básico de URLs válidas"""
        
        urls_validas = []
        
        for _, row in df.iterrows():
            url = row.get('url', '')
            
            # Filtros básicos
            if not url or not isinstance(url, str):
                self.stats['urls_filtradas'] += 1
                continue
            
            # Remove extensões não-HTML
            extensoes_bloqueadas = (
                '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.doc', '.xls', 
                '.zip', '.js', '.css', '.mp3', '.mp4', '.xml', '.json'
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
    
    def _revalidar_titles_paralelo(self, urls: list, max_threads: int) -> list:
        """🔄 Revalidação de titles em paralelo"""
        
        print(f"🔄 Revalidando titles com {max_threads} threads...")
        
        resultados = []
        
        # Sessão reutilizável
        session = requests.Session()
        session.headers.update(HEADERS)
        
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            # Submete tasks
            future_to_url = {
                executor.submit(self._revalidar_title_url, url, session): url 
                for url in urls
            }
            
            # Processa resultados
            for future in tqdm(as_completed(future_to_url), total=len(urls), desc="🎯 Validação Title Cirúrgica"):
                url = future_to_url[future]
                try:
                    resultado = future.result()
                    resultados.append(resultado)
                    self.stats['urls_processadas'] += 1
                    
                    if resultado.get('sucesso', False):
                        self.stats['urls_com_sucesso'] += 1
                        
                        # Conta tipos de problemas
                        tipo = resultado.get('validacao', {}).get('tipo_problema', 'VALIDO')
                        if tipo == 'TAG_AUSENTE':
                            self.stats['tags_ausentes'] += 1
                        elif tipo == 'TITLE_VAZIO':
                            self.stats['titles_vazios'] += 1
                        elif tipo == 'ERRO_PARSING':
                            self.stats['erros_parsing'] += 1
                        elif tipo == 'VALIDO':
                            self.stats['titles_validos'] += 1
                    else:
                        self.stats['erros'] += 1
                        
                except Exception as e:
                    self.stats['erros'] += 1
                    resultados.append({
                        'url': url,
                        'sucesso': False,
                        'erro': str(e),
                        'validacao': {
                            'tem_problema': True,
                            'tipo_problema': 'ERRO_REQUEST',
                            'gravidade': 'MEDIO'
                        }
                    })
        
        session.close()
        return resultados
    
    def _revalidar_title_url(self, url: str, session: requests.Session) -> dict:
        """🎯 Revalidação CIRÚRGICA de title em uma URL"""
        
        try:
            # Request
            response = session.get(url, timeout=(5, 15), verify=False, allow_redirects=True)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Validação CIRÚRGICA
            validacao = validar_title_cirurgico(soup)
            
            return {
                'url': url,
                'sucesso': True,
                'validacao': validacao
            }
            
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'validacao': {
                    'tem_problema': True,
                    'tipo_problema': 'ERRO_REQUEST',
                    'gravidade': 'MEDIO',
                    'title_encontrado': None,
                    'title_texto': '',
                    'html_title_tag': f'ERRO REQUEST: {str(e)}',
                    'descricao': f'Erro na requisição: {str(e)}',
                    'recomendacao': 'Verificar conectividade com a URL'
                }
            }
    
    def _gerar_aba_title_ausente(self, resultados: list) -> pd.DataFrame:
        """📋 Gera DataFrame para aba Title_Ausente"""
        
        rows = []
        
        for resultado in resultados:
            if not resultado.get('sucesso', False):
                continue
            
            validacao = resultado.get('validacao', {})
            
            # Só adiciona se tem problema
            if validacao.get('tem_problema', False):
                url = resultado['url']
                
                rows.append({
                    'URL': url,
                    'Tipo_Problema': validacao.get('tipo_problema', 'DESCONHECIDO'),
                    'Gravidade': validacao.get('gravidade', 'MEDIO'),
                    'Title_Encontrado': validacao.get('title_texto', ''),
                    'HTML_Title_Tag': validacao.get('html_title_tag', ''),
                    'Descricao': validacao.get('descricao', ''),
                    'Recomendacao': validacao.get('recomendacao', '')
                })
        
        if not rows:
            # DataFrame vazio se não há problemas
            return pd.DataFrame(columns=[
                'URL', 'Tipo_Problema', 'Gravidade', 'Title_Encontrado',
                'HTML_Title_Tag', 'Descricao', 'Recomendacao'
            ])
        
        df = pd.DataFrame(rows)
        
        # Ordenação por gravidade
        gravidade_order = {'CRITICO': 1, 'ALTO': 2, 'MEDIO': 3, 'BAIXO': 4}
        df['sort_gravidade'] = df['Gravidade'].map(gravidade_order).fillna(99)
        df = df.sort_values(['sort_gravidade', 'URL'])
        df = df.drop('sort_gravidade', axis=1)
        
        return df
    
    def _atualizar_excel_com_nova_aba(self, excel_original: str, excel_saida: str, df_titles: pd.DataFrame):
        """📤 Atualiza Excel com nova aba Title_Ausente"""
        
        try:
            # Lê todas as abas do Excel original
            with pd.ExcelFile(excel_original) as xls:
                dict_dfs = {}
                for sheet_name in xls.sheet_names:
                    if sheet_name != 'Title_Ausente':  # Remove aba antiga se existir
                        dict_dfs[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name)
            
            # Adiciona nova aba title_ausente
            dict_dfs['Title_Ausente'] = df_titles
            
            # Salva Excel atualizado
            with pd.ExcelWriter(excel_saida, engine='xlsxwriter') as writer:
                for sheet_name, df in dict_dfs.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"✅ Excel atualizado: {excel_saida}")
            print(f"📋 Aba 'Title_Ausente' criada com {len(df_titles)} problemas REAIS")
            
        except Exception as e:
            print(f"❌ Erro atualizando Excel: {e}")
            # Fallback: salva só a aba
            df_titles.to_excel(excel_saida.replace('.xlsx', '_title_ausente_only.xlsx'), index=False)
    
    def _exibir_estatisticas_finais(self):
        """📊 Exibe estatísticas CIRÚRGICAS"""
        
        print(f"\n📊 ESTATÍSTICAS TITLE CIRÚRGICO:")
        print(f"   📈 URLs processadas: {self.stats['urls_processadas']}")
        print(f"   🚫 URLs filtradas: {self.stats['urls_filtradas']}")
        print(f"   ✅ URLs com sucesso: {self.stats['urls_com_sucesso']}")
        print(f"   ❌ Erros de request: {self.stats['erros']}")
        print(f"   🚨 Tags <title> ausentes: {self.stats['tags_ausentes']}")
        print(f"   🕳️ Titles vazios: {self.stats['titles_vazios']}")
        print(f"   ⚠️ Erros de parsing: {self.stats['erros_parsing']}")
        print(f"   ✅ Titles válidos: {self.stats['titles_validos']}")
        print(f"   ⏰ Tempo total: {self.stats['tempo_total']:.1f}s")
        
        total_problemas = self.stats['tags_ausentes'] + self.stats['titles_vazios'] + self.stats['erros_parsing']
        print(f"   🎯 Total de problemas REAIS: {total_problemas}")
        
        if self.stats['urls_processadas'] > 0:
            taxa_sucesso = (self.stats['urls_com_sucesso'] / self.stats['urls_processadas']) * 100
            urls_por_segundo = self.stats['urls_processadas'] / self.stats['tempo_total']
            print(f"   📊 Taxa de sucesso: {taxa_sucesso:.1f}%")
            print(f"   🚀 Velocidade: {urls_por_segundo:.1f} URLs/segundo")

# ========================
# 🚀 FUNÇÃO STANDALONE
# ========================

def revalidar_titles_excel_cirurgico(excel_path: str, output_path: str = None):
    """🚀 Função standalone CIRÚRGICA para titles"""
    
    revalidador = RevalidadorTitleCirurgico()
    return revalidador.revalidar_excel_completo(excel_path, output_path)

# ========================
# 🧪 TESTE STANDALONE
# ========================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("❌ Uso: python revalidador_title_cirurgico.py <arquivo.xlsx> [arquivo_saida.xlsx]")
        sys.exit(1)
    
    excel_input = sys.argv[1]
    excel_output = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        arquivo_final = revalidar_titles_excel_cirurgico(excel_input, excel_output)
        print(f"🎉 REVALIDAÇÃO TITLE CIRÚRGICA CONCLUÍDA!")
        print(f"📁 Arquivo final: {arquivo_final}")
        print(f"🎯 CRITÉRIO: Só titles REALMENTE ausentes ou vazios (DOM puro)")
    except Exception as e:
        print(f"💥 Erro: {e}")
        sys.exit(1)