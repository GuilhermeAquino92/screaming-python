# exporters/sheets/h1_h2_problemas_sheet.py - COM ENGINE CIRÚRGICA
# 🔍 ENGINE CIRÚRGICA: Detecta duplicação real de textos H1/H2 entre páginas

import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from exporters.base_exporter import BaseSheetExporter

class H1H2ProblemasSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para análise de H1/H2"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        return session

    def _extrair_h1_h2_textos(self, url: str) -> dict:
        """🎯 Extrai textos reais de H1 e H2 via DOM"""
        
        try:
            response = self.session.get(url, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extrai todos os H1 e H2 com texto válido
            h1_textos = []
            h2_textos = []
            
            # H1s
            for h1_tag in soup.find_all('h1'):
                texto = h1_tag.get_text().strip()
                if texto and len(texto) > 0:  # Só textos não vazios
                    # Limpa texto (remove quebras excessivas, espaços múltiplos)
                    texto_limpo = ' '.join(texto.split())
                    if len(texto_limpo) > 2:  # Ignora textos muito curtos
                        h1_textos.append(texto_limpo)
            
            # H2s
            for h2_tag in soup.find_all('h2'):
                texto = h2_tag.get_text().strip()
                if texto and len(texto) > 0:  # Só textos não vazios
                    # Limpa texto (remove quebras excessivas, espaços múltiplos)
                    texto_limpo = ' '.join(texto.split())
                    if len(texto_limpo) > 2:  # Ignora textos muito curtos
                        h2_textos.append(texto_limpo)
            
            return {
                'url': url,
                'sucesso': True,
                'h1_textos': h1_textos,
                'h2_textos': h2_textos,
                'h1_count': len(h1_textos),
                'h2_count': len(h2_textos)
            }
            
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'h1_textos': [],
                'h2_textos': [],
                'h1_count': 0,
                'h2_count': 0
            }

    def _filtrar_urls_validas(self, urls: list) -> list:
        """🧹 Remove URLs inválidas para análise H1/H2"""
        urls_validas = []
        
        for url in urls:
            if not url or pd.isna(url):
                continue
            
            url_str = str(url).strip()
            
            # Filtros básicos
            if not url_str.startswith(('http://', 'https://')):
                continue
            
            # Remove paginações (não relevantes para análise de duplicação)
            if any(param in url_str.lower() for param in ['?page=', '&page=', '/page/']):
                continue
            
            # Remove extensões não-HTML
            if any(url_str.lower().endswith(ext) for ext in [
                '.pdf', '.jpg', '.png', '.gif', '.zip', '.rar',
                '.js', '.css', '.xml', '.json'
            ]):
                continue
            
            urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _analisar_h1_h2_paralelo(self, urls: list) -> list:
        """🚀 Análise paralela de H1/H2"""
        
        print(f"🔍 Análise H1/H2 duplicados iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._extrair_h1_h2_textos, url): url 
                for url in urls
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress indicator a cada 50 URLs
                if len(resultados) % 50 == 0:
                    print(f"⚡ Analisadas: {len(resultados)}/{len(urls)}")
        
        return resultados

    def _detectar_duplicacoes(self, resultados: list) -> tuple:
        """🔍 Detecta textos H1/H2 duplicados entre páginas"""
        
        h1_textos_map = defaultdict(list)  # {texto_h1: [lista_urls]}
        h2_textos_map = defaultdict(list)  # {texto_h2: [lista_urls]}
        
        # Mapeia textos para URLs
        for resultado in resultados:
            if not resultado.get('sucesso', False):
                continue
            
            url = resultado['url']
            
            # Mapeia H1s
            for h1_texto in resultado['h1_textos']:
                h1_textos_map[h1_texto].append(url)
            
            # Mapeia H2s
            for h2_texto in resultado['h2_textos']:
                h2_textos_map[h2_texto].append(url)
        
        # Filtra apenas duplicados (mesmo texto em múltiplas páginas)
        h1_duplicados = {texto: urls for texto, urls in h1_textos_map.items() if len(urls) > 1}
        h2_duplicados = {texto: urls for texto, urls in h2_textos_map.items() if len(urls) > 1}
        
        return h1_duplicados, h2_duplicados

    def export(self):
        """🔍 Gera aba CIRÚRGICA de H1/H2 duplicados"""
        try:
            print(f"🔍 H1/H2 DUPLICADOS - ENGINE CIRÚRGICA")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_para_analisar = self.df['url'].dropna().unique().tolist()
            urls_filtradas = self._filtrar_urls_validas(urls_para_analisar)
            
            print(f"   📊 URLs inicial: {len(urls_para_analisar)}")
            print(f"   🧹 URLs válidas: {len(urls_filtradas)}")
            print(f"   🎯 Foco: Mesmo texto H1/H2 usado em múltiplas páginas")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para análise")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tag', 'Tipo_Problema', 'Texto_Heading', 'Total_URLs_Afetadas',
                    'Gravidade', 'Impacto_SEO'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="H1_H2_Problemas")
                return df_vazio
            
            # 🔍 ANÁLISE CIRÚRGICA PARALELA
            resultados = self._analisar_h1_h2_paralelo(urls_filtradas)
            
            # 🔍 DETECTA DUPLICAÇÕES
            h1_duplicados, h2_duplicados = self._detectar_duplicacoes(resultados)
            
            # 📋 GERA LINHAS PARA O DATAFRAME
            rows = []
            
            # H1 DUPLICADOS
            for h1_texto, urls_list in h1_duplicados.items():
                urls_ordenadas = sorted(urls_list)
                grupo_key = f'H1_DUPLICADO_{h1_texto[:50]}'
                
                # Linha de cabeçalho do grupo
                rows.append({
                    'URL': f'>>> H1 DUPLICADO EM {len(urls_list)} PÁGINAS <<<',
                    'Tag': 'H1',
                    'Tipo_Problema': 'DUPLICADO',
                    'Texto_Heading': h1_texto[:100] + ('...' if len(h1_texto) > 100 else ''),
                    'Total_URLs_Afetadas': len(urls_list),
                    'Gravidade': 'CRITICO',
                    'Impacto_SEO': 'CRITICO - H1 duplicado entre páginas confunde mecanismos de busca',
                    'Grupo_Ordenacao': f'{grupo_key}_000_CABECALHO'
                })
                
                # Linhas individuais para cada URL
                for i, url in enumerate(urls_ordenadas):
                    rows.append({
                        'URL': url,
                        'Tag': 'H1',
                        'Tipo_Problema': 'DUPLICADO',
                        'Texto_Heading': f'[MESMO H1 #{i+1}] {h1_texto[:80]}{"..." if len(h1_texto) > 80 else ""}',
                        'Total_URLs_Afetadas': len(urls_list),
                        'Gravidade': 'CRITICO',
                        'Impacto_SEO': 'CRITICO - H1 duplicado entre páginas confunde mecanismos de busca',
                        'Grupo_Ordenacao': f'{grupo_key}_{i+1:03d}_{url}'
                    })
            
            # H2 DUPLICADOS
            for h2_texto, urls_list in h2_duplicados.items():
                urls_ordenadas = sorted(urls_list)
                grupo_key = f'H2_DUPLICADO_{h2_texto[:50]}'
                
                # Linha de cabeçalho do grupo
                rows.append({
                    'URL': f'>>> H2 DUPLICADO EM {len(urls_list)} PÁGINAS <<<',
                    'Tag': 'H2',
                    'Tipo_Problema': 'DUPLICADO',
                    'Texto_Heading': h2_texto[:100] + ('...' if len(h2_texto) > 100 else ''),
                    'Total_URLs_Afetadas': len(urls_list),
                    'Gravidade': 'ALTO',
                    'Impacto_SEO': 'ALTO - H2 duplicado entre páginas pode prejudicar SEO',
                    'Grupo_Ordenacao': f'{grupo_key}_000_CABECALHO'
                })
                
                # Linhas individuais para cada URL
                for i, url in enumerate(urls_ordenadas):
                    rows.append({
                        'URL': url,
                        'Tag': 'H2',
                        'Tipo_Problema': 'DUPLICADO',
                        'Texto_Heading': f'[MESMO H2 #{i+1}] {h2_texto[:80]}{"..." if len(h2_texto) > 80 else ""}',
                        'Total_URLs_Afetadas': len(urls_list),
                        'Gravidade': 'ALTO',
                        'Impacto_SEO': 'ALTO - H2 duplicado entre páginas pode prejudicar SEO',
                        'Grupo_Ordenacao': f'{grupo_key}_{i+1:03d}_{url}'
                    })
            
            # Se não encontrou duplicações
            if not rows:
                print(f"   🎉 PERFEITO: Nenhum texto H1/H2 duplicado encontrado!")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tag', 'Tipo_Problema', 'Texto_Heading', 'Total_URLs_Afetadas',
                    'Gravidade', 'Impacto_SEO'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="H1_H2_Problemas")
                return df_vazio
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR TAG E AGRUPAMENTO
            tag_order = {'H1': 0, 'H2': 1}
            df_problemas['tag_sort'] = df_problemas['Tag'].map(tag_order)
            
            df_problemas = df_problemas.sort_values([
                'tag_sort',           # 1. H1 primeiro
                'Grupo_Ordenacao'     # 2. Mantém agrupamento correto
            ], ascending=[True, True])
            
            # Remove colunas auxiliares
            df_problemas = df_problemas.drop(['tag_sort', 'Grupo_Ordenacao'], axis=1, errors='ignore')
            
            # 📤 EXPORTA
            df_problemas.to_excel(self.writer, index=False, sheet_name="H1_H2_Problemas")
            
            # 📊 ESTATÍSTICAS
            urls_com_sucesso = len([r for r in resultados if r.get('sucesso', False)])
            total_h1_duplicado = len(h1_duplicados)
            total_h2_duplicado = len(h2_duplicados)
            urls_h1_afetadas = sum(len(urls) for urls in h1_duplicados.values())
            urls_h2_afetadas = sum(len(urls) for urls in h2_duplicados.values())
            
            print(f"   ✅ URLs analisadas: {urls_com_sucesso}")
            print(f"   🔄 H1s com texto duplicado: {total_h1_duplicado} textos afetando {urls_h1_afetadas} URLs")
            print(f"   🔄 H2s com texto duplicado: {total_h2_duplicado} textos afetando {urls_h2_afetadas} URLs")
            print(f"   📋 Total de linhas na planilha: {len(df_problemas)}")
            print(f"   📋 Aba 'H1_H2_Problemas' criada com análise CIRÚRGICA")
            print(f"   🎯 Foco: mesmo texto H1/H2 usado em múltiplas páginas")
            print(f"   ℹ️ H1/H2 ausentes estão na aba 'Estrutura_Headings'")
            
            return df_problemas
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico H1/H2: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'URL', 'Tag', 'Tipo_Problema', 'Texto_Heading', 'Total_URLs_Afetadas',
                'Gravidade', 'Impacto_SEO'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="H1_H2_Problemas")
            return df_erro