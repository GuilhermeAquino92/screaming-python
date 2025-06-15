# exporters/sheets/title_duplicado_sheet.py - CIRÚRGICO COM SEPARADORES
# 🔄 ENGINE CIRÚRGICA: Detecta titles duplicados reais entre páginas com agrupamento visual

import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from exporters.base_exporter import BaseSheetExporter

class TitleDuplicadoSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para análise de titles"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        return session

    def _extrair_title_real(self, url: str) -> dict:
        """🎯 Extrai title real via DOM"""
        
        try:
            response = self.session.get(url, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extrai title real
            title_tag = soup.find('title')
            title_text = title_tag.get_text().strip() if title_tag else ""
            
            # Limpa texto (remove quebras excessivas, espaços múltiplos)
            if title_text:
                title_limpo = ' '.join(title_text.split())
                return {
                    'url': url,
                    'sucesso': True,
                    'title_text': title_limpo,
                    'title_length': len(title_limpo)
                }
            else:
                return {
                    'url': url,
                    'sucesso': True,
                    'title_text': '',
                    'title_length': 0
                }
            
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'title_text': '',
                'title_length': 0
            }

    def _filtrar_urls_validas(self, urls_df) -> list:
        """🧹 Remove URLs inválidas + FILTRA APENAS STATUS 200 + SEM PAGINAÇÃO"""
        urls_validas = []
        
        for _, row in urls_df.iterrows():
            url = row.get('url', '')
            
            if not url or pd.isna(url):
                continue
            
            url_str = str(url).strip()
            
            # Filtros básicos
            if not url_str.startswith(('http://', 'https://')):
                continue
            
            # Remove extensões não-HTML
            if any(url_str.lower().endswith(ext) for ext in [
                '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.rar',
                '.js', '.css', '.mp3', '.mp4', '.xml', '.json'
            ]):
                continue
            
            # 🎯 FILTRO CRUCIAL: SÓ PÁGINAS 200 OK
            status_code = row.get('status_code_http', row.get('status_code', None))
            if status_code is not None:
                try:
                    status_num = int(float(status_code))
                    if status_num != 200:
                        continue  # Pula páginas que não são 200 OK
                except (ValueError, TypeError):
                    continue  # Pula se não conseguir converter status
            
            # 🚫 FILTRO PAGINAÇÃO: Remove URLs com parâmetros de paginação
            url_lower = url_str.lower()
            pagination_params = [
                '?page=', '&page=', '?p=', '&p=',
                '?pagina=', '&pagina=', '?pg=', '&pg=',
                '/page/', '/p/', '/pagina/', '/pg/',
                '?offset=', '&offset=', '?start=', '&start=',
                '?pagenum=', '&pagenum=', '?paged=', '&paged='
            ]
            
            is_pagination = any(pagination_param in url_lower for pagination_param in pagination_params)
            if is_pagination:
                # DEBUG: Mostra URLs de paginação que estão sendo filtradas
                print(f"      🚫 Filtrado (paginação): {url_str}")
                continue  # Pula URLs de paginação
            
            urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _analisar_titles_paralelo(self, urls: list) -> list:
        """🚀 Análise paralela de titles"""
        
        print(f"🔄 Análise de titles duplicados iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._extrair_title_real, url): url 
                for url in urls
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress indicator a cada 50 URLs
                if len(resultados) % 50 == 0:
                    print(f"⚡ Analisados: {len(resultados)}/{len(urls)}")
        
        return resultados

    def _detectar_duplicacoes(self, resultados: list) -> dict:
        """🔍 Detecta titles duplicados entre páginas"""
        
        titles_map = defaultdict(list)  # {title_text: [lista_urls]}
        
        # Mapeia titles para URLs
        for resultado in resultados:
            if not resultado.get('sucesso', False):
                continue
            
            url = resultado['url']
            title_text = resultado['title_text']
            
            # Só considera titles não vazios
            if title_text and len(title_text) > 3:  # Ignora titles muito curtos
                titles_map[title_text].append(url)
        
        # Filtra apenas duplicados (mesmo title em múltiplas páginas)
        titles_duplicados = {title: urls for title, urls in titles_map.items() if len(urls) > 1}
        
        return titles_duplicados

    def export(self):
        """🔄 Gera aba CIRÚRGICA de titles duplicados com separadores"""
        try:
            print(f"🔄 TITLE DUPLICADO - ENGINE CIRÚRGICA")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_filtradas = self._filtrar_urls_validas(self.df)
            
            print(f"   📊 URLs no DataFrame: {len(self.df)}")
            print(f"   🧹 URLs válidas (200 OK, sem paginação): {len(urls_filtradas)}")
            print(f"   🎯 Foco: Mesmo title usado em múltiplas páginas (EXCETO paginação)")
            print(f"   🔍 FILTROS: Status 200 OK + Sem ?page=, &page=, /page/, etc.")
            print(f"   🚫 IGNORA: Paginação normal (?page=2, ?p=3, /page/4, etc.)")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para análise")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Title', 'Total_URLs_Afetadas', 'Tipo_Linha'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Duplicado")
                return df_vazio
            
            # 🔄 ANÁLISE CIRÚRGICA PARALELA
            resultados = self._analisar_titles_paralelo(urls_filtradas)
            
            # 🔍 DETECTA DUPLICAÇÕES
            titles_duplicados = self._detectar_duplicacoes(resultados)
            
            # 📋 GERA LINHAS COM SEPARADORES VISUAIS
            rows = []
            
            if not titles_duplicados:
                print(f"   🎉 PERFEITO: Nenhum title duplicado encontrado!")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Title', 'Total_URLs_Afetadas', 'Tipo_Linha'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Duplicado")
                return df_vazio
            
            # Ordena por quantidade de duplicação (maior primeiro)
            titles_ordenados = sorted(
                titles_duplicados.items(), 
                key=lambda x: len(x[1]), 
                reverse=True
            )
            
            for title_text, urls_list in titles_ordenados:
                urls_ordenadas = sorted(urls_list)
                grupo_key = f'TITLE_DUPLICADO_{title_text[:50]}'
                
                # Linha de cabeçalho do grupo
                rows.append({
                    'URL': f'>>> TITLE DUPLICADO EM {len(urls_list)} PÁGINAS <<<',
                    'Title': '',
                    'Total_URLs_Afetadas': len(urls_list),
                    'Tipo_Linha': 'CABECALHO',
                    'Grupo_Ordenacao': f'{grupo_key}_000_CABECALHO'
                })
                
                # Linhas individuais para cada URL - FORMATO LIMPO
                for i, url in enumerate(urls_ordenadas):
                    rows.append({
                        'URL': url,
                        'Title': f'"{title_text}"',
                        'Total_URLs_Afetadas': len(urls_list),
                        'Tipo_Linha': 'URL_INDIVIDUAL',
                        'Grupo_Ordenacao': f'{grupo_key}_{i+1:03d}_{url}'
                    })
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO MANTÉM AGRUPAMENTO
            df_problemas = df_problemas.sort_values(['Grupo_Ordenacao'], ascending=[True])
            
            # Remove colunas auxiliares
            df_problemas = df_problemas.drop(['Grupo_Ordenacao'], axis=1, errors='ignore')
            
            # 📤 EXPORTA
            df_problemas.to_excel(self.writer, index=False, sheet_name="Title_Duplicado")
            
            # 📊 ESTATÍSTICAS
            urls_com_sucesso = len([r for r in resultados if r.get('sucesso', False)])
            total_titles_duplicados = len(titles_duplicados)
            urls_afetadas = sum(len(urls) for urls in titles_duplicados.values())
            
            print(f"   ✅ URLs analisadas: {urls_com_sucesso}")
            print(f"   🔄 Titles com duplicação: {total_titles_duplicados} textos afetando {urls_afetadas} URLs")
            print(f"   📋 Total de linhas na planilha: {len(df_problemas)}")
            print(f"   📋 Aba 'Title_Duplicado' criada com análise CIRÚRGICA")
            print(f"   🎯 Formato: URL | Title com separadores visuais")
            
            return df_problemas
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico title duplicado: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'URL', 'Title', 'Total_URLs_Afetadas', 'Tipo_Linha'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="Title_Duplicado")
            return df_erro