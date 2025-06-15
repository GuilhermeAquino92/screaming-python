# exporters/sheets/errors_http_sheet.py - ENGINE CIRÚRGICA
# 🌐 ENGINE CIRÚRGICA: Análise completa de erros HTTP (timeouts, DNS, SSL, etc.)

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter
import ssl
import socket

class ErrorsHTTPSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para análise de erros HTTP"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        })
        return session

    def _analisar_error_http_cirurgico(self, url: str) -> dict:
        """🌐 Análise cirúrgica de erros HTTP gerais"""
        
        tempo_inicio = __import__('time').time()
        
        try:
            response = self.session.get(url, timeout=15, verify=False, allow_redirects=True)
            tempo_total = __import__('time').time() - tempo_inicio
            
            # Se chegou aqui, não houve erro de conexão
            # Mas pode ter sido um status HTTP normal (200, 3xx, 4xx, 5xx)
            # Esta engine só captura erros de CONEXÃO, não status HTTP
            return {
                'url': url,
                'sucesso': True,
                'tem_error_http': False,
                'status_code': response.status_code,
                'tempo_resposta': tempo_total,
                'motivo': f'Conexão OK - Status {response.status_code}'
            }
            
        except requests.exceptions.Timeout:
            tempo_total = __import__('time').time() - tempo_inicio
            return {
                'url': url,
                'sucesso': True,
                'tem_error_http': True,
                'tipo_erro': 'Timeout',
                'erro_detalhado': 'Servidor não respondeu em 15 segundos',
                'tempo_tentativa': tempo_total,
                'gravidade': 'ALTO',
                'sugestao_acao': 'Verificar desempenho do servidor',
                'categoria': 'Performance'
            }
            
        except requests.exceptions.SSLError as e:
            tempo_total = __import__('time').time() - tempo_inicio
            ssl_detail = str(e)
            return {
                'url': url,
                'sucesso': True,
                'tem_error_http': True,
                'tipo_erro': 'SSL Error',
                'erro_detalhado': f'Certificado SSL inválido: {ssl_detail[:100]}',
                'tempo_tentativa': tempo_total,
                'gravidade': 'CRÍTICO',
                'sugestao_acao': 'Verificar/renovar certificado SSL',
                'categoria': 'Segurança'
            }
            
        except requests.exceptions.ConnectionError as e:
            tempo_total = __import__('time').time() - tempo_inicio
            error_detail = str(e)
            
            # Analisa tipo específico de erro de conexão
            if 'Name or service not known' in error_detail or 'getaddrinfo failed' in error_detail:
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_error_http': True,
                    'tipo_erro': 'DNS Error',
                    'erro_detalhado': 'Domínio não resolve (DNS falhou)',
                    'tempo_tentativa': tempo_total,
                    'gravidade': 'CRÍTICO',
                    'sugestao_acao': 'Verificar configuração DNS',
                    'categoria': 'DNS'
                }
            elif 'Connection refused' in error_detail:
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_error_http': True,
                    'tipo_erro': 'Connection Refused',
                    'erro_detalhado': 'Servidor recusou conexão (porta fechada)',
                    'tempo_tentativa': tempo_total,
                    'gravidade': 'ALTO',
                    'sugestao_acao': 'Verificar se servidor está ativo',
                    'categoria': 'Conectividade'
                }
            elif 'Network is unreachable' in error_detail:
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_error_http': True,
                    'tipo_erro': 'Network Unreachable',
                    'erro_detalhado': 'Rede inacessível',
                    'tempo_tentativa': tempo_total,
                    'gravidade': 'MÉDIO',
                    'sugestao_acao': 'Verificar conectividade de rede',
                    'categoria': 'Rede'
                }
            else:
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_error_http': True,
                    'tipo_erro': 'Connection Error',
                    'erro_detalhado': f'Erro de conexão: {error_detail[:100]}',
                    'tempo_tentativa': tempo_total,
                    'gravidade': 'ALTO',
                    'sugestao_acao': 'Investigar problema de conectividade',
                    'categoria': 'Conectividade'
                }
                
        except requests.exceptions.TooManyRedirects:
            tempo_total = __import__('time').time() - tempo_inicio
            return {
                'url': url,
                'sucesso': True,
                'tem_error_http': True,
                'tipo_erro': 'Too Many Redirects',
                'erro_detalhado': 'Loop infinito de redirects detectado',
                'tempo_tentativa': tempo_total,
                'gravidade': 'ALTO',
                'sugestao_acao': 'Corrigir cadeia de redirects',
                'categoria': 'Redirects'
            }
            
        except requests.exceptions.InvalidURL:
            tempo_total = __import__('time').time() - tempo_inicio
            return {
                'url': url,
                'sucesso': True,
                'tem_error_http': True,
                'tipo_erro': 'Invalid URL',
                'erro_detalhado': 'URL malformada ou inválida',
                'tempo_tentativa': tempo_total,
                'gravidade': 'MÉDIO',
                'sugestao_acao': 'Corrigir formato da URL',
                'categoria': 'URL'
            }
            
        except requests.exceptions.InvalidSchema:
            tempo_total = __import__('time').time() - tempo_inicio
            return {
                'url': url,
                'sucesso': True,
                'tem_error_http': True,
                'tipo_erro': 'Invalid Schema',
                'erro_detalhado': 'Protocolo não suportado (deve ser http/https)',
                'tempo_tentativa': tempo_total,
                'gravidade': 'MÉDIO',
                'sugestao_acao': 'Usar protocolo http:// ou https://',
                'categoria': 'URL'
            }
            
        except Exception as e:
            tempo_total = __import__('time').time() - tempo_inicio
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'tem_error_http': True,
                'tipo_erro': 'Unknown Error',
                'erro_detalhado': f'Erro desconhecido: {str(e)[:100]}',
                'tempo_tentativa': tempo_total,
                'gravidade': 'VERIFICAR',
                'sugestao_acao': 'Investigar erro específico',
                'categoria': 'Outros'
            }

    def _filtrar_urls_validas(self, urls_df) -> list:
        """🧹 Remove URLs inválidas (mas aceita mais tipos para detectar erros)"""
        urls_validas = []
        
        for _, row in urls_df.iterrows():
            url = row.get('url', '')
            
            if not url or pd.isna(url):
                continue
            
            url_str = str(url).strip()
            
            # Filtros muito básicos (queremos detectar erros, então aceita mais)
            if url_str and len(url_str) > 5:
                urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _analisar_errors_http_paralelo(self, urls: list) -> list:
        """🚀 Análise paralela de erros HTTP"""
        
        print(f"🌐 Análise de erros HTTP iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:  # Menos threads para erros
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._analisar_error_http_cirurgico, url): url 
                for url in urls
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress indicator a cada 25 URLs
                if len(resultados) % 25 == 0:
                    print(f"⚡ Analisados: {len(resultados)}/{len(urls)}")
        
        return resultados

    def export(self):
        """🌐 Gera aba CIRÚRGICA de erros HTTP gerais"""
        try:
            print(f"🌐 ERRORS HTTP - ENGINE CIRÚRGICA")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_filtradas = self._filtrar_urls_validas(self.df)
            
            print(f"   📊 URLs no DataFrame: {len(self.df)}")
            print(f"   🧹 URLs válidas: {len(urls_filtradas)}")
            print(f"   🎯 Foco: Erros de conexão (timeouts, DNS, SSL, rede)")
            print(f"   ℹ️ NÃO inclui: Status HTTP 4xx/5xx (têm abas próprias)")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para análise")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tipo_Erro', 'Erro_Detalhado', 'Tempo_Tentativa', 
                    'Gravidade', 'Sugestao_Acao', 'Categoria'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Errors_HTTP")
                return df_vazio
            
            # 🌐 ANÁLISE CIRÚRGICA PARALELA
            resultados = self._analisar_errors_http_paralelo(urls_filtradas)
            
            # 📋 GERA LINHAS APENAS PARA ERROS HTTP
            rows = []
            
            for resultado in resultados:
                # SÓ INCLUI SE TEM ERROR HTTP (conexão, não status)
                if resultado.get('tem_error_http', False):
                    rows.append({
                        'URL': resultado['url'],
                        'Tipo_Erro': resultado.get('tipo_erro', 'Unknown'),
                        'Erro_Detalhado': resultado.get('erro_detalhado', 'Sem detalhes'),
                        'Tempo_Tentativa': f"{resultado.get('tempo_tentativa', 0):.2f}s",
                        'Gravidade': resultado.get('gravidade', 'VERIFICAR'),
                        'Sugestao_Acao': resultado.get('sugestao_acao', 'Investigar'),
                        'Categoria': resultado.get('categoria', 'Outros')
                    })
            
            # Se não encontrou erros HTTP
            if not rows:
                print(f"   🎉 PERFEITO: Nenhum erro de conexão HTTP encontrado!")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tipo_Erro', 'Erro_Detalhado', 'Tempo_Tentativa', 
                    'Gravidade', 'Sugestao_Acao', 'Categoria'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Errors_HTTP")
                return df_vazio
            
            df_errors = pd.DataFrame(rows)
            
            # 🌐 ORDENAÇÃO POR GRAVIDADE E CATEGORIA
            gravidade_order = {'CRÍTICO': 1, 'ALTO': 2, 'MÉDIO': 3, 'BAIXO': 4, 'VERIFICAR': 5}
            categoria_order = {'DNS': 1, 'Segurança': 2, 'Performance': 3, 'Conectividade': 4, 'Redirects': 5, 'URL': 6, 'Outros': 7}
            
            df_errors['sort_gravidade'] = df_errors['Gravidade'].map(gravidade_order).fillna(99)
            df_errors['sort_categoria'] = df_errors['Categoria'].map(categoria_order).fillna(99)
            df_errors = df_errors.sort_values(['sort_gravidade', 'sort_categoria', 'URL'])
            df_errors = df_errors.drop(['sort_gravidade', 'sort_categoria'], axis=1)
            
            # 📤 EXPORTA
            df_errors.to_excel(self.writer, index=False, sheet_name="Errors_HTTP")
            
            # 📊 ESTATÍSTICAS
            urls_analisadas = len([r for r in resultados if r.get('sucesso', False) or r.get('tem_error_http', False)])
            total_errors = len(rows)
            
            # Stats por tipo
            stats_tipo = df_errors['Tipo_Erro'].value_counts()
            stats_categoria = df_errors['Categoria'].value_counts()
            stats_gravidade = df_errors['Gravidade'].value_counts()
            
            print(f"   ✅ URLs analisadas: {urls_analisadas}")
            print(f"   🌐 Erros HTTP encontrados: {total_errors}")
            
            # Estatísticas por tipo
            for tipo, count in stats_tipo.items():
                print(f"      • {tipo}: {count} erros")
            
            # Estatísticas por categoria
            for categoria, count in stats_categoria.items():
                print(f"      • Categoria {categoria}: {count} erros")
            
            print(f"   📋 Aba 'Errors_HTTP' criada com análise CIRÚRGICA")
            print(f"   🎯 Foco em problemas de conectividade e infraestrutura")
            
            return df_errors
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico HTTP: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'URL', 'Tipo_Erro', 'Erro_Detalhado', 'Tempo_Tentativa', 
                'Gravidade', 'Sugestao_Acao', 'Categoria'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="Errors_HTTP")
            return df_erro