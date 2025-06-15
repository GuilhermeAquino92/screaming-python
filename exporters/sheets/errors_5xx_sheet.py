# exporters/sheets/errors_5xx_sheet.py - ENGINE CIRÚRGICA
# 💥 ENGINE CIRÚRGICA: Análise completa de erros 5xx para correção de servidor

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter

class Errors5xxSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para análise de erros 5xx"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        })
        return session

    def _analisar_error_5xx_cirurgico(self, url: str) -> dict:
        """💥 Análise cirúrgica de erros 5xx"""
        
        try:
            response = self.session.get(url, timeout=15, verify=False, allow_redirects=True)
            status_code = response.status_code
            
            # Só processa se for 5xx
            if 500 <= status_code < 600:
                # Analisa o tipo de erro
                tipo_erro = self._classificar_error_5xx(status_code)
                gravidade = self._analisar_gravidade(status_code)
                
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_error_5xx': True,
                    'status_code': status_code,
                    'tipo_erro': tipo_erro,
                    'gravidade': gravidade,
                    'tempo_resposta': response.elapsed.total_seconds(),
                    'server': response.headers.get('server', 'Desconhecido'),
                    'retry_after': response.headers.get('retry-after', ''),
                    'error_details': self._extrair_detalhes_erro(response),
                    'headers_debug': self._extrair_headers_debug(response.headers)
                }
            else:
                # Não é erro 5xx
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_error_5xx': False,
                    'status_code': status_code,
                    'motivo': f'Status {status_code} não é 5xx'
                }
            
        except requests.exceptions.Timeout:
            return {
                'url': url,
                'sucesso': True,
                'tem_error_5xx': True,
                'status_code': 'TIMEOUT',
                'tipo_erro': 'Timeout do Servidor',
                'gravidade': 'CRÍTICO',
                'tempo_resposta': 15.0,
                'server': 'Desconhecido',
                'retry_after': '',
                'error_details': 'Servidor não respondeu em 15 segundos',
                'headers_debug': 'Timeout - sem headers'
            }
        except requests.exceptions.ConnectionError:
            return {
                'url': url,
                'sucesso': True,
                'tem_error_5xx': True,
                'status_code': 'CONNECTION_ERROR',
                'tipo_erro': 'Erro de Conexão',
                'gravidade': 'CRÍTICO',
                'tempo_resposta': 0,
                'server': 'Inacessível',
                'retry_after': '',
                'error_details': 'Não foi possível conectar ao servidor',
                'headers_debug': 'Conexão falhada - sem headers'
            }
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'tem_error_5xx': False,
                'status_code': 'ERROR',
                'motivo': f'Erro na análise: {str(e)}'
            }

    def _classificar_error_5xx(self, status_code) -> str:
        """🏷️ Classifica tipo de erro 5xx"""
        error_types = {
            500: 'Internal Server Error',
            501: 'Not Implemented',
            502: 'Bad Gateway',
            503: 'Service Unavailable',
            504: 'Gateway Timeout',
            505: 'HTTP Version Not Supported',
            507: 'Insufficient Storage',
            508: 'Loop Detected',
            510: 'Not Extended',
            511: 'Network Authentication Required'
        }
        return error_types.get(status_code, f'Erro 5xx ({status_code})')

    def _analisar_gravidade(self, status_code) -> str:
        """🎯 Analisa gravidade do erro 5xx"""
        if status_code == 500:
            return 'CRÍTICO - Erro interno do servidor'
        elif status_code == 502:
            return 'ALTO - Gateway incorreto'
        elif status_code == 503:
            return 'MÉDIO - Serviço temporariamente indisponível'
        elif status_code == 504:
            return 'ALTO - Timeout do gateway'
        elif status_code == 'TIMEOUT':
            return 'CRÍTICO - Servidor não responde'
        elif status_code == 'CONNECTION_ERROR':
            return 'CRÍTICO - Servidor inacessível'
        else:
            return f'VERIFICAR - Erro {status_code} raro'

    def _extrair_detalhes_erro(self, response) -> str:
        """📋 Extrai detalhes do erro"""
        detalhes = []
        
        # Verifica se há página de erro customizada
        if response.content:
            content_text = response.text[:500]  # Primeiros 500 chars
            if any(keyword in content_text.lower() for keyword in [
                'error', 'erro', 'exception', 'internal server error',
                'service unavailable', 'gateway', 'timeout'
            ]):
                detalhes.append("Página de erro detectada")
        
        # Verifica tamanho da resposta
        content_length = len(response.content) if response.content else 0
        if content_length == 0:
            detalhes.append("Resposta vazia")
        elif content_length < 100:
            detalhes.append("Resposta muito pequena")
        
        return ' | '.join(detalhes) if detalhes else 'Sem detalhes específicos'

    def _extrair_headers_debug(self, headers) -> str:
        """🔧 Extrai headers úteis para debug"""
        headers_debug = []
        
        if 'server' in headers:
            headers_debug.append(f"Server: {headers['server']}")
        if 'x-powered-by' in headers:
            headers_debug.append(f"Powered-By: {headers['x-powered-by']}")
        if 'content-type' in headers:
            headers_debug.append(f"Content-Type: {headers['content-type']}")
        if 'retry-after' in headers:
            headers_debug.append(f"Retry-After: {headers['retry-after']}")
        
        return ' | '.join(headers_debug) if headers_debug else 'Headers básicos'

    def _filtrar_urls_validas(self, urls_df) -> list:
        """🧹 Remove URLs inválidas"""
        urls_validas = []
        
        for _, row in urls_df.iterrows():
            url = row.get('url', '')
            
            if not url or pd.isna(url):
                continue
            
            url_str = str(url).strip()
            
            # Filtros básicos
            if not url_str.startswith(('http://', 'https://')):
                continue
            
            # Remove extensões que raramente dão 5xx
            if any(url_str.lower().endswith(ext) for ext in [
                '.jpg', '.jpeg', '.png', '.gif', '.css', '.js'
            ]):
                continue
            
            urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _analisar_errors_5xx_paralelo(self, urls: list) -> list:
        """🚀 Análise paralela de erros 5xx"""
        
        print(f"💥 Análise de erros 5xx iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:  # Menos threads para erros
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._analisar_error_5xx_cirurgico, url): url 
                for url in urls
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress indicator a cada 25 URLs (menos frequente)
                if len(resultados) % 25 == 0:
                    print(f"⚡ Analisados: {len(resultados)}/{len(urls)}")
        
        return resultados

    def export(self):
        """💥 Gera aba CIRÚRGICA de erros 5xx"""
        try:
            print(f"💥 ERRORS 5XX - ENGINE CIRÚRGICA")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_filtradas = self._filtrar_urls_validas(self.df)
            
            print(f"   📊 URLs no DataFrame: {len(self.df)}")
            print(f"   🧹 URLs válidas: {len(urls_filtradas)}")
            print(f"   🎯 Foco: Erros 5xx (500, 502, 503, 504) + timeouts para correção técnica")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para análise")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Status', 'Tipo_Erro', 'Gravidade', 'Tempo_Resposta', 
                    'Server', 'Retry_After', 'Error_Details', 'Headers_Debug'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Errors_5xx")
                return df_vazio
            
            # 💥 ANÁLISE CIRÚRGICA PARALELA
            resultados = self._analisar_errors_5xx_paralelo(urls_filtradas)
            
            # 📋 GERA LINHAS APENAS PARA ERROS 5XX
            rows = []
            
            for resultado in resultados:
                # SÓ INCLUI SE TEM ERROR 5XX
                if resultado.get('tem_error_5xx', False) and resultado.get('sucesso', False):
                    rows.append({
                        'URL': resultado['url'],
                        'Status': resultado['status_code'],
                        'Tipo_Erro': resultado['tipo_erro'],
                        'Gravidade': resultado['gravidade'],
                        'Tempo_Resposta': f"{resultado['tempo_resposta']:.2f}s",
                        'Server': resultado['server'],
                        'Retry_After': resultado['retry_after'] or 'Não especificado',
                        'Error_Details': resultado['error_details'],
                        'Headers_Debug': resultado['headers_debug']
                    })
            
            # Se não encontrou erros 5xx
            if not rows:
                print(f"   🎉 PERFEITO: Nenhum erro 5xx encontrado!")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Status', 'Tipo_Erro', 'Gravidade', 'Tempo_Resposta', 
                    'Server', 'Retry_After', 'Error_Details', 'Headers_Debug'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Errors_5xx")
                return df_vazio
            
            df_errors = pd.DataFrame(rows)
            
            # 💥 ORDENAÇÃO POR GRAVIDADE E STATUS
            gravidade_order = {'CRÍTICO': 1, 'ALTO': 2, 'MÉDIO': 3, 'VERIFICAR': 4}
            df_errors['sort_gravidade'] = df_errors['Gravidade'].str.split(' -').str[0].map(gravidade_order).fillna(99)
            df_errors = df_errors.sort_values(['sort_gravidade', 'Status', 'URL'])
            df_errors = df_errors.drop('sort_gravidade', axis=1)
            
            # 📤 EXPORTA
            df_errors.to_excel(self.writer, index=False, sheet_name="Errors_5xx")
            
            # 📊 ESTATÍSTICAS
            urls_analisadas = len([r for r in resultados if r.get('sucesso', False)])
            total_errors = len(rows)
            
            # Stats por tipo
            stats_status = df_errors['Status'].value_counts()
            stats_gravidade = df_errors['Gravidade'].str.split(' -').str[0].value_counts()
            
            print(f"   ✅ URLs analisadas: {urls_analisadas}")
            print(f"   💥 Erros 5xx encontrados: {total_errors}")
            
            # Estatísticas por status
            for status, count in stats_status.items():
                print(f"      • {status}: {count} erros")
            
            # Estatísticas por gravidade
            for gravidade, count in stats_gravidade.items():
                print(f"      • Gravidade {gravidade}: {count} erros")
            
            print(f"   📋 Aba 'Errors_5xx' criada com análise CIRÚRGICA")
            print(f"   🎯 Foco em correção técnica urgente de servidor")
            
            return df_errors
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico 5xx: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'URL', 'Status', 'Tipo_Erro', 'Gravidade', 'Tempo_Resposta', 
                'Server', 'Retry_After', 'Error_Details', 'Headers_Debug'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="Errors_5xx")
            return df_erro