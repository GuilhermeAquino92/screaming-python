# exporters/sheets/status_http_sheet.py - COM ENGINE CIRÚRGICA
# 🌐 ENGINE CIRÚRGICA: Validação real de status HTTP com análise completa

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter

class StatusHTTPSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para verificação de status HTTP"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        })
        return session

    def _verificar_status_detalhado(self, url: str) -> dict:
        """🌐 Verificação detalhada de status HTTP"""
        
        try:
            response = self.session.get(url, timeout=10, verify=False, allow_redirects=True)
            
            # Análise do status
            status_code = response.status_code
            status_categoria = self._classificar_status(status_code)
            
            # Análise de redirecionamentos
            historico_redirects = []
            if response.history:
                for redirect in response.history:
                    historico_redirects.append({
                        'url': redirect.url,
                        'status': redirect.status_code
                    })
            
            # Análise de headers importantes
            headers_relevantes = self._extrair_headers_relevantes(response.headers)
            
            # Análise de performance
            tempo_resposta = response.elapsed.total_seconds()
            performance_categoria = self._classificar_performance(tempo_resposta)
            
            return {
                'url': url,
                'sucesso': True,
                'status_code': status_code,
                'status_categoria': status_categoria,
                'status_descricao': self._descrever_status(status_code),
                'url_final': response.url,
                'houve_redirect': len(response.history) > 0,
                'numero_redirects': len(response.history),
                'historico_redirects': historico_redirects,
                'tempo_resposta': tempo_resposta,
                'performance_categoria': performance_categoria,
                'headers_relevantes': headers_relevantes,
                'content_type': response.headers.get('content-type', ''),
                'server': response.headers.get('server', ''),
                'tamanho_content': len(response.content) if response.content else 0
            }
            
        except requests.exceptions.Timeout:
            return {
                'url': url,
                'sucesso': False,
                'status_code': 'TIMEOUT',
                'status_categoria': 'ERRO',
                'status_descricao': 'Timeout na requisição',
                'url_final': url,
                'houve_redirect': False,
                'numero_redirects': 0,
                'historico_redirects': [],
                'tempo_resposta': 10.0,
                'performance_categoria': 'MUITO_LENTO',
                'headers_relevantes': {},
                'content_type': '',
                'server': '',
                'tamanho_content': 0
            }
        except requests.exceptions.ConnectionError:
            return {
                'url': url,
                'sucesso': False,
                'status_code': 'CONNECTION_ERROR',
                'status_categoria': 'ERRO',
                'status_descricao': 'Erro de conexão',
                'url_final': url,
                'houve_redirect': False,
                'numero_redirects': 0,
                'historico_redirects': [],
                'tempo_resposta': 0,
                'performance_categoria': 'ERRO',
                'headers_relevantes': {},
                'content_type': '',
                'server': '',
                'tamanho_content': 0
            }
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'status_code': 'ERROR',
                'status_categoria': 'ERRO',
                'status_descricao': f'Erro: {str(e)}',
                'url_final': url,
                'houve_redirect': False,
                'numero_redirects': 0,
                'historico_redirects': [],
                'tempo_resposta': 0,
                'performance_categoria': 'ERRO',
                'headers_relevantes': {},
                'content_type': '',
                'server': '',
                'tamanho_content': 0
            }

    def _classificar_status(self, status_code) -> str:
        """📊 Classifica status HTTP por categoria"""
        
        if isinstance(status_code, str):
            return 'ERRO'
        
        if 200 <= status_code < 300:
            return 'SUCESSO'
        elif 300 <= status_code < 400:
            return 'REDIRECT'
        elif 400 <= status_code < 500:
            return 'ERRO_CLIENTE'
        elif 500 <= status_code < 600:
            return 'ERRO_SERVIDOR'
        else:
            return 'DESCONHECIDO'

    def _descrever_status(self, status_code) -> str:
        """📝 Descrição detalhada do status"""
        
        status_descriptions = {
            200: 'OK - Página carregada com sucesso',
            301: 'Moved Permanently - Redirecionamento permanente',
            302: 'Found - Redirecionamento temporário',
            304: 'Not Modified - Não modificado',
            400: 'Bad Request - Requisição inválida',
            401: 'Unauthorized - Não autorizado',
            403: 'Forbidden - Acesso negado',
            404: 'Not Found - Página não encontrada',
            410: 'Gone - Página removida permanentemente',
            500: 'Internal Server Error - Erro interno do servidor',
            502: 'Bad Gateway - Gateway incorreto',
            503: 'Service Unavailable - Serviço indisponível',
            504: 'Gateway Timeout - Timeout do gateway'
        }
        
        if isinstance(status_code, str):
            return status_code
        
        return status_descriptions.get(status_code, f'Status {status_code}')

    def _classificar_performance(self, tempo_resposta: float) -> str:
        """⚡ Classifica performance da resposta"""
        
        if tempo_resposta < 1.0:
            return 'RAPIDO'
        elif tempo_resposta < 3.0:
            return 'NORMAL'
        elif tempo_resposta < 5.0:
            return 'LENTO'
        else:
            return 'MUITO_LENTO'

    def _extrair_headers_relevantes(self, headers) -> dict:
        """📋 Extrai headers relevantes para SEO"""
        
        headers_importantes = [
            'cache-control', 'expires', 'last-modified', 'etag',
            'x-robots-tag', 'x-frame-options', 'strict-transport-security'
        ]
        
        headers_relevantes = {}
        for header in headers_importantes:
            if header in headers:
                headers_relevantes[header] = headers[header]
        
        return headers_relevantes

    def _filtrar_urls_validas(self, urls: list) -> list:
        """🧹 Remove URLs inválidas para verificação HTTP"""
        urls_validas = []
        
        for url in urls:
            if not url or pd.isna(url):
                continue
            
            url_str = str(url).strip()
            
            # Filtros básicos
            if not url_str.startswith(('http://', 'https://')):
                continue
            
            urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _verificar_status_paralelo(self, urls: list) -> list:
        """🚀 Verificação paralela de status HTTP"""
        
        print(f"🌐 Verificação HTTP cirúrgica iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._verificar_status_detalhado, url): url 
                for url in urls
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress indicator a cada 50 URLs
                if len(resultados) % 50 == 0:
                    print(f"⚡ Verificadas: {len(resultados)}/{len(urls)}")
        
        return resultados

    def export(self):
        """🌐 Gera aba CIRÚRGICA de Status HTTP"""
        try:
            print(f"🌐 STATUS HTTP - ENGINE CIRÚRGICA")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_para_verificar = self.df['url'].dropna().unique().tolist()
            urls_filtradas = self._filtrar_urls_validas(urls_para_verificar)
            
            print(f"   📊 URLs inicial: {len(urls_para_verificar)}")
            print(f"   🧹 URLs válidas: {len(urls_filtradas)}")
            print(f"   🎯 Foco: Status real, redirects, performance, headers SEO")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para verificação")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Status_Code', 'Status_Categoria', 'Status_Descricao', 'URL_Final',
                    'Houve_Redirect', 'Numero_Redirects', 'Tempo_Resposta', 'Performance',
                    'Content_Type', 'Server', 'Tamanho_Content'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Status_HTTP")
                return df_vazio
            
            # 🌐 VERIFICAÇÃO CIRÚRGICA PARALELA
            resultados = self._verificar_status_paralelo(urls_filtradas)
            
            # 📋 GERA LINHAS PARA O DATAFRAME
            rows = []
            
            for resultado in resultados:
                # Monta histórico de redirects para exibição
                historico_str = ""
                if resultado['historico_redirects']:
                    historico_str = " → ".join([
                        f"{r['status']} ({r['url'][:50]}...)" 
                        for r in resultado['historico_redirects']
                    ])
                
                rows.append({
                    'URL': resultado['url'],
                    'Status_Code': resultado['status_code'],
                    'Status_Categoria': resultado['status_categoria'],
                    'Status_Descricao': resultado['status_descricao'],
                    'URL_Final': resultado['url_final'] if resultado['url_final'] != resultado['url'] else '',
                    'Houve_Redirect': 'Sim' if resultado['houve_redirect'] else 'Não',
                    'Numero_Redirects': resultado['numero_redirects'],
                    'Historico_Redirects': historico_str,
                    'Tempo_Resposta': f"{resultado['tempo_resposta']:.2f}s",
                    'Performance': resultado['performance_categoria'],
                    'Content_Type': resultado['content_type'],
                    'Server': resultado['server'],
                    'Tamanho_Content': f"{resultado['tamanho_content']:,} bytes" if resultado['tamanho_content'] > 0 else ''
                })
            
            df_status = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR STATUS E PERFORMANCE
            categoria_order = {'ERRO': 1, 'ERRO_SERVIDOR': 2, 'ERRO_CLIENTE': 3, 'REDIRECT': 4, 'SUCESSO': 5}
            performance_order = {'ERRO': 1, 'MUITO_LENTO': 2, 'LENTO': 3, 'NORMAL': 4, 'RAPIDO': 5}
            
            df_status['categoria_sort'] = df_status['Status_Categoria'].map(categoria_order).fillna(99)
            df_status['performance_sort'] = df_status['Performance'].map(performance_order).fillna(99)
            
            df_status = df_status.sort_values([
                'categoria_sort',     # 1. Erros primeiro
                'performance_sort',   # 2. Performance
                'URL'                 # 3. URL alfabética
            ], ascending=[True, True, True])
            
            # Remove colunas auxiliares
            df_status = df_status.drop(['categoria_sort', 'performance_sort'], axis=1)
            
            # 📤 EXPORTA
            df_status.to_excel(self.writer, index=False, sheet_name="Status_HTTP")
            
            # 📊 ESTATÍSTICAS
            urls_com_sucesso = len([r for r in resultados if r.get('sucesso', False)])
            urls_com_erro = len([r for r in rows if r['Status_Categoria'] in ['ERRO', 'ERRO_CLIENTE', 'ERRO_SERVIDOR']])
            urls_com_redirect = len([r for r in rows if r['Houve_Redirect'] == 'Sim'])
            tempo_medio = sum([r['tempo_resposta'] for r in resultados]) / len(resultados) if resultados else 0
            
            print(f"   ✅ URLs verificadas: {urls_com_sucesso}")
            print(f"   🚨 URLs com erro: {urls_com_erro}")
            print(f"   🔄 URLs com redirect: {urls_com_redirect}")
            print(f"   ⚡ Tempo médio de resposta: {tempo_medio:.2f}s")
            print(f"   📋 Aba 'Status_HTTP' criada com análise CIRÚRGICA")
            print(f"   🎯 Inclui: Status real, redirects, performance, headers SEO")
            
            return df_status
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico HTTP: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'URL', 'Status_Code', 'Status_Categoria', 'Status_Descricao', 'URL_Final',
                'Houve_Redirect', 'Numero_Redirects', 'Tempo_Resposta', 'Performance',
                'Content_Type', 'Server', 'Tamanho_Content'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="Status_HTTP")
            return df_erro