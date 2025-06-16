# exporters/sheets/errors_4xx_sheet.py - ENGINE CIRÚRGICA v3.0 HARDENED
# ❌ ENGINE CIRÚRGICA: Erros 4xx + URL SANITIZER INTEGRADO
# 🔧 v3.0: PARA DE SER COVARDE - sanitiza URLs sem protocolo e TENTA!

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter

class Errors4xxSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para análise de erros 4xx"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        })
        return session

    def _analisar_error_4xx_cirurgico(self, url: str) -> dict:
        """❌ Análise cirúrgica de erros 4xx"""
        
        try:
            response = self.session.get(url, timeout=10, verify=False, allow_redirects=True)
            status_code = response.status_code
            
            # Só processa se for 4xx
            if 400 <= status_code < 500:
                # Analisa o tipo de erro
                tipo_erro = self._classificar_error_4xx(status_code)
                prioridade = self._analisar_prioridade(status_code, url)
                
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_error_4xx': True,
                    'status_code': status_code,
                    'tipo_erro': tipo_erro,
                    'prioridade': prioridade,
                    'tempo_resposta': response.elapsed.total_seconds(),
                    'content_type': response.headers.get('content-type', 'Desconhecido'),
                    'content_length': len(response.content) if response.content else 0,
                    'tem_pagina_erro': self._analisar_pagina_erro(response),
                    'sugestao_acao': self._sugerir_acao(status_code, url),
                    'possivel_origem': self._identificar_origem_erro(url)
                }
            else:
                # Não é erro 4xx
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_error_4xx': False,
                    'status_code': status_code,
                    'motivo': f'Status {status_code} não é 4xx'
                }
            
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'tem_error_4xx': False,
                'status_code': 'ERROR',
                'motivo': f'Erro na análise: {str(e)}'
            }

    def _classificar_error_4xx(self, status_code) -> str:
        """🏷️ Classifica tipo de erro 4xx"""
        error_types = {
            400: 'Bad Request',
            401: 'Unauthorized',
            403: 'Forbidden',
            404: 'Not Found',
            405: 'Method Not Allowed',
            406: 'Not Acceptable',
            408: 'Request Timeout',
            409: 'Conflict',
            410: 'Gone',
            411: 'Length Required',
            413: 'Payload Too Large',
            414: 'URI Too Long',
            415: 'Unsupported Media Type',
            429: 'Too Many Requests'
        }
        return error_types.get(status_code, f'Erro 4xx ({status_code})')

    def _analisar_prioridade(self, status_code, url: str) -> str:
        """🎯 Analisa prioridade de correção"""
        if status_code == 404:
            # 404 em página importante = alta prioridade
            if any(importante in url.lower() for importante in [
                '/home', '/index', '/sobre', '/contato', '/servicos', '/produtos'
            ]):
                return 'ALTA - Página importante inacessível'
            else:
                return 'MÉDIA - Página não encontrada'
        elif status_code == 403:
            return 'ALTA - Acesso negado precisa revisão'
        elif status_code == 410:
            return 'BAIXA - Página removida permanentemente'
        elif status_code == 401:
            return 'MÉDIA - Problema de autenticação'
        elif status_code == 429:
            return 'ALTA - Limite de requests atingido'
        else:
            return f'VERIFICAR - Status {status_code} precisa análise'

    def _analisar_pagina_erro(self, response) -> str:
        """📄 Analisa se tem página de erro customizada"""
        if not response.content:
            return 'Sem conteúdo'
        
        content_text = response.text[:1000].lower()  # Primeiros 1000 chars
        
        # Verifica se é página de erro customizada
        error_indicators = [
            'página não encontrada', 'page not found', '404', 'erro 404',
            'not found', 'página inexistente', 'conteúdo não encontrado',
            'acesso negado', 'forbidden', '403', 'não autorizado'
        ]
        
        has_error_page = any(indicator in content_text for indicator in error_indicators)
        
        if has_error_page:
            return 'Página de erro customizada'
        elif len(response.content) < 500:
            return 'Resposta muito pequena'
        else:
            return 'Conteúdo padrão do servidor'

    def _sugerir_acao(self, status_code, url: str) -> str:
        """💡 Sugere ação corretiva"""
        if status_code == 404:
            return 'Verificar URL ou criar redirect 301'
        elif status_code == 403:
            return 'Revisar permissões de acesso'
        elif status_code == 410:
            return 'Remover links para esta página'
        elif status_code == 401:
            return 'Verificar configuração de autenticação'
        elif status_code == 429:
            return 'Revisar rate limiting do servidor'
        else:
            return f'Investigar causa específica do erro {status_code}'

    def _identificar_origem_erro(self, url: str) -> str:
        """🔍 Identifica possível origem do erro"""
        if '/wp-admin' in url or '/admin' in url:
            return 'Área administrativa'
        elif '/api/' in url:
            return 'Endpoint de API'
        elif any(ext in url.lower() for ext in ['.pdf', '.doc', '.zip']):
            return 'Arquivo/Download'
        elif '?' in url and '=' in url:
            return 'Página dinâmica'
        elif url.count('/') > 4:
            return 'Página profunda'
        else:
            return 'Página regular'

    def _sanitizar_url_hardened(self, url_raw: str) -> str:
        """🔧 SANITIZER v3.0: PARA DE SER COVARDE - força HTTPS e tenta!"""
        
        if not url_raw or pd.isna(url_raw):
            return None
        
        url = str(url_raw).strip()
        
        if not url:
            return None
        
        # 🔧 STEP 1: Remove espaços e caracteres problemáticos
        import re
        url = re.sub(r'\s+', '', url)  # Remove todos os espaços
        url = url.replace('\n', '').replace('\r', '').replace('\t', '')
        
        # 🔧 STEP 2: Corrige protocolos malformados
        if url.startswith('http//') or url.startswith('https//'):
            url = url.replace('//', '://', 1)
        
        # 🔧 STEP 3: FORÇA HTTPS se não tem protocolo (HARDENED!)
        if not url.startswith(('http://', 'https://')):
            # Se parece com domínio, força https://
            if '.' in url and not url.startswith(('/', '#', '?')):
                url = f"https://{url}"
            else:
                return None  # URL inválida
        
        # 🔧 STEP 4: Remove fragmentos e parâmetros problemáticos
        if '#' in url:
            url = url.split('#')[0]
        
        # 🔧 STEP 5: Normaliza múltiplas barras
        if '://' in url:
            protocol, rest = url.split('://', 1)
            rest = re.sub(r'/+', '/', rest)  # Remove múltiplas barras
            url = f"{protocol}://{rest}"
        
        # 🔧 STEP 6: Remove barra final desnecessária
        if url.endswith('/') and url.count('/') > 3:
            url = url.rstrip('/')
        
        # 🔧 STEP 7: Valida URL final
        if len(url) > 2000:  # URLs muito longas
            return None
        
        if not ('.' in url and ('://' in url)):
            return None
        
        return url

    def _filtrar_urls_validas(self, urls_input) -> list:
        """🧹 HARDENED v3.0: SANITIZA e NÃO É MAIS COVARDE!"""
        
        # 🔧 EXTRAÇÃO UNIVERSAL DE URLs
        urls_candidatas = []
        
        if isinstance(urls_input, pd.DataFrame):
            urls_candidatas = urls_input
        elif isinstance(urls_input, (list, pd.Series)):
            df_mock = pd.DataFrame({'url': urls_input})
            urls_candidatas = df_mock
        else:
            try:
                df_mock = pd.DataFrame({'url': list(urls_input)})
                urls_candidatas = df_mock
            except:
                print(f"⚠️ Erro: Não conseguiu processar input tipo {type(urls_input)}")
                return []
        
        urls_validas = []
        stats = {'total': 0, 'sanitizadas': 0, 'forcadas_https': 0, 'descartadas': 0}
        
        for _, row in urls_candidatas.iterrows():
            url_raw = row.get('url', '')
            stats['total'] += 1
            
            if not url_raw or pd.isna(url_raw):
                stats['descartadas'] += 1
                continue
            
            # 🔧 SANITIZAÇÃO HARDENED
            url_sanitizada = self._sanitizar_url_hardened(url_raw)
            
            if not url_sanitizada:
                stats['descartadas'] += 1
                continue
            
            # Conta estatísticas
            if url_sanitizada != str(url_raw).strip():
                stats['sanitizadas'] += 1
                
            if not str(url_raw).strip().startswith(('http://', 'https://')) and url_sanitizada.startswith('https://'):
                stats['forcadas_https'] += 1
            
            # 🔧 Remove extensões que raramente dão 4xx úteis
            if any(url_sanitizada.lower().endswith(ext) for ext in [
                '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico',
                '.css', '.js', '.woff', '.woff2', '.ttf'
            ]):
                stats['descartadas'] += 1
                continue
            
            urls_validas.append(url_sanitizada)
        
        # Remove duplicatas finais
        urls_unicas = list(set(urls_validas))
        
        print(f"   🔧 SANITIZAÇÃO HARDENED v3.0:")
        print(f"      📊 Total processadas: {stats['total']}")
        print(f"      🔧 URLs sanitizadas: {stats['sanitizadas']}")
        print(f"      🚀 Forçadas para HTTPS: {stats['forcadas_https']}")
        print(f"      ❌ Descartadas: {stats['descartadas']}")
        print(f"      ✅ URLs válidas únicas: {len(urls_unicas)}")
        
        return urls_unicas

    def _analisar_errors_4xx_paralelo(self, urls: list) -> list:
        """🚀 Análise paralela de erros 4xx"""
        
        print(f"❌ Análise HARDENED de erros 4xx iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._analisar_error_4xx_cirurgico, url): url 
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

    def export(self):
        """❌ Gera aba CIRÚRGICA HARDENED de erros 4xx"""
        try:
            print(f"❌ ERRORS 4XX - ENGINE CIRÚRGICA v3.0 HARDENED")
            
            # 📋 PREPARAÇÃO DOS DADOS COM SANITIZAÇÃO
            urls_filtradas = self._filtrar_urls_validas(self.df)
            
            print(f"   📊 URLs no DataFrame: {len(self.df) if hasattr(self.df, '__len__') else 'N/A'}")
            print(f"   🎯 Foco: Erros 4xx (404, 403, 401, 410) para correção de conteúdo")
            print(f"   🔧 v3.0: URL Sanitizer integrado - PARA DE SER COVARDE!")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida após sanitização")
                self._criar_aba_vazia()
                return pd.DataFrame()
            
            # ❌ ANÁLISE CIRÚRGICA PARALELA
            resultados = self._analisar_errors_4xx_paralelo(urls_filtradas)
            
            # 📋 GERA LINHAS APENAS PARA ERROS 4XX
            rows = []
            
            for resultado in resultados:
                # SÓ INCLUI SE TEM ERROR 4XX
                if resultado.get('tem_error_4xx', False) and resultado.get('sucesso', False):
                    rows.append({
                        'URL': resultado['url'],
                        'Status': resultado['status_code'],
                        'Tipo_Erro': resultado['tipo_erro'],
                        'Prioridade': resultado['prioridade'],
                        'Tempo_Resposta': f"{resultado['tempo_resposta']:.2f}s",
                        'Content_Type': resultado['content_type'],
                        'Tem_Pagina_Erro': resultado['tem_pagina_erro'],
                        'Sugestao_Acao': resultado['sugestao_acao'],
                        'Possivel_Origem': resultado['possivel_origem']
                    })
            
            # Se não encontrou erros 4xx
            if not rows:
                print(f"   🎉 PERFEITO: Nenhum erro 4xx encontrado!")
                self._criar_aba_vazia()
                return pd.DataFrame()
            
            df_errors = pd.DataFrame(rows)
            
            # ❌ ORDENAÇÃO POR PRIORIDADE E STATUS
            prioridade_order = {'ALTA': 1, 'MÉDIA': 2, 'BAIXA': 3, 'VERIFICAR': 4}
            df_errors['sort_prioridade'] = df_errors['Prioridade'].str.split(' -').str[0].map(prioridade_order).fillna(99)
            df_errors = df_errors.sort_values(['sort_prioridade', 'Status', 'URL'])
            df_errors = df_errors.drop('sort_prioridade', axis=1)
            
            # 📤 EXPORTA
            df_errors.to_excel(self.writer, index=False, sheet_name="Errors_4xx")
            
            # 📊 ESTATÍSTICAS
            urls_analisadas = len([r for r in resultados if r.get('sucesso', False)])
            total_errors = len(rows)
            
            # Stats por tipo
            stats_status = df_errors['Status'].value_counts()
            stats_prioridade = df_errors['Prioridade'].str.split(' -').str[0].value_counts()
            
            print(f"   ✅ URLs analisadas: {urls_analisadas}")
            print(f"   ❌ Erros 4xx encontrados: {total_errors}")
            
            # Estatísticas por status
            for status, count in stats_status.items():
                print(f"      • {status}: {count} erros")
            
            # Estatísticas por prioridade
            for prioridade, count in stats_prioridade.items():
                print(f"      • Prioridade {prioridade}: {count} erros")
            
            print(f"   📋 Aba 'Errors_4xx' criada com análise CIRÚRGICA HARDENED")
            print(f"   🎯 Agora captura URLs sem protocolo que eram jogadas fora!")
            
            return df_errors
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico 4xx v3.0: {e}")
            import traceback
            traceback.print_exc()
            
            self._criar_aba_vazia()
            return pd.DataFrame()

    def _criar_aba_vazia(self):
        """📋 Cria aba vazia quando não há dados"""
        df_vazio = pd.DataFrame(columns=[
            'URL', 'Status', 'Tipo_Erro', 'Prioridade', 'Tempo_Resposta', 
            'Content_Type', 'Tem_Pagina_Erro', 'Sugestao_Acao', 'Possivel_Origem'
        ])
        df_vazio.to_excel(self.writer, index=False, sheet_name="Errors_4xx")