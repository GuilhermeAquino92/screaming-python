# ssl_problemas_sheet.py - Engine Cirúrgica SSL Enterprise 🔒

import pandas as pd
import requests
import ssl
import socket
import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import warnings
warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

# ========================
# 🔒 CONFIGURAÇÃO SSL ENTERPRISE
# ========================

# SSL Status blacklist
SSL_PROBLEMAS_CRITICOS = [
    'expired',
    'self_signed',
    'chain_incomplete',
    'hostname_mismatch',
    'revoked',
    'weak_cipher'
]

# Grades SSL
SSL_GRADES = {
    'A+': {'score': 100, 'status': 'EXCELENTE'},
    'A': {'score': 90, 'status': 'MUITO_BOM'},
    'B': {'score': 80, 'status': 'BOM'},
    'C': {'score': 70, 'status': 'REGULAR'},
    'D': {'score': 60, 'status': 'RUIM'},
    'F': {'score': 0, 'status': 'CRITICO'}
}

def calcular_threads_ssl():
    """🧮 Calcula threads otimizadas para SSL"""
    import os
    return min(max(os.cpu_count() // 2, 2), 20)

# ========================
# 🔍 VALIDADOR SSL CIRÚRGICO
# ========================

def verificar_ssl_cirurgico(url: str, timeout: int = 10) -> dict:
    """🔍 Verificação SSL cirúrgica completa"""
    
    resultado = {
        'url': url,
        'dominio': '',
        'tem_problema': False,
        'problema_principal': 'SSL_VALIDO',
        'problemas_detalhados': [],
        'grade_ssl': 'A',
        'score_ssl': 90,
        'certificado_valido': True,
        'expira_em_dias': None,
        'data_expiracao': '',
        'emissor': '',
        'sujeito': '',
        'algoritmo_assinatura': '',
        'protocolo_tls': '',
        'cipher_suite': '',
        'chain_completa': True,
        'hsts_ativo': False,
        'mixed_content_risk': False,
        'recomendacoes': [],
        'timestamp_verificacao': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    try:
        # Parse da URL
        parsed = urlparse(url)
        dominio = parsed.netloc
        resultado['dominio'] = dominio
        
        if not dominio:
            resultado.update({
                'tem_problema': True,
                'problema_principal': 'URL_INVALIDA',
                'grade_ssl': 'F',
                'score_ssl': 0,
                'certificado_valido': False
            })
            return resultado
        
        # Só verifica HTTPS
        if parsed.scheme != 'https':
            resultado.update({
                'tem_problema': True,
                'problema_principal': 'NAO_HTTPS',
                'grade_ssl': 'F',
                'score_ssl': 0,
                'certificado_valido': False,
                'recomendacoes': ['Migrar para HTTPS']
            })
            return resultado
        
        # 1. VERIFICAÇÃO DE CONEXÃO SSL
        print(f"🔍 Verificando SSL: {dominio}")
        
        port = parsed.port or 443
        
        # Cria contexto SSL
        context = ssl.create_default_context()
        
        with socket.create_connection((dominio, port), timeout=timeout) as sock:
            with context.wrap_socket(sock, server_hostname=dominio) as ssock:
                
                # Pega certificado
                cert = ssock.getpeercert()
                cipher = ssock.cipher()
                version = ssock.version()
                
                # 2. ANÁLISE DO CERTIFICADO
                if cert:
                    resultado['emissor'] = cert.get('issuer', [{}])[0].get('organizationName', 'N/A')
                    resultado['sujeito'] = cert.get('subject', [{}])[0].get('commonName', dominio)
                    
                    # Data de expiração
                    not_after = cert.get('notAfter', '')
                    if not_after:
                        try:
                            expiry_date = datetime.datetime.strptime(not_after, '%b %d %H:%M:%S %Y %Z')
                            dias_para_expirar = (expiry_date - datetime.datetime.now()).days
                            
                            resultado['expira_em_dias'] = dias_para_expirar
                            resultado['data_expiracao'] = expiry_date.strftime('%Y-%m-%d')
                            
                            # Verifica se está expirando
                            if dias_para_expirar < 0:
                                resultado['problemas_detalhados'].append('Certificado expirado')
                                resultado['tem_problema'] = True
                                resultado['problema_principal'] = 'CERT_EXPIRADO'
                            elif dias_para_expirar < 30:
                                resultado['problemas_detalhados'].append(f'Expira em {dias_para_expirar} dias')
                                resultado['recomendacoes'].append('Renovar certificado SSL')
                        except:
                            resultado['problemas_detalhados'].append('Erro ao verificar expiração')
                
                # 3. ANÁLISE DO CIPHER E PROTOCOLO
                if cipher:
                    resultado['cipher_suite'] = cipher[0] if cipher[0] else 'N/A'
                    resultado['algoritmo_assinatura'] = cipher[1] if len(cipher) > 1 else 'N/A'
                
                if version:
                    resultado['protocolo_tls'] = version
                    
                    # Verifica protocolos fracos
                    if version in ['TLSv1', 'TLSv1.1', 'SSLv2', 'SSLv3']:
                        resultado['problemas_detalhados'].append(f'Protocolo fraco: {version}')
                        resultado['tem_problema'] = True
                        if not resultado['problema_principal'] or resultado['problema_principal'] == 'SSL_VALIDO':
                            resultado['problema_principal'] = 'PROTOCOLO_FRACO'
        
        # 4. VERIFICAÇÃO HTTP RESPONSE COM HEADERS ENTERPRISE
        try:
            # Headers enterprise para evitar blocks de CDN/Firewall
            headers_enterprise = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            
            response = requests.get(url, headers=headers_enterprise, timeout=timeout, verify=True)
            
            # HSTS Headers
            if 'strict-transport-security' in response.headers:
                resultado['hsts_ativo'] = True
            else:
                resultado['problemas_detalhados'].append('HSTS não configurado')
                resultado['recomendacoes'].append('Configurar HSTS headers')
            
            # Mixed content check básico
            if 'http://' in response.text and parsed.scheme == 'https':
                resultado['mixed_content_risk'] = True
                resultado['problemas_detalhados'].append('Risco de mixed content detectado')
                resultado['recomendacoes'].append('Verificar links HTTP em página HTTPS')
        
        except requests.exceptions.SSLError as ssl_err:
            resultado.update({
                'tem_problema': True,
                'problema_principal': 'SSL_ERROR',
                'certificado_valido': False,
                'problemas_detalhados': [f'Erro SSL: {str(ssl_err)}']
            })
        except requests.exceptions.ConnectionError:
            resultado['problemas_detalhados'].append('Erro de conexão durante verificação HTTP')
        except Exception as http_err:
            resultado['problemas_detalhados'].append(f'Erro HTTP: {str(http_err)}')
        
        # 5. CÁLCULO DA GRADE SSL
        score = 100
        grade = 'A+'
        
        # Penalizações
        if resultado['problemas_detalhados']:
            score -= len(resultado['problemas_detalhados']) * 10
        
        if not resultado['hsts_ativo']:
            score -= 5
        
        if resultado['mixed_content_risk']:
            score -= 15
        
        if resultado['protocolo_tls'] in ['TLSv1', 'TLSv1.1']:
            score -= 20
        
        if resultado['expira_em_dias'] and resultado['expira_em_dias'] < 30:
            score -= 10
        
        # Determina grade
        if score >= 95:
            grade = 'A+'
        elif score >= 85:
            grade = 'A'
        elif score >= 75:
            grade = 'B'
        elif score >= 65:
            grade = 'C'
        elif score >= 50:
            grade = 'D'
        else:
            grade = 'F'
        
        resultado['score_ssl'] = max(score, 0)
        resultado['grade_ssl'] = grade
        
        # Define se tem problema baseado na grade
        if grade in ['D', 'F'] or resultado['tem_problema']:
            resultado['tem_problema'] = True
            if resultado['problema_principal'] == 'SSL_VALIDO':
                resultado['problema_principal'] = 'SSL_DEGRADADO'
    
    except socket.timeout:
        resultado.update({
            'tem_problema': True,
            'problema_principal': 'TIMEOUT_SSL',
            'grade_ssl': 'F',
            'score_ssl': 0,
            'certificado_valido': False,
            'problemas_detalhados': ['Timeout na conexão SSL']
        })
    except socket.gaierror:
        resultado.update({
            'tem_problema': True,
            'problema_principal': 'DNS_ERROR',
            'grade_ssl': 'F',
            'score_ssl': 0,
            'certificado_valido': False,
            'problemas_detalhados': ['Erro de resolução DNS']
        })
    except ssl.SSLError as ssl_err:
        resultado.update({
            'tem_problema': True,
            'problema_principal': 'SSL_HANDSHAKE_ERROR',
            'grade_ssl': 'F',
            'score_ssl': 0,
            'certificado_valido': False,
            'problemas_detalhados': [f'Erro SSL: {str(ssl_err)}']
        })
    except Exception as e:
        resultado.update({
            'tem_problema': True,
            'problema_principal': 'ERRO_DESCONHECIDO',
            'grade_ssl': 'F',
            'score_ssl': 0,
            'certificado_valido': False,
            'problemas_detalhados': [f'Erro: {str(e)}']
        })
    
    return resultado

def verificar_ssl_multiplas_urls(urls: list, max_threads: int = None) -> list:
    """🔍 Verifica SSL de múltiplas URLs em paralelo"""
    
    if not urls:
        return []
    
    max_threads = max_threads or calcular_threads_ssl()
    resultados = []
    
    print(f"🔍 Verificando SSL de {len(urls)} URLs com {max_threads} threads...")
    
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_url = {executor.submit(verificar_ssl_cirurgico, url): url for url in urls}
        
        for future in as_completed(future_to_url):
            try:
                resultado = future.result(timeout=30)
                resultados.append(resultado)
                
                # Log do progresso
                if len(resultados) % 10 == 0:
                    print(f"   📊 Processadas: {len(resultados)}/{len(urls)}")
                    
            except Exception as e:
                url = future_to_url[future]
                print(f"   ❌ Erro em {url}: {e}")
                
                # Adiciona resultado de erro
                resultados.append({
                    'url': url,
                    'dominio': urlparse(url).netloc,
                    'tem_problema': True,
                    'problema_principal': 'ERRO_VERIFICACAO',
                    'grade_ssl': 'F',
                    'score_ssl': 0,
                    'certificado_valido': False,
                    'problemas_detalhados': [f'Erro na verificação: {str(e)}']
                })
    
    print(f"✅ Verificação SSL concluída: {len(resultados)} resultados")
    return resultados

# ========================
# 🔒 ENGINE CIRÚRGICA SSL
# ========================

class SSLProblemasSheet:
    """🔒 Engine cirúrgica para problemas SSL"""
    
    def __init__(self, df, writer):
        self.df = df
        self.writer = writer
        self.sheet_name = 'SSL_Problemas'
    
    def export(self):
        """🔒 Exporta aba SSL_Problemas com análise cirúrgica"""
        
        try:
            print(f"🔒 Analisando problemas SSL...")
            
            # Extrai URLs únicas
            urls = self._extrair_urls_unicas()
            
            if not urls:
                print(f"   ⚠️ Nenhuma URL para verificar SSL")
                self._criar_aba_vazia()
                return
            
            print(f"   📊 Verificando SSL de {len(urls)} URLs...")
            
            # Verifica SSL
            resultados_ssl = verificar_ssl_multiplas_urls(urls, max_threads=15)
            
            # Filtra só problemas
            problemas_ssl = [r for r in resultados_ssl if r.get('tem_problema', False)]
            
            if not problemas_ssl:
                print(f"   ✅ Nenhum problema SSL encontrado!")
                self._criar_aba_sem_problemas()
                return
            
            # Cria DataFrame
            df_ssl = self._criar_dataframe_ssl(problemas_ssl)
            
            # Exporta aba
            self._exportar_aba_ssl(df_ssl)
            
            print(f"   ✅ SSL_Problemas: {len(problemas_ssl)} problemas encontrados")
            
        except Exception as e:
            print(f"   ❌ Erro na engine SSL: {e}")
            self._criar_aba_vazia()
    
    def _extrair_urls_unicas(self) -> list:
        """📊 Extrai URLs únicas do DataFrame"""
        
        if 'url' not in self.df.columns:
            return []
        
        urls = self.df['url'].dropna().unique().tolist()
        
        # Filtra só HTTPS
        urls_https = [url for url in urls if isinstance(url, str) and url.startswith('https://')]
        
        # Remove duplicatas de domínio (só verifica o domínio principal)
        dominios_verificados = set()
        urls_filtradas = []
        
        for url in urls_https:
            try:
                dominio = urlparse(url).netloc
                if dominio not in dominios_verificados:
                    dominios_verificados.add(dominio)
                    urls_filtradas.append(url)
            except:
                continue
        
        return urls_filtradas[:50]  # Limita a 50 domínios
    
    def _criar_dataframe_ssl(self, problemas_ssl: list) -> pd.DataFrame:
        """📊 Cria DataFrame dos problemas SSL"""
        
        rows = []
        
        for ssl_data in problemas_ssl:
            # Formata problemas
            problemas_texto = '; '.join(ssl_data.get('problemas_detalhados', []))
            recomendacoes_texto = '; '.join(ssl_data.get('recomendacoes', []))
            
            # Determina gravidade
            grade = ssl_data.get('grade_ssl', 'F')
            if grade in ['F']:
                gravidade = 'CRITICO'
            elif grade in ['D']:
                gravidade = 'ALTO'
            elif grade in ['C']:
                gravidade = 'MEDIO'
            else:
                gravidade = 'BAIXO'
            
            # Formata dias para expiração
            expira_em = ssl_data.get('expira_em_dias', None)
            if expira_em is not None:
                if expira_em < 0:
                    status_expiracao = 'EXPIRADO'
                elif expira_em < 30:
                    status_expiracao = f'EXPIRA_EM_{expira_em}_DIAS'
                else:
                    status_expiracao = 'VALIDO'
            else:
                status_expiracao = 'N/A'
            
            rows.append({
                'Dominio': ssl_data.get('dominio', ''),
                'URL_Verificada': ssl_data.get('url', ''),
                'Problema_Principal': ssl_data.get('problema_principal', ''),
                'Grade_SSL': grade,
                'Score_SSL': ssl_data.get('score_ssl', 0),
                'Gravidade': gravidade,
                'Certificado_Valido': 'SIM' if ssl_data.get('certificado_valido', False) else 'NAO',
                'Status_Expiracao': status_expiracao,
                'Data_Expiracao': ssl_data.get('data_expiracao', ''),
                'Protocolo_TLS': ssl_data.get('protocolo_tls', 'N/A'),
                'HSTS_Ativo': 'SIM' if ssl_data.get('hsts_ativo', False) else 'NAO',
                'Mixed_Content_Risk': 'SIM' if ssl_data.get('mixed_content_risk', False) else 'NAO',
                'Emissor_Certificado': ssl_data.get('emissor', 'N/A'),
                'Problemas_Detalhados': problemas_texto,
                'Recomendacoes': recomendacoes_texto,
                'Timestamp_Verificacao': ssl_data.get('timestamp_verificacao', '')
            })
        
        df = pd.DataFrame(rows)
        
        # Ordenação por gravidade e grade
        gravidade_order = {'CRITICO': 1, 'ALTO': 2, 'MEDIO': 3, 'BAIXO': 4}
        df['sort_gravidade'] = df['Gravidade'].map(gravidade_order).fillna(99)
        
        grade_order = {'F': 1, 'D': 2, 'C': 3, 'B': 4, 'A': 5, 'A+': 6}
        df['sort_grade'] = df['Grade_SSL'].map(grade_order).fillna(99)
        
        df = df.sort_values(['sort_gravidade', 'sort_grade', 'Dominio'])
        df = df.drop(['sort_gravidade', 'sort_grade'], axis=1)
        
        return df
    
    def _exportar_aba_ssl(self, df_ssl: pd.DataFrame):
        """📤 Exporta aba SSL com formatação"""
        
        df_ssl.to_excel(self.writer, sheet_name=self.sheet_name, index=False)
        
        # Formatação básica
        worksheet = self.writer.sheets[self.sheet_name]
        
        # Headers em negrito
        if hasattr(self.writer, 'book'):
            header_format = self.writer.book.add_format({
                'bold': True,
                'bg_color': '#D7E4BC',
                'border': 1
            })
            
            # Formato para problemas críticos
            critico_format = self.writer.book.add_format({
                'bg_color': '#FFE6E6',
                'border': 1
            })
            
            # Formato para problemas altos
            alto_format = self.writer.book.add_format({
                'bg_color': '#FFF2E6',
                'border': 1
            })
            
            # Aplica formatação nos headers
            for col_num, value in enumerate(df_ssl.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Aplica cores baseadas na gravidade
            for row_num in range(1, len(df_ssl) + 1):
                gravidade = df_ssl.iloc[row_num - 1]['Gravidade']
                
                if gravidade == 'CRITICO':
                    for col_num in range(len(df_ssl.columns)):
                        cell_value = df_ssl.iloc[row_num - 1, col_num]
                        worksheet.write(row_num, col_num, cell_value, critico_format)
                elif gravidade == 'ALTO':
                    for col_num in range(len(df_ssl.columns)):
                        cell_value = df_ssl.iloc[row_num - 1, col_num]
                        worksheet.write(row_num, col_num, cell_value, alto_format)
            
            # Ajusta largura das colunas
            worksheet.set_column(0, 0, 20)  # Dominio
            worksheet.set_column(1, 1, 40)  # URL_Verificada
            worksheet.set_column(2, 2, 20)  # Problema_Principal
            worksheet.set_column(3, 3, 10)  # Grade_SSL
            worksheet.set_column(4, 4, 10)  # Score_SSL
            worksheet.set_column(13, 13, 50)  # Problemas_Detalhados
            worksheet.set_column(14, 14, 50)  # Recomendacoes
    
    def _criar_aba_vazia(self):
        """📄 Cria aba vazia quando não há dados"""
        
        df_vazio = pd.DataFrame({
            'Dominio': [],
            'Problema_Principal': [],
            'Grade_SSL': [],
            'Gravidade': [],
            'Problemas_Detalhados': [],
            'Recomendacoes': []
        })
        
        df_vazio.to_excel(self.writer, sheet_name=self.sheet_name, index=False)
    
    def _criar_aba_sem_problemas(self):
        """✅ Cria aba indicando que não há problemas"""
        
        df_ok = pd.DataFrame({
            'Status': ['TODOS_CERTIFICADOS_VALIDOS'],
            'Verificacao': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            'Observacao': ['Nenhum problema SSL crítico encontrado']
        })
        
        df_ok.to_excel(self.writer, sheet_name=self.sheet_name, index=False)

# ========================
# 🧪 TESTE STANDALONE
# ========================

if __name__ == "__main__":
    # Teste básico
    url_teste = "https://example.com"
    resultado = verificar_ssl_cirurgico(url_teste)
    
    print(f"🧪 TESTE SSL CIRÚRGICO")
    print(f"URL: {url_teste}")
    print(f"Problema: {resultado['tem_problema']}")
    print(f"Grade: {resultado['grade_ssl']}")
    print(f"Score: {resultado['score_ssl']}")
    print(f"Problemas: {resultado['problemas_detalhados']}")