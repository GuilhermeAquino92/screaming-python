# exporters/sheets/redirects_3xx_sheet.py - ENGINE CIRÚRGICA
# 🔄 ENGINE CIRÚRGICA: Análise completa de redirects 3xx para otimização SEO

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter

class Redirects3xxSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para análise de redirects"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        })
        return session

    def _analisar_redirect_cirurgico(self, url: str) -> dict:
        """🔄 Análise cirúrgica de redirects 3xx"""
        
        try:
            # Configuração para capturar redirects
            response = self.session.get(url, timeout=10, verify=False, allow_redirects=True)
            
            # Verifica se houve redirect
            if response.history:
                # Pega o primeiro redirect da cadeia
                primeiro_redirect = response.history[0]
                status_code = primeiro_redirect.status_code
                
                # Só processa se for 3xx
                if 300 <= status_code < 400:
                    # Analisa o tipo de redirect
                    tipo_redirect = self._classificar_redirect(status_code)
                    impacto_seo = self._analisar_impacto_seo(status_code)
                    
                    return {
                        'url': url,
                        'sucesso': True,
                        'tem_redirect': True,
                        'status_code': status_code,
                        'url_destino': response.url,
                        'tipo_redirect': tipo_redirect,
                        'impacto_seo': impacto_seo,
                        'cadeia_redirects': len(response.history),
                        'tempo_resposta': response.elapsed.total_seconds(),
                        'headers_relevantes': self._extrair_headers_redirect(primeiro_redirect.headers)
                    }
                else:
                    # Redirect não é 3xx
                    return {
                        'url': url,
                        'sucesso': True,
                        'tem_redirect': False,
                        'status_code': status_code,
                        'motivo': f'Redirect {status_code} não é 3xx'
                    }
            else:
                # Sem redirect
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_redirect': False,
                    'status_code': response.status_code,
                    'motivo': 'Sem redirect'
                }
            
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'tem_redirect': False,
                'status_code': 'ERROR',
                'motivo': f'Erro: {str(e)}'
            }

    def _classificar_redirect(self, status_code: int) -> str:
        """🏷️ Classifica tipo de redirect"""
        redirect_types = {
            301: 'Permanente',
            302: 'Temporário',
            303: 'See Other',
            307: 'Temporário (Preserva Método)',
            308: 'Permanente (Preserva Método)'
        }
        return redirect_types.get(status_code, f'3xx ({status_code})')

    def _analisar_impacto_seo(self, status_code: int) -> str:
        """🎯 Analisa impacto SEO do redirect"""
        if status_code == 301:
            return 'NEUTRO - Transfere autoridade'
        elif status_code == 302:
            return 'MÉDIO - Pode confundir crawlers'
        elif status_code == 308:
            return 'NEUTRO - Transfere autoridade'
        elif status_code == 307:
            return 'MÉDIO - Temporário, não transfere autoridade'
        else:
            return f'VERIFICAR - Status {status_code} raro'

    def _extrair_headers_redirect(self, headers) -> str:
        """📋 Extrai headers relevantes para redirects"""
        headers_importantes = []
        
        if 'location' in headers:
            headers_importantes.append(f"Location: {headers['location']}")
        if 'cache-control' in headers:
            headers_importantes.append(f"Cache-Control: {headers['cache-control']}")
        if 'expires' in headers:
            headers_importantes.append(f"Expires: {headers['expires']}")
        
        return ' | '.join(headers_importantes) if headers_importantes else 'Sem headers relevantes'

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
            
            # Remove extensões que provavelmente não redirectam
            if any(url_str.lower().endswith(ext) for ext in [
                '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.rar',
                '.js', '.css', '.mp3', '.mp4', '.xml', '.json'
            ]):
                continue
            
            urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _analisar_redirects_paralelo(self, urls: list) -> list:
        """🚀 Análise paralela de redirects"""
        
        print(f"🔄 Análise de redirects 3xx iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._analisar_redirect_cirurgico, url): url 
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
        """🔄 Gera aba CIRÚRGICA de redirects 3xx"""
        try:
            print(f"🔄 REDIRECTS 3XX - ENGINE CIRÚRGICA")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_filtradas = self._filtrar_urls_validas(self.df)
            
            print(f"   📊 URLs no DataFrame: {len(self.df)}")
            print(f"   🧹 URLs válidas: {len(urls_filtradas)}")
            print(f"   🎯 Foco: Redirects 3xx (301, 302, 307, 308) para otimização SEO")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para análise")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Status', 'Tipo_Redirect', 'URL_Destino', 'Impacto_SEO', 
                    'Cadeia_Redirects', 'Tempo_Resposta', 'Headers_Relevantes'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Redirects_3xx")
                return df_vazio
            
            # 🔄 ANÁLISE CIRÚRGICA PARALELA
            resultados = self._analisar_redirects_paralelo(urls_filtradas)
            
            # 📋 GERA LINHAS APENAS PARA REDIRECTS 3XX
            rows = []
            
            for resultado in resultados:
                # SÓ INCLUI SE TEM REDIRECT 3XX
                if resultado.get('tem_redirect', False) and resultado.get('sucesso', False):
                    rows.append({
                        'URL': resultado['url'],
                        'Status': resultado['status_code'],
                        'Tipo_Redirect': resultado['tipo_redirect'],
                        'URL_Destino': resultado['url_destino'],
                        'Impacto_SEO': resultado['impacto_seo'],
                        'Cadeia_Redirects': resultado['cadeia_redirects'],
                        'Tempo_Resposta': f"{resultado['tempo_resposta']:.2f}s",
                        'Headers_Relevantes': resultado['headers_relevantes']
                    })
            
            # Se não encontrou redirects
            if not rows:
                print(f"   🎉 PERFEITO: Nenhum redirect 3xx encontrado!")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Status', 'Tipo_Redirect', 'URL_Destino', 'Impacto_SEO', 
                    'Cadeia_Redirects', 'Tempo_Resposta', 'Headers_Relevantes'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Redirects_3xx")
                return df_vazio
            
            df_redirects = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR IMPACTO SEO E STATUS
            impacto_order = {'VERIFICAR': 1, 'MÉDIO': 2, 'NEUTRO': 3}
            df_redirects['sort_impacto'] = df_redirects['Impacto_SEO'].str.split(' -').str[0].map(impacto_order).fillna(99)
            df_redirects = df_redirects.sort_values(['sort_impacto', 'Status', 'URL'])
            df_redirects = df_redirects.drop('sort_impacto', axis=1)
            
            # 📤 EXPORTA
            df_redirects.to_excel(self.writer, index=False, sheet_name="Redirects_3xx")
            
            # 📊 ESTATÍSTICAS
            urls_analisadas = len([r for r in resultados if r.get('sucesso', False)])
            total_redirects = len(rows)
            
            # Stats por tipo
            stats_status = df_redirects['Status'].value_counts()
            stats_impacto = df_redirects['Impacto_SEO'].str.split(' -').str[0].value_counts()
            
            print(f"   ✅ URLs analisadas: {urls_analisadas}")
            print(f"   🔄 Redirects 3xx encontrados: {total_redirects}")
            
            # Estatísticas por status
            for status, count in stats_status.items():
                print(f"      • {status}: {count} redirects")
            
            # Estatísticas por impacto
            for impacto, count in stats_impacto.items():
                print(f"      • Impacto {impacto}: {count} redirects")
            
            print(f"   📋 Aba 'Redirects_3xx' criada com análise CIRÚRGICA")
            print(f"   🎯 Foco em otimização SEO e consolidação de links")
            
            return df_redirects
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico redirects: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'URL', 'Status', 'Tipo_Redirect', 'URL_Destino', 'Impacto_SEO', 
                'Cadeia_Redirects', 'Tempo_Resposta', 'Headers_Relevantes'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="Redirects_3xx")
            return df_erro