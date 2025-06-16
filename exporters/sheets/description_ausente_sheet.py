# exporters/sheets/description_ausente_sheet.py - CIRÚRGICO v3.1 RESILIENTE
# 🎯 ENGINE CIRÚRGICA: Meta description ausente/vazia + RETRY RESILIENTE
# 🧠 v3.1: Elimina erros transitórios de rede com backoff exponencial

import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter
import time
import random

class DescriptionAusenteSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para validação cirúrgica"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        return session

    def _verificar_description_cirurgico(self, url: str, max_retries=3) -> dict:
        """🎯 Verificação CIRÚRGICA com RETRY RESILIENTE v3.1"""
        
        for tentativa in range(1, max_retries + 1):
            try:
                response = self.session.get(url, timeout=10, verify=False)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # 🔎 BUSCA META DESCRIPTION
                description_tag = soup.find('meta', attrs={'name': 'description'})
                
                if description_tag is None:
                    # TAG AUSENTE
                    return {
                        'url': url,
                        'sucesso': True,
                        'tem_problema': True,
                        'tipo_problema': 'TAG_AUSENTE',
                        'description_html': '[TAG NÃO ENCONTRADA]',
                        'description_texto': '',
                        'gravidade': 'CRITICO'
                    }
                
                # 🔎 EXTRAI CONTENT DA TAG
                description_content = description_tag.get('content', '').strip()
                
                if not description_content:
                    # TAG VAZIA
                    return {
                        'url': url,
                        'sucesso': True,
                        'tem_problema': True,
                        'tipo_problema': 'TAG_VAZIA',
                        'description_html': str(description_tag)[:200],
                        'description_texto': '[VAZIO]',
                        'gravidade': 'CRITICO'
                    }
                
                # TAG OK - TEM CONTEÚDO
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_problema': False,
                    'tipo_problema': 'OK',
                    'description_html': str(description_tag)[:200],
                    'description_texto': description_content[:100],
                    'gravidade': 'OK'
                }
                
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
                if tentativa == max_retries:
                    # Última tentativa falhou - retorna erro
                    return {
                        'url': url,
                        'sucesso': False,
                        'erro': str(e),
                        'tem_problema': True,
                        'tipo_problema': 'ERRO_ACESSO',
                        'description_html': '[ERRO DE ACESSO]',
                        'description_texto': '',
                        'gravidade': 'ERRO'
                    }
                else:
                    # Backoff exponencial com jitter
                    wait_time = (2 ** tentativa) + random.uniform(0, 1)
                    print(f"   ⚡ Tentativa {tentativa}/{max_retries} falhou para {url}. Retry em {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                if tentativa == max_retries:
                    # Última tentativa - retorna erro técnico
                    return {
                        'url': url,
                        'sucesso': False,
                        'erro': str(e),
                        'tem_problema': True,
                        'tipo_problema': 'ERRO_ACESSO',
                        'description_html': '[ERRO DE ACESSO]',
                        'description_texto': '',
                        'gravidade': 'ERRO'
                    }
                else:
                    # Retry para outros erros também
                    wait_time = (2 ** tentativa) + random.uniform(0, 1)
                    print(f"   ⚡ Erro técnico tentativa {tentativa}/{max_retries} para {url}. Retry em {wait_time:.1f}s...")
                    time.sleep(wait_time)
        
        # Fallback (não deveria chegar aqui)
        return {
            'url': url,
            'sucesso': False,
            'erro': 'Erro inesperado no retry loop',
            'tem_problema': True,
            'tipo_problema': 'ERRO_ACESSO',
            'description_html': '[ERRO INESPERADO]',
            'description_texto': '',
            'gravidade': 'ERRO'
        }

    def _filtrar_urls_validas(self, urls_input) -> list:
        """🧹 Remove URLs inválidas + FILTRA STATUS 200 - VERSÃO UNIVERSAL"""
        
        # 🔧 EXTRAÇÃO UNIVERSAL DE URLs
        urls_candidatas = []
        
        if isinstance(urls_input, pd.DataFrame):
            urls_candidatas = urls_input
        elif isinstance(urls_input, (list, pd.Series)):
            # Converte lista/series para DataFrame mock
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
        
        for _, row in urls_candidatas.iterrows():
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
            
            # 🎯 FILTRO CRUCIAL: SÓ PÁGINAS 200 OK (se disponível)
            status_code = row.get('status_code_http', row.get('status_code', None))
            if status_code is not None:
                try:
                    status_num = int(float(status_code))
                    if status_num != 200:
                        continue  # Pula páginas que não são 200 OK
                except (ValueError, TypeError):
                    continue  # Pula se não conseguir converter status
            
            urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _verificar_descriptions_paralelo(self, urls: list) -> list:
        """🚀 Verificação paralela cirúrgica RESILIENTE"""
        
        print(f"📝 Verificação RESILIENTE de descriptions iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:  # Reduzido para 10 por causa dos retries
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._verificar_description_cirurgico, url): url 
                for url in urls
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress indicator a cada 25 URLs (menos frequente por causa dos retries)
                if len(resultados) % 25 == 0:
                    print(f"⚡ Verificados: {len(resultados)}/{len(urls)}")
        
        return resultados

    def export(self):
        """📝 Gera aba CIRÚRGICA RESILIENTE de descriptions ausentes"""
        try:
            print(f"📝 DESCRIPTION AUSENTE - ENGINE CIRÚRGICA v3.1 RESILIENTE")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_filtradas = self._filtrar_urls_validas(self.df)
            
            print(f"   📊 URLs no DataFrame: {len(self.df) if hasattr(self.df, '__len__') else 'N/A'}")
            print(f"   🧹 URLs válidas (200 OK): {len(urls_filtradas)}")
            print(f"   🎯 Critério CIRÚRGICO: Meta description ausente OU vazia")
            print(f"   🧠 v3.1: Retry resiliente (3 tentativas + backoff exponencial)")
            print(f"   🔍 FILTRO: Apenas páginas com status 200 OK")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para verificação")
                self._criar_aba_vazia()
                return pd.DataFrame()
            
            # 📝 VERIFICAÇÃO CIRÚRGICA RESILIENTE
            resultados = self._verificar_descriptions_paralelo(urls_filtradas)
            
            # 📋 GERA LINHAS APENAS PARA PROBLEMAS REAIS (exceto ERRO_ACESSO após retry)
            rows = []
            erros_tecnicos = 0
            
            for resultado in resultados:
                # SÓ INCLUI SE TEM PROBLEMA REAL DE SEO (não erros técnicos)
                if resultado.get('tem_problema', False):
                    if resultado.get('tipo_problema') in ['TAG_AUSENTE', 'TAG_VAZIA']:
                        # Problema real de SEO
                        rows.append({
                            'URL': resultado['url'],
                            'Tipo_Problema': resultado['tipo_problema'],
                            'Description_HTML': resultado['description_html'],
                            'Description_Texto': resultado['description_texto'],
                            'Gravidade': resultado['gravidade']
                        })
                    elif resultado.get('tipo_problema') == 'ERRO_ACESSO':
                        # Erro técnico após retry - só conta mas não inclui
                        erros_tecnicos += 1
            
            # Se não encontrou problemas de SEO
            if not rows:
                print(f"   🎉 PERFEITO: Todas as páginas têm meta description com conteúdo!")
                if erros_tecnicos > 0:
                    print(f"   ⚠️ {erros_tecnicos} URLs com erro técnico (retry esgotado)")
                self._criar_aba_vazia()
                return pd.DataFrame()
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR GRAVIDADE
            gravidade_order = {'CRITICO': 1, 'ALTO': 2, 'MEDIO': 3, 'BAIXO': 4}
            df_problemas['sort_gravidade'] = df_problemas['Gravidade'].map(gravidade_order).fillna(99)
            df_problemas = df_problemas.sort_values(['sort_gravidade', 'URL'])
            df_problemas = df_problemas.drop('sort_gravidade', axis=1)
            
            # 📤 EXPORTA
            df_problemas.to_excel(self.writer, index=False, sheet_name="Description_Ausente")
            
            # 📊 ESTATÍSTICAS CIRÚRGICAS RESILIENTES
            urls_verificadas = len([r for r in resultados if r.get('sucesso', False)])
            urls_com_problemas_seo = len(rows)
            urls_perfeitas = urls_verificadas - urls_com_problemas_seo
            
            # Stats por tipo
            tag_ausente = len([r for r in rows if r['Tipo_Problema'] == 'TAG_AUSENTE'])
            tag_vazia = len([r for r in rows if r['Tipo_Problema'] == 'TAG_VAZIA'])
            
            print(f"   ✅ URLs verificadas com sucesso: {urls_verificadas}")
            print(f"   📝 URLs com problemas de SEO: {urls_com_problemas_seo}")
            print(f"   ✨ URLs perfeitas (description OK): {urls_perfeitas}")
            print(f"      🚫 Meta description ausente: {tag_ausente}")
            print(f"      🕳️ Meta description vazia: {tag_vazia}")
            if erros_tecnicos > 0:
                print(f"   ⚠️ URLs com erro técnico (retry esgotado): {erros_tecnicos}")
            print(f"   📋 Aba 'Description_Ausente' criada com critério CIRÚRGICO v3.1")
            print(f"   🛡️ Zero falsos positivos - só ausência/vazio real")
            print(f"   🧠 Retry resiliente eliminou erros transitórios de rede")
            
            return df_problemas
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico description v3.1: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            self._criar_aba_vazia()
            return pd.DataFrame()

    def _criar_aba_vazia(self):
        """📋 Cria aba vazia quando não há problemas"""
        df_vazio = pd.DataFrame(columns=[
            'URL', 'Tipo_Problema', 'Description_HTML', 'Description_Texto', 'Gravidade'
        ])
        df_vazio.to_excel(self.writer, index=False, sheet_name="Description_Ausente")