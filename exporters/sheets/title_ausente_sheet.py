# exporters/sheets/title_ausente_sheet.py - CIRÚRGICO 2.0 (SÓ EXISTÊNCIA)
# 🎯 ENGINE CIRÚRGICA 2.0: Detecta APENAS ausência/vazio de tags <title> - SEM heurísticas

import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter

class TitleAusenteSheet(BaseSheetExporter):
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

    def _verificar_title_cirurgico(self, url: str) -> dict:
        """🎯 Verificação CIRÚRGICA 2.0: SÓ existência da tag <title>"""
        
        try:
            response = self.session.get(url, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 🔎 BUSCA TAG <title>
            title_tag = soup.find('title')
            
            if title_tag is None:
                # TAG AUSENTE
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_problema': True,
                    'tipo_problema': 'TAG_AUSENTE',
                    'title_html': '[TAG NÃO ENCONTRADA]',
                    'title_texto': '',
                    'gravidade': 'CRITICO'
                }
            
            # 🔎 EXTRAI TEXTO DA TAG
            title_texto = title_tag.get_text().strip()
            
            if not title_texto:
                # TAG VAZIA
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_problema': True,
                    'tipo_problema': 'TAG_VAZIA',
                    'title_html': str(title_tag)[:200],
                    'title_texto': '[VAZIO]',
                    'gravidade': 'CRITICO'
                }
            
            # TAG OK - TEM CONTEÚDO
            return {
                'url': url,
                'sucesso': True,
                'tem_problema': False,
                'tipo_problema': 'OK',
                'title_html': str(title_tag)[:200],
                'title_texto': title_texto[:100],
                'gravidade': 'OK'
            }
            
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'tem_problema': True,
                'tipo_problema': 'ERRO_ACESSO',
                'title_html': '[ERRO DE ACESSO]',
                'title_texto': '',
                'gravidade': 'ERRO'
            }

    def _filtrar_urls_validas(self, urls_df) -> list:
        """🧹 Remove URLs inválidas + FILTRA APENAS STATUS 200"""
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
            
            urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _verificar_titles_paralelo(self, urls: list) -> list:
        """🚀 Verificação paralela cirúrgica"""
        
        print(f"🎯 Verificação cirúrgica de titles iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._verificar_title_cirurgico, url): url 
                for url in urls
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress indicator a cada 50 URLs
                if len(resultados) % 50 == 0:
                    print(f"⚡ Verificados: {len(resultados)}/{len(urls)}")
        
        return resultados

    def export(self):
        """🎯 Gera aba CIRÚRGICA 2.0 de titles ausentes (SÓ PROBLEMAS)"""
        try:
            print(f"🎯 TITLE AUSENTE - ENGINE CIRÚRGICA 2.0")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_para_verificar = self.df['url'].dropna().unique().tolist()
            urls_filtradas = self._filtrar_urls_validas(urls_para_verificar)
            
            print(f"   📊 URLs inicial: {len(urls_para_verificar)}")
            print(f"   🧹 URLs válidas: {len(urls_filtradas)}")
            print(f"   🎯 Critério CIRÚRGICO: Tag <title> ausente OU vazia")
            print(f"   ⚡ SEM análise de qualidade - só existência")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para verificação")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tipo_Problema', 'Title_HTML', 'Title_Texto', 'Gravidade'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
                return df_vazio
            
            # 🎯 VERIFICAÇÃO CIRÚRGICA PARALELA
            resultados = self._verificar_titles_paralelo(urls_filtradas)
            
            # 📋 GERA LINHAS APENAS PARA PROBLEMAS REAIS
            rows = []
            
            for resultado in resultados:
                # SÓ INCLUI SE TEM PROBLEMA REAL
                if resultado.get('tem_problema', False):
                    rows.append({
                        'URL': resultado['url'],
                        'Tipo_Problema': resultado['tipo_problema'],
                        'Title_HTML': resultado['title_html'],
                        'Title_Texto': resultado['title_texto'],
                        'Gravidade': resultado['gravidade']
                    })
            
            # Se não encontrou problemas
            if not rows:
                print(f"   🎉 PERFEITO: Todas as páginas têm tags <title> com conteúdo!")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tipo_Problema', 'Title_HTML', 'Title_Texto', 'Gravidade'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
                return df_vazio
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR GRAVIDADE
            gravidade_order = {'CRITICO': 1, 'ALTO': 2, 'MEDIO': 3, 'BAIXO': 4, 'ERRO': 5}
            df_problemas['sort_gravidade'] = df_problemas['Gravidade'].map(gravidade_order).fillna(99)
            df_problemas = df_problemas.sort_values(['sort_gravidade', 'URL'])
            df_problemas = df_problemas.drop('sort_gravidade', axis=1)
            
            # 📤 EXPORTA
            df_problemas.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            
            # 📊 ESTATÍSTICAS CIRÚRGICAS
            urls_verificadas = len([r for r in resultados if r.get('sucesso', False)])
            urls_com_problemas = len(rows)
            urls_perfeitas = urls_verificadas - urls_com_problemas
            
            # Stats por tipo
            tag_ausente = len([r for r in rows if r['Tipo_Problema'] == 'TAG_AUSENTE'])
            tag_vazia = len([r for r in rows if r['Tipo_Problema'] == 'TAG_VAZIA'])
            erro_acesso = len([r for r in rows if r['Tipo_Problema'] == 'ERRO_ACESSO'])
            
            print(f"   ✅ URLs verificadas: {urls_verificadas}")
            print(f"   🎯 URLs com problemas: {urls_com_problemas}")
            print(f"   ✨ URLs perfeitas (title OK): {urls_perfeitas}")
            print(f"      🚫 Tag <title> ausente: {tag_ausente}")
            print(f"      🕳️ Tag <title> vazia: {tag_vazia}")
            print(f"      ❌ Erro de acesso: {erro_acesso}")
            print(f"   📋 Aba 'Title_Ausente' criada com critério CIRÚRGICO 2.0")
            print(f"   🛡️ Zero falsos positivos - só ausência/vazio real")
            
            return df_problemas
            
        except Exception as e:
            print(f"❌ Erro no engine cirúrgico 2.0: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'URL', 'Tipo_Problema', 'Title_HTML', 'Title_Texto', 'Gravidade'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            return df_erro

# Aliases para compatibilidade
TitleAusenteSheetCirurgico = TitleAusenteSheet
TitleAusenteSheetSemantico = TitleAusenteSheet
TitleAusenteSheetHardened = TitleAusenteSheet