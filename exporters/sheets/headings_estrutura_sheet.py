# exporters/sheets/headings_estrutura_sheet.py - COM LÓGICA CIRÚRGICA
# 🔬 ENGINE CIRÚRGICA: Análise estrutural completa de headings H1-H6

import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter

class HeadingsEstruturaSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para análise estrutural"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        return session

    def _analisar_estrutura_headings(self, url: str) -> dict:
        """🔬 Análise estrutural completa dos headings"""
        
        try:
            response = self.session.get(url, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Contadores por nível
            contadores = {}
            textos_headings = {}
            
            for i in range(1, 7):
                tags = soup.find_all(f'h{i}')
                contadores[f'h{i}_total'] = len(tags)
                textos_headings[f'h{i}_textos'] = [tag.get_text().strip() for tag in tags if tag.get_text().strip()]
            
            # ANÁLISES ESTRUTURAIS
            problemas = []
            gravidade_maxima = 'OK'
            
            # 1. ANÁLISE H1
            h1_total = contadores['h1_total']
            h1_ausente = h1_total == 0
            h1_duplicado = h1_total > 1
            
            if h1_ausente:
                problemas.append("H1 ausente (crítico para SEO)")
                gravidade_maxima = 'CRITICO'
            elif h1_duplicado:
                problemas.append(f"Múltiplos H1 ({h1_total} encontrados)")
                gravidade_maxima = 'CRITICO' if gravidade_maxima != 'CRITICO' else gravidade_maxima
            
            # 2. HIERARQUIA LÓGICA
            hierarquia_ok = self._validar_hierarquia(contadores)
            if not hierarquia_ok['valida']:
                problemas.extend(hierarquia_ok['problemas'])
                if gravidade_maxima not in ['CRITICO']:
                    gravidade_maxima = 'ALTO'
            
            # 3. PROBLEMAS COMUNS
            problemas_comuns = self._detectar_problemas_comuns(contadores)
            if problemas_comuns:
                problemas.extend(problemas_comuns)
                if gravidade_maxima not in ['CRITICO', 'ALTO']:
                    gravidade_maxima = 'MEDIO'
            
            # STATUS FINAL
            status_geral = 'OK' if not problemas else gravidade_maxima
            comentario = ' | '.join(problemas) if problemas else 'Estrutura de headings adequada'
            
            return {
                'url': url,
                'sucesso': True,
                'h1_total': h1_total,
                'h1_ausente': h1_ausente,
                'h1_duplicado': h1_duplicado,
                'h2_total': contadores['h2_total'],
                'h3_total': contadores['h3_total'],
                'h4_total': contadores['h4_total'],
                'h5_total': contadores['h5_total'],
                'h6_total': contadores['h6_total'],
                'hierarquia_ok': hierarquia_ok['valida'],
                'status_geral': status_geral,
                'comentario': comentario,
                'h1_textos': ' | '.join(textos_headings['h1_textos'][:3]) if textos_headings['h1_textos'] else '',
                'problemas_detectados': len(problemas),
                'detalhes_problemas': problemas
            }
            
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'h1_total': 0,
                'h1_ausente': True,
                'h1_duplicado': False,
                'h2_total': 0,
                'h3_total': 0,
                'h4_total': 0,
                'h5_total': 0,
                'h6_total': 0,
                'hierarquia_ok': False,
                'status_geral': 'ERRO',
                'comentario': f'Erro ao processar: {str(e)}',
                'h1_textos': '',
                'problemas_detectados': 0,
                'detalhes_problemas': []
            }
    
    def _validar_hierarquia(self, contadores: dict) -> dict:
        """🔍 Valida hierarquia lógica dos headings"""
        
        problemas = []
        
        # Se tem H1, estrutura básica está ok
        if contadores['h1_total'] == 0:
            return {'valida': False, 'problemas': ['H1 ausente compromete hierarquia']}
        
        # Verifica saltos na hierarquia
        niveis = [
            ('h1', contadores['h1_total']),
            ('h2', contadores['h2_total']),
            ('h3', contadores['h3_total']),
            ('h4', contadores['h4_total']),
            ('h5', contadores['h5_total']),
            ('h6', contadores['h6_total'])
        ]
        
        # Se tem H3 mas não tem H2 (salto comum)
        if contadores['h3_total'] > 0 and contadores['h2_total'] == 0:
            problemas.append("H3 usado sem H2 (salto na hierarquia)")
        
        # Se tem H4 mas não tem H2 ou H3
        if contadores['h4_total'] > 0 and (contadores['h2_total'] == 0 or contadores['h3_total'] == 0):
            problemas.append("H4 usado com hierarquia incompleta")
        
        # Se tem muitos níveis sem estrutura lógica
        niveis_usados = sum(1 for _, qtd in niveis if qtd > 0)
        if niveis_usados > 4:
            problemas.append("Muitos níveis de heading (simplicidade recomendada)")
        
        return {
            'valida': len(problemas) == 0,
            'problemas': problemas
        }
    
    def _detectar_problemas_comuns(self, contadores: dict) -> list:
        """🎯 Detecta problemas comuns de SEO"""
        
        problemas = []
        
        # Página só com H1 (falta estrutura)
        total_headings = sum(contadores.values())
        if total_headings == contadores['h1_total'] and contadores['h1_total'] == 1:
            problemas.append("Apenas H1 presente (falta estrutura de conteúdo)")
        
        # Excesso de H2 (possível problema de estrutura)
        if contadores['h2_total'] > 10:
            problemas.append(f"Muitos H2 ({contadores['h2_total']}) - revisar estrutura")
        
        # Uso apenas de níveis baixos sem H1/H2
        if (contadores['h1_total'] == 0 and contadores['h2_total'] == 0 and 
            (contadores['h3_total'] > 0 or contadores['h4_total'] > 0)):
            problemas.append("Uso de H3/H4 sem estrutura base (H1/H2)")
        
        return problemas

    def _filtrar_urls_validas(self, urls: list) -> list:
        """🧹 Remove URLs inválidas para análise estrutural"""
        urls_validas = []
        
        for url in urls:
            if not url or pd.isna(url):
                continue
            
            url_str = str(url).strip()
            
            # Filtros básicos
            if not url_str.startswith(('http://', 'https://')):
                continue
            
            # Filtros de URL indesejadas para análise estrutural
            if any(skip in url_str.lower() for skip in [
                '.pdf', '.jpg', '.png', '.gif', '.zip', '.rar',
                'mailto:', 'tel:', 'javascript:', '#',
                '?page=', '&page=', '/page/'
            ]):
                continue
            
            urls_validas.append(url_str)
        
        return list(set(urls_validas))  # Remove duplicatas

    def _analisar_estrutura_paralelo(self, urls: list) -> list:
        """🚀 Análise estrutural paralela"""
        
        print(f"🔬 Análise estrutural iniciada: {len(urls)} URLs")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Submete todas as URLs
            future_to_url = {
                executor.submit(self._analisar_estrutura_headings, url): url 
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

    def export(self):
        """🔬 Gera aba CIRÚRGICA de estrutura de headings"""
        try:
            print(f"🔬 ESTRUTURA HEADINGS - ENGINE CIRÚRGICA")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_para_analisar = self.df['url'].dropna().unique().tolist()
            urls_filtradas = self._filtrar_urls_validas(urls_para_analisar)
            
            print(f"   📊 URLs inicial: {len(urls_para_analisar)}")
            print(f"   🧹 URLs válidas: {len(urls_filtradas)}")
            print(f"   🎯 Foco: H1 ausente, múltiplos H1, hierarquia, estrutura SEO")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL válida para análise")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'H1_Total', 'H1_Ausente', 'H1_Duplicado', 'H2_Total', 'H3_Total',
                    'H4_Total', 'H5_Total', 'H6_Total', 'Hierarquia_OK', 'Status_Geral',
                    'Comentario', 'H1_Textos', 'Problemas_Count'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Estrutura_Headings")
                return df_vazio
            
            # 🔬 ANÁLISE ESTRUTURAL PARALELA
            resultados = self._analisar_estrutura_paralelo(urls_filtradas)
            
            # 📋 GERA LINHAS PARA O DATAFRAME
            rows = []
            
            for resultado in resultados:
                if not resultado.get('sucesso', False):
                    # Inclui erros também
                    rows.append({
                        'URL': resultado['url'],
                        'H1_Total': 0,
                        'H1_Ausente': 'Sim',
                        'H1_Duplicado': 'Não',
                        'H2_Total': 0,
                        'H3_Total': 0,
                        'H4_Total': 0,
                        'H5_Total': 0,
                        'H6_Total': 0,
                        'Hierarquia_OK': 'Não',
                        'Status_Geral': 'ERRO',
                        'Comentario': resultado.get('comentario', 'Erro no processamento'),
                        'H1_Textos': '',
                        'Problemas_Count': 1
                    })
                    continue
                
                rows.append({
                    'URL': resultado['url'],
                    'H1_Total': resultado['h1_total'],
                    'H1_Ausente': 'Sim' if resultado['h1_ausente'] else 'Não',
                    'H1_Duplicado': 'Sim' if resultado['h1_duplicado'] else 'Não',
                    'H2_Total': resultado['h2_total'],
                    'H3_Total': resultado['h3_total'],
                    'H4_Total': resultado['h4_total'],
                    'H5_Total': resultado['h5_total'],
                    'H6_Total': resultado['h6_total'],
                    'Hierarquia_OK': 'Sim' if resultado['hierarquia_ok'] else 'Não',
                    'Status_Geral': resultado['status_geral'],
                    'Comentario': resultado['comentario'],
                    'H1_Textos': resultado['h1_textos'],
                    'Problemas_Count': resultado['problemas_detectados']
                })
            
            # Se não encontrou problemas
            if not rows:
                print(f"   🎉 PERFEITO: Estrutura de headings adequada em todas as URLs!")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'H1_Total', 'H1_Ausente', 'H1_Duplicado', 'H2_Total', 'H3_Total',
                    'H4_Total', 'H5_Total', 'H6_Total', 'Hierarquia_OK', 'Status_Geral',
                    'Comentario', 'H1_Textos', 'Problemas_Count'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Estrutura_Headings")
                return df_vazio
            
            df_estrutura = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR GRAVIDADE DOS PROBLEMAS
            gravidade_order = {'CRITICO': 1, 'ALTO': 2, 'MEDIO': 3, 'BAIXO': 4, 'OK': 5, 'ERRO': 6}
            df_estrutura['sort_gravidade'] = df_estrutura['Status_Geral'].map(gravidade_order).fillna(99)
            df_estrutura = df_estrutura.sort_values(['sort_gravidade', 'Problemas_Count', 'URL'], ascending=[True, False, True])
            df_estrutura = df_estrutura.drop('sort_gravidade', axis=1)
            
            # 📤 EXPORTA
            df_estrutura.to_excel(self.writer, index=False, sheet_name="Estrutura_Headings")
            
            # 📊 ESTATÍSTICAS
            urls_com_sucesso = len([r for r in resultados if r.get('sucesso', False)])
            urls_com_problemas = len([r for r in rows if r['Status_Geral'] not in ['OK', 'ERRO']])
            h1_ausente = len([r for r in rows if r['H1_Ausente'] == 'Sim'])
            h1_duplicado = len([r for r in rows if r['H1_Duplicado'] == 'Sim'])
            hierarquia_problemas = len([r for r in rows if r['Hierarquia_OK'] == 'Não'])
            
            print(f"   ✅ URLs analisadas: {urls_com_sucesso}")
            print(f"   🎯 URLs com problemas estruturais: {urls_com_problemas}")
            print(f"   🚫 H1 ausente: {h1_ausente} páginas")
            print(f"   🔄 H1 duplicado: {h1_duplicado} páginas") 
            print(f"   🏗️ Hierarquia quebrada: {hierarquia_problemas} páginas")
            print(f"   📋 Aba 'Estrutura_Headings' criada com análise CIRÚRGICA")
            print(f"   🎯 Foco em problemas estruturais críticos para SEO")
            
            return df_estrutura
            
        except Exception as e:
            print(f"❌ Erro no engine estrutural: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'URL', 'H1_Total', 'H1_Ausente', 'H1_Duplicado', 'H2_Total', 'H3_Total',
                'H4_Total', 'H5_Total', 'H6_Total', 'Hierarquia_OK', 'Status_Geral',
                'Comentario', 'H1_Textos', 'Problemas_Count'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="Estrutura_Headings")
            return df_erro