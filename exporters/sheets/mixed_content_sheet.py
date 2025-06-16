# exporters/sheets/mixed_content_sheet.py - ENGINE CIRÚRGICA v3.0 FINAL
# 🔒 SEMI-CONSOLIDATOR DOM-AWARE: Template consolidado + Content granular
# 🧠 v3.0: Template (HEAD/FOOTER/HEADER) agrupado + Content (MAIN/SIDEBAR/MODAL) detalhado

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import re

class MixedContentSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        self.session = self._criar_sessao_otimizada()
        
    def _criar_sessao_otimizada(self) -> requests.Session:
        """🚀 Sessão otimizada para análise de mixed content"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        })
        return session

    def _analisar_mixed_content_dom_aware(self, url: str) -> dict:
        """🔒 Análise DOM AWARE completa de mixed content"""
        
        try:
            if not url.startswith('https://'):
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_mixed_content': False,
                    'motivo': 'URL não é HTTPS'
                }
            
            response = self.session.get(url, timeout=15, verify=False, allow_redirects=True)
            
            if response.status_code != 200:
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_mixed_content': False,
                    'motivo': f'Status {response.status_code}'
                }
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            mixed_content_issues = self._scan_dom_cirurgico_v3(soup, url)
            
            if not mixed_content_issues:
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_mixed_content': False,
                    'motivo': 'Nenhum mixed content encontrado'
                }
            else:
                return {
                    'url': url,
                    'sucesso': True,
                    'tem_mixed_content': True,
                    'total_issues': len(mixed_content_issues),
                    'issues_detalhados': mixed_content_issues
                }
                
        except requests.exceptions.Timeout:
            return {
                'url': url,
                'sucesso': False,
                'erro': 'Timeout',
                'tem_mixed_content': False
            }
        except Exception as e:
            return {
                'url': url,
                'sucesso': False,
                'erro': str(e),
                'tem_mixed_content': False
            }

    def _scan_dom_cirurgico_v3(self, soup: BeautifulSoup, base_url: str) -> list:
        """🧠 Scanner DOM CIRÚRGICO v3.0 - FULL SCAN DOM com localização"""
        
        issues = []
        
        # 🔥 FULL SCAN: TODAS AS TAGS E ATRIBUTOS (EXCETO SCRIPTS)
        url_patterns = [
            # Recursos de mídia
            ('img', 'src'),
            ('img', 'data-src'),  # Lazy loading
            ('video', 'src'),
            ('video', 'poster'),
            ('audio', 'src'),
            ('source', 'src'),
            ('source', 'srcset'),
            
            # iframes e embeds (scripts HTTP são bloqueados pelo navegador)
            ('iframe', 'src'),
            ('embed', 'src'),
            ('object', 'data'),
            
            # Links e CSS
            ('link', 'href'),
            ('a', 'href'),
            
            # Formulários
            ('form', 'action'),
            
            # Background images via atributos
            ('div', 'data-bg'),
            ('section', 'data-background'),
            ('header', 'data-bg-image'),
        ]
        
        for tag_name, attr_name in url_patterns:
            elements = soup.find_all(tag_name, {attr_name: True})
            
            for element in elements:
                url_value = element.get(attr_name, '').strip()
                
                if not url_value:
                    continue
                
                # Normaliza URL
                if url_value.startswith('//'):
                    url_absoluta = 'http:' + url_value
                elif url_value.startswith('/'):
                    url_absoluta = urljoin(base_url, url_value)
                elif url_value.startswith('http://'):
                    url_absoluta = url_value
                elif url_value.startswith('https://'):
                    continue  # HTTPS é seguro
                else:
                    url_absoluta = urljoin(base_url, url_value)
                
                # Verifica se é HTTP inseguro
                if url_absoluta.startswith('http://'):
                    severidade = self._classificar_severidade(tag_name, attr_name)
                    localizacao_dom = self._detectar_localizacao_dom(element)
                    prioridade_correcao = self._calcular_prioridade_correcao(localizacao_dom, tag_name)
                    contexto_semantico = self._extrair_contexto_semantico(element)
                    path_dom = self._extrair_path_dom(element)
                    
                    issues.append({
                        'tipo': tag_name,
                        'url_recurso': url_value,
                        'url_absoluta': url_absoluta,
                        'severidade': severidade,
                        'localizacao_dom': localizacao_dom,
                        'prioridade_correcao': prioridade_correcao,
                        'atributo': attr_name,
                        'contexto_semantico': contexto_semantico,
                        'path_dom': path_dom,
                        'tag_completa': str(element)[:200]
                    })
        
        # 🎨 SCAN CSS: Inline styles e external stylesheets
        css_issues = self._scan_css_mixed_content(soup, base_url)
        issues.extend(css_issues)
        
        # 📱 SCAN BACKGROUND IMAGES: Via style attributes
        bg_issues = self._scan_background_images(soup, base_url)
        issues.extend(bg_issues)
        
        return issues

    def _scan_css_mixed_content(self, soup: BeautifulSoup, base_url: str) -> list:
        """🎨 Scanner específico para CSS mixed content"""
        css_issues = []
        
        # 1. Style tags inline
        style_elements = soup.find_all('style')
        for element in style_elements:
            css_content = element.get_text()
            localizacao_dom = self._detectar_localizacao_dom(element)
            
            http_urls = re.findall(r'url\(["\']?(http://[^"\')]+)["\']?\)', css_content)
            for css_url in http_urls:
                css_issues.append({
                    'tipo': 'css_inline',
                    'url_recurso': css_url,
                    'url_absoluta': css_url,
                    'severidade': 'MÉDIO',
                    'localizacao_dom': localizacao_dom,
                    'prioridade_correcao': self._calcular_prioridade_correcao(localizacao_dom, 'css'),
                    'atributo': 'url()',
                    'contexto_semantico': f'CSS inline em {localizacao_dom}',
                    'path_dom': self._extrair_path_dom(element),
                    'tag_completa': f'<style>...{css_url}...</style>'
                })
        
        # 2. External stylesheets (opcional: fetch e analisa)
        link_elements = soup.find_all('link', rel='stylesheet', href=True)
        for element in link_elements:
            href = element.get('href')
            if href and href.startswith('https://'):
                try:
                    # Busca CSS externo para analisar
                    css_url = urljoin(base_url, href)
                    css_response = self.session.get(css_url, timeout=5)
                    css_content = css_response.text
                    
                    localizacao_dom = self._detectar_localizacao_dom(element)
                    
                    http_urls = re.findall(r'url\(["\']?(http://[^"\')]+)["\']?\)', css_content)
                    for css_http_url in http_urls:
                        css_issues.append({
                            'tipo': 'css_external',
                            'url_recurso': css_http_url,
                            'url_absoluta': css_http_url,
                            'severidade': 'MÉDIO',
                            'localizacao_dom': localizacao_dom,
                            'prioridade_correcao': self._calcular_prioridade_correcao(localizacao_dom, 'css'),
                            'atributo': 'url()',
                            'contexto_semantico': f'CSS externo: {href}',
                            'path_dom': self._extrair_path_dom(element),
                            'tag_completa': f'External CSS: {css_http_url}'
                        })
                except:
                    # Se falhar ao buscar CSS externo, ignora
                    continue
        
        return css_issues

    def _scan_background_images(self, soup: BeautifulSoup, base_url: str) -> list:
        """📱 Scanner para background images via style attribute"""
        bg_issues = []
        
        # Busca elementos com style attribute
        elements_with_style = soup.find_all(attrs={'style': True})
        
        for element in elements_with_style:
            style_content = element.get('style', '')
            localizacao_dom = self._detectar_localizacao_dom(element)
            
            # Procura background-image: url(http://...)
            bg_urls = re.findall(r'background-image\s*:\s*url\(["\']?(http://[^"\')]+)["\']?\)', style_content)
            bg_urls.extend(re.findall(r'background\s*:\s*[^;]*url\(["\']?(http://[^"\')]+)["\']?\)', style_content))
            
            for bg_url in bg_urls:
                bg_issues.append({
                    'tipo': 'background_image',
                    'url_recurso': bg_url,
                    'url_absoluta': bg_url,
                    'severidade': 'MÉDIO',
                    'localizacao_dom': localizacao_dom,
                    'prioridade_correcao': self._calcular_prioridade_correcao(localizacao_dom, 'background'),
                    'atributo': 'style',
                    'contexto_semantico': f'Background image em {element.name}',
                    'path_dom': self._extrair_path_dom(element),
                    'tag_completa': f'<{element.name} style="...{bg_url}...">'
                })
        
        return bg_issues

    def _detectar_localizacao_dom(self, element) -> str:
        """🧠 Detecta localização semântica no DOM"""
        
        # 1. HEAD
        if element.find_parent("head"):
            return "HEAD"
        
        # 2. BODY com análise semântica
        if element.find_parent("body"):
            
            # 2.1 FOOTER
            for parent in element.parents:
                if parent.name == 'footer':
                    return "FOOTER"
                
                parent_class = ' '.join(parent.get('class', [])).lower()
                parent_id = parent.get('id', '').lower()
                
                if any(keyword in parent_class or keyword in parent_id for keyword in ['footer', 'rodape', 'bottom']):
                    return "FOOTER"
            
            # 2.2 HEADER
            for parent in element.parents:
                if parent.name == 'header':
                    return "HEADER"
                
                parent_class = ' '.join(parent.get('class', [])).lower()
                parent_id = parent.get('id', '').lower()
                
                if any(keyword in parent_class or keyword in parent_id for keyword in ['header', 'cabecalho', 'top', 'nav', 'navbar']):
                    return "HEADER"
            
            # 2.3 SIDEBAR
            for parent in element.parents:
                parent_class = ' '.join(parent.get('class', [])).lower()
                parent_id = parent.get('id', '').lower()
                
                if any(keyword in parent_class or keyword in parent_id for keyword in ['sidebar', 'aside', 'lateral']):
                    return "SIDEBAR"
            
            # 2.4 MODAL
            for parent in element.parents:
                parent_class = ' '.join(parent.get('class', [])).lower()
                parent_id = parent.get('id', '').lower()
                
                if any(keyword in parent_class or keyword in parent_id for keyword in ['modal', 'popup', 'overlay', 'dialog']):
                    return "MODAL"
            
            # 2.5 MAIN_CONTENT
            for parent in element.parents:
                parent_class = ' '.join(parent.get('class', [])).lower()
                parent_id = parent.get('id', '').lower()
                
                if any(keyword in parent_class or keyword in parent_id for keyword in ['main', 'content', 'conteudo', 'article']):
                    return "MAIN_CONTENT"
            
            return "BODY"
        
        return "UNKNOWN"

    def _extrair_contexto_semantico(self, element) -> str:
        """📋 Extrai contexto semântico melhorado"""
        contexto_parts = []
        
        # Localização DOM
        localizacao = self._detectar_localizacao_dom(element)
        contexto_parts.append(f"DOM: {localizacao}")
        
        # ID do elemento
        if element.get('id'):
            contexto_parts.append(f"ID: {element['id']}")
        
        # Classe do elemento
        if element.get('class'):
            classes = ' '.join(element['class'][:2])
            contexto_parts.append(f"Class: {classes}")
        
        # Alt text para imagens
        if element.name == 'img' and element.get('alt'):
            contexto_parts.append(f"Alt: {element['alt'][:30]}")
        
        # Title
        if element.get('title'):
            contexto_parts.append(f"Title: {element['title'][:30]}")
        
        # Parent context
        immediate_parent = element.parent
        if immediate_parent and immediate_parent.name != 'body':
            parent_info = immediate_parent.name
            if immediate_parent.get('class'):
                parent_class = ' '.join(immediate_parent['class'][:1])
                parent_info += f".{parent_class}"
            contexto_parts.append(f"Parent: {parent_info}")
        
        return ' | '.join(contexto_parts) if contexto_parts else 'Sem contexto'

    def _extrair_path_dom(self, element) -> str:
        """🗺️ Extrai caminho DOM para debug avançado"""
        path_parts = []
        
        current = element
        while current and current.name and current.name != '[document]':
            part = current.name
            
            if current.get('id'):
                part += f"#{current['id']}"
            
            if current.get('class'):
                first_class = current['class'][0]
                part += f".{first_class}"
            
            path_parts.append(part)
            current = current.parent
            
            if len(path_parts) >= 5:
                break
        
        return ' > '.join(reversed(path_parts))

    def _calcular_prioridade_correcao(self, localizacao_dom: str, tag_name: str) -> str:
        """🎯 Calcula prioridade de correção baseada em localização DOM"""
        
        # Scripts HTTP são bloqueados pelo navegador - não precisamos verificar
        
        if localizacao_dom == "HEAD":
            if tag_name in ['link', 'style']:
                return "ALTA"
            else:
                return "MÉDIA"
        elif localizacao_dom == "FOOTER":
            return "BAIXA"
        elif localizacao_dom == "HEADER":
            return "ALTA"
        elif localizacao_dom == "MAIN_CONTENT":
            if tag_name in ['img', 'video']:
                return "ALTA"
            else:
                return "MÉDIA"
        elif localizacao_dom == "SIDEBAR":
            return "BAIXA"
        elif localizacao_dom == "MODAL":
            return "MÉDIA"
        else:
            return "MÉDIA"

    def _classificar_severidade(self, tag_name: str, attr_name: str) -> str:
        """🎯 Classifica severidade do mixed content"""
        
        # Scripts HTTP são automaticamente bloqueados pelos navegadores modernos
        if tag_name in ['iframe', 'embed', 'object']:
            return 'CRÍTICO'
        elif tag_name == 'form' or (tag_name == 'link' and attr_name == 'href'):
            return 'ALTO'
        elif tag_name in ['img', 'video', 'audio', 'source']:
            return 'MÉDIO'
        else:
            return 'BAIXO'

    def _is_template_location(self, localizacao_dom: str) -> bool:
        """🏗️ v3.0: Detecta se localização é de template (consolidar)"""
        template_locations = ['HEAD', 'FOOTER', 'HEADER']
        return localizacao_dom in template_locations

    def _filtrar_urls_https(self, urls_input) -> list:
        """🧹 Filtra apenas URLs HTTPS válidas - UNIVERSAL"""
        
        urls_candidatas = []
        
        if isinstance(urls_input, pd.DataFrame):
            if 'url' in urls_input.columns:
                urls_candidatas = urls_input['url'].dropna().tolist()
            else:
                urls_candidatas = urls_input.iloc[:, 0].dropna().tolist()
        elif isinstance(urls_input, (list, pd.Series)):
            urls_candidatas = [str(url) for url in urls_input if url and not pd.isna(url)]
        else:
            try:
                urls_candidatas = list(urls_input)
            except:
                return []
        
        urls_validas = []
        for url in urls_candidatas:
            if not url:
                continue
            
            url_str = str(url).strip()
            
            if url_str.startswith('https://'):
                urls_validas.append(url_str)
        
        return list(set(urls_validas))

    def _analisar_mixed_content_paralelo(self, urls: list) -> list:
        """🚀 Análise paralela de mixed content"""
        
        print(f"🔒 Análise SEMI-CONSOLIDATOR v3.0 iniciada: {len(urls)} URLs HTTPS")
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            future_to_url = {
                executor.submit(self._analisar_mixed_content_dom_aware, url): url 
                for url in urls
            }
            
            for future in as_completed(future_to_url):
                resultado = future.result()
                resultados.append(resultado)
                
                if len(resultados) % 10 == 0:
                    print(f"⚡ Analisados: {len(resultados)}/{len(urls)}")
        
        return resultados

    def _processar_semi_consolidator_v3(self, resultados: list) -> pd.DataFrame:
        """🔥 v3.0: SEMI-CONSOLIDATOR - Template agrupado + Content detalhado"""
        
        print(f"   🔥 v3.0: Aplicando SEMI-CONSOLIDATOR DOM-AWARE...")
        
        # Estruturas separadas para template vs content
        template_consolidator = {}  # Para HEAD/FOOTER/HEADER
        content_rows = []           # Para MAIN_CONTENT/SIDEBAR/MODAL/BODY
        
        # FASE 1: Separa template vs content
        for resultado in resultados:
            if resultado.get('tem_mixed_content', False) and resultado.get('sucesso', False):
                pagina_url = resultado['url']
                
                for issue in resultado.get('issues_detalhados', []):
                    localizacao = issue['localizacao_dom']
                    
                    if self._is_template_location(localizacao):
                        # 🏗️ TEMPLATE: Consolida (1 linha por localização+recurso)
                        consolidation_key = (localizacao, issue['url_absoluta'])
                        
                        if consolidation_key not in template_consolidator:
                            template_consolidator[consolidation_key] = {
                                'localizacao_dom': localizacao,
                                'recurso_http': issue['url_absoluta'],
                                'tipo_elemento': issue['tipo'],
                                'severidade': issue['severidade'],
                                'prioridade_correcao': issue['prioridade_correcao'],
                                'atributo': issue['atributo'],
                                'paginas_afetadas': set(),
                                'url_original': issue['url_recurso'],
                                'contexto_semantico': issue['contexto_semantico'],
                                'path_dom': issue['path_dom']
                            }
                        
                        template_consolidator[consolidation_key]['paginas_afetadas'].add(pagina_url)
                    
                    else:
                        # 📱 CONTENT: Mantém granular (linha por página+recurso)
                        content_rows.append({
                            'URL_Página': pagina_url,
                            'Recurso_HTTP': issue['url_absoluta'],
                            'Tipo_Elemento': issue['tipo'],
                            'Localização_DOM': localizacao,
                            'Modo_Análise': 'GRANULAR',
                            'Páginas_Afetadas': 1,
                            'Prioridade_Correção': issue['prioridade_correcao'],
                            'Severidade': issue['severidade'],
                            'Contexto_Semântico': issue['contexto_semantico'],
                            'Path_DOM': issue['path_dom'],
                            'Atributo': issue['atributo'],
                            'URL_Original': issue['url_recurso'],
                            'Recomendação_HTTPS': issue['url_absoluta'].replace('http://', 'https://'),
                            'Estratégia_Correção': f"Correção pontual em {pagina_url}"
                        })
        
        # FASE 2: Converte template consolidado para rows
        template_rows = []
        for (localizacao, recurso), data in template_consolidator.items():
            num_paginas = len(data['paginas_afetadas'])
            
            template_rows.append({
                'URL_Página': f"[TEMPLATE] {num_paginas} páginas",
                'Recurso_HTTP': data['recurso_http'],
                'Tipo_Elemento': data['tipo_elemento'],
                'Localização_DOM': data['localizacao_dom'],
                'Modo_Análise': 'CONSOLIDADO',
                'Páginas_Afetadas': num_paginas,
                'Prioridade_Correção': data['prioridade_correcao'],
                'Severidade': data['severidade'],
                'Contexto_Semântico': data['contexto_semantico'],
                'Path_DOM': data['path_dom'],
                'Atributo': data['atributo'],
                'URL_Original': data['url_original'],
                'Recomendação_HTTPS': data['recurso_http'].replace('http://', 'https://'),
                'Estratégia_Correção': f"Corrigir no template → resolve {num_paginas} páginas automaticamente"
            })
        
        # FASE 3: Combina template + content
        all_rows = template_rows + content_rows
        
        if not all_rows:
            return pd.DataFrame()
        
        df_semi_consolidado = pd.DataFrame(all_rows)
        
        # FASE 4: Ordenação inteligente v3.0
        modo_order = {'CONSOLIDADO': 1, 'GRANULAR': 2}  # Template primeiro
        prioridade_order = {'URGENTE': 1, 'ALTA': 2, 'MÉDIA': 3, 'BAIXA': 4}
        severidade_order = {'CRÍTICO': 1, 'ALTO': 2, 'MÉDIO': 3, 'BAIXO': 4}
        
        df_semi_consolidado['sort_modo'] = df_semi_consolidado['Modo_Análise'].map(modo_order)
        df_semi_consolidado['sort_prioridade'] = df_semi_consolidado['Prioridade_Correção'].map(prioridade_order)
        df_semi_consolidado['sort_severidade'] = df_semi_consolidado['Severidade'].map(severidade_order)
        
        df_semi_consolidado = df_semi_consolidado.sort_values([
            'sort_modo',           # CONSOLIDADO primeiro
            'Páginas_Afetadas',    # Mais páginas primeiro
            'sort_prioridade',
            'sort_severidade',
            'URL_Página'
        ], ascending=[True, False, True, True, True])
        
        # Remove colunas de ordenação
        df_semi_consolidado = df_semi_consolidado.drop([
            'sort_modo', 'sort_prioridade', 'sort_severidade'
        ], axis=1)
        
        return df_semi_consolidado

    def _gerar_relatorio_v3(self, resultados: list, df_final: pd.DataFrame):
        """🧠 v3.0: Relatório SEMI-CONSOLIDATOR"""
        
        urls_analisadas = len([r for r in resultados if r.get('sucesso', False)])
        
        if df_final.empty:
            print(f"\n🎉 PERFEITO: Nenhum mixed content encontrado!")
            print(f"   ✅ URLs HTTPS analisadas: {urls_analisadas}")
            return
        
        # Estatísticas por modo
        consolidado_count = len(df_final[df_final['Modo_Análise'] == 'CONSOLIDADO'])
        granular_count = len(df_final[df_final['Modo_Análise'] == 'GRANULAR'])
        
        # Total de páginas afetadas
        total_paginas_template = df_final[df_final['Modo_Análise'] == 'CONSOLIDADO']['Páginas_Afetadas'].sum()
        total_paginas_content = len(df_final[df_final['Modo_Análise'] == 'GRANULAR'])
        
        print(f"\n🎯 RESUMO SEMI-CONSOLIDATOR v3.0:")
        print(f"   ✅ URLs HTTPS analisadas: {urls_analisadas}")
        print(f"   🔒 Total de recursos HTTP encontrados: {len(df_final)}")
        
        print(f"\n🏗️ TEMPLATE RESOURCES (CONSOLIDADOS):")
        print(f"   • {consolidado_count} recursos únicos de template")
        print(f"   • {total_paginas_template} páginas afetadas por template")
        print(f"   • Correção no CMS/template resolve múltiplas páginas")
        
        print(f"\n📱 CONTENT RESOURCES (GRANULARES):")
        print(f"   • {granular_count} recursos em conteúdo específico")
        print(f"   • {total_paginas_content} páginas com problemas pontuais")
        print(f"   • Requer correção página por página")
        
        # Top 5 recursos de template por impacto
        template_resources = df_final[df_final['Modo_Análise'] == 'CONSOLIDADO'].nlargest(5, 'Páginas_Afetadas')
        if not template_resources.empty:
            print(f"\n🔥 TOP 5 RECURSOS DE TEMPLATE (MAIOR IMPACTO):")
            for i, (_, row) in enumerate(template_resources.iterrows()):
                print(f"   {i+1}. {row['Recurso_HTTP']}")
                print(f"      📍 {row['Localização_DOM']} | {row['Páginas_Afetadas']} páginas | {row['Prioridade_Correção']}")
        
        # Estatísticas por localização
        loc_stats = df_final.groupby('Localização_DOM').agg({
            'Páginas_Afetadas': 'sum',
            'Recurso_HTTP': 'count'
        }).sort_values('Páginas_Afetadas', ascending=False)
        
        print(f"\n🧠 ANÁLISE POR LOCALIZAÇÃO DOM:")
        for localizacao, stats in loc_stats.iterrows():
            modo = "CONSOLIDADO" if localizacao in ['HEAD', 'FOOTER', 'HEADER'] else "GRANULAR"
            print(f"   • {localizacao}: {stats['Recurso_HTTP']} recursos ({stats['Páginas_Afetadas']} páginas) - {modo}")
        
        # Priorização estratégica
        print(f"\n🎯 PRIORIZAÇÃO ESTRATÉGICA:")
        prio_stats = df_final['Prioridade_Correção'].value_counts()
        for prioridade, count in prio_stats.items():
            paginas_prio = df_final[df_final['Prioridade_Correção'] == prioridade]['Páginas_Afetadas'].sum()
            urgencia = {
                'URGENTE': '🚨 IMEDIATO',
                'ALTA': '⚡ Esta semana', 
                'MÉDIA': '📅 Este mês',
                'BAIXA': '🕐 Quando possível'
            }.get(prioridade, '❓')
            print(f"   • {prioridade}: {count} recursos ({paginas_prio} páginas) - {urgencia}")
        
        print(f"\n🏁 CONCLUSÃO v3.0:")
        print(f"   🏗️ Template: {consolidado_count} recursos → Correção global")
        print(f"   📱 Content: {granular_count} recursos → Correção pontual")
        
        print(f"\n   📋 Aba 'Mixed_Content' criada com SEMI-CONSOLIDATOR v3.0")
        print(f"   🇧🇷 MÁQUINA DE GUERRA:")
        print(f"      • Template consolidado (elimina spam)")
        print(f"      • Content granular (inspeção pontual)")
        print(f"      • Full DOM scan (img, iframe, css, background, etc)")
        print(f"      • Priorização inteligente por localização")
        print(f"      • Scripts HTTP ignorados (bloqueados pelo navegador)")

    def export(self):
        """🔒 Gera aba SEMI-CONSOLIDATOR v3.0 - MÁQUINA DE GUERRA"""
        try:
            print(f"🔒 MIXED CONTENT SEMI-CONSOLIDATOR v3.0 - MÁQUINA DE GUERRA")
            
            # 📋 PREPARAÇÃO DOS DADOS
            urls_filtradas = self._filtrar_urls_https(self.df)
            
            print(f"   📊 URLs no DataFrame: {len(self.df) if hasattr(self.df, '__len__') else 'N/A'}")
            print(f"   🧹 URLs HTTPS válidas: {len(urls_filtradas)}")
            print(f"   🎯 Modo: SEMI-CONSOLIDATOR (Template consolidado + Content granular)")
            print(f"   🧠 v3.0: Full DOM scan com localização inteligente")
            
            if not urls_filtradas:
                print(f"   ⚠️ Nenhuma URL HTTPS encontrada")
                self._criar_aba_vazia_v3()
                return pd.DataFrame()
            
            # 🔒 ANÁLISE PARALELA COMPLETA
            resultados = self._analisar_mixed_content_paralelo(urls_filtradas)
            
            # 🔥 SEMI-CONSOLIDATOR ENGINE v3.0
            df_final = self._processar_semi_consolidator_v3(resultados)
            
            if df_final.empty:
                print(f"   🎉 PERFEITO: Nenhum mixed content encontrado!")
                self._criar_aba_vazia_v3()
                return pd.DataFrame()
            
            # 📤 EXPORTA VERSÃO SEMI-CONSOLIDADA
            df_final.to_excel(self.writer, index=False, sheet_name="Mixed_Content")
            
            # 🧠 RELATÓRIO v3.0
            self._gerar_relatorio_v3(resultados, df_final)
            
            return df_final
            
        except Exception as e:
            print(f"❌ Erro no SEMI-CONSOLIDATOR v3.0: {e}")
            import traceback
            traceback.print_exc()
            
            self._criar_aba_vazia_v3()
            return pd.DataFrame()

    def _criar_aba_vazia_v3(self):
        """📋 Cria aba vazia SEMI-CONSOLIDATOR v3.0 quando não há dados"""
        df_vazio = pd.DataFrame(columns=[
            'URL_Página', 'Recurso_HTTP', 'Tipo_Elemento', 'Localização_DOM',
            'Modo_Análise', 'Páginas_Afetadas', 'Prioridade_Correção', 'Severidade',
            'Contexto_Semântico', 'Path_DOM', 'Atributo', 'URL_Original',
            'Recomendação_HTTPS', 'Estratégia_Correção'
        ])
        df_vazio.to_excel(self.writer, index=False, sheet_name="Mixed_Content") 