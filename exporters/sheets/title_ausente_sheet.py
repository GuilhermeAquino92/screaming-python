# exporters/sheets/title_ausente_sheet.py

import pandas as pd
from exporters.base_exporter import BaseSheetExporter
from urllib.parse import urlparse

class TitleAusenteSheet(BaseSheetExporter):
    def export(self):
        """📋 Gera aba otimizada para páginas SEM title tag (crítico para SEO)"""
        try:
            rows = []
            
            # 🚫 FILTROS: Remove paginações, parâmetros desnecessários E páginas com erro
            df_filtrado = self.df[~self.df["url"].str.contains(r"\?page=\d+", na=False)].copy()
            df_filtrado = df_filtrado[~df_filtrado["url"].str.contains(r"&page=\d+", na=False)]
            
            # 🚫 FILTRO CRÍTICO: Só analisa páginas funcionais (status 200 e 3xx)
            status_col = 'status_code_http' if 'status_code_http' in df_filtrado.columns else 'status_code'
            if status_col in df_filtrado.columns:
                # Mantém apenas: 200 (OK), 301/302 (redirecionamentos), 3xx (outros redirecionamentos)
                df_filtrado = df_filtrado[
                    (df_filtrado[status_col] == 200) | 
                    (df_filtrado[status_col].astype(str).str.startswith('3'))
                ].copy()
                print(f"   🔍 Filtrando apenas páginas funcionais (200 e 3xx): {len(df_filtrado)} URLs")
            
            for _, row in df_filtrado.iterrows():
                url = row.get('url', '')
                title = str(row.get('title', '')).strip()
                status_code = row.get('status_code_http', row.get('status_code', 'N/A'))
                
                # ✅ GARANTIA: Só processa páginas que já passaram pelo filtro (200 e 3xx)
                if status_code not in [200] and not str(status_code).startswith('3'):
                    continue  # Pula páginas com erro (segurança extra)
                
                # 🚨 DETECTA APENAS TITLE AUSENTE (mais restritivo)
                problemas_title = []
                gravidade = 'CRITICO'  # Title ausente é sempre crítico
                tipo_problema = 'AUSENTE'
                
                # ✅ CONDIÇÕES ESPECÍFICAS PARA TITLE AUSENTE:
                title_ausente = False
                
                # 1. TITLE COMPLETAMENTE AUSENTE (tag <title> não existe)
                if not title or title == '' or title.lower() == 'none':
                    problemas_title.append('Title tag completamente ausente')
                    title_ausente = True
                
                # 2. TITLE SÓ COM ESPAÇOS EM BRANCO
                elif title.isspace():
                    problemas_title.append('Title contém apenas espaços em branco')
                    title_ausente = True
                
                # 3. TITLE COM APENAS CARACTERES ESPECIAIS/NÚMEROS SEM SENTIDO
                elif len(title.strip()) == 0:
                    problemas_title.append('Title vazio após remoção de espaços')
                    title_ausente = True
                
                # 🤖 4. FILTRO INTELIGENTE: TITLES TÉCNICOS SUSPEITOS (possível JS dinâmico)
                elif self._is_title_placeholder_tecnico(title):
                    problemas_title.append('Title técnico suspeito - possível conteúdo dinâmico JS')
                    tipo_problema = 'SUSPEITO_JS'
                    gravidade = 'ALTO'  # Alto, mas não crítico como ausente real
                    title_ausente = True
                
                # 🚫 NÃO INCLUI: titles curtos, genéricos normais, etc. (esses são outros problemas)
                
                # Se encontrou APENAS problema de title ausente, adiciona à lista
                if title_ausente:
                    # 🔍 ANÁLISE DE CONTEXTO DA URL
                    parsed_url = urlparse(url)
                    path_parts = [part for part in parsed_url.path.split('/') if part]
                    
                    # Tenta inferir o tipo de página pela URL
                    tipo_pagina = 'Página Geral'
                    if not path_parts or url.endswith('/'):
                        tipo_pagina = 'Homepage/Landing'
                    elif any(keyword in url.lower() for keyword in ['produto', 'product', 'item']):
                        tipo_pagina = 'Página de Produto'
                    elif any(keyword in url.lower() for keyword in ['categoria', 'category', 'collection']):
                        tipo_pagina = 'Página de Categoria'
                    elif any(keyword in url.lower() for keyword in ['blog', 'artigo', 'post', 'noticia']):
                        tipo_pagina = 'Conteúdo/Blog'
                    elif any(keyword in url.lower() for keyword in ['contato', 'contact', 'sobre', 'about']):
                        tipo_pagina = 'Página Institucional'
                    
                    # 🎯 DETERMINA PRIORIDADE DE CORREÇÃO
                    prioridade_correcao = 'MEDIA'
                    if tipo_pagina in ['Homepage/Landing', 'Página de Produto']:
                        prioridade_correcao = 'MAXIMA'
                    elif tipo_pagina in ['Página de Categoria', 'Conteúdo/Blog']:
                        prioridade_correcao = 'ALTA'
                    elif status_code == 200:
                        prioridade_correcao = 'ALTA'  # Páginas funcionais têm prioridade
                    
                    # 📊 CALCULA IMPACTO SEO 
                    if tipo_problema == 'SUSPEITO_JS':
                        impacto_seo = 'ALTO - Title suspeito, verificar se carregado por JavaScript'
                    else:
                        impacto_seo = 'CRITICO - Title ausente impede indexação adequada pelo Google'
                    
                    # 💡 GERA RECOMENDAÇÃO ESPECÍFICA 
                    if tipo_problema == 'SUSPEITO_JS':
                        recomendacao = self._gerar_recomendacao_suspeito(tipo_pagina, title)
                    else:
                        recomendacao = self._gerar_recomendacao_ausente(tipo_pagina, url)
                    
                    rows.append({
                        'URL': url,
                        'Status_HTTP': status_code,
                        'Tipo_Problema': tipo_problema,
                        'Problema_Detectado': ' | '.join(problemas_title),
                        'Title_Atual': title[:100] + ('...' if len(title) > 100 else ''),
                        'Tamanho_Title': len(title),
                        'Tipo_Pagina': tipo_pagina,
                        'Gravidade': gravidade,
                        'Prioridade_Correcao': prioridade_correcao,
                        'Impacto_SEO': impacto_seo,
                        'Recomendacao': recomendacao
                    })
            
            # Se não encontrou nenhum problema
            if not rows:
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Status_HTTP', 'Tipo_Problema', 'Problema_Detectado', 'Title_Atual',
                    'Tamanho_Title', 'Tipo_Pagina', 'Gravidade', 'Prioridade_Correcao',
                    'Impacto_SEO', 'Recomendacao'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
                print(f"✅ Aba Title_Ausente criada sem dados (todos os titles estão OK!)")
                return df_vazio
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR PRIORIDADE E GRAVIDADE
            prioridade_order = {'MAXIMA': 0, 'ALTA': 1, 'MEDIA': 2, 'BAIXA': 3}
            gravidade_order = {'CRITICO': 0, 'ALTO': 1, 'MEDIO': 2, 'BAIXO': 3}
            tipo_order = {
                'AUSENTE': 0,           # Mais crítico - realmente sem title
                'ESPACOS_VAZIOS': 1,    # Muito crítico - title vazio
                'SUSPEITO_JS': 2        # Alto mas suspeito - verificar manualmente
            }
            
            df_problemas['prioridade_sort'] = df_problemas['Prioridade_Correcao'].map(prioridade_order)
            df_problemas['gravidade_sort'] = df_problemas['Gravidade'].map(gravidade_order)
            df_problemas['tipo_sort'] = df_problemas['Tipo_Problema'].map(tipo_order)
            
            # Ordena por: prioridade → gravidade → tipo → URL
            df_problemas = df_problemas.sort_values([
                'prioridade_sort',    # 1. Máxima prioridade primeiro
                'gravidade_sort',     # 2. Críticos primeiro
                'tipo_sort',          # 3. Por tipo de problema
                'URL'                 # 4. URLs alfabéticas
            ], ascending=[True, True, True, True])
            
            # Remove colunas auxiliares
            df_problemas = df_problemas.drop(['prioridade_sort', 'gravidade_sort', 'tipo_sort'], axis=1, errors='ignore')
            
            # Reordena colunas para melhor visualização
            colunas_ordenadas = [
                'URL', 'Status_HTTP', 'Tipo_Problema', 'Problema_Detectado', 
                'Prioridade_Correcao', 'Gravidade', 'Tipo_Pagina', 
                'Title_Atual', 'Tamanho_Title', 'Impacto_SEO', 'Recomendacao'
            ]
            
            df_problemas = df_problemas[colunas_ordenadas]
            
            df_problemas.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            
            # 📊 ESTATÍSTICAS DETALHADAS
            stats = df_problemas.groupby(['Tipo_Problema', 'Gravidade']).size().reset_index(name='Count')
            total_criticos = len(df_problemas[df_problemas['Gravidade'] == 'CRITICO'])
            total_altos = len(df_problemas[df_problemas['Gravidade'] == 'ALTO'])
            total_maxima_prioridade = len(df_problemas[df_problemas['Prioridade_Correcao'] == 'MAXIMA'])
            
            # 📊 ESTATÍSTICAS DETALHADAS
            stats = df_problemas.groupby(['Tipo_Problema', 'Gravidade']).size().reset_index(name='Count')
            total_criticos = len(df_problemas[df_problemas['Gravidade'] == 'CRITICO'])
            total_altos = len(df_problemas[df_problemas['Gravidade'] == 'ALTO'])
            total_suspeitos_js = len(df_problemas[df_problemas['Tipo_Problema'] == 'SUSPEITO_JS'])
            total_maxima_prioridade = len(df_problemas[df_problemas['Prioridade_Correcao'] == 'MAXIMA'])
            
            print(f"✅ Aba Title_Ausente criada com {len(df_problemas)} problemas:")
            print(f"   🚨 Críticos: {total_criticos} (correção imediata obrigatória)")
            print(f"   ⚠️ Altos: {total_altos} (inclui {total_suspeitos_js} suspeitos de JS dinâmico)")
            print(f"   🎯 Máxima prioridade: {total_maxima_prioridade} (páginas importantes)")
            print(f"   🤖 Filtro JS ativo: detecta titles técnicos suspeitos automaticamente")
            
            # Detalhamento por tipo
            for _, stat in stats.iterrows():
                tipo = stat['Tipo_Problema']
                gravidade = stat['Gravidade']
                count = stat['Count']
                if tipo == 'SUSPEITO_JS':
                    print(f"   🤖 {tipo} ({gravidade}): {count} páginas - VERIFICAR MANUALMENTE")
                else:
                    print(f"   📋 {tipo} ({gravidade}): {count} páginas")
            
            return df_problemas
            
        except Exception as e:
            print(f"⚠️ Erro gerando aba Title_Ausente: {e}")
            # Em caso de erro, cria DataFrame vazio
            df_vazio = pd.DataFrame(columns=[
                'URL', 'Status_HTTP', 'Tipo_Problema', 'Problema_Detectado', 'Title_Atual',
                'Tamanho_Title', 'Tipo_Pagina', 'Gravidade', 'Prioridade_Correcao',
                'Impacto_SEO', 'Recomendacao'
            ])
            df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            return df_vazio
    
    def _is_title_placeholder_tecnico(self, title):
        """🤖 Detecta titles que parecem placeholders técnicos ou gerados por JS"""
        if not title:
            return False
            
        title_lower = title.lower().strip()
        
        # 🎯 PALAVRAS TÉCNICAS SUSPEITAS (indicam possível JS dinâmico)
        placeholders_tecnicos = [
            # Sanitizers e processadores
            'apisanitizer', 'sanitizer', 'htmlsanitizer', 'domSanitizer',
            
            # Loaders e componentes
            'loader', 'loading', 'preloader', 'spinner',
            'component', 'widget', 'module', 'app',
            
            # Frameworks e bibliotecas
            'react', 'angular', 'vue', 'jquery', 'bootstrap',
            'main', 'index', 'app', 'root', 'container',
            
            # Estados de carregamento
            'initializing', 'starting', 'booting', 'init',
            'undefined', 'null', 'empty', 'blank',
            
            # APIs e endpoints
            'api', 'endpoint', 'service', 'handler',
            'controller', 'router', 'middleware',
            
            # Desenvolvimento/Debug
            'debug', 'test', 'dev', 'development',
            'localhost', 'staging', 'temp', 'temporary'
        ]
        
        # 🔍 VERIFICA SE É PLACEHOLDER TÉCNICO
        if title_lower in placeholders_tecnicos:
            return True
            
        # 🔍 VERIFICA PADRÕES SUSPEITOS
        suspeitos_patterns = [
            len(title_lower) <= 4,  # Muito curto e técnico
            title_lower.startswith(('js_', 'app_', 'ui_', 'api_')),  # Prefixos técnicos
            title_lower.endswith(('_app', '_js', '_component', '_widget')),  # Sufixos técnicos
            all(c.islower() or c.isdigit() for c in title_lower),  # Tudo minúsculo (camelCase quebrado)
        ]
        
        return any(suspeitos_patterns)
    
    def _gerar_recomendacao_ausente(self, tipo_pagina, url):
        """💡 Gera recomendação específica para páginas SEM title"""
        
        if tipo_pagina == 'Homepage/Landing':
            return 'URGENTE: Adicionar <title>Nome da Empresa - Serviços Principais | Palavras-chave</title>'
        elif tipo_pagina == 'Página de Produto':
            return 'URGENTE: Adicionar <title>Nome do Produto - Categoria | Nome da Empresa</title>'
        elif tipo_pagina == 'Página de Categoria':
            return 'URGENTE: Adicionar <title>Nome da Categoria - Produtos/Serviços | Nome da Empresa</title>'
        elif tipo_pagina == 'Conteúdo/Blog':
            return 'URGENTE: Adicionar <title>Título do Artigo | Nome do Site/Blog</title>'
        elif tipo_pagina == 'Página Institucional':
            return 'URGENTE: Adicionar <title>Nome da Seção (Sobre, Contato, etc.) | Nome da Empresa</title>'
        else:
            return 'URGENTE: Adicionar tag <title> descritiva e única para esta página'
    
    def _gerar_recomendacao_suspeito(self, tipo_pagina, title_atual):
        """🤖 Gera recomendação específica para titles suspeitos de JS dinâmico"""
        
        base_recomendacao = ""
        if tipo_pagina == 'Homepage/Landing':
            base_recomendacao = '"Nome da Empresa - Serviços Principais | Palavras-chave"'
        elif tipo_pagina == 'Página de Produto':
            base_recomendacao = '"Nome do Produto - Categoria | Nome da Empresa"'
        elif tipo_pagina == 'Página de Categoria':
            base_recomendacao = '"Nome da Categoria - Produtos/Serviços | Nome da Empresa"'
        elif tipo_pagina == 'Conteúdo/Blog':
            base_recomendacao = '"Título do Artigo | Nome do Site/Blog"'
        else:
            base_recomendacao = '"Title descritivo e único para esta página"'
        
        return f'VERIFICAR: Title "{title_atual}" parece placeholder técnico. Implementar {base_recomendacao} no HTML inicial (server-side), não via JavaScript.'