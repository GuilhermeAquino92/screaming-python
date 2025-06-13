# title_ausente_sheet_SEMANTICO.py - Sistema de Validação Semântica Completo

import pandas as pd
from exporters.base_exporter import BaseSheetExporter
from urllib.parse import urlparse
import re

class TitleAusenteSheetSemantico(BaseSheetExporter):
    def export(self):
        """📋 Sistema de Validação Semântica: Físico → Técnico → Semântico"""
        try:
            rows = []
            
            # 🚫 FILTROS: Remove paginações e páginas com erro
            df_filtrado = self.df[~self.df["url"].str.contains(r"\?page=\d+", na=False)].copy()
            df_filtrado = df_filtrado[~df_filtrado["url"].str.contains(r"&page=\d+", na=False)]
            
            # 🚫 FILTRO: Só páginas funcionais (200 e 3xx)
            status_col = 'status_code_http' if 'status_code_http' in df_filtrado.columns else 'status_code'
            if status_col in df_filtrado.columns:
                df_filtrado = df_filtrado[
                    (df_filtrado[status_col] == 200) | 
                    (df_filtrado[status_col].astype(str).str.startswith('3'))
                ].copy()
                print(f"   🔍 Analisando {len(df_filtrado)} URLs funcionais (200 e 3xx)")
            
            for _, row in df_filtrado.iterrows():
                url = row.get('url', '')
                title = row.get('title', '')
                status_code = row.get('status_code_http', row.get('status_code', 'N/A'))
                
                # Pula páginas com erro (segurança extra)
                if status_code not in [200] and not str(status_code).startswith('3'):
                    continue
                
                # 🎯 MATRIZ DE VALIDAÇÃO SEMÂNTICA
                resultado_validacao = self._validar_title_semantico(title, url)
                
                if resultado_validacao['tem_problema']:
                    # 🔍 ANÁLISE DE CONTEXTO DA URL
                    tipo_pagina = self._inferir_tipo_pagina(url)
                    prioridade_correcao = self._calcular_prioridade(tipo_pagina, status_code, resultado_validacao['tipo_problema'])
                    impacto_seo = self._calcular_impacto_seo(resultado_validacao['tipo_problema'], tipo_pagina)
                    recomendacao = self._gerar_recomendacao_semantica(resultado_validacao, tipo_pagina, url)
                    
                    rows.append({
                        'URL': url,
                        'Status_HTTP': status_code,
                        'Tipo_Problema': resultado_validacao['tipo_problema'],
                        'Categoria_Validacao': resultado_validacao['categoria'],
                        'Problema_Detectado': resultado_validacao['descricao'],
                        'Title_Atual': resultado_validacao['title_display'],
                        'Tamanho_Title': resultado_validacao['tamanho'],
                        'Score_Semantico': resultado_validacao['score_semantico'],
                        'Tipo_Pagina': tipo_pagina,
                        'Gravidade': resultado_validacao['gravidade'],
                        'Prioridade_Correcao': prioridade_correcao,
                        'Impacto_SEO': impacto_seo,
                        'Recomendacao': recomendacao,
                        'Detalhes_Tecnico': resultado_validacao['detalhes_tecnico']
                    })
            
            # Se não encontrou problemas
            if not rows:
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Status_HTTP', 'Tipo_Problema', 'Categoria_Validacao', 'Problema_Detectado',
                    'Title_Atual', 'Tamanho_Title', 'Score_Semantico', 'Tipo_Pagina', 'Gravidade',
                    'Prioridade_Correcao', 'Impacto_SEO', 'Recomendacao', 'Detalhes_Tecnico'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
                print(f"✅ Validação semântica: Todos os títulos estão adequados!")
                return df_vazio
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR MATRIZ SEMÂNTICA
            categoria_order = {
                'FISICO': 0,        # Mais crítico - ausência física
                'TECNICO': 1,       # Crítico - problemas técnicos
                'SEMANTICO': 2      # Importante - problemas de qualidade
            }
            
            tipo_order = {
                'AUSENTE': 0,              # Físico
                'NULO': 1,                 # Físico
                'VAZIO': 2,                # Físico
                'PLACEHOLDER_JS': 3,       # Técnico
                'PLACEHOLDER_GENERICO': 4, # Técnico
                'INVALIDO_SEMANTICO': 5,   # Semântico
                'CURTO_DEMAIS': 6,         # Semântico
                'GENERICO_DEMAIS': 7       # Semântico
            }
            
            prioridade_order = {'MAXIMA': 0, 'ALTA': 1, 'MEDIA': 2, 'BAIXA': 3}
            gravidade_order = {'CRITICO': 0, 'ALTO': 1, 'MEDIO': 2, 'BAIXO': 3}
            
            df_problemas['categoria_sort'] = df_problemas['Categoria_Validacao'].map(categoria_order)
            df_problemas['tipo_sort'] = df_problemas['Tipo_Problema'].map(tipo_order)
            df_problemas['prioridade_sort'] = df_problemas['Prioridade_Correcao'].map(prioridade_order)
            df_problemas['gravidade_sort'] = df_problemas['Gravidade'].map(gravidade_order)
            
            # Ordena por: categoria → tipo → prioridade → gravidade → score → URL
            df_problemas = df_problemas.sort_values([
                'categoria_sort',     # 1. Físico → Técnico → Semântico
                'tipo_sort',          # 2. Tipo específico do problema
                'prioridade_sort',    # 3. Prioridade da página
                'gravidade_sort',     # 4. Gravidade do problema
                'Score_Semantico',    # 5. Score semântico (ASC = pior primeiro)
                'URL'                 # 6. URLs alfabéticas
            ], ascending=[True, True, True, True, True, True])
            
            # Remove colunas auxiliares
            df_problemas = df_problemas.drop(['categoria_sort', 'tipo_sort', 'prioridade_sort', 'gravidade_sort'], axis=1, errors='ignore')
            
            # Reordena colunas para melhor visualização
            colunas_ordenadas = [
                'URL', 'Status_HTTP', 'Categoria_Validacao', 'Tipo_Problema', 
                'Problema_Detectado', 'Score_Semantico', 'Prioridade_Correcao', 'Gravidade',
                'Tipo_Pagina', 'Title_Atual', 'Tamanho_Title', 'Impacto_SEO',
                'Recomendacao', 'Detalhes_Tecnico'
            ]
            
            df_problemas = df_problemas[colunas_ordenadas]
            
            df_problemas.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            
            # 📊 ESTATÍSTICAS DA MATRIZ SEMÂNTICA
            self._exibir_estatisticas_semanticas(df_problemas)
            
            return df_problemas
            
        except Exception as e:
            print(f"⚠️ Erro na validação semântica: {e}")
            df_vazio = pd.DataFrame(columns=[
                'URL', 'Status_HTTP', 'Categoria_Validacao', 'Tipo_Problema', 'Problema_Detectado',
                'Title_Atual', 'Tamanho_Title', 'Score_Semantico', 'Tipo_Pagina', 'Gravidade',
                'Prioridade_Correcao', 'Impacto_SEO', 'Recomendacao', 'Detalhes_Tecnico'
            ])
            df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            return df_vazio
    
    def _validar_title_semantico(self, title, url):
        """🎯 Matriz de Validação Semântica: Físico → Técnico → Semântico"""
        
        # 📊 NÍVEL 1: VALIDAÇÃO FÍSICA (ausência total)
        if title is None:
            return {
                'tem_problema': True,
                'categoria': 'FISICO',
                'tipo_problema': 'NULO',
                'gravidade': 'CRITICO',
                'descricao': 'Title é None/null no sistema',
                'title_display': '[NULL]',
                'tamanho': 0,
                'score_semantico': 0,
                'detalhes_tecnico': 'Campo title retornou None/null'
            }
        
        if not isinstance(title, str):
            title_str = str(title).strip()
            if title_str in ['', 'nan', 'None', 'null']:
                return {
                    'tem_problema': True,
                    'categoria': 'FISICO',
                    'tipo_problema': 'AUSENTE',
                    'gravidade': 'CRITICO',
                    'descricao': 'Title não é string válida',
                    'title_display': f'[{title_str}]',
                    'tamanho': 0,
                    'score_semantico': 0,
                    'detalhes_tecnico': f'Tipo: {type(title)}, Valor: {title_str}'
                }
            title = title_str
        
        title_limpo = title.strip()
        
        if not title_limpo:
            return {
                'tem_problema': True,
                'categoria': 'FISICO',
                'tipo_problema': 'VAZIO',
                'gravidade': 'CRITICO',
                'descricao': 'Title é string vazia ou apenas espaços',
                'title_display': '[VAZIO]',
                'tamanho': len(title),
                'score_semantico': 0,
                'detalhes_tecnico': f'String original: "{title}"'
            }
        
        # 🔧 NÍVEL 2: VALIDAÇÃO TÉCNICA (placeholders e problemas técnicos)
        validacao_tecnica = self._validar_nivel_tecnico(title_limpo)
        if validacao_tecnica['tem_problema']:
            return validacao_tecnica
        
        # 🎯 NÍVEL 3: VALIDAÇÃO SEMÂNTICA (qualidade para SEO)
        return self._validar_nivel_semantico(title_limpo, url)
    
    def _validar_nivel_tecnico(self, title):
        """🔧 Validação Técnica: Placeholders e problemas técnicos"""
        title_lower = title.lower().strip()
        
        # 🤖 PLACEHOLDERS DE JAVASCRIPT/FRAMEWORKS
        placeholders_js = [
            'app', 'react app', 'vue app', 'angular app',
            'loading', 'carregando', 'loader', 'preloader',
            'initializing', 'starting', 'booting', 'init',
            'component', 'widget', 'module', 'main', 'index',
            'undefined', 'null', 'empty', 'blank'
        ]
        
        if title_lower in placeholders_js:
            return {
                'tem_problema': True,
                'categoria': 'TECNICO',
                'tipo_problema': 'PLACEHOLDER_JS',
                'gravidade': 'ALTO',
                'descricao': 'Title é placeholder de JavaScript/Framework',
                'title_display': title,
                'tamanho': len(title),
                'score_semantico': 10,
                'detalhes_tecnico': f'Placeholder JS detectado: "{title_lower}"'
            }
        
        # 🏷️ PLACEHOLDERS GENÉRICOS
        placeholders_genericos = [
            'title', 'página', 'page', 'site', 'website',
            'untitled', 'sem título', 'no title', 'sem nome',
            'document', 'documento', 'html', 'web page',
            'default', 'padrão', 'template', 'modelo'
        ]
        
        if title_lower in placeholders_genericos:
            return {
                'tem_problema': True,
                'categoria': 'TECNICO',
                'tipo_problema': 'PLACEHOLDER_GENERICO',
                'gravidade': 'ALTO',
                'descricao': 'Title é placeholder genérico sem valor',
                'title_display': title,
                'tamanho': len(title),
                'score_semantico': 15,
                'detalhes_tecnico': f'Placeholder genérico: "{title_lower}"'
            }
        
        # 🔍 PADRÕES SUSPEITOS
        if len(title_lower) <= 3 and not any(c.isalpha() for c in title_lower):
            return {
                'tem_problema': True,
                'categoria': 'TECNICO',
                'tipo_problema': 'PLACEHOLDER_JS',
                'gravidade': 'ALTO',
                'descricao': 'Title muito curto sem letras (suspeito)',
                'title_display': title,
                'tamanho': len(title),
                'score_semantico': 5,
                'detalhes_tecnico': f'Apenas {len(title)} caracteres, sem letras'
            }
        
        return {'tem_problema': False}
    
    def _validar_nivel_semantico(self, title, url):
        """🎯 Validação Semântica: Qualidade do título para SEO"""
        title_lower = title.lower().strip()
        
        # 📏 TÍTULOS MUITO CURTOS (sem valor semântico)
        if len(title) < 10:
            return {
                'tem_problema': True,
                'categoria': 'SEMANTICO',
                'tipo_problema': 'CURTO_DEMAIS',
                'gravidade': 'MEDIO',
                'descricao': f'Title muito curto ({len(title)} caracteres) para SEO',
                'title_display': title,
                'tamanho': len(title),
                'score_semantico': 20 + len(title),  # Score baseado no tamanho
                'detalhes_tecnico': f'Mínimo recomendado: 30-60 caracteres'
            }
        
        # 🏠 TÍTULOS INVÁLIDOS SEMANTICAMENTE
        titulos_invalidos_semanticos = [
            # Homepage genérica
            'home', 'início', 'inicial', 'principal',
            'página inicial', 'página principal', 'main page',
            'welcome', 'bem-vindo', 'bem-vindos',
            
            # Navegação genérica
            'voltar', 'back', 'anterior', 'próximo', 'next',
            'menu', 'navigation', 'navegação',
            
            # Conteúdo genérico
            'conteúdo', 'content', 'texto', 'text',
            'informação', 'information', 'info',
            'dados', 'data', 'detalhes', 'details',
            
            # Ações genéricas
            'clique aqui', 'click here', 'saiba mais', 'leia mais',
            'ver mais', 'veja mais', 'continue', 'continuar'
        ]
        
        # Verifica títulos exatos
        if title_lower in titulos_invalidos_semanticos:
            return {
                'tem_problema': True,
                'categoria': 'SEMANTICO',
                'tipo_problema': 'INVALIDO_SEMANTICO',
                'gravidade': 'MEDIO',
                'descricao': 'Title semanticamente inválido para SEO',
                'title_display': title,
                'tamanho': len(title),
                'score_semantico': 30,
                'detalhes_tecnico': f'Título genérico sem valor SEO: "{title_lower}"'
            }
        
        # 🔍 TÍTULOS GENÉRICOS DEMAIS (contém apenas palavras genéricas)
        palavras_genericas = [
            'empresa', 'company', 'negócio', 'business',
            'serviço', 'service', 'produto', 'product',
            'loja', 'store', 'shop', 'site', 'website',
            'portal', 'sistema', 'system', 'plataforma', 'platform'
        ]
        
        # Verifica se o título é só empresa + palavra genérica
        palavras_title = re.findall(r'\b\w+\b', title_lower)
        if len(palavras_title) <= 3:  # Títulos muito simples
            palavras_nao_genericas = [p for p in palavras_title if p not in palavras_genericas]
            if len(palavras_nao_genericas) <= 1:  # Só 1 palavra específica ou menos
                return {
                    'tem_problema': True,
                    'categoria': 'SEMANTICO',
                    'tipo_problema': 'GENERICO_DEMAIS',
                    'gravidade': 'MEDIO',
                    'descricao': 'Title genérico demais, baixo valor para SEO',
                    'title_display': title,
                    'tamanho': len(title),
                    'score_semantico': 40,
                    'detalhes_tecnico': f'Apenas {len(palavras_nao_genericas)} palavra(s) específica(s)'
                }
        
        # ✅ TÍTULO APROVADO NA VALIDAÇÃO SEMÂNTICA
        # Calcula score baseado na qualidade
        score = self._calcular_score_semantico(title, url)
        
        return {
            'tem_problema': False,
            'score_semantico': score
        }
    
    def _calcular_score_semantico(self, title, url):
        """📊 Calcula score semântico do título (0-100, maior = melhor)"""
        score = 50  # Base
        
        # Tamanho ideal (30-60 caracteres)
        tamanho = len(title)
        if 30 <= tamanho <= 60:
            score += 20
        elif 20 <= tamanho < 30 or 60 < tamanho <= 80:
            score += 10
        elif tamanho < 20 or tamanho > 80:
            score -= 10
        
        # Tem separador (| - :)
        if any(sep in title for sep in ['|', '-', ':', '•']):
            score += 15
        
        # Não é apenas maiúscula
        if not title.isupper():
            score += 10
        
        # Tem palavras-chave específicas (não genéricas)
        palavras = re.findall(r'\b\w{3,}\b', title.lower())
        palavras_especificas = len([p for p in palavras if p not in [
            'empresa', 'company', 'site', 'página', 'page', 'home'
        ]])
        score += min(palavras_especificas * 5, 20)
        
        return min(max(score, 0), 100)
    
    def _inferir_tipo_pagina(self, url):
        """🔍 Infere tipo da página pela URL"""
        parsed_url = urlparse(url)
        path_parts = [part for part in parsed_url.path.split('/') if part]
        url_lower = url.lower()
        
        if not path_parts or url.endswith('/'):
            return 'Homepage/Landing'
        elif any(keyword in url_lower for keyword in ['produto', 'product', 'item', '/p/']):
            return 'Página de Produto'
        elif any(keyword in url_lower for keyword in ['categoria', 'category', 'collection', '/c/']):
            return 'Página de Categoria'
        elif any(keyword in url_lower for keyword in ['blog', 'artigo', 'post', 'noticia', 'news']):
            return 'Conteúdo/Blog'
        elif any(keyword in url_lower for keyword in ['contato', 'contact', 'sobre', 'about', 'quem-somos']):
            return 'Página Institucional'
        elif any(keyword in url_lower for keyword in ['busca', 'search', 'pesquisa']):
            return 'Página de Busca'
        elif any(keyword in url_lower for keyword in ['checkout', 'carrinho', 'cart', 'pagamento']):
            return 'Página de Compra'
        else:
            return 'Página Geral'
    
    def _calcular_prioridade(self, tipo_pagina, status_code, tipo_problema):
        """🎯 Calcula prioridade de correção"""
        # Base por tipo de página
        if tipo_pagina in ['Homepage/Landing', 'Página de Produto']:
            prioridade_base = 'MAXIMA'
        elif tipo_pagina in ['Página de Categoria', 'Conteúdo/Blog']:
            prioridade_base = 'ALTA'
        elif tipo_pagina in ['Página Institucional']:
            prioridade_base = 'MEDIA'
        else:
            prioridade_base = 'MEDIA'
        
        # Ajusta por tipo de problema
        if tipo_problema in ['AUSENTE', 'NULO', 'VAZIO']:
            return 'MAXIMA'  # Sempre máxima para problemas físicos
        elif tipo_problema in ['PLACEHOLDER_JS', 'PLACEHOLDER_GENERICO']:
            return 'ALTA' if prioridade_base in ['MAXIMA', 'ALTA'] else 'MEDIA'
        elif tipo_problema in ['INVALIDO_SEMANTICO']:
            return prioridade_base  # Mantém prioridade da página
        else:
            # Diminui um nível para problemas menos críticos
            ordem = ['MAXIMA', 'ALTA', 'MEDIA', 'BAIXA']
            idx = ordem.index(prioridade_base)
            return ordem[min(idx + 1, len(ordem) - 1)]
    
    def _calcular_impacto_seo(self, tipo_problema, tipo_pagina):
        """📊 Calcula impacto SEO do problema"""
        if tipo_problema in ['AUSENTE', 'NULO', 'VAZIO']:
            return 'CRITICO - Title ausente impede indexação adequada pelo Google'
        elif tipo_problema in ['PLACEHOLDER_JS']:
            return 'ALTO - Title placeholder pode ser ignorado pelos buscadores'
        elif tipo_problema in ['PLACEHOLDER_GENERICO']:
            return 'ALTO - Title genérico não diferencia a página nos resultados'
        elif tipo_problema in ['INVALIDO_SEMANTICO']:
            if tipo_pagina in ['Homepage/Landing', 'Página de Produto']:
                return 'ALTO - Title sem valor semântico prejudica CTR e ranking'
            else:
                return 'MEDIO - Title pouco descritivo reduz relevância'
        elif tipo_problema in ['CURTO_DEMAIS']:
            return 'MEDIO - Title curto desperdiça espaço valioso nos resultados'
        elif tipo_problema in ['GENERICO_DEMAIS']:
            return 'MEDIO - Title genérico reduz diferenciação nos SERPs'
        else:
            return 'BAIXO - Problema menor na otimização'
    
    def _gerar_recomendacao_semantica(self, resultado, tipo_pagina, url):
        """💡 Gera recomendação baseada na validação semântica"""
        tipo_problema = resultado['tipo_problema']
        categoria = resultado['categoria']
        
        if categoria == 'FISICO':
            return self._recomendacao_fisica(tipo_pagina)
        elif categoria == 'TECNICO':
            return self._recomendacao_tecnica(tipo_problema, tipo_pagina, resultado['title_display'])
        elif categoria == 'SEMANTICO':
            return self._recomendacao_semantica(tipo_problema, tipo_pagina, resultado['title_display'])
        else:
            return 'Revisar título para melhorar SEO'
    
    def _recomendacao_fisica(self, tipo_pagina):
        """🚨 Recomendações para problemas físicos"""
        templates = {
            'Homepage/Landing': 'Nome da Empresa - Serviços Principais | Palavras-chave',
            'Página de Produto': 'Nome do Produto - Categoria | Nome da Empresa',
            'Página de Categoria': 'Nome da Categoria - Produtos/Serviços | Nome da Empresa',
            'Conteúdo/Blog': 'Título do Artigo | Nome do Site/Blog',
            'Página Institucional': 'Nome da Seção (Sobre, Contato, etc.) | Nome da Empresa'
        }
        
        template = templates.get(tipo_pagina, 'Título Descritivo e Único da Página | Nome da Empresa')
        return f'URGENTE: Implementar <title>{template}</title> no HTML'
    
    def _recomendacao_tecnica(self, tipo_problema, tipo_pagina, title_atual):
        """🔧 Recomendações para problemas técnicos"""
        if tipo_problema == 'PLACEHOLDER_JS':
            return f'IMPORTANTE: Substituir "{title_atual}" por título definitivo no HTML inicial (server-side), não via JavaScript'
        elif tipo_problema == 'PLACEHOLDER_GENERICO':
            return f'IMPORTANTE: Substituir "{title_atual}" por título específico e descritivo da página'
        else:
            return 'Corrigir problema técnico no título'
    
    def _recomendacao_semantica(self, tipo_problema, tipo_pagina, title_atual):
        """🎯 Recomendações para problemas semânticos"""
        if tipo_problema == 'INVALIDO_SEMANTICO':
            return f'MELHORAR: Substituir "{title_atual}" por título que descreva o conteúdo específico da página'
        elif tipo_problema == 'CURTO_DEMAIS':
            return f'EXPANDIR: Título "{title_atual}" muito curto. Adicionar palavras-chave e contexto (ideal: 30-60 caracteres)'
        elif tipo_problema == 'GENERICO_DEMAIS':
            return f'ESPECIFICAR: Título "{title_atual}" genérico demais. Adicionar detalhes únicos da página'
        else:
            return 'Otimizar título para melhorar SEO'
    
    def _exibir_estatisticas_semanticas(self, df_problemas):
        """📊 Exibe estatísticas da validação semântica"""
        total = len(df_problemas)
        print(f"✅ Validação Semântica concluída com {total} problemas encontrados:")
        
        # Por categoria
        categorias = df_problemas['Categoria_Validacao'].value_counts()
        for categoria, count in categorias.items():
            emoji = {'FISICO': '🚨', 'TECNICO': '🔧', 'SEMANTICO': '🎯'}
            print(f"   {emoji.get(categoria, '📋')} {categoria}: {count} problemas")
        
        # Por tipo de problema
        print(f"\n📋 Detalhamento por tipo:")
        tipos = df_problemas.groupby(['Categoria_Validacao', 'Tipo_Problema']).size()
        for (categoria, tipo), count in tipos.items():
            print(f"   • {tipo}: {count}")
        
        # Por prioridade
        prioridades = df_problemas['Prioridade_Correcao'].value_counts()
        print(f"\n🎯 Por prioridade de correção:")
        for prioridade, count in prioridades.items():
            emoji = {'MAXIMA': '🔥', 'ALTA': '⚠️', 'MEDIA': '📋', 'BAIXA': '💡'}
            print(f"   {emoji.get(prioridade, '•')} {prioridade}: {count}")
        
        # Score médio
        if 'Score_Semantico' in df_problemas.columns:
            score_medio = df_problemas['Score_Semantico'].mean()
            print(f"\n📊 Score semântico médio dos problemas: {score_medio:.1f}/100")

print("🎉 SISTEMA DE VALIDAÇÃO SEMÂNTICA IMPLEMENTADO!")
print("📋 Matriz: Físico → Técnico → Semântico")
print("🎯 Categorias:")
print("   🚨 FÍSICO: Ausente, Nulo, Vazio")
print("   🔧 TÉCNICO: Placeholders JS, Placeholders Genéricos")
print("   🎯 SEMÂNTICO: Inválido, Curto, Genérico demais")
print("📊 Score semântico: 0-100 (maior = melhor qualidade)")