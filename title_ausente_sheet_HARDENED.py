# title_ausente_sheet_HARDENED.py - 100% BLINDADO contra crawling sujo

import pandas as pd
from exporters.base_exporter import BaseSheetExporter
from urllib.parse import urlparse
import re

# 🛡️ EXTENSÕES BLOQUEADAS - Lista completa do URLManagerSEO
EXTENSOES_INVALIDAS = (
    # Imagens
    '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.ico', '.bmp', '.tiff',
    # Documentos
    '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.odt', '.ods', '.odp',
    # Arquivos comprimidos
    '.zip', '.rar', '.7z', '.tar', '.gz', '.bz2',
    # Scripts e estilos
    '.js', '.css', '.scss', '.less', '.coffee',
    # Mídia
    '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.wav', '.ogg',
    # Dados/config
    '.xml', '.json', '.csv', '.txt', '.log', '.sql', '.ini', '.conf',
    # Fontes
    '.woff', '.woff2', '.ttf', '.eot', '.otf',
    # Outros
    '.swf', '.dmg', '.exe', '.msi', '.deb', '.rpm', '.apk',
    # Feeds e APIs (com extensão)
    '.rss', '.atom', '.feed'
)

# 🚫 DIRETÓRIOS BLOQUEADOS - Do URLManagerSEO
DIRETORIOS_INVALIDOS = [
    '/wp-admin/', '/admin/', '/administrator/', '/wp-includes/', '/wp-content/uploads/',
    '/assets/', '/static/', '/media/', '/files/', '/downloads/', '/uploads/',
    '/api/', '/rest/', '/ajax/', '/json/', '/xml/',
    '/feed/', '/rss/', '/atom/', '/sitemap/', '/robots.txt',
    '/.well-known/', '/cgi-bin/', '/tmp/', '/temp/',
    '/node_modules/', '/vendor/', '/cache/', '/backup/'
]

# 🚫 PARÂMETROS SUSPEITOS
PARAMETROS_INVALIDOS = [
    'download=', 'file=', 'attachment=', 'export=', 'format=pdf', 'format=csv',
    'print=', 'preview=', 'thumbnail=', 'resize=', 'crop='
]

def is_url_ignorada(url):
    """🛡️ Filtro HARDENED - Replica URLManagerSEO"""
    if not url or not isinstance(url, str):
        return True
    
    url_lower = url.lower().strip()
    
    # 1. Extensões bloqueadas
    if url_lower.endswith(EXTENSOES_INVALIDAS):
        return True
    
    # 2. Diretórios bloqueados
    if any(dir_bloq in url_lower for dir_bloq in DIRETORIOS_INVALIDOS):
        return True
    
    # 3. Parâmetros suspeitos
    if any(param in url_lower for param in PARAMETROS_INVALIDOS):
        return True
    
    # 4. URLs com múltiplos pontos (suspeito de arquivo)
    parsed = urlparse(url_lower)
    path = parsed.path
    if path.count('.') > 1:  # Ex: /arquivo.backup.sql
        return True
    
    # 5. URLs com extensão escondida no query
    query = parsed.query
    if any(ext.replace('.', '') in query for ext in EXTENSOES_INVALIDAS[:10]):  # Primeiras 10 extensões
        return True
    
    return False

def is_content_type_html(content_type):
    """📄 Valida se Content-Type é HTML válido"""
    if not content_type or not isinstance(content_type, str):
        return False
    
    content_type_lower = content_type.lower().strip()
    
    # Content-Types válidos para SEO
    tipos_validos = [
        'text/html',
        'application/xhtml+xml',
        'text/xhtml'
    ]
    
    return any(tipo in content_type_lower for tipo in tipos_validos)

def sanitizar_title_hardened(title):
    """🧹 Sanitização HARDENED do title"""
    # 1. Valores nulos/None
    if title is None:
        return None, 'NULO'
    
    # 2. Não é string - converte
    if not isinstance(title, str):
        title_str = str(title).strip()
        # Verifica se é pandas NaN disfarçado
        if title_str.lower() in ['nan', 'none', 'null', '<na>', '']:
            return None, 'PANDAS_NAN'
        title = title_str
    
    # 3. String vazia ou só espaços
    title_limpo = title.strip()
    if not title_limpo:
        return '', 'VAZIO'
    
    # 4. Placeholders comuns
    title_lower = title_limpo.lower()
    placeholders_suspeitos = [
        'untitled', 'no title', 'sem título', 'title', 'página',
        'document', 'default', 'loading', 'app', 'react app',
        'vue app', 'angular', 'index', 'main', 'home'
    ]
    
    if title_lower in placeholders_suspeitos:
        return title_limpo, 'PLACEHOLDER'
    
    # 5. Títulos muito curtos (suspeitos)
    if len(title_limpo) <= 3:
        return title_limpo, 'MUITO_CURTO'
    
    # 6. OK - título válido
    return title_limpo, 'VALIDO'

class TitleAusenteSheetHardened(BaseSheetExporter):
    def export(self):
        """🛡️ Validador HARDENED - 100% blindado contra crawling sujo"""
        try:
            print(f"🛡️ Iniciando validação HARDENED de titles...")
            
            rows = []
            stats = {
                'total_inicial': len(self.df),
                'filtrado_url': 0,
                'filtrado_status': 0,
                'filtrado_content_type': 0,
                'filtrado_paginacao': 0,
                'analisados': 0,
                'problemas_encontrados': 0
            }
            
            # 🚫 FILTRO 1: Remove paginações (básico)
            df_work = self.df.copy()
            df_work = df_work[~df_work["url"].str.contains(r"\?page=\d+", na=False)]
            df_work = df_work[~df_work["url"].str.contains(r"&page=\d+", na=False)]
            stats['filtrado_paginacao'] = stats['total_inicial'] - len(df_work)
            
            print(f"   📊 Após filtro paginação: {len(df_work)} URLs")
            
            for _, row in df_work.iterrows():
                url = row.get('url', '')
                
                # 🛡️ FILTRO 2: URLs ignoradas (HARDENED)
                if is_url_ignorada(url):
                    stats['filtrado_url'] += 1
                    continue
                
                # 🛡️ FILTRO 3: Status HTTP (só 200 e 3xx)
                status_code = row.get('status_code_http', row.get('status_code', None))
                if status_code is not None:
                    try:
                        status_num = int(float(status_code))
                        if status_num != 200 and not (300 <= status_num < 400):
                            stats['filtrado_status'] += 1
                            continue
                    except (ValueError, TypeError):
                        stats['filtrado_status'] += 1
                        continue
                
                # 🛡️ FILTRO 4: Content-Type (só HTML)
                content_type = row.get('tipo_conteudo_http', row.get('tipo_conteudo', None))
                if content_type is not None and not is_content_type_html(content_type):
                    stats['filtrado_content_type'] += 1
                    continue
                
                stats['analisados'] += 1
                
                # 🧹 SANITIZAÇÃO HARDENED DO TITLE
                title_original = row.get('title', None)
                title_sanitizado, tipo_problema = sanitizar_title_hardened(title_original)
                
                # 🎯 SÓ ADICIONA SE TEM PROBLEMA REAL
                if tipo_problema != 'VALIDO':
                    stats['problemas_encontrados'] += 1
                    
                    # 🔍 ANÁLISE CONTEXTUAL
                    tipo_pagina = self._inferir_tipo_pagina(url)
                    gravidade = self._calcular_gravidade(tipo_problema, tipo_pagina)
                    prioridade = self._calcular_prioridade(tipo_problema, tipo_pagina)
                    impacto_seo = self._calcular_impacto_seo(tipo_problema)
                    recomendacao = self._gerar_recomendacao(tipo_problema, tipo_pagina, url)
                    
                    # 📊 SCORE PARA ORDENAÇÃO
                    score_problema = self._calcular_score_problema(tipo_problema)
                    
                    rows.append({
                        'URL': url,
                        'Status_HTTP': status_code or 'N/A',
                        'Content_Type': content_type or 'N/A',
                        'Tipo_Problema': tipo_problema,
                        'Problema_Detectado': self._descrever_problema(tipo_problema),
                        'Title_Atual': self._formatar_title_display(title_original, tipo_problema),
                        'Title_Sanitizado': title_sanitizado or '[VAZIO]',
                        'Tamanho_Original': len(str(title_original)) if title_original else 0,
                        'Tipo_Pagina': tipo_pagina,
                        'Gravidade': gravidade,
                        'Prioridade_Correcao': prioridade,
                        'Score_Problema': score_problema,
                        'Impacto_SEO': impacto_seo,
                        'Recomendacao': recomendacao,
                        'Detalhes_Tecnico': self._gerar_detalhes_tecnicos(title_original, tipo_problema)
                    })
            
            # 📊 ESTATÍSTICAS FINAIS
            self._exibir_estatisticas_hardened(stats)
            
            # 🔄 PROCESSAMENTO FINAL
            if not rows:
                print(f"✅ PERFEITO: Nenhum problema de title encontrado!")
                df_vazio = self._criar_dataframe_vazio()
                df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
                return df_vazio
            
            # 📋 CRIA E ORDENA DATAFRAME
            df_problemas = pd.DataFrame(rows)
            df_problemas = self._ordenar_por_prioridade(df_problemas)
            
            # 📤 EXPORTA
            df_problemas.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            
            print(f"📋 Title_Ausente HARDENED: {len(df_problemas)} problemas críticos encontrados")
            self._exibir_resumo_problemas(df_problemas)
            
            return df_problemas
            
        except Exception as e:
            print(f"❌ Erro crítico no validador HARDENED: {e}")
            import traceback
            traceback.print_exc()
            
            df_erro = self._criar_dataframe_vazio()
            df_erro.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            return df_erro
    
    def _inferir_tipo_pagina(self, url):
        """🔍 Análise contextual da página"""
        url_lower = url.lower()
        
        # Homepage
        parsed = urlparse(url_lower)
        if parsed.path in ['/', '/index', '/home', '/inicio']:
            return 'Homepage'
        
        # Produtos/Serviços (alta prioridade SEO)
        if any(x in url_lower for x in ['/produto', '/product', '/servico', '/service', '/item']):
            return 'Produto/Serviço'
        
        # Categoria (importante SEO)
        if any(x in url_lower for x in ['/categoria', '/category', '/secao', '/section']):
            return 'Categoria'
        
        # Blog/Conteúdo (importante SEO)
        if any(x in url_lower for x in ['/blog', '/artigo', '/post', '/noticia', '/news']):
            return 'Blog/Conteúdo'
        
        # Institucional (média prioridade)
        if any(x in url_lower for x in ['/sobre', '/about', '/quem-somos', '/empresa', '/historia']):
            return 'Institucional'
        
        # Contato (baixa prioridade SEO)
        if any(x in url_lower for x in ['/contato', '/contact', '/fale-conosco']):
            return 'Contato'
        
        return 'Página Interna'
    
    def _calcular_gravidade(self, tipo_problema, tipo_pagina):
        """⚠️ Gravidade baseada em problema + contexto"""
        gravidades_base = {
            'NULO': 'CRITICO',
            'PANDAS_NAN': 'CRITICO',
            'VAZIO': 'CRITICO',
            'PLACEHOLDER': 'ALTO',
            'MUITO_CURTO': 'MEDIO'
        }
        
        gravidade = gravidades_base.get(tipo_problema, 'BAIXO')
        
        # Amplifica para páginas críticas
        if tipo_pagina in ['Homepage', 'Produto/Serviço'] and gravidade in ['MEDIO', 'BAIXO']:
            gravidade = 'ALTO'
        
        return gravidade
    
    def _calcular_prioridade(self, tipo_problema, tipo_pagina):
        """🎯 Prioridade de correção"""
        # Problemas físicos sempre críticos
        if tipo_problema in ['NULO', 'PANDAS_NAN', 'VAZIO']:
            if tipo_pagina in ['Homepage', 'Produto/Serviço']:
                return 'URGENTE'
            elif tipo_pagina in ['Categoria', 'Blog/Conteúdo']:
                return 'ALTA'
            else:
                return 'MEDIA'
        
        # Problemas técnicos
        elif tipo_problema in ['PLACEHOLDER', 'MUITO_CURTO']:
            if tipo_pagina in ['Homepage', 'Produto/Serviço']:
                return 'ALTA'
            else:
                return 'MEDIA'
        
        return 'BAIXA'
    
    def _calcular_impacto_seo(self, tipo_problema):
        """📈 Impacto no SEO"""
        impactos = {
            'NULO': 'CRITICO - Página invisível para Google',
            'PANDAS_NAN': 'CRITICO - Página invisível para Google', 
            'VAZIO': 'CRITICO - Página invisível para Google',
            'PLACEHOLDER': 'ALTO - Título genérico prejudica ranking',
            'MUITO_CURTO': 'MEDIO - Título insuficiente para relevância'
        }
        
        return impactos.get(tipo_problema, 'BAIXO - Impacto mínimo')
    
    def _calcular_score_problema(self, tipo_problema):
        """📊 Score numérico para ordenação (menor = mais grave)"""
        scores = {
            'NULO': 1,
            'PANDAS_NAN': 2,
            'VAZIO': 3,
            'PLACEHOLDER': 4,
            'MUITO_CURTO': 5
        }
        
        return scores.get(tipo_problema, 99)
    
    def _gerar_recomendacao(self, tipo_problema, tipo_pagina, url):
        """💡 Recomendação específica e prática"""
        if tipo_problema in ['NULO', 'PANDAS_NAN', 'VAZIO']:
            sugestao = self._sugerir_title_por_url(url, tipo_pagina)
            return f'URGENTE: Adicionar <title>{sugestao}</title> à página'
        
        elif tipo_problema == 'PLACEHOLDER':
            sugestao = self._sugerir_title_por_url(url, tipo_pagina)
            return f'Substituir placeholder por: <title>{sugestao}</title>'
        
        elif tipo_problema == 'MUITO_CURTO':
            return 'Expandir título para 15-60 caracteres com palavras-chave relevantes'
        
        return 'Revisar título para melhor otimização SEO'
    
    def _sugerir_title_por_url(self, url, tipo_pagina):
        """💡 Sugestão inteligente baseada na URL"""
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '').split('.')[0].title()
        
        if tipo_pagina == 'Homepage':
            return f'{domain} - Página Inicial'
        elif tipo_pagina == 'Produto/Serviço':
            return f'Produto/Serviço | {domain}'
        elif tipo_pagina == 'Blog/Conteúdo':
            return f'Artigo | {domain}'
        elif tipo_pagina == 'Categoria':
            return f'Categoria | {domain}'
        else:
            # Tenta extrair da URL
            path_parts = [p for p in parsed.path.split('/') if p]
            if path_parts:
                page_name = path_parts[-1].replace('-', ' ').replace('_', ' ').title()
                return f'{page_name} | {domain}'
            
            return f'{tipo_pagina} | {domain}'
    
    def _descrever_problema(self, tipo_problema):
        """📝 Descrição clara do problema"""
        descricoes = {
            'NULO': 'Title é None/null - campo não existe',
            'PANDAS_NAN': 'Title é NaN do pandas - dado corrompido',
            'VAZIO': 'Title é string vazia ou só espaços',
            'PLACEHOLDER': 'Title é placeholder genérico sem valor',
            'MUITO_CURTO': 'Title muito curto (≤3 caracteres)'
        }
        
        return descricoes.get(tipo_problema, 'Problema desconhecido')
    
    def _formatar_title_display(self, title_original, tipo_problema):
        """🎨 Formata title para visualização"""
        if tipo_problema == 'NULO':
            return '[NULL]'
        elif tipo_problema == 'PANDAS_NAN':
            return '[NaN]'
        elif tipo_problema == 'VAZIO':
            return '[VAZIO]'
        else:
            return str(title_original)[:100] + ('...' if len(str(title_original)) > 100 else '')
    
    def _gerar_detalhes_tecnicos(self, title_original, tipo_problema):
        """🔧 Detalhes técnicos para debug"""
        detalhes = f'Tipo original: {type(title_original)}'
        
        if title_original is not None:
            detalhes += f', Valor: "{str(title_original)[:50]}"'
            detalhes += f', Tamanho: {len(str(title_original))}'
        
        detalhes += f', Classificação: {tipo_problema}'
        
        return detalhes
    
    def _ordenar_por_prioridade(self, df):
        """📊 Ordenação inteligente por prioridade"""
        # Mapas para ordenação
        prioridade_map = {'URGENTE': 1, 'ALTA': 2, 'MEDIA': 3, 'BAIXA': 4}
        gravidade_map = {'CRITICO': 1, 'ALTO': 2, 'MEDIO': 3, 'BAIXO': 4}
        
        df['prioridade_sort'] = df['Prioridade_Correcao'].map(prioridade_map).fillna(99)
        df['gravidade_sort'] = df['Gravidade'].map(gravidade_map).fillna(99)
        
        # Ordena: Score → Prioridade → Gravidade → URL
        df = df.sort_values([
            'Score_Problema', 'prioridade_sort', 'gravidade_sort', 'URL'
        ], ascending=[True, True, True, True])
        
        # Remove colunas auxiliares
        df = df.drop(['prioridade_sort', 'gravidade_sort'], axis=1, errors='ignore')
        
        return df
    
    def _criar_dataframe_vazio(self):
        """📋 DataFrame vazio com estrutura correta"""
        return pd.DataFrame(columns=[
            'URL', 'Status_HTTP', 'Content_Type', 'Tipo_Problema', 'Problema_Detectado',
            'Title_Atual', 'Title_Sanitizado', 'Tamanho_Original', 'Tipo_Pagina',
            'Gravidade', 'Prioridade_Correcao', 'Score_Problema', 'Impacto_SEO',
            'Recomendacao', 'Detalhes_Tecnico'
        ])
    
    def _exibir_estatisticas_hardened(self, stats):
        """📊 Exibe estatísticas detalhadas"""
        print(f"\n📊 ESTATÍSTICAS HARDENED:")
        print(f"   📥 URLs inicial: {stats['total_inicial']}")
        print(f"   🚫 Filtrados por URL inválida: {stats['filtrado_url']}")
        print(f"   🚫 Filtrados por status HTTP: {stats['filtrado_status']}")
        print(f"   🚫 Filtrados por Content-Type: {stats['filtrado_content_type']}")
        print(f"   🚫 Filtrados por paginação: {stats['filtrado_paginacao']}")
        print(f"   ✅ URLs analisados: {stats['analisados']}")
        print(f"   ⚠️ Problemas encontrados: {stats['problemas_encontrados']}")
        
        if stats['analisados'] > 0:
            taxa_problemas = (stats['problemas_encontrados'] / stats['analisados']) * 100
            print(f"   📈 Taxa de problemas: {taxa_problemas:.1f}%")
    
    def _exibir_resumo_problemas(self, df):
        """📋 Resumo dos tipos de problemas encontrados"""
        print(f"\n📋 RESUMO DOS PROBLEMAS:")
        for tipo in df['Tipo_Problema'].value_counts().head(5).items():
            print(f"   • {tipo[0]}: {tipo[1]} URLs")
        
        print(f"\n🎯 TOP 5 PRIORIDADES:")
        for prioridade in df['Prioridade_Correcao'].value_counts().head(5).items():
            print(f"   • {prioridade[0]}: {prioridade[1]} URLs")