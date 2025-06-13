from urllib.parse import urlparse, urljoin, parse_qs
from typing import List, Set, Dict, Tuple
import re
from collections import defaultdict

class URLManagerSEO:
    """🎯 URL Manager calibrado para auditorias SEO por tipo de site"""
    
    # 📋 PERFIS PRÉ-CONFIGURADOS PARA SEO
    PERFIS_SEO = {
        'blog': {
            'categoria': 15,      # Poucas categorias principais
            'tag': 8,            # Tags mais relevantes apenas
            'arquivo': 5,        # Archive mensal/anual limitado
            'paginacao': 8,      # Algumas páginas de paginação
            'busca': 3,          # Mínimo de busca
            'api': 0,            # Zero APIs
            'admin': 0,          # Zero admin
            'conteudo': 9999     # CONTEÚDO É REI em blogs
        },
        
        'ecommerce': {
            'categoria': 30,      # Categorias importantes para e-commerce
            'tag': 5,            # Tags menos relevantes
            'arquivo': 3,        # Archive mínimo
            'paginacao': 15,     # Paginação de produtos relevante
            'busca': 5,          # Alguns resultados de busca
            'api': 0,            # Zero APIs
            'admin': 0,          # Zero admin
            'produto': 9999,     # PRODUTOS SÃO REI
            'conteudo': 9999     # Conteúdo também importante
        },
        
        'saas': {
            'categoria': 10,      # Funcionalidades/features
            'tag': 5,            # Poucos tags
            'arquivo': 3,        # Archive mínimo
            'paginacao': 5,      # Paginação limitada
            'busca': 3,          # Busca mínima
            'api': 8,            # Documentação API importante
            'admin': 0,          # Zero admin
            'conteudo': 9999,    # Blog/help center
            'documentacao': 9999  # Docs são críticas
        },
        
        'institucional': {
            'categoria': 8,       # Serviços/produtos
            'tag': 3,            # Poucos tags
            'arquivo': 2,        # Archive mínimo
            'paginacao': 3,      # Paginação muito limitada
            'busca': 2,          # Busca mínima
            'api': 0,            # Zero APIs
            'admin': 0,          # Zero admin
            'conteudo': 9999     # Conteúdo institucional
        },
        
        'portal': {
            'categoria': 40,      # Muitas seções
            'tag': 20,           # Sistema de tags relevante
            'arquivo': 15,       # Archive histórico importante
            'paginacao': 25,     # Paginação extensa
            'busca': 10,         # Sistema de busca relevante
            'api': 0,            # Zero APIs
            'admin': 0,          # Zero admin
            'conteudo': 9999     # Muito conteúdo
        }
    }
    
    def __init__(self, dominio_base: str, max_urls: int = 1000, perfil_seo: str = 'blog'):
        self.dominio_base = dominio_base
        self.max_urls = max_urls
        self.perfil_seo = perfil_seo
        
        # Estados
        self.visitadas: Set[str] = set()
        self.fila_prioridade: List[Tuple[int, str, int]] = []  # (prioridade, url, nivel)
        self.urls_por_nivel: Dict[int, Set[str]] = defaultdict(set)
        self.contador_por_tipo: Dict[str, int] = defaultdict(int)
        
        # 🎯 CONFIGURAÇÃO SEO CALIBRADA
        self.max_por_tipo = self.PERFIS_SEO.get(perfil_seo, self.PERFIS_SEO['blog']).copy()
        
        # 🚫 BLACKLIST AGRESSIVA PARA SEO
        self.extensoes_ignoradas = {
            '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp', '.ico',
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.7z',
            '.js', '.css', '.mp3', '.mp4', '.avi', '.mov', '.xml', '.json',
            '.woff', '.woff2', '.ttf', '.eot', '.swf', '.flv', '.wmv'
        }
        
        # 🚫 PARÂMETROS DE TRACKING PARA IGNORAR
        self.parametros_tracking = {
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'gclid', 'fbclid', 'dclid', '_ga', '_gid', 'mc_cid', 'mc_eid',
            'ref', 'source', 'campaign', 'medium', 'term', 'content'
        }
        
        print(f"🎯 URLManager SEO inicializado:")
        print(f"   Perfil: {perfil_seo}")
        print(f"   Limites por tipo: {self.max_por_tipo}")
    
    def adicionar_url(self, url: str, nivel: int, url_origem: str = "") -> bool:
        """➕ Adiciona URL com validação SEO aprimorada"""
        
        # 🚫 Validações básicas
        if not self._url_e_valida_seo(url):
            return False
        
        url_limpa = self._limpar_url_seo(url)
        
        # 🚫 Já visitada ou na fila
        if url_limpa in self.visitadas:
            return False
        
        if any(url_limpa == item[1] for item in self.fila_prioridade):
            return False
        
        # 🚫 Limite de URLs atingido
        if len(self.visitadas) + len(self.fila_prioridade) >= self.max_urls:
            return False
        
        # 🎯 Calcula prioridade SEO
        prioridade = self._calcular_prioridade_seo(url_limpa, nivel, url_origem)
        
        # 🚫 Controle de tipos SEO
        tipo_url = self._identificar_tipo_url_seo(url_limpa)
        if not self._pode_adicionar_tipo_seo(tipo_url):
            return False
        
        # ✅ Adiciona à fila
        self.fila_prioridade.append((prioridade, url_limpa, nivel))
        self.urls_por_nivel[nivel].add(url_limpa)
        self.contador_por_tipo[tipo_url] += 1
        
        return True
    
    def _url_e_valida_seo(self, url: str) -> bool:
        """🔍 Validação específica para SEO audit"""
        
        if not url or not isinstance(url, str):
            return False
        
        try:
            parsed = urlparse(url)
            
            # Validações básicas
            if not parsed.scheme or not parsed.netloc:
                return False
            
            if parsed.netloc != self.dominio_base:
                return False
            
            # 🚫 BLACKLIST SEO ESPECÍFICA
            path_lower = parsed.path.lower()
            
            # Extensões irrelevantes para SEO
            if any(path_lower.endswith(ext) for ext in self.extensoes_ignoradas):
                return False
            
            # URLs de sistema/desenvolvimento
            blacklist_paths = [
                '/wp-admin', '/admin', '/administrator', '/login', '/wp-login',
                '/wp-content', '/wp-includes', '/wp-json', '/xmlrpc.php',
                '/feed', '/rss', '/sitemap', '/robots.txt', '/.well-known',
                '/cdn-cgi', '/wp-cron.php', '/readme.html', '/license.txt'
            ]
            
            if any(blacklist in path_lower for blacklist in blacklist_paths):
                return False
            
            # URLs com muitos parâmetros (spam)
            if len(parse_qs(parsed.query)) > 4:
                return False
            
            # URLs muito longas (potencial spam)
            if len(url) > 200:
                return False
            
            # URLs com caracteres suspeitos
            if any(char in url for char in ['<', '>', '"', "'", '`', '|']):
                return False
            
            return True
            
        except:
            return False
    
    def _limpar_url_seo(self, url: str) -> str:
        """🧹 Limpeza avançada para SEO"""
        
        try:
            parsed = urlparse(url)
            
            # Remove fragmento
            url_limpa = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            
            # 🧹 LIMPEZA AGRESSIVA DE PARÂMETROS
            if parsed.query:
                params = parse_qs(parsed.query)
                
                # Parâmetros SEO importantes para manter
                params_seo_importantes = {
                    'page', 'p', 'pagina', 'offset',  # Paginação
                    'categoria', 'category', 'cat',    # Categorização
                    'tag', 'tags', 'etiqueta',        # Tags
                    'id', 'post_id', 'product_id',    # IDs
                    'slug', 'name', 'title',          # Slugs
                    'type', 'post_type', 'format',    # Tipos
                    's', 'q', 'search', 'query',      # Busca (limitado)
                    'year', 'month', 'date'           # Arquivo temporal
                }
                
                params_limpos = {}
                
                for key, values in params.items():
                    key_lower = key.lower()
                    
                    # Remove parâmetros de tracking
                    if key_lower in self.parametros_tracking:
                        continue
                    
                    # Mantém apenas parâmetros SEO relevantes
                    if key_lower in params_seo_importantes and values:
                        params_limpos[key] = values[0]
                
                if params_limpos:
                    query_limpa = '&'.join(f"{k}={v}" for k, v in params_limpos.items())
                    url_limpa += f"?{query_limpa}"
            
            # Normaliza trailing slash
            if url_limpa.endswith('/') and len(parsed.path) > 1:
                url_limpa = url_limpa.rstrip('/')
            
            return url_limpa
            
        except:
            return url
    
    def _identificar_tipo_url_seo(self, url: str) -> str:
        """🏷️ Identificação de tipo focada em SEO"""
        
        path = urlparse(url).path.lower()
        query = urlparse(url).query.lower()
        
        # 🎯 DETECÇÃO ESPECÍFICA PARA SEO
        
        # Produtos (e-commerce)
        if any(termo in path for termo in ['/produto', '/product', '/item', '/p/']):
            return 'produto'
        
        # Conteúdo principal
        elif any(termo in path for termo in ['/blog', '/post', '/artigo', '/article', '/news', '/noticia']):
            return 'conteudo'
        
        # Documentação (SaaS/Tech)
        elif any(termo in path for termo in ['/docs', '/doc', '/help', '/support', '/guide', '/tutorial']):
            return 'documentacao'
        
        # Categorias
        elif any(termo in path for termo in ['/categoria', '/category', '/cat', '/secao', '/section']):
            return 'categoria'
        
        # Tags
        elif any(termo in path for termo in ['/tag', '/tags', '/etiqueta', '/label']):
            return 'tag'
        
        # Archive temporal
        elif any(termo in path for termo in ['/arquivo', '/archive', '/data', '/ano', '/year', '/mes', '/month']):
            return 'arquivo'
        
        # Paginação
        elif any(termo in query for termo in ['page=', 'pagina=', 'p=']) or '/page/' in path:
            return 'paginacao'
        
        # Busca
        elif any(termo in path for termo in ['/search', '/busca', '/pesquisa', '/find']) or 's=' in query:
            return 'busca'
        
        # API/JSON
        elif any(termo in path for termo in ['/api', '/json', '/xml', '/rss', '/feed']):
            return 'api'
        
        # Admin
        elif any(termo in path for termo in ['/admin', '/wp-admin', '/administrator', '/painel']):
            return 'admin'
        
        # Homepage
        elif path in ['', '/']:
            return 'homepage'
        
        # Páginas institucionais importantes
        elif any(termo in path for termo in ['/sobre', '/about', '/contato', '/contact', '/servicos', '/services']):
            return 'institucional'
        
        else:
            return 'conteudo'  # Default: trata como conteúdo
    
    def _calcular_prioridade_seo(self, url: str, nivel: int, url_origem: str) -> int:
        """🎯 Prioridade específica para valor SEO"""
        
        prioridade = 100  # Base
        
        # 📍 Nível (mais baixo = maior prioridade SEO)
        prioridade += nivel * 15
        
        # 🎯 PRIORIDADE POR VALOR SEO
        path = urlparse(url).path.lower()
        tipo_url = self._identificar_tipo_url_seo(url)
        
        # Máxima prioridade SEO
        if tipo_url == 'homepage':
            prioridade -= 60
        elif tipo_url == 'institucional':
            prioridade -= 40
        elif tipo_url == 'conteudo':
            prioridade -= 35
        elif tipo_url == 'produto':
            prioridade -= 30
        elif tipo_url == 'documentacao':
            prioridade -= 25
        elif tipo_url == 'categoria':
            prioridade -= 15
        
        # Penalidades SEO
        elif tipo_url == 'paginacao':
            prioridade += 25
        elif tipo_url == 'busca':
            prioridade += 30
        elif tipo_url == 'api':
            prioridade += 35
        elif tipo_url == 'admin':
            prioridade += 100  # Evita ao máximo
        
        # 🔍 Indicadores de qualidade SEO
        
        # URLs curtas e limpa = melhor SEO
        if len(url) < 80:
            prioridade -= 5
        elif len(url) > 150:
            prioridade += 10
        
        # Sem parâmetros = melhor SEO
        if '?' not in url:
            prioridade -= 5
        
        # Estrutura hierárquica clara
        path_segments = [seg for seg in path.split('/') if seg]
        if 2 <= len(path_segments) <= 4:  # Estrutura ideal
            prioridade -= 3
        
        return max(0, prioridade)
    
    def _pode_adicionar_tipo_seo(self, tipo: str) -> bool:
        """🚦 Controle rigoroso para maximizar valor SEO"""
        
        limite = self.max_por_tipo.get(tipo, 50)  # Default moderado
        atual = self.contador_por_tipo[tipo]
        
        # Lógica especial para tipos críticos
        if tipo in ['conteudo', 'produto', 'documentacao']:
            # Estes tipos são prioritários - limite muito alto
            return atual < 9999
        
        return atual < limite
    
    def obter_proxima_url(self) -> Tuple[str, int] | None:
        """🔄 Obtém próxima URL priorizando valor SEO"""
        
        if not self.fila_prioridade:
            return None
        
        # Ordena por prioridade SEO
        self.fila_prioridade.sort(key=lambda x: x[0])
        
        prioridade, url, nivel = self.fila_prioridade.pop(0)
        self.visitadas.add(url)
        
        return url, nivel
    
    def adicionar_lote_urls_seo(self, urls: List[str], nivel: int, url_origem: str = "") -> int:
        """📦 Adiciona lote priorizando valor SEO"""
        
        adicionadas = 0
        
        # Pré-filtra e ordena por valor SEO
        urls_com_valor_seo = []
        for url in urls:
            if self._url_e_valida_seo(url):
                url_limpa = self._limpar_url_seo(url)
                tipo_seo = self._identificar_tipo_url_seo(url_limpa)
                prioridade_seo = self._calcular_prioridade_seo(url_limpa, nivel, url_origem)
                
                # Score SEO composto
                score_seo = prioridade_seo
                if tipo_seo in ['conteudo', 'produto', 'documentacao']:
                    score_seo -= 20  # Boost para conteúdo valioso
                
                urls_com_valor_seo.append((score_seo, url_limpa, tipo_seo))
        
        # Ordena por valor SEO
        urls_com_valor_seo.sort(key=lambda x: x[0])
        
        for score, url, tipo in urls_com_valor_seo:
            if self.adicionar_url(url, nivel, url_origem):
                adicionadas += 1
            
            # Para se atingir limite
            if len(self.visitadas) + len(self.fila_prioridade) >= self.max_urls:
                break
        
        return adicionadas
    
    def obter_relatorio_seo(self) -> Dict:
        """📊 Relatório específico para auditoria SEO"""
        
        stats = {
            'perfil_seo': self.perfil_seo,
            'urls_visitadas': len(self.visitadas),
            'urls_na_fila': len(self.fila_prioridade),
            'total_descobertas': len(self.visitadas) + len(self.fila_prioridade),
            'distribuicao_por_tipo': dict(self.contador_por_tipo),
            'urls_por_nivel': {k: len(v) for k, v in self.urls_por_nivel.items()},
            'limites_configurados': self.max_por_tipo,
            'cobertura_por_tipo': {}
        }
        
        # Calcula % de cobertura por tipo
        for tipo, atual in self.contador_por_tipo.items():
            limite = self.max_por_tipo.get(tipo, 50)
            if limite > 0:
                cobertura = min(100, (atual / limite) * 100)
                stats['cobertura_por_tipo'][tipo] = f"{cobertura:.1f}%"
        
        return stats

# ========================
# 🎯 CONFIGURAÇÃO RÁPIDA POR TIPO DE SITE
# ========================

def criar_url_manager_para_blog(dominio_base: str, max_urls: int = 1000) -> URLManagerSEO:
    """📝 Configuração otimizada para blogs"""
    return URLManagerSEO(dominio_base, max_urls, 'blog')

def criar_url_manager_para_ecommerce(dominio_base: str, max_urls: int = 1000) -> URLManagerSEO:
    """🛒 Configuração otimizada para e-commerce"""
    return URLManagerSEO(dominio_base, max_urls, 'ecommerce')

def criar_url_manager_para_saas(dominio_base: str, max_urls: int = 1000) -> URLManagerSEO:
    """⚙️ Configuração otimizada para SaaS"""
    return URLManagerSEO(dominio_base, max_urls, 'saas')

def criar_url_manager_para_institucional(dominio_base: str, max_urls: int = 1000) -> URLManagerSEO:
    """🏢 Configuração otimizada para sites institucionais"""
    return URLManagerSEO(dominio_base, max_urls, 'institucional')

def criar_url_manager_para_portal(dominio_base: str, max_urls: int = 1000) -> URLManagerSEO:
    """📰 Configuração otimizada para portais de notícias"""
    return URLManagerSEO(dominio_base, max_urls, 'portal')