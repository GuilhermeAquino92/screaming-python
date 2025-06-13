# main_hibrido.py - Sistema Híbrido Inteligente com Playwright Enterprise

import pandas as pd
import os
import asyncio
import sys
from urllib.parse import urlparse
from crawler import rastrear_profundo as crawler_requests
from status_checker import verificar_status_http
from metatags import extrair_metatags
from validador_headings import validar_headings
from http_inseguro import extrair_http_inseguros
from exporters.excel_manager import exportar_relatorio_completo
import warnings

# 🆕 Import Playwright (com fallback)
try:
    from crawler_playwright import rastrear_playwright_profundo
    PLAYWRIGHT_AVAILABLE = True
    print("✅ Playwright Enterprise disponível")
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️ Playwright não instalado. Modo híbrido desabilitado.")
    print("   Para instalar: pip install playwright && playwright install chromium")

warnings.filterwarnings("ignore")

# ========================
# 🎯 CONFIGURAÇÕES HÍBRIDAS
# ========================
url_inicial = "https://gndisul.com.br/"
MAX_URLS = 3000
MAX_WORKERS = 20
MAX_DEPTH = 3
FORCAR_REINDEXACAO = False
PASTA_SAIDA = "output"
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, "relatorio_seo_hibrido.xlsx")
os.makedirs(PASTA_SAIDA, exist_ok=True)

# 🧠 CONFIGURAÇÕES INTELIGENTES
MODO_CRAWLER = "AUTO"  # AUTO, REQUESTS, PLAYWRIGHT, HIBRIDO
LIMITE_JS_DETECTION = 50  # URLs para testar se precisa JS
FALLBACK_ATIVO = True  # Se Playwright falhar, usa Requests
USE_SELENIUM_SE_ERRO = True  # Selenium como último recurso

# ========================
# 🤖 Detecção Inteligente de Site
# ========================
async def detectar_necessidade_js(url_inicial, limite=50):
    """🧠 Testa amostra de URLs para ver se site precisa de JavaScript"""
    
    if not PLAYWRIGHT_AVAILABLE:
        return False, "Playwright não disponível"
    
    print(f"🔍 Testando se site precisa de JavaScript renderização...")
    print(f"📊 Analisando até {limite} URLs de amostra...")
    
    try:
        # 🚀 Crawl pequeno com Playwright para teste
        amostra_resultados = await rastrear_playwright_profundo(
            url_inicial,
            max_urls=limite,
            max_depth=2,
            browser_pool_size=2
        )
        
        if not amostra_resultados:
            return False, "Nenhuma URL coletada para análise"
        
        # 📊 Analisa resultados
        total_urls = len(amostra_resultados)
        urls_com_js = len([r for r in amostra_resultados if r.get("needs_javascript", False)])
        percentual_js = (urls_com_js / total_urls * 100) if total_urls > 0 else 0
        
        # 🔍 Coleta razões detalhadas
        razoes_js = []
        for resultado in amostra_resultados:
            if resultado.get("needs_javascript", False):
                razao = resultado.get("js_detection_reason", "Desconhecido")
                if razao not in razoes_js:
                    razoes_js.append(razao)
        
        # 🎯 Decisão inteligente
        precisa_js = percentual_js >= 30  # Se 30%+ das páginas precisam JS
        
        print(f"📊 RESULTADO DA ANÁLISE:")
        print(f"   🔢 URLs testadas: {total_urls}")
        print(f"   🤖 URLs que precisam JS: {urls_com_js} ({percentual_js:.1f}%)")
        print(f"   📋 Principais razões: {', '.join(razoes_js[:3])}")
        print(f"   🎯 Decisão: {'PRECISA JavaScript' if precisa_js else 'Sites estáticos OK'}")
        
        razao_final = f"{percentual_js:.1f}% das páginas precisam JS. Razões: {', '.join(razoes_js[:2])}"
        
        return precisa_js, razao_final
        
    except Exception as e:
        print(f"⚠️ Erro na detecção JS: {e}")
        return True, f"Erro na detecção, usando Playwright por segurança: {str(e)}"

# ========================
# 🚀 Sistema de Crawler Híbrido
# ========================
async def executar_crawler_hibrido():
    """🎯 Sistema inteligente que escolhe melhor crawler"""
    
    print(f"🚀 Sistema Híbrido Inteligente Iniciado!")
    print(f"🎯 URL: {url_inicial}")
    print(f"📊 Configuração: {MAX_URLS} URLs, profundidade {MAX_DEPTH}")
    
    urls_com_dados = []
    metodo_usado = "INDEFINIDO"
    
    # 🧠 MODO AUTOMÁTICO - Detecção Inteligente
    if MODO_CRAWLER == "AUTO" and PLAYWRIGHT_AVAILABLE:
        print(f"\n🧠 MODO AUTO: Detectando melhor crawler...")
        
        try:
            precisa_js, razao = await detectar_necessidade_js(url_inicial, LIMITE_JS_DETECTION)
            
            if precisa_js:
                print(f"🎯 DECISÃO: Usar Playwright Enterprise (renderização JS necessária)")
                print(f"📝 Razão: {razao}")
                metodo_escolhido = "PLAYWRIGHT"
            else:
                print(f"🎯 DECISÃO: Usar Requests Otimizado (site estático detectado)")
                print(f"📝 Razão: {razao}")
                metodo_escolhido = "REQUESTS"
                
        except Exception as e:
            print(f"⚠️ Erro na detecção automática: {e}")
            print(f"🔄 Fallback: Usando Playwright por segurança")
            metodo_escolhido = "PLAYWRIGHT"
    else:
        metodo_escolhido = MODO_CRAWLER
    
    # 🚀 EXECUÇÃO DO CRAWLER ESCOLHIDO
    if metodo_escolhido == "PLAYWRIGHT" and PLAYWRIGHT_AVAILABLE:
        try:
            print(f"\n🎭 Executando Playwright Enterprise...")
            urls_com_dados = await rastrear_playwright_profundo(
                url_inicial,
                max_urls=MAX_URLS,
                max_depth=MAX_DEPTH,
                forcar_reindexacao=FORCAR_REINDEXACAO,
                browser_pool_size=3
            )
            metodo_usado = "PLAYWRIGHT"
            print("✅ Playwright Enterprise concluído com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro com Playwright: {e}")
            if FALLBACK_ATIVO:
                print(f"🔄 Executando fallback para Requests...")
                metodo_escolhido = "REQUESTS"
            else:
                raise e
    
    if metodo_escolhido == "REQUESTS":
        try:
            print(f"\n⚡ Executando Requests Otimizado...")
            urls_com_dados = crawler_requests(
                url_inicial,
                max_urls=MAX_URLS,
                max_depth=MAX_DEPTH,
                max_workers=MAX_WORKERS,
                forcar_reindexacao=FORCAR_REINDEXACAO
            )
            metodo_usado = "REQUESTS"
            print("✅ Requests Otimizado concluído com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro com Requests: {e}")
            if FALLBACK_ATIVO and USE_SELENIUM_SE_ERRO:
                print(f"🔄 Tentando Selenium como último recurso...")
                try:
                    from crawler_selenium import rastrear_selenium_profundo
                    urls_com_dados = rastrear_selenium_profundo(
                        url_inicial,
                        max_urls=MAX_URLS,
                        forcar_reindexacao=FORCAR_REINDEXACAO
                    )
                    metodo_usado = "SELENIUM"
                    print("✅ Selenium concluído com sucesso!")
                except Exception as selenium_error:
                    print(f"❌ Erro também com Selenium: {selenium_error}")
                    urls_com_dados = []
                    metodo_usado = "ERRO"
            else:
                urls_com_dados = []
                metodo_usado = "ERRO"
    
    # 🎯 MODO HÍBRIDO - Usa ambos para comparação
    if metodo_escolhido == "HIBRIDO" and PLAYWRIGHT_AVAILABLE:
        print(f"\n🔄 MODO HÍBRIDO: Executando Requests + Playwright...")
        
        try:
            # Executa ambos em paralelo (cuidado com recursos)
            print(f"⚡ Executando Requests primeiro...")
            urls_requests = crawler_requests(
                url_inicial,
                max_urls=MAX_URLS//2,  # Divide URLs
                max_depth=MAX_DEPTH,
                max_workers=MAX_WORKERS//2,
                forcar_reindexacao=FORCAR_REINDEXACAO
            )
            
            print(f"🎭 Executando Playwright para comparação...")
            urls_playwright = await rastrear_playwright_profundo(
                url_inicial,
                max_urls=MAX_URLS//2,
                max_depth=MAX_DEPTH,
                forcar_reindexacao=FORCAR_REINDEXACAO,
                browser_pool_size=2
            )
            
            # Combina resultados (prioriza Playwright para dados SEO)
            urls_dict = {}
            
            # Adiciona dados do Requests
            for item in urls_requests:
                url = item.get('url')
                if url:
                    urls_dict[url] = item
            
            # Atualiza/sobrescreve com dados do Playwright (mais precisos)
            for item in urls_playwright:
                url = item.get('url')
                if url:
                    if url in urls_dict:
                        # Mantém dados do Requests, atualiza com dados SEO do Playwright
                        urls_dict[url].update({
                            k: v for k, v in item.items() 
                            if k in ['title', 'description', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                                   'needs_javascript', 'structured_data_count']
                        })
                    else:
                        urls_dict[url] = item
            
            urls_com_dados = list(urls_dict.values())
            metodo_usado = "HÍBRIDO"
            print(f"✅ Modo Híbrido concluído! Combinados {len(urls_dict)} URLs únicos")
            
        except Exception as e:
            print(f"❌ Erro no modo híbrido: {e}")
            print(f"🔄 Fallback para Requests apenas...")
            urls_com_dados = crawler_requests(
                url_inicial,
                max_urls=MAX_URLS,
                max_depth=MAX_DEPTH,
                max_workers=MAX_WORKERS,
                forcar_reindexacao=FORCAR_REINDEXACAO
            )
            metodo_usado = "REQUESTS_FALLBACK"
    
    return urls_com_dados, metodo_usado

# ========================
# 🔍 Processamento de Dados Híbrido
# ========================
def processar_dados_hibridos(df, metodo_usado):
    """🎯 Processa dados considerando o método usado"""
    
    print(f"\n📊 Processando dados coletados via {metodo_usado}...")
    
    # ✅ COMPATIBILIDADE - Normaliza nomes de colunas
    colunas_mapping = {
        'status_code': 'status_code_http',  # Padroniza para auditoria
    }
    
    for old_col, new_col in colunas_mapping.items():
        if old_col in df.columns and new_col not in df.columns:
            df = df.rename(columns={old_col: new_col})
    
    # 🎯 DADOS ESPECÍFICOS DO PLAYWRIGHT
    if metodo_usado in ["PLAYWRIGHT", "HÍBRIDO"]:
        print(f"🎭 Dados Playwright detectados - processamento avançado...")
        
        # Estatísticas de JavaScript
        if 'needs_javascript' in df.columns:
            total_js = len(df[df['needs_javascript'] == True])
            total_estatico = len(df[df['needs_javascript'] == False])
            print(f"   🤖 Sites com JS: {total_js}")
            print(f"   📄 Sites estáticos: {total_estatico}")
        
        # Structured Data
        if 'structured_data_count' in df.columns:
            com_structured = len(df[df['structured_data_count'] > 0])
            print(f"   📊 Páginas com dados estruturados: {com_structured}")
        
        # Console Errors
        if 'console_errors' in df.columns:
            com_erros = len(df[df['console_errors'].apply(lambda x: len(x) > 0 if isinstance(x, list) else False)])
            print(f"   🚨 Páginas com erros JavaScript: {com_erros}")
    
    # 🔍 DADOS ESPECÍFICOS DO REQUESTS
    elif metodo_usado == "REQUESTS":
        print(f"⚡ Dados Requests detectados - processamento otimizado...")
        
        if 'response_time' in df.columns:
            tempo_medio = df['response_time'].mean()
            print(f"   ⏱️ Tempo médio de resposta: {tempo_medio:.2f}ms")
    
    return df

# ========================
# 🎯 MAIN ASSÍNCRONO HÍBRIDO
# ========================
async def main_hibrido():
    """🚀 Função principal híbrida assíncrona"""
    
    print("=" * 60)
    print("🚀 SISTEMA SEO HÍBRIDO ENTERPRISE")
    print("=" * 60)
    
    # 🔍 FASE 1: Crawling Híbrido Inteligente
    print(f"\n📡 FASE 1: CRAWLING HÍBRIDO INTELIGENTE")
    urls_com_dados, metodo_usado = await executar_crawler_hibrido()
    
    if not urls_com_dados:
        print("❌ ERRO CRÍTICO: Nenhuma URL coletada!")
        sys.exit(1)
    
    df = pd.DataFrame(urls_com_dados)
    print(f"📊 DataFrame criado: {len(df)} URLs, colunas: {list(df.columns)}")
    
    # 🎯 FASE 2: Processamento de Dados
    print(f"\n🔧 FASE 2: PROCESSAMENTO DE DADOS")
    df = processar_dados_hibridos(df, metodo_usado)
    
    # 🚫 FASE 3: Filtros de Qualidade
    print(f"\n🎯 FASE 3: FILTROS DE QUALIDADE")
    EXTENSOES_INVALIDAS = [
        '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp', '.ico',
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.7z',
        '.js', '.css', '.mp3', '.mp4', '.avi', '.mov'
    ]
    
    urls_antes = len(df)
    df = df[~df['url'].str.lower().str.contains('|'.join([f'{ext}$' for ext in EXTENSOES_INVALIDAS]), na=False)]
    urls_depois = len(df)
    print(f"📝 Filtro HTML aplicado: {urls_antes} → {urls_depois} URLs (-{urls_antes-urls_depois} arquivos)")
    
    # 🔍 FASE 4: Verificações Adicionais (apenas se necessário)
    print(f"\n🔍 FASE 4: VERIFICAÇÕES ADICIONAIS")
    
    # Status HTTP - só se Playwright não coletou ou dados inconsistentes
    if metodo_usado != "PLAYWRIGHT" or 'status_code_http' not in df.columns:
        print(f"🔍 Verificando status HTTP independente...")
        urls_http = df['url'].dropna().unique().tolist()
        df_status = pd.DataFrame(verificar_status_http(urls_http, max_threads=50))
        df = df.merge(df_status, on='url', how='left', suffixes=('', '_check'))
        
        # Usa verificação independente se não tinha dados
        if 'status_code_http' not in df.columns and 'status_code_http_check' in df.columns:
            df['status_code_http'] = df['status_code_http_check']
    else:
        print(f"✅ Status HTTP já coletado via {metodo_usado}")
    
    # Metatags - só se Playwright não coletou dados completos
    if metodo_usado != "PLAYWRIGHT" or 'title' not in df.columns:
        print(f"🔎 Extraindo metatags independente...")
        urls_meta = df['url'].dropna().unique().tolist()
        df_meta = pd.DataFrame(extrair_metatags(urls_meta, max_threads=50))
        df = df.merge(df_meta, on='url', how='left', suffixes=('_pw', ''))
        
        # Prioriza dados do Playwright se existirem
        for col in ['title', 'description']:
            if f'{col}_pw' in df.columns:
                df[col] = df[f'{col}_pw'].fillna(df[col])
                df = df.drop(f'{col}_pw', axis=1)
    else:
        print(f"✅ Metatags já coletadas via {metodo_usado}")
    
    # Headings - SEMPRE executar para análise CSS detalhada
    print(f"🧠 Validando headings com detecção CSS...")
    urls_head = df['url'].dropna().unique().tolist()
    df_head = pd.DataFrame(validar_headings(urls_head, max_threads=30))
    df = df.merge(df_head, on='url', how='left', suffixes=('_pw', '_css'))
    
    # Combina dados de heading quando disponível
    for col in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        if f'{col}_pw' in df.columns and f'{col}_css' in df.columns:
            # Prioriza dados do CSS analyzer (mais preciso para análise)
            df[col] = df[f'{col}_css'].fillna(df[f'{col}_pw'])
        elif f'{col}_css' in df.columns:
            df[col] = df[f'{col}_css']
        elif f'{col}_pw' in df.columns:
            df[col] = df[f'{col}_pw']
    
    # Limpa colunas temporárias
    colunas_temp = [col for col in df.columns if col.endswith('_pw') or col.endswith('_css')]
    df = df.drop(colunas_temp, axis=1, errors='ignore')
    
    # HTTP Inseguro
    print(f"🚨 Analisando HTTP inseguro...")
    urls_http_inseguro = df['url'].dropna().unique().tolist()
    df_http = pd.DataFrame(extrair_http_inseguros(urls_http_inseguro, max_threads=40))
    
    # 📋 FASE 5: Auditorias SEO
    print(f"\n📋 FASE 5: AUDITORIAS SEO")
    
    # Garante colunas necessárias
    for col in ['title', 'description']:
        if col not in df.columns:
            df[col] = ''
    
    # Filtro para auditoria (remove paginação)
    df_filtrado = df[~df["url"].str.contains(r"\?page=\d+", na=False)].copy()
    
    # Auditorias originais
    df_title_ausente = df_filtrado[df_filtrado["title"].str.strip() == ""].copy()
    df_description_ausente = df_filtrado[df_filtrado["description"].str.strip() == ""].copy()
    
    # Duplicados
    title_count = df_filtrado["title"].dropna().str.strip()
    desc_count = df_filtrado["description"].dropna().str.strip()
    title_count = title_count[title_count != ""]
    desc_count = desc_count[desc_count != ""]
    
    titles_duplicados = title_count.value_counts()
    descs_duplicados = desc_count.value_counts()
    titles_duplicados = titles_duplicados[titles_duplicados > 1].index.tolist()
    descs_duplicados = descs_duplicados[descs_duplicados > 1].index.tolist()
    
    df_title_duplicado = df_filtrado[df_filtrado["title"].isin(titles_duplicados)].copy()
    df_description_duplicado = df_filtrado[df_filtrado["description"].isin(descs_duplicados)].copy()
    
    # Erros HTTP
    status_col = 'status_code_http' if 'status_code_http' in df.columns else 'status_code'
    if status_col in df.columns:
        df_errors = df[df[status_col].astype(str).str.startswith(('3', '4', '5'))].copy()
        if not df_errors.empty:
            df_errors["tipo_erro"] = df_errors[status_col].astype(str).str[0] + "xx"
    else:
        df_errors = pd.DataFrame()
    
    # 📊 FASE 6: Estatísticas Finais
    print(f"\n📊 FASE 6: ESTATÍSTICAS FINAIS")
    print(f"🎯 Método usado: {metodo_usado}")
    print(f"📝 Total de URLs: {len(df)}")
    
    if status_col in df.columns:
        status_200 = len(df[df[status_col] == 200])
        status_3xx = len(df[df[status_col].astype(str).str.startswith('3')])
        status_4xx = len(df[df[status_col].astype(str).str.startswith('4')])
        status_5xx = len(df[df[status_col].astype(str).str.startswith('5')])
        
        print(f"✅ Status 200: {status_200}")
        print(f"🔄 Redirecionamentos: {status_3xx}")
        print(f"❌ Erros 4xx: {status_4xx}")
        print(f"🚨 Erros 5xx: {status_5xx}")
    
    print(f"📋 Title ausente: {len(df_title_ausente)}")
    print(f"📋 Description ausente: {len(df_description_ausente)}")
    print(f"🔄 Title duplicado: {len(df_title_duplicado)}")
    print(f"🔄 Description duplicado: {len(df_description_duplicado)}")
    
    # Estatísticas de headings
    if 'headings_vazios_count' in df.columns:
        total_vazios = df['headings_vazios_count'].sum()
        urls_vazios = len(df[df['headings_vazios_count'] > 0])
        print(f"🕳️ Headings vazios: {total_vazios} em {urls_vazios} URLs")
    
    if 'headings_ocultos_count' in df.columns:
        total_ocultos = df['headings_ocultos_count'].sum()
        urls_ocultos = len(df[df['headings_ocultos_count'] > 0])
        print(f"🎨 Headings ocultos CSS: {total_ocultos} em {urls_ocultos} URLs")
    
    # 📊 FASE 7: Exportação
    print(f"\n🚀 FASE 7: EXPORTAÇÃO DO RELATÓRIO")
    
    auditorias = {
        "df_title_ausente": df_title_ausente,
        "df_description_ausente": df_description_ausente,
        "df_title_duplicado": df_title_duplicado,
        "df_description_duplicado": df_description_duplicado,
        "df_errors": df_errors
    }
    
    # Adiciona metadados do crawler usado
    df['crawler_method'] = metodo_usado
    df['crawler_version'] = 'Híbrido v2.0'
    
    exportar_relatorio_completo(df, df_http, auditorias, ARQUIVO_SAIDA)
    
    print(f"\n🎉 SISTEMA HÍBRIDO CONCLUÍDO COM SUCESSO!")
    print(f"📁 Relatório: {ARQUIVO_SAIDA}")
    print(f"🎯 Método usado: {metodo_usado}")
    print(f"📊 {len(df)} URLs processadas")

# ========================
# 🚀 Função Síncrona de Compatibilidade
# ========================
def main_sync():
    """🔄 Wrapper síncrono para compatibilidade"""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(main_hibrido())
    except KeyboardInterrupt:
        print("\n⚠️ Operação cancelada pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro crítico: {e}")
        import traceback
        traceback.print_exc()

# ========================
# 🎯 EXECUÇÃO
# ========================
if __name__ == "__main__":
    print("🚀 Iniciando Sistema SEO Híbrido Enterprise...")
    
    if PLAYWRIGHT_AVAILABLE:
        print("✅ Playwright disponível - Modo híbrido ativo")
    else:
        print("⚠️ Playwright não disponível - Modo Requests apenas")
    
    main_sync()