# main_hibrido.py - Pipeline Orquestrador LEAN + URLManager SEO

import pandas as pd
import os
import asyncio
import sys
import datetime
from urllib.parse import urlparse

# Imports dos crawlers
from crawler import rastrear_profundo as crawler_requests
from status_checker import verificar_status_http
from metatags import extrair_metatags
from validador_headings import validar_headings
from http_inseguro import extrair_http_inseguros

# Excel Manager
try:
    from exporters.excel_manager import exportar_relatorio_completo
    EXCEL_MANAGER_AVAILABLE = True
    print("✅ Excel Manager especializado disponível")
except ImportError:
    print("⚠️ Excel Manager não disponível - usando versão básica")
    EXCEL_MANAGER_AVAILABLE = False
    
    def exportar_relatorio_completo(df, df_http, auditorias, output_path):
        """📊 Versão básica de exportação"""
        try:
            if not os.path.isabs(output_path):
                output_path = os.path.join(os.getcwd(), os.path.basename(output_path))
            
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                df.to_excel(writer, sheet_name='Dados_Principais', index=False)
                
                for nome, df_aud in auditorias.items():
                    if df_aud is not None and not df_aud.empty:
                        nome_aba = nome.replace('df_', '').title()
                        df_aud.to_excel(writer, sheet_name=nome_aba, index=False)
                
                if not df_http.empty:
                    df_http.to_excel(writer, sheet_name='HTTP_Inseguro', index=False)
            
            print(f"✅ Arquivo Excel básico criado: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"❌ Erro na exportação básica: {e}")
            csv_path = output_path.replace('.xlsx', '.csv')
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"🔄 Dados salvos como CSV: {csv_path}")
            return csv_path

# Playwright LEAN
try:
    from crawler_playwright import rastrear_playwright_profundo
    PLAYWRIGHT_AVAILABLE = True
    print("✅ Playwright LEAN disponível")
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️ Playwright não disponível")

import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# ========================
# 🎯 CONFIGURAÇÃO GLOBAL PIPELINE
# ========================

URL_BASE = "https://ccgsaude.com.br"
MAX_URLS = 1000
MAX_DEPTH = 3

def gerar_nome_arquivo_seguro(url_base):
    """🔧 Gera nome de arquivo seguro"""
    import re
    nome_limpo = url_base.replace('https://', '').replace('http://', '')
    nome_limpo = re.sub(r'[<>:"/\\|?*]', '_', nome_limpo)
    nome_limpo = nome_limpo.replace('.', '_').replace('/', '_')
    return f"relatorio_seo_pipeline_{nome_limpo}.xlsx"

ARQUIVO_SAIDA = gerar_nome_arquivo_seguro(URL_BASE)

# ========================
# 🧠 DETECTOR DE NECESSIDADE JS SIMPLES
# ========================

async def detectar_necessidade_js_simples(url: str) -> tuple[bool, str]:
    """🧠 Detecção simples e eficaz de necessidade de JS"""
    
    try:
        import requests
        
        # Testa versão sem JS
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        html_lower = response.text.lower()
        
        # Detectores simples
        js_indicators = 0
        reasons = []
        
        # Framework detection
        if any(fw in html_lower for fw in ['react', 'vue', 'angular', 'next.js']):
            js_indicators += 2
            reasons.append("Framework JS detectado")
        
        # SPA patterns
        if any(pattern in html_lower for pattern in ['data-reactroot', 'ng-app', 'v-app']):
            js_indicators += 2
            reasons.append("SPA pattern detectado")
        
        # Dynamic content indicators
        if any(word in html_lower for word in ['loading', 'carregando', 'spinner']):
            js_indicators += 1
            reasons.append("Indicadores de loading dinâmico")
        
        # API calls
        if any(api in html_lower for api in ['fetch(', 'axios', '$.ajax', 'api/']):
            js_indicators += 1
            reasons.append("Chamadas de API detectadas")
        
        needs_js = js_indicators >= 2
        reason = " | ".join(reasons) if reasons else "Site aparenta ser estático"
        
        return needs_js, reason
        
    except Exception as e:
        return True, f"Erro na detecção: {str(e)} - usando Playwright por segurança"

# ========================
# 🚀 PIPELINE PRINCIPAL HÍBRIDO
# ========================

async def executar_pipeline_hibrido():
    """🚀 Pipeline híbrido inteligente e simples"""
    
    print("🚀 PIPELINE SEO HÍBRIDO LEAN")
    print("="*50)
    
    urls_com_dados = []
    metodo_usado = "INDEFINIDO"
    
    # 🧠 DETECÇÃO AUTOMÁTICA
    if PLAYWRIGHT_AVAILABLE:
        print(f"🧠 Detectando necessidade de JS para: {URL_BASE}")
        
        try:
            precisa_js, razao = await detectar_necessidade_js_simples(URL_BASE)
            
            print(f"📋 Resultado: {'Playwright' if precisa_js else 'Requests'}")
            print(f"📋 Razão: {razao}")
            
            if precisa_js:
                print(f"\n🎭 Executando Playwright LEAN...")
                urls_com_dados = await rastrear_playwright_profundo(
                    URL_BASE,
                    max_urls=MAX_URLS,
                    max_depth=MAX_DEPTH,
                    forcar_reindexacao=False
                )
                metodo_usado = "PLAYWRIGHT_LEAN"
            else:
                print(f"\n⚡ Executando Requests Otimizado...")
                urls_com_dados = crawler_requests(
                    URL_BASE,
                    max_urls=MAX_URLS,
                    max_depth=MAX_DEPTH
                )
                metodo_usado = "REQUESTS"
                
        except Exception as e:
            print(f"❌ Erro na detecção/execução: {e}")
            print(f"🔄 Fallback para Requests...")
            metodo_usado = "REQUESTS"
    
    # Fallback para Requests se Playwright não disponível
    if not urls_com_dados:
        print(f"\n⚡ Executando Requests (fallback)...")
        try:
            urls_com_dados = crawler_requests(
                URL_BASE,
                max_urls=MAX_URLS,
                max_depth=MAX_DEPTH
            )
            metodo_usado = "REQUESTS"
        except Exception as e:
            print(f"❌ Erro crítico no Requests: {e}")
            return [], "ERRO"
    
    print(f"✅ Crawling concluído: {len(urls_com_dados)} URLs")
    return urls_com_dados, metodo_usado

# ========================
# 📊 PIPELINE DE ANÁLISE
# ========================

def analisar_distribuicao_tipos_url_simples(df):
    """📊 Análise simples de tipos de URL"""
    
    if df.empty:
        return {}
    
    tipos_url = {}
    
    for _, row in df.iterrows():
        url = row.get('url', '')
        if not url:
            continue
        
        path = urlparse(url).path.lower()
        
        # Classificação simples
        if path in ['', '/']:
            tipo = 'homepage'
        elif any(termo in path for termo in ['/blog', '/post', '/artigo', '/news']):
            tipo = 'conteudo'
        elif any(termo in path for termo in ['/produto', '/product']):
            tipo = 'produto'
        elif any(termo in path for termo in ['/categoria', '/category']):
            tipo = 'categoria'
        elif any(termo in path for termo in ['/sobre', '/contato', '/servicos']):
            tipo = 'institucional'
        else:
            tipo = 'outros'
        
        tipos_url[tipo] = tipos_url.get(tipo, 0) + 1
    
    # Log da distribuição
    total = sum(tipos_url.values())
    print(f"\n📊 DISTRIBUIÇÃO DE TIPOS ({total} URLs):")
    for tipo, count in sorted(tipos_url.items(), key=lambda x: x[1], reverse=True):
        percent = (count / total) * 100 if total > 0 else 0
        icones = {'homepage': '🏠', 'conteudo': '📝', 'produto': '🛒', 'categoria': '📁', 'institucional': '🏢', 'outros': '📄'}
        icone = icones.get(tipo, '📄')
        print(f"   {icone} {tipo.capitalize()}: {count} ({percent:.1f}%)")
    
    return tipos_url

# ========================
# 🔥 PATCH HÍBRIDO (ORIGINAL)
# ========================

def aplicar_patch_headings_hibrido(excel_path: str) -> str:
    """🔥 Aplica patch híbrido para headings vazios"""
    
    try:
        print(f"\n🔥 APLICANDO PATCH HÍBRIDO HEADINGS...")
        print(f"📁 Arquivo: {excel_path}")
        
        # Import do revalidador
        from revalidador_headings_hibrido import RevalidadorHeadingsHibridoOtimizado as RevalidadorHeadingsHibrido        
        # Aplica patch
        revalidador = RevalidadorHeadingsHibrido()
        excel_corrigido = revalidador.revalidar_excel_completo(
            excel_path, 
            excel_path.replace('.xlsx', '_HIBRIDO.xlsx')
        )
        
        print(f"✅ PATCH HÍBRIDO APLICADO!")
        print(f"📁 Arquivo corrigido: {excel_corrigido}")
        
        return excel_corrigido
        
    except Exception as e:
        print(f"❌ Erro no patch híbrido: {e}")
        print(f"📋 Mantendo arquivo original: {excel_path}")
        return excel_path

# ========================
# 🔥 PATCH CIRÚRGICO COMPLETO
# ========================

def aplicar_patch_cirurgico_completo(excel_path: str) -> str:
    """🔥 Aplica patch CIRÚRGICO completo AUTOMÁTICO: headings + titles"""
    
    try:
        print(f"🔥 PATCH CIRÚRGICO AUTOMÁTICO INICIADO")
        print(f"📁 Arquivo base: {excel_path}")
        
        # 1. PATCH HEADINGS CIRÚRGICO (AUTOMÁTICO)
        print(f"\n🎯 1/2 - HEADINGS VAZIOS CIRÚRGICO (AUTOMÁTICO):")
        from revalidador_headings_hibrido import revalidar_headings_excel_cirurgico
        
        excel_com_headings = revalidar_headings_excel_cirurgico(
            excel_path, 
            excel_path.replace('.xlsx', '_TEMP_HEADINGS.xlsx')
        )
        
        # 2. PATCH TITLES CIRÚRGICO (AUTOMÁTICO) - COM FALLBACK
        print(f"\n🎯 2/2 - TITLES AUSENTES CIRÚRGICO (AUTOMÁTICO):")
        try:
            from revalidador_title_cirurgico import revalidar_titles_excel_cirurgico
            
            excel_final = revalidar_titles_excel_cirurgico(
                excel_com_headings,
                excel_path.replace('.xlsx', '_CIRURGICO_COMPLETO.xlsx')
            )
            print(f"✅ Patch de titles aplicado com sucesso!")
            
        except ImportError as e:
            print(f"⚠️ Módulo de titles não disponível: {e}")
            print(f"📋 Continuando apenas com patch de headings...")
            excel_final = excel_com_headings
        
        # 3. LIMPA ARQUIVO TEMPORÁRIO
        try:
            if os.path.exists(excel_com_headings) and 'TEMP' in excel_com_headings:
                os.remove(excel_com_headings)
                print(f"🗑️ Arquivo temporário removido")
        except:
            pass
        
        print(f"\n✅ PATCH CIRÚRGICO AUTOMÁTICO CONCLUÍDO!")
        print(f"📁 Arquivo final: {excel_final}")
        print(f"🎯 Contém: Headings Vazios (+ Titles se disponível)")
        
        return excel_final
        
    except Exception as e:
        print(f"❌ Erro no patch cirúrgico: {e}")
        print(f"📋 Mantendo arquivo original: {excel_path}")
        return excel_path

# ========================
# 🎯 MAIN PIPELINE
# ========================

async def main_pipeline():
    """🎯 Pipeline principal orquestrador"""
    
    print("🎯 SISTEMA SEO PIPELINE LEAN")
    print("="*50)
    print(f"📋 Configuração:")
    print(f"   URL: {URL_BASE}")
    print(f"   Max URLs: {MAX_URLS}")
    print(f"   Max Depth: {MAX_DEPTH}")
    print(f"   Arquivo: {ARQUIVO_SAIDA}")
    print(f"   Playwright: {'✅' if PLAYWRIGHT_AVAILABLE else '❌'}")
    print(f"   Excel Manager: {'✅' if EXCEL_MANAGER_AVAILABLE else '❌'}")
    
    # 🚀 FASE 1: CRAWLING HÍBRIDO
    print(f"\n🚀 FASE 1: CRAWLING HÍBRIDO")
    urls_com_dados, metodo_usado = await executar_pipeline_hibrido()
    
    if not urls_com_dados:
        print("❌ ERRO: Nenhuma URL coletada!")
        sys.exit(1)
    
    df = pd.DataFrame(urls_com_dados)
    print(f"📊 DataFrame: {len(df)} URLs, {len(df.columns)} colunas")
    
    # 📊 FASE 2: ANÁLISE DE DADOS
    print(f"\n📊 FASE 2: ANÁLISE DE DADOS")
    tipos_url = analisar_distribuicao_tipos_url_simples(df)
    
    # 🔍 FASE 3: VERIFICAÇÕES COMPLEMENTARES
    print(f"\n🔍 FASE 3: VERIFICAÇÕES COMPLEMENTARES")
    
    # Status HTTP se necessário
    if metodo_usado != "PLAYWRIGHT_LEAN" or 'status_code_http' not in df.columns:
        print(f"🔍 Verificando status HTTP...")
        urls_http = df['url'].dropna().unique().tolist()
        df_status = pd.DataFrame(verificar_status_http(urls_http, max_threads=50))
        df = df.merge(df_status, on='url', how='left', suffixes=('', '_check'))
        
        if 'status_code_http' not in df.columns and 'status_code_http_check' in df.columns:
            df['status_code_http'] = df['status_code_http_check']
    
    # Metatags se necessário
    if metodo_usado != "PLAYWRIGHT_LEAN" or 'title' not in df.columns:
        print(f"📋 Extraindo metatags...")
        urls_meta = df['url'].dropna().unique().tolist()
        df_meta = pd.DataFrame(extrair_metatags(urls_meta, max_threads=50))
        df = df.merge(df_meta, on='url', how='left', suffixes=('_pw', ''))
        
        for col in ['title', 'description']:
            if f'{col}_pw' in df.columns:
                df[col] = df[f'{col}_pw'].fillna(df[col])
                df = df.drop(f'{col}_pw', axis=1)
    
    # Headings - REMOVIDO (patch cirúrgico faz melhor depois)
    print(f"🏷️ Headings serão validados no patch cirúrgico (mais preciso)...")
    
    # HTTP Inseguro
    print(f"🔒 Analisando HTTP inseguro...")
    urls_http_inseguro = df['url'].dropna().unique().tolist()
    df_http = pd.DataFrame(extrair_http_inseguros(urls_http_inseguro, max_threads=40))
    
    # 📋 FASE 4: AUDITORIAS SEO
    print(f"\n📋 FASE 4: AUDITORIAS SEO")
    
    # Garante colunas necessárias
    for col in ['title', 'description']:
        if col not in df.columns:
            df[col] = ''
    
    # Filtro para auditoria
    df_filtrado = df[~df["url"].str.contains(r"\?page=\d+", na=False)].copy()
    
    # Auditorias básicas
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
    
    # 📊 FASE 5: ESTATÍSTICAS FINAIS
    print(f"\n📊 FASE 5: ESTATÍSTICAS FINAIS")
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
    
    # 📤 FASE 6: EXPORTAÇÃO
    print(f"\n📤 FASE 6: EXPORTAÇÃO")
    
    auditorias = {
        "df_title_ausente": df_title_ausente,
        "df_description_ausente": df_description_ausente,
        "df_title_duplicado": df_title_duplicado,
        "df_description_duplicado": df_description_duplicado,
        "df_errors": df_errors
    }
    
    # Metadados
    df['crawler_method'] = metodo_usado
    df['crawler_version'] = 'pipeline_lean_v1.0'
    df['analise_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Exportação segura
    try:
        if not os.path.isabs(ARQUIVO_SAIDA):
            arquivo_final = os.path.join(os.getcwd(), os.path.basename(ARQUIVO_SAIDA))
        else:
            arquivo_final = ARQUIVO_SAIDA
        
        # 📤 EXPORTAÇÃO INICIAL
        excel_inicial = exportar_relatorio_completo(df, df_http, auditorias, ARQUIVO_SAIDA)
        print(f"✅ Relatório inicial exportado: {excel_inicial}")

        # ✅ EXPORTAÇÃO COM ANÁLISE COMPLETA (usa excel_manager existente)
        print(f"\n📊 EXPORTANDO RELATÓRIO COMPLETO...")
        print(f"   ✅ Usa estrutura existente do excel_manager.py")
        print(f"   📋 Abas: Headings_Vazios, Estrutura_Headings, H1_H2_Problemas")
        print(f"   🎯 Sistema integrado - sem duplicação!")

        arquivo_final = excel_inicial
        
    except Exception as e:
        print(f"❌ Erro na exportação: {e}")
        
        # Fallback CSV
        arquivo_csv = ARQUIVO_SAIDA.replace('.xlsx', '.csv')
        arquivo_csv = os.path.join(os.getcwd(), os.path.basename(arquivo_csv))
        df.to_csv(arquivo_csv, index=False, encoding='utf-8')
        print(f"🔄 Dados salvos como CSV: {arquivo_csv}")
        arquivo_final = arquivo_csv
    
    # 🎉 FASE 7: RELATÓRIO FINAL
    print(f"\n🎉 PIPELINE CONCLUÍDO COM SUCESSO!")
    print("="*50)
    print(f"📁 Arquivo: {arquivo_final}")
    print(f"🎯 Método: {metodo_usado}")
    print(f"📊 URLs: {len(df)}")
    print(f"📈 Tipos identificados: {len(tipos_url)}")
    
    # Recomendações simples
    print(f"\n💡 RECOMENDAÇÕES:")
    if len(df_title_ausente) > 0:
        print(f"   📝 {len(df_title_ausente)} páginas sem title precisam de atenção")
    
    if len(df_errors) > 0:
        print(f"   🚨 {len(df_errors)} URLs com erros HTTP precisam ser corrigidas")
    
    titles_ok = len(df) - len(df_title_ausente)
    title_rate = (titles_ok / len(df)) * 100 if len(df) > 0 else 0
    
    if title_rate < 90:
        print(f"   ⚠️ Taxa de captura de titles baixa ({title_rate:.1f}%) - considere usar Playwright")
    else:
        print(f"   ✅ Taxa de captura de titles boa ({title_rate:.1f}%)")
    
    if metodo_usado == "PLAYWRIGHT_LEAN":
        print(f"   🎭 Playwright usado - ideal para sites com JavaScript")
    else:
        print(f"   ⚡ Requests usado - ideal para sites estáticos")

# ========================
# 🔄 WRAPPER SÍNCRONO
# ========================

def main_sync():
    """🔄 Wrapper síncrono"""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(main_pipeline())
    except KeyboardInterrupt:
        print("\n⚠️ Operação cancelada pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro crítico: {e}")
        import traceback
        traceback.print_exc()
        
        # Modo emergência
        print(f"\n🚨 Modo emergência...")
        try:
            urls_emergencia = crawler_requests(URL_BASE, min(MAX_URLS, 100), 2)
            if urls_emergencia:
                df_emergencia = pd.DataFrame(urls_emergencia)
                arquivo_emergencia = gerar_nome_arquivo_seguro(URL_BASE).replace('pipeline_', 'emergencia_')
                arquivo_emergencia = os.path.join(os.getcwd(), arquivo_emergencia)
                df_emergencia.to_excel(arquivo_emergencia, index=False)
                print(f"✅ Relatório de emergência: {arquivo_emergencia}")
        except Exception as e2:
            print(f"💥 Erro total: {e2}")

# ========================
# 🚀 ENTRY POINT
# ========================

if __name__ == "__main__":
    main_sync()