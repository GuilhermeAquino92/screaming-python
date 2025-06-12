# main.py - Atualizado para crawler otimizado

import pandas as pd
import os
from crawler import rastrear_profundo as crawler_requests
# from crawler_selenium import rastrear_selenium_profundo as crawler_selenium  # Mantenha se tiver
from status_checker import verificar_status_http
from metatags import extrair_metatags
from validador_headings import validar_headings
from http_inseguro import extrair_http_inseguros
from exporters.excel_manager import exportar_relatorio_completo
import warnings

warnings.filterwarnings("ignore")

# ====================
# CONFIGURAÇÕES
# ====================
url_inicial = "https://ccgsaude.com.br/"
MAX_URLS = 3000
MAX_WORKERS = 20  # Nova configuração para threading
MAX_DEPTH = 3     # Nova configuração para profundidade
USE_SELENIUM_SE_ERRO = True
FORCAR_REINDEXACAO = False
PASTA_SAIDA = "output"
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, "urls_com_status_novo.xlsx")
os.makedirs(PASTA_SAIDA, exist_ok=True)

# ====================
# CRAWLER OTIMIZADO
# ====================
print("📡 Iniciando rastreamento otimizado...")

try:
    urls_com_dados = crawler_requests(
        url_inicial,
        max_urls=MAX_URLS,
        max_depth=MAX_DEPTH,
        max_workers=MAX_WORKERS,
        forcar_reindexacao=FORCAR_REINDEXACAO
    )
    print("✅ Rastreamento com Requests otimizado concluído.")
except Exception as e:
    print(f"⚠️ Erro com Requests: {e}")
    if USE_SELENIUM_SE_ERRO:
        print("🔁 Tentando com Selenium...")
        try:
            # Importa selenium crawler se necessário
            from crawler_selenium import rastrear_selenium_profundo as crawler_selenium
            urls_com_dados = crawler_selenium(
                url_inicial,
                max_urls=MAX_URLS,
                forcar_reindexacao=FORCAR_REINDEXACAO
            )
            print("✅ Rastreamento com Selenium concluído.")
        except ImportError:
            print("❌ Crawler Selenium não disponível. Continuando com dados parciais do Requests...")
            urls_com_dados = []
        except Exception as selenium_error:
            print(f"❌ Erro também com Selenium: {selenium_error}")
            urls_com_dados = []
    else:
        print("❌ Crawler falhou. Criando lista vazia...")
        urls_com_dados = []

df = pd.DataFrame(urls_com_dados)

# ====================
# COMPATIBILIDADE - RENOMEAR COLUNAS
# ====================
# O crawler otimizado retorna nomes diferentes, vamos padronizar
if 'status_code' not in df.columns and 'status_code' in df.columns:
    df = df.rename(columns={'status_code': 'status_code'})

# Adicionar coluna 'status_code' se não existir (compatibilidade)
if 'status_code' not in df.columns:
    df['status_code'] = None

print(f"📊 DataFrame criado com {len(df)} linhas e colunas: {list(df.columns)}")

# ====================
# FILTRAR APENAS HTML
# ====================
EXTENSOES_INVALIDAS = [
    '.jpg', '.jpeg', '.png', '.gif', '.svg',
    '.webp', '.bmp', '.ico', '.pdf',
    '.doc', '.docx', '.xls', '.xlsx',
    '.zip', '.rar', '.7z', '.js', '.css',
    '.mp3', '.mp4', '.avi', '.mov'
]

print(f"📝 URLs antes do filtro: {len(df)}")
df = df[~df['url'].str.lower().str.contains('|'.join([f'{ext}$' for ext in EXTENSOES_INVALIDAS]), na=False)]
print(f"📝 URLs após filtro HTML: {len(df)}")

# ====================
# STATUS HTTP - SEMPRE EXECUTAR PARA AUDITORIA COMPLETA
# ====================
urls_http = df['url'].dropna().unique().tolist()

# SEMPRE verifica status HTTP para auditoria completa, mesmo que o crawler tenha dados
print(f"🔍 Verificando status HTTP em {len(urls_http)} URLs únicas (auditoria completa)...")
try:
    df_status = pd.DataFrame(verificar_status_http(urls_http, max_threads=50))
    
    # Se o crawler já tinha status, compara e mantém o mais confiável
    if 'status_code' in df.columns and not df['status_code'].isna().all():
        print("ℹ️ Comparando status do crawler com verificação independente...")
        df = df.merge(df_status, on='url', how='left', suffixes=('_crawler', '_check'))
        
        # Prioriza verificação independente para auditoria
        df['status_code_final'] = df['status_code_http'].fillna(df['status_code_crawler'])
        df['status_code_http'] = df['status_code_final']
        
        # Remove colunas temporárias
        df = df.drop(columns=['status_code_crawler', 'status_code_check', 'status_code_final'], errors='ignore')
    else:
        df = df.merge(df_status, on='url', how='left')
        
except Exception as e:
    print(f"⚠️ Erro na verificação de status HTTP: {e}")
    # Se falhar, usa dados do crawler se disponível
    if 'status_code' in df.columns:
        df['status_code_http'] = df['status_code']
    else:
        df['status_code_http'] = None

# ====================
# METATAGS - PROCESSA TODAS AS URLs (incluindo erros para auditoria SEO)
# ====================
urls_meta = df['url'].dropna().unique().tolist()
print(f"🔎 Extraindo metatags de {len(urls_meta)} URLs únicas (incluindo URLs com erro para auditoria completa)...")

if urls_meta:
    df_meta = pd.DataFrame(extrair_metatags(urls_meta, max_threads=50))
    df = df.merge(df_meta, on='url', how='left')
else:
    print("⚠️ Nenhuma URL encontrada para extração de metatags")
    # Adiciona colunas vazias para compatibilidade
    df['title'] = ''
    df['description'] = ''

# ====================
# HEADINGS - PROCESSA TODAS AS URLs (auditoria completa)
# ====================
urls_head = df['url'].dropna().unique().tolist()
print(f"🧠 Validando headings em {len(urls_head)} URLs únicas (auditoria completa incluindo erros)...")

if urls_head:
    df_head = pd.DataFrame(validar_headings(urls_head, max_threads=50))
    df = df.merge(df_head, on='url', how='left')
else:
    print("⚠️ Nenhuma URL encontrada para validação de headings")

# ====================
# HTTP INSEGURO - PROCESSA TODAS AS URLs (auditoria de segurança completa)
# ====================
urls_http_inseguro = df['url'].dropna().unique().tolist()
print(f"🚨 Buscando http:// inseguro em {len(urls_http_inseguro)} URLs únicas (auditoria de segurança completa)...")

if urls_http_inseguro:
    df_http = pd.DataFrame(extrair_http_inseguros(urls_http_inseguro, max_threads=40))
else:
    print("⚠️ Nenhuma URL encontrada para verificação de HTTP inseguro")
    df_http = pd.DataFrame(columns=['url'])  # DataFrame vazio

# ====================
# AUDITORIA: TITLES & DESCRIPTIONS - LÓGICA ORIGINAL PRESERVADA
# ====================
print("📋 Gerando abas de auditoria (lógica original preservada)...")

# Garante que as colunas existem
if 'title' not in df.columns:
    df['title'] = ''
if 'description' not in df.columns:
    df['description'] = ''

# FILTRO ORIGINAL - Remove paginação para auditoria
df_filtrado = df[~df["url"].str.contains(r"\?page=\d+", na=False)].copy()

# LÓGICA ORIGINAL - Title e Description ausentes
df_title_ausente = df_filtrado[df_filtrado["title"].str.strip() == ""].copy()
df_description_ausente = df_filtrado[df_filtrado["description"].str.strip() == ""].copy()

# LÓGICA ORIGINAL - Duplicados com value_counts()
title_count = df_filtrado["title"].dropna().str.strip()
desc_count = df_filtrado["description"].dropna().str.strip()

# Remove strings vazias ANTES do value_counts (lógica original)
title_count = title_count[title_count != ""]
desc_count = desc_count[desc_count != ""]

# Value counts para encontrar duplicados (lógica original)
titles_duplicados = title_count.value_counts()
descs_duplicados = desc_count.value_counts()

# Filtra apenas os que aparecem mais de 1 vez (lógica original)
titles_duplicados = titles_duplicados[titles_duplicados > 1].index.tolist()
descs_duplicados = descs_duplicados[descs_duplicados > 1].index.tolist()

# DataFrames de duplicados (lógica original)
df_title_duplicado = df_filtrado[df_filtrado["title"].isin(titles_duplicados)].copy()
df_description_duplicado = df_filtrado[df_filtrado["description"].isin(descs_duplicados)].copy()

# ====================
# ERROS HTTP - LÓGICA ORIGINAL PRESERVADA
# ====================
# LÓGICA ORIGINAL: startswith(('3', '4', '5'))
if 'status_code_http' in df.columns:
    df_errors = df[df['status_code_http'].astype(str).str.startswith(('3', '4', '5'))].copy()
    if not df_errors.empty:
        df_errors["tipo_erro"] = df_errors['status_code_http'].astype(str).str[0] + "xx"
else:
    print("⚠️ Coluna status_code_http não encontrada para análise de erros")
    df_errors = pd.DataFrame()

# ====================
# ESTATÍSTICAS FINAIS
# ====================
print("\n📊 ESTATÍSTICAS FINAIS - AUDITORIA SEO COMPLETA:")
print(f"📝 Total de URLs processadas: {len(df)}")

# Determina qual coluna de status usar
status_col = 'status_code_http' if 'status_code_http' in df.columns else 'status_code'

if status_col in df.columns and not df[status_col].isna().all():
    status_200 = len(df[df[status_col] == 200])
    status_3xx = len(df[df[status_col].astype(str).str.startswith('3')])
    status_4xx = len(df[df[status_col].astype(str).str.startswith('4')])
    status_5xx = len(df[df[status_col].astype(str).str.startswith('5')])
    
    print(f"✅ URLs com status 200: {status_200}")
    print(f"🔄 URLs com redirecionamento (3xx): {status_3xx}")
    print(f"❌ URLs com erro cliente (4xx): {status_4xx}")
    print(f"🚨 URLs com erro servidor (5xx): {status_5xx}")
    print(f"📊 Total de URLs com problemas: {len(df_errors)} (importante para SEO!)")
else:
    print("⚠️ Dados de status HTTP não disponíveis")

if 'response_time' in df.columns:
    tempo_medio = df['response_time'].mean()
    print(f"⚡ Tempo médio de resposta: {tempo_medio:.2f}ms")

print(f"📋 Title ausente: {len(df_title_ausente)} (crítico para SEO)")
print(f"📋 Description ausente: {len(df_description_ausente)} (importante para SEO)")
print(f"📋 Title duplicado: {len(df_title_duplicado)} (problema de SEO)")
print(f"📋 Description duplicado: {len(df_description_duplicado)} (problema de SEO)")

# ====================
# EXPORTAÇÃO - ESTRUTURA ORIGINAL PRESERVADA
# ====================

auditorias = {
    "df_title_ausente": df_title_ausente,
    "df_description_ausente": df_description_ausente,
    "df_title_duplicado": df_title_duplicado,
    "df_description_duplicado": df_description_duplicado,
    "df_errors": df_errors
}

exportar_relatorio_completo(df, df_http, auditorias, ARQUIVO_SAIDA)
