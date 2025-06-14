# debug_gndi_cache.py - Debugger específico para o cache do GNDI

import pandas as pd
import pickle
import os
from datetime import datetime

def find_gndi_cache():
    """🔍 Encontra cache específico do GNDI"""
    
    print("🔍 PROCURANDO CACHE DO GNDI")
    print("="*30)
    
    cache_files = []
    for file in os.listdir('.'):
        if file.startswith('.cache_') and file.endswith('.pkl'):
            cache_files.append(file)
    
    print(f"📂 Caches encontrados:")
    for i, cache_file in enumerate(cache_files):
        size = os.path.getsize(cache_file)
        modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
        print(f"   {i+1}. {cache_file} ({size:,} bytes, {modified})")
    
    # Procura especificamente pelo GNDI
    gndi_caches = [f for f in cache_files if 'gndi' in f.lower() or 'gndisul' in f.lower()]
    
    if gndi_caches:
        cache_to_use = gndi_caches[0]
        print(f"🎯 Cache GNDI encontrado: {cache_to_use}")
    else:
        # Se não encontrar, pergunta qual usar
        print(f"\n❓ Cache GNDI não encontrado. Qual cache você quer analisar?")
        
        if not cache_files:
            print("❌ Nenhum cache disponível")
            return None
        
        # Usa o maior cache (provavelmente tem mais dados)
        cache_to_use = max(cache_files, key=os.path.getsize)
        print(f"🔄 Usando o maior cache: {cache_to_use}")
    
    return cache_to_use

def debug_specific_cache(cache_file):
    """🔬 Debug detalhado do cache específico"""
    
    print(f"\n🔬 DEBUGANDO CACHE: {cache_file}")
    print("="*50)
    
    try:
        # Carrega dados
        with open(cache_file, 'rb') as f:
            crawler_data = pickle.load(f)
        
        print(f"📊 Registros carregados: {len(crawler_data)}")
        
        if not crawler_data:
            print("❌ Cache vazio")
            return
        
        # Analisa primeiro registro
        first_record = crawler_data[0]
        print(f"\n📋 ESTRUTURA DO PRIMEIRO REGISTRO:")
        
        if isinstance(first_record, dict):
            for key, value in first_record.items():
                value_type = type(value).__name__
                
                # Mostra valor truncado
                if isinstance(value, (list, dict)):
                    value_preview = f"{value_type} com {len(value)} itens"
                    if value:
                        value_preview += f" - exemplo: {str(value[0] if isinstance(value, list) else list(value.items())[0])[:100]}"
                elif isinstance(value, str):
                    value_preview = f"'{value[:100]}{'...' if len(value) > 100 else ''}'"
                else:
                    value_preview = str(value)
                
                print(f"   {key}: {value_type} = {value_preview}")
        
        # Converte para DataFrame
        df = pd.DataFrame(crawler_data)
        print(f"\n📊 DataFrame: {len(df)} linhas, {len(df.columns)} colunas")
        print(f"🏷️ Colunas: {list(df.columns)}")
        
        # Identifica colunas problemáticas
        problematic_columns = find_problematic_columns(df)
        
        # Testa exportação com limpeza
        if problematic_columns:
            test_with_data_cleaning(df, problematic_columns)
        else:
            test_full_export(df)
        
    except Exception as e:
        print(f"❌ Erro analisando cache: {e}")
        import traceback
        traceback.print_exc()

def find_problematic_columns(df):
    """🚨 Identifica colunas que podem causar problemas"""
    
    print(f"\n🚨 DETECTANDO COLUNAS PROBLEMÁTICAS")
    print("="*40)
    
    problematic = []
    
    for col in df.columns:
        issues = []
        
        # Verifica se é object (pode ter tipos complexos)
        if df[col].dtype == 'object':
            sample = df[col].dropna().head(5)
            
            for idx, value in sample.items():
                if isinstance(value, (list, dict, tuple)):
                    issues.append(f"Contém {type(value).__name__}")
                    break
                elif isinstance(value, str) and len(value) > 1000:
                    issues.append(f"String muito longa ({len(value)} chars)")
                    break
                elif isinstance(value, str):
                    # Verifica caracteres problemáticos
                    try:
                        value.encode('ascii')
                    except UnicodeEncodeError:
                        issues.append("Caracteres não-ASCII")
                        break
        
        if issues:
            problematic.append((col, issues))
            print(f"   ⚠️ {col}: {', '.join(issues)}")
        else:
            print(f"   ✅ {col}: OK")
    
    return problematic

def test_with_data_cleaning(df, problematic_columns):
    """🧪 Testa exportação com limpeza de dados"""
    
    print(f"\n🧪 TESTANDO COM LIMPEZA DE DADOS")
    print("="*40)
    
    df_clean = df.copy()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Aplica limpezas específicas
    for col, issues in problematic_columns:
        print(f"🔧 Limpando coluna: {col}")
        
        if df_clean[col].dtype == 'object':
            def clean_value(x):
                if x is None:
                    return ''
                elif isinstance(x, (list, dict, tuple)):
                    return str(x)[:500]  # Trunca representação string
                elif isinstance(x, str):
                    # Remove caracteres problemáticos e trunca
                    clean_str = x.encode('ascii', errors='ignore').decode('ascii')
                    return clean_str[:500]
                else:
                    return str(x)
            
            df_clean[col] = df_clean[col].apply(clean_value)
    
    # Testa exportação
    test_file = f"debug_cleaned_{timestamp}.xlsx"
    
    try:
        print(f"📝 Exportando dados limpos...")
        
        with pd.ExcelWriter(test_file, engine='xlsxwriter') as writer:
            df_clean.to_excel(writer, sheet_name='Dados_Limpos', index=False)
        
        # Valida arquivo
        if validate_excel_file(test_file):
            print(f"🎉 SUCESSO! Dados limpos exportados: {test_file}")
            
            # Mostra o que foi limpo
            print(f"\n📋 Limpezas aplicadas:")
            for col, issues in problematic_columns:
                print(f"   • {col}: {', '.join(issues)}")
            
            return test_file
        else:
            print(f"❌ Ainda há problemas após limpeza")
            
    except Exception as e:
        print(f"❌ Erro na exportação limpa: {e}")
        import traceback
        traceback.print_exc()
    
    return None

def test_full_export(df):
    """🧪 Testa exportação completa se não há problemas óbvios"""
    
    print(f"\n🧪 TESTANDO EXPORTAÇÃO COMPLETA")
    print("="*35)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    test_file = f"debug_full_export_{timestamp}.xlsx"
    
    try:
        with pd.ExcelWriter(test_file, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Dados_Completos', index=False)
        
        if validate_excel_file(test_file):
            print(f"✅ Exportação completa bem-sucedida: {test_file}")
        else:
            print(f"❌ Exportação completa falhou - arquivo corrompido")
            
            # Tenta com engine diferente
            test_file2 = f"debug_openpyxl_{timestamp}.xlsx"
            print(f"🔄 Tentando com OpenPyXL...")
            
            with pd.ExcelWriter(test_file2, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Dados_Completos', index=False)
            
            if validate_excel_file(test_file2):
                print(f"✅ OpenPyXL funcionou: {test_file2}")
            else:
                print(f"❌ OpenPyXL também falhou")
        
    except Exception as e:
        print(f"❌ Erro na exportação: {e}")
        import traceback
        traceback.print_exc()

def validate_excel_file(filepath):
    """✅ Valida arquivo Excel"""
    
    if not os.path.exists(filepath):
        return False
    
    try:
        with pd.ExcelFile(filepath, engine='openpyxl') as xls:
            sheet_names = xls.sheet_names
        return True
    except Exception:
        return False

def create_safe_exporter():
    """🛡️ Cria função de exportação segura baseada nos achados"""
    
    print(f"\n🛡️ CRIANDO EXPORTADOR SEGURO")
    print("="*35)
    
    safe_code = '''
def exportar_dados_seguros(df, output_path):
    """📊 Exportador seguro baseado no debug"""
    
    # Limpa dados problemáticos
    df_safe = df.copy()
    
    for col in df_safe.columns:
        if df_safe[col].dtype == 'object':
            def safe_clean(x):
                if x is None:
                    return ''
                elif isinstance(x, (list, dict, tuple)):
                    return str(x)[:300]  # Trunca objetos complexos
                elif isinstance(x, str):
                    # Remove caracteres não-ASCII e trunca
                    clean_str = x.encode('ascii', errors='ignore').decode('ascii')
                    return clean_str[:500]
                else:
                    return str(x)
            
            df_safe[col] = df_safe[col].apply(safe_clean)
    
    # Exporta com context manager
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        df_safe.to_excel(writer, sheet_name='Dados_Seguros', index=False)
    
    return output_path
'''
    
    # Salva função segura
    with open('exportador_seguro.py', 'w', encoding='utf-8') as f:
        f.write(safe_code)
    
    print(f"💾 Exportador seguro salvo: exportador_seguro.py")

if __name__ == "__main__":
    cache_file = find_gndi_cache()
    
    if cache_file:
        debug_specific_cache(cache_file)
        create_safe_exporter()
    else:
        print("❌ Nenhum cache encontrado para análise")
    
    print(f"\n🎯 PRÓXIMOS PASSOS:")
    print(f"1. Identifique as colunas problemáticas listadas acima")
    print(f"2. Use o exportador seguro gerado")
    print(f"3. Teste o arquivo Excel gerado")
    print(f"4. Se funcionar, integre a limpeza no crawler principal")