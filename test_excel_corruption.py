# test_excel_corruption.py - Diagnóstico de Corrupção Excel

import pandas as pd
import os
import tempfile
from datetime import datetime

def test_excel_engines():
    """🔬 Testa diferentes engines do Excel para identificar problema"""
    
    print("🔬 DIAGNÓSTICO DE CORRUPÇÃO EXCEL")
    print("="*50)
    
    # Dados de teste simples
    df_test = pd.DataFrame({
        'url': ['https://example.com', 'https://test.com', 'https://sample.com'],
        'title': ['Example Title', 'Test Title', 'Sample Title'],
        'status_code': [200, 404, 200],
        'description': ['Example desc', 'Test desc', 'Sample desc']
    })
    
    engines = ['xlsxwriter', 'openpyxl']
    
    for engine in engines:
        print(f"\n🧪 Testando engine: {engine}")
        
        try:
            # Arquivo temporário único
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            test_file = f"test_excel_{engine}_{timestamp}.xlsx"
            
            # TESTE 1: Excel simples
            print(f"   📝 Teste 1: Excel básico...")
            with pd.ExcelWriter(test_file, engine=engine) as writer:
                df_test.to_excel(writer, sheet_name='Teste', index=False)
            
            # Valida arquivo
            if validate_excel_file(test_file):
                print(f"   ✅ Teste 1 PASSOU - {engine} funciona básico")
            else:
                print(f"   ❌ Teste 1 FALHOU - {engine} corrompeu arquivo básico")
                continue
            
            # TESTE 2: Múltiplas abas
            print(f"   📝 Teste 2: Múltiplas abas...")
            test_file2 = f"test_multi_{engine}_{timestamp}.xlsx"
            
            with pd.ExcelWriter(test_file2, engine=engine) as writer:
                df_test.to_excel(writer, sheet_name='Aba1', index=False)
                df_test.to_excel(writer, sheet_name='Aba2', index=False)
                df_test.to_excel(writer, sheet_name='Aba3', index=False)
            
            if validate_excel_file(test_file2):
                print(f"   ✅ Teste 2 PASSOU - {engine} funciona múltiplas abas")
            else:
                print(f"   ❌ Teste 2 FALHOU - {engine} corrompeu múltiplas abas")
                continue
            
            # TESTE 3: Dados problemáticos
            print(f"   📝 Teste 3: Dados problemáticos...")
            df_problematic = pd.DataFrame({
                'url': ['https://example.com'],
                'lista': [['item1', 'item2']],  # Lista - pode causar problema
                'dict': [{'key': 'value'}],     # Dict - pode causar problema
                'none_value': [None],           # None - pode causar problema
                'long_text': ['x' * 1000],      # Texto longo
                'special_chars': ['áéíóú@#$%&*()']  # Caracteres especiais
            })
            
            test_file3 = f"test_problem_{engine}_{timestamp}.xlsx"
            
            try:
                with pd.ExcelWriter(test_file3, engine=engine) as writer:
                    df_problematic.to_excel(writer, sheet_name='Problematic', index=False)
                
                if validate_excel_file(test_file3):
                    print(f"   ✅ Teste 3 PASSOU - {engine} lida com dados problemáticos")
                else:
                    print(f"   ⚠️ Teste 3 FALHOU - {engine} não lida com dados problemáticos")
                    
            except Exception as e:
                print(f"   ❌ Teste 3 ERRO - {engine} falhou: {e}")
            
            # Limpa arquivos de teste
            for test_f in [test_file, test_file2, test_file3]:
                if os.path.exists(test_f):
                    os.remove(test_f)
            
        except Exception as e:
            print(f"   💥 ERRO CRÍTICO com {engine}: {e}")

def validate_excel_file(filepath):
    """✅ Valida se arquivo Excel pode ser lido"""
    
    if not os.path.exists(filepath):
        return False
    
    try:
        # Testa com openpyxl
        with pd.ExcelFile(filepath, engine='openpyxl') as xls:
            sheet_names = xls.sheet_names
        
        # Testa leitura de dados
        df = pd.read_excel(filepath, engine='openpyxl')
        
        return True
        
    except Exception as e:
        print(f"      🔍 Erro na validação: {e}")
        return False

def test_real_data_simulation():
    """🎯 Simula dados reais do crawler para teste"""
    
    print(f"\n🎯 SIMULANDO DADOS REAIS DO CRAWLER")
    print("="*40)
    
    # Simula estrutura real do DataFrame do crawler
    df_real = pd.DataFrame({
        'url': [
            'https://gndisul.com.br',
            'https://gndisul.com.br/sobre',
            'https://gndisul.com.br/contato'
        ],
        'title': [
            'GNDI Sul - Medicina Diagnóstica',
            'Sobre a GNDI Sul',
            'Contato - GNDI Sul'
        ],
        'description': [
            'Centro de medicina diagnóstica com exames laboratoriais',
            'Conheça nossa história e missão',
            'Entre em contato conosco'
        ],
        'status_code_http': [200, 200, 200],
        'h1': [1, 1, 1],
        'h2': [3, 2, 1],
        'h1_texts': [
            ['GNDI Sul'],
            ['Sobre Nós'],
            ['Contato']
        ],
        'needs_javascript': [True, False, False],
        'crawler_method': ['PLAYWRIGHT_LEAN', 'PLAYWRIGHT_LEAN', 'PLAYWRIGHT_LEAN']
    })
    
    # Simula auditorias
    auditorias_real = {
        'df_title_ausente': pd.DataFrame({'url': []}),  # Vazio
        'df_description_ausente': pd.DataFrame({'url': []}),  # Vazio
        'df_title_duplicado': pd.DataFrame({'url': []}),  # Vazio
        'df_description_duplicado': pd.DataFrame({'url': []}),  # Vazio
        'df_errors': pd.DataFrame({'url': [], 'status_code': []})  # Vazio
    }
    
    # Simula HTTP inseguro
    df_http_real = pd.DataFrame({
        'url': [],
        'tipo': [],
        'trecho': []
    })
    
    # Testa exportação com dados reais
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    test_file = f"test_real_data_{timestamp}.xlsx"
    
    try:
        print(f"📝 Testando com estrutura real de dados...")
        
        with pd.ExcelWriter(test_file, engine='xlsxwriter') as writer:
            # Aba principal
            df_real.to_excel(writer, sheet_name='Dados_Principais', index=False)
            
            # Abas de auditoria
            for nome, df_aud in auditorias_real.items():
                nome_aba = nome.replace('df_', '').title()[:31]
                df_aud.to_excel(writer, sheet_name=nome_aba, index=False)
            
            # HTTP inseguro
            df_http_real.to_excel(writer, sheet_name='HTTP_Inseguro', index=False)
        
        if validate_excel_file(test_file):
            print(f"✅ SUCESSO - Dados reais exportados corretamente")
            print(f"📁 Arquivo: {test_file}")
            
            # Mostra informações do arquivo
            size = os.path.getsize(test_file)
            print(f"📊 Tamanho: {size} bytes")
            
            # Não remove o arquivo para análise
            return test_file
        else:
            print(f"❌ FALHA - Dados reais causaram corrupção")
            return None
            
    except Exception as e:
        print(f"💥 ERRO - Exceção ao processar dados reais: {e}")
        import traceback
        traceback.print_exc()
        return None

def diagnose_system():
    """🔍 Diagnóstica sistema e dependências"""
    
    print(f"\n🔍 DIAGNÓSTICO DO SISTEMA")
    print("="*30)
    
    try:
        import pandas as pd
        print(f"📊 Pandas: {pd.__version__}")
    except:
        print(f"❌ Pandas não disponível")
    
    try:
        import xlsxwriter
        print(f"📝 XlsxWriter: {xlsxwriter.__version__}")
    except:
        print(f"❌ XlsxWriter não disponível")
    
    try:
        import openpyxl
        print(f"📑 OpenPyXL: {openpyxl.__version__}")
    except:
        print(f"❌ OpenPyXL não disponível")
    
    # Testa permissões de escrita
    try:
        temp_file = f"test_permissions_{datetime.now().strftime('%H%M%S')}.txt"
        with open(temp_file, 'w') as f:
            f.write("test")
        os.remove(temp_file)
        print(f"✅ Permissões de escrita: OK")
    except Exception as e:
        print(f"❌ Problema de permissões: {e}")

if __name__ == "__main__":
    diagnose_system()
    test_excel_engines()
    test_file = test_real_data_simulation()
    
    print(f"\n🎯 PRÓXIMOS PASSOS:")
    print(f"1. Analise os resultados dos testes acima")
    print(f"2. Se algum teste passou, use esse engine")
    print(f"3. Se todos falharam, há problema no sistema/dependências")
    if test_file:
        print(f"4. Teste abrir o arquivo: {test_file}")
    print(f"5. Reporte os resultados para análise detalhada")