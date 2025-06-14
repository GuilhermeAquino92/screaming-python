# excel_manager.py - CORREÇÃO para Pipeline Híbrido

import os
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def safe_clean_value(x):
    """🛡️ Limpa valores individuais - VERSÃO CORRIGIDA"""
    
    # Primeiro, verifica se é um array numpy ou similar
    if hasattr(x, '__len__') and hasattr(x, 'dtype'):
        # É um array numpy ou similar
        if hasattr(x, 'size') and x.size == 0:
            # Array vazio
            return ''
        elif isinstance(x, np.ndarray):
            # Array não vazio - converte para string
            str_repr = str(x.tolist() if x.size <= 10 else f"{x.tolist()[:10]}...")
            return str_repr[:300] + '...' if len(str_repr) > 300 else str_repr
    
    # Verificação para None - deve vir antes de pd.isna para arrays
    if x is None:
        return ''
    
    # Verificação para pandas NA - com tratamento de array
    try:
        if pd.isna(x):
            return ''
    except ValueError:
        # Se pd.isna falhar (como com arrays), trata como objeto complexo
        pass
    
    # Trata objetos complexos
    if isinstance(x, (list, dict, tuple, set)):
        # Converte objetos complexos para string truncada
        str_repr = str(x)
        return str_repr[:300] + '...' if len(str_repr) > 300 else str_repr
    elif isinstance(x, str):
        # Limpa strings problemáticas
        try:
            # Remove caracteres de controle
            clean_str = ''.join(char for char in x if ord(char) >= 32 or char in '\t\n\r')
            # Trunca se muito longo
            return clean_str[:500] + '...' if len(clean_str) > 500 else clean_str
        except:
            return str(x)[:500]
    else:
        # Para qualquer outro tipo, converte para string
        try:
            return str(x)
        except:
            return ''

def clean_dataframe_for_excel(df):
    """🧹 Limpa DataFrame para exportação segura no Excel"""
    
    df_clean = df.copy()
    logger.info(f"🧹 Limpando DataFrame: {len(df_clean)} linhas, {len(df_clean.columns)} colunas")
    
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            
            # Aplica limpeza
            original_sample = df_clean[col].dropna().head(1)
            if not original_sample.empty:
                sample_value = original_sample.iloc[0]
                if isinstance(sample_value, (list, dict, tuple, set)):
                    logger.warning(f"⚠️ Coluna {col} contém objetos complexos - convertendo para string")
                
            df_clean[col] = df_clean[col].apply(safe_clean_value)
    
    return df_clean

def exportar_relatorio_completo(df, df_http, auditorias, output_path):
    """📊 Exporta relatório completo com MÁXIMA SEGURANÇA contra corrupção"""
    
    # 🛡️ VALIDAÇÃO ROBUSTA DO CAMINHO
    try:
        if not output_path or output_path.strip() == '':
            raise ValueError("Caminho do arquivo não pode ser vazio")
        
        if not os.path.isabs(output_path):
            output_path = os.path.abspath(output_path)
        
        diretorio_pai = os.path.dirname(output_path)
        
        if diretorio_pai and diretorio_pai != '' and not os.path.exists(diretorio_pai):
            os.makedirs(diretorio_pai, exist_ok=True)
            print(f"📁 Diretório criado: {diretorio_pai}")
        
        if not diretorio_pai or diretorio_pai == '':
            output_path = os.path.join(os.getcwd(), os.path.basename(output_path))
            print(f"💾 Usando diretório atual: {output_path}")
        
    except Exception as e:
        print(f"⚠️ Erro na validação do caminho: {e}")
        nome_arquivo = os.path.basename(output_path) if output_path else "relatorio_seo.xlsx"
        output_path = os.path.join(os.getcwd(), nome_arquivo)
        print(f"🔄 Fallback para: {output_path}")
    
    try:
        # 🧹 LIMPEZA CRÍTICA DOS DADOS
        print("🧹 Limpando dados para exportação segura...")
        df_clean = clean_dataframe_for_excel(df)
        df_http_clean = clean_dataframe_for_excel(df_http) if not df_http.empty else df_http
        
        # Limpa auditorias
        auditorias_clean = {}
        for nome, df_aud in auditorias.items():
            if df_aud is not None and not df_aud.empty:
                auditorias_clean[nome] = clean_dataframe_for_excel(df_aud)
            else:
                auditorias_clean[nome] = df_aud
        
        print("✅ Limpeza de dados concluída")
        
        # 🔧 TENTA IMPORTAR EXPORTADORES ESPECIALIZADOS
        try:
            from exporters.sheets.resumo_sheet import ResumoSheet
            from exporters.sheets.status_http_sheet import StatusHTTPSheet
            from exporters.sheets.metatags_sheet import MetatagsSheet
            from exporters.sheets.headings_estrutura_sheet import HeadingsEstruturaSheet
            from exporters.sheets.headings_vazios_sheet import HeadingsVaziosSheet
            from exporters.sheets.h1_h2_problemas_sheet import H1H2ProblemasSheet
            from exporters.sheets.http_inseguro_sheet import HTTPInseguroSheet
            from exporters.sheets.auditoria_sheets import AuditoriaSheets
            from exporters.sheets.errors_sheet import ErrorsSheet
            EXPORTERS_AVAILABLE = True
            print("✅ Exportadores especializados disponíveis")
        except ImportError as e:
            print(f"⚠️ Exportadores especializados não disponíveis: {e}")
            EXPORTERS_AVAILABLE = False
        
        print("📋 Iniciando exportação Excel com dados limpos...")
        
        # 🔒 CONTEXT MANAGER OBRIGATÓRIO + DADOS LIMPOS
        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            
            # Configurações básicas
            if hasattr(writer, 'book'):
                writer.book.default_url_format = writer.book.add_format({'font_color': 'black', 'underline': False})
            
            if EXPORTERS_AVAILABLE:
                # 📊 VERSÃO COMPLETA COM DADOS LIMPOS
                
                # 1. ABA RESUMO
                try:
                    ResumoSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Resumo' criada")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Resumo, usando fallback: {e}")
                    df_clean.to_excel(writer, sheet_name='Resumo', index=False)
                
                # 2. ABA STATUS HTTP
                try:
                    StatusHTTPSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Status_HTTP' criada")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Status_HTTP, usando fallback: {e}")
                    basic_cols = [col for col in ['url', 'status_code_http', 'status_code'] if col in df_clean.columns]
                    if basic_cols:
                        df_clean[basic_cols].to_excel(writer, sheet_name='Status_HTTP', index=False)
                
                # 3. ABA METATAGS
                try:
                    MetatagsSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Metatags' criada")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Metatags, usando fallback: {e}")
                    meta_cols = [col for col in ['url', 'title', 'description'] if col in df_clean.columns]
                    if meta_cols:
                        df_clean[meta_cols].to_excel(writer, sheet_name='Metatags', index=False)
                
                # 4-6. ABAS DE HEADINGS (com fallback individual) - RESTAURADAS!
                heading_sheets = [
                    ('HeadingsEstruturaSheet', 'Estrutura_Headings'),
                    ('H1H2ProblemasSheet', 'H1_H2_Problemas'),
                    ('HeadingsVaziosSheet', 'Headings_Vazios')
                ]
                
                for sheet_class, sheet_name in heading_sheets:
                    try:
                        if sheet_class == 'HeadingsEstruturaSheet':
                            HeadingsEstruturaSheet(df_clean, writer).export()
                        elif sheet_class == 'H1H2ProblemasSheet':
                            H1H2ProblemasSheet(df_clean, writer).export()
                        elif sheet_class == 'HeadingsVaziosSheet':
                            HeadingsVaziosSheet(df_clean, writer, ordenacao_tipo='url_primeiro').export()
                        print(f"   ✅ Aba '{sheet_name}' criada")
                    except Exception as e:
                        print(f"   ⚠️ Erro na aba {sheet_name}, criando vazia: {e}")
                        pd.DataFrame({'url': [], 'problema': []}).to_excel(writer, sheet_name=sheet_name[:31], index=False)
                
                # 7. ABA HTTP INSEGURO - RESTAURADA!
                try:
                    HTTPInseguroSheet(df_http_clean, writer).export()
                    print("   ✅ Aba 'HTTP_Inseguro' criada")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba HTTP_Inseguro, usando fallback: {e}")
                    if not df_http_clean.empty:
                        df_http_clean.to_excel(writer, sheet_name='HTTP_Inseguro', index=False)
                    else:
                        pd.DataFrame({'url': [], 'problema': []}).to_excel(writer, sheet_name='HTTP_Inseguro', index=False)
                
                # 8. ABAS DE AUDITORIA
                try:
                    AuditoriaSheets(df_clean, auditorias_clean, writer).export()
                    print("   ✅ Abas de Auditoria criadas")
                except Exception as e:
                    print(f"   ⚠️ Erro nas abas de Auditoria, usando fallback: {e}")
                    # Fallback manual para auditorias
                    for nome, df_aud in auditorias_clean.items():
                        try:
                            if df_aud is not None and not df_aud.empty:
                                nome_aba = nome.replace('df_', '').replace('_', ' ').title()[:31]
                                df_aud.to_excel(writer, sheet_name=nome_aba, index=False)
                            else:
                                nome_aba = nome.replace('df_', '').replace('_', ' ').title()[:31]
                                pd.DataFrame({'url': []}).to_excel(writer, sheet_name=nome_aba, index=False)
                        except Exception as e_aud:
                            print(f"     ⚠️ Erro na auditoria {nome}: {e_aud}")
                
                # 9. ABA ERRORS
                try:
                    ErrorsSheet(auditorias_clean.get("df_errors", pd.DataFrame()), writer).export()
                    print("   ✅ Aba 'Errors_HTTP' criada")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Errors_HTTP, usando fallback: {e}")
                    if "df_errors" in auditorias_clean and not auditorias_clean["df_errors"].empty:
                        auditorias_clean["df_errors"].to_excel(writer, sheet_name='Errors_HTTP', index=False)
                    else:
                        pd.DataFrame({'url': [], 'erro': []}).to_excel(writer, sheet_name='Errors_HTTP', index=False)
            
            else:
                # 📋 VERSÃO BÁSICA COM DADOS LIMPOS
                print("   🔄 Usando exportação básica com dados limpos...")
                
                # Aba principal
                df_clean.to_excel(writer, sheet_name='Dados_Completos', index=False)
                print("   ✅ Aba 'Dados_Completos' criada")
                
                # Abas de auditoria básicas
                for nome, df_aud in auditorias_clean.items():
                    try:
                        if df_aud is not None and not df_aud.empty:
                            nome_aba = nome.replace('df_', '').replace('_', ' ').title()[:31]
                            df_aud.to_excel(writer, sheet_name=nome_aba, index=False)
                            print(f"   ✅ Aba '{nome_aba}' criada")
                        else:
                            nome_aba = nome.replace('df_', '').replace('_', ' ').title()[:31]
                            pd.DataFrame({'url': []}).to_excel(writer, sheet_name=nome_aba, index=False)
                    except Exception as e_basic:
                        print(f"   ⚠️ Erro na aba básica {nome}: {e_basic}")
                
                # HTTP Inseguro básico
                try:
                    if not df_http_clean.empty:
                        df_http_clean.to_excel(writer, sheet_name='HTTP_Inseguro', index=False)
                        print("   ✅ Aba 'HTTP_Inseguro' criada")
                    else:
                        pd.DataFrame({'url': []}).to_excel(writer, sheet_name='HTTP_Inseguro', index=False)
                except Exception as e_http:
                    print(f"   ⚠️ Erro na aba HTTP_Inseguro: {e_http}")
            
            # Context manager fecha automaticamente
        
        print(f"\n🎉 RELATÓRIO SEO EXPORTADO COM DADOS LIMPOS: {output_path}")
        
        # 🔍 VALIDAÇÃO FINAL
        if os.path.exists(output_path):
            tamanho_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"📁 Arquivo criado com sucesso ({tamanho_mb:.1f} MB)")
            
            # Validação adicional
            try:
                with pd.ExcelFile(output_path, engine='openpyxl') as test_file:
                    num_sheets = len(test_file.sheet_names)
                    print(f"✅ Arquivo validado: {num_sheets} abas criadas")
                    print(f"📋 Abas: {', '.join(test_file.sheet_names)}")
            except Exception as validation_error:
                print(f"⚠️ Aviso na validação: {validation_error}")
        else:
            print(f"❌ Arquivo não foi criado: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Erro na criação do Excel: {e}")
        import traceback
        traceback.print_exc()
        
        # 🚨 FALLBACK EXTREMO - CSV
        try:
            output_csv = output_path.replace('.xlsx', '.csv')
            df_simple = clean_dataframe_for_excel(df)
            df_simple.to_csv(output_csv, index=False, encoding='utf-8')
            print(f"🔄 Dados salvos como CSV: {output_csv}")
            return output_csv
        except Exception as e2:
            print(f"💥 Erro total na exportação: {e2}")
            raise e2