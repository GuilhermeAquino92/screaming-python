# excel_manager.py - ATUALIZADO COM MIXED CONTENT SHEET ENGINE

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
    """📊 Exporta relatório completo - TODAS AS ENGINES CIRÚRGICAS + MIXED CONTENT"""
    
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
        
        # 🔧 IMPORTS DIRETOS - TODAS AS ENGINES CIRÚRGICAS + MIXED CONTENT SHEET
        try:
            from exporters.sheets.resumo_sheet import ResumoSheet
            from exporters.sheets.status_http_sheet import StatusHTTPSheet
            from exporters.sheets.metatags_sheet import MetatagsSheet
            from exporters.sheets.headings_estrutura_sheet import HeadingsEstruturaSheet
            from exporters.sheets.headings_vazios_sheet import HeadingsVaziosSheet
            from exporters.sheets.h1_h2_problemas_sheet import H1H2ProblemasSheet
            from exporters.sheets.title_ausente_sheet import TitleAusenteSheet
            from exporters.sheets.description_ausente_sheet import DescriptionAusenteSheet
            from exporters.sheets.title_duplicado_sheet import TitleDuplicadoSheet
            from exporters.sheets.description_duplicado_sheet import DescriptionDuplicadoSheet
            from exporters.sheets.redirects_3xx_sheet import Redirects3xxSheet
            from exporters.sheets.errors_5xx_sheet import Errors5xxSheet
            from exporters.sheets.errors_4xx_sheet import Errors4xxSheet
            from exporters.sheets.errors_http_sheet import ErrorsHTTPSheet
            from exporters.sheets.ssl_problemas_sheet import SSLProblemasSheet
            from exporters.sheets.http_inseguro_sheet import HTTPInseguroSheet
            # 🔒 NOVA SHEET ENGINE - SIMPLES E LIMPA
            from exporters.sheets.mixed_content_sheet import MixedContentSheet
            EXPORTERS_AVAILABLE = True
            print("✅ Exportadores especializados disponíveis (TODAS AS ENGINES + MIXED CONTENT)")
        except ImportError as e:
            print(f"⚠️ Exportadores especializados não disponíveis: {e}")
            EXPORTERS_AVAILABLE = False
        
        print("📋 Iniciando exportação Excel - VERSÃO CIRÚRGICA COMPLETA + MIXED CONTENT...")
        
        # 🔒 CONTEXT MANAGER + TODAS AS ABAS DIRETAS
        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            
            # Configurações básicas
            if hasattr(writer, 'book'):
                writer.book.default_url_format = writer.book.add_format({'font_color': 'black', 'underline': False})
            
            if EXPORTERS_AVAILABLE:
                # 📊 TODAS AS ENGINES CIRÚRGICAS ATIVAS
                
                # 1. ABA RESUMO
                try:
                    ResumoSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Resumo' criada")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Resumo: {e}")
                    df_clean.to_excel(writer, sheet_name='Resumo', index=False)
                
                # 2. ABA STATUS HTTP CIRÚRGICA
                try:
                    StatusHTTPSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Status_HTTP' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Status_HTTP: {e}")
                    try:
                        StatusHTTPSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'status': []}).to_excel(writer, sheet_name='Status_HTTP', index=False)
                
                # 3. ABA METATAGS CIRÚRGICA
                try:
                    MetatagsSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Metatags' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Metatags: {e}")
                    try:
                        MetatagsSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'title': [], 'description': []}).to_excel(writer, sheet_name='Metatags', index=False)
                
                # 4. ABA ESTRUTURA HEADINGS CIRÚRGICA
                try:
                    HeadingsEstruturaSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Estrutura_Headings' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Estrutura_Headings: {e}")
                    try:
                        HeadingsEstruturaSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'estrutura': []}).to_excel(writer, sheet_name='Estrutura_Headings', index=False)
                
                # 5. ABA H1 H2 PROBLEMAS CIRÚRGICA
                try:
                    H1H2ProblemasSheet(df_clean, writer).export()
                    print("   ✅ Aba 'H1_H2_Problemas' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba H1_H2_Problemas: {e}")
                    try:
                        H1H2ProblemasSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'problema': []}).to_excel(writer, sheet_name='H1_H2_Problemas', index=False)
                
                # 6. ABA HEADINGS VAZIOS CIRÚRGICA
                try:
                    HeadingsVaziosSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Headings_Vazios' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Headings_Vazios: {e}")
                    try:
                        HeadingsVaziosSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'heading_vazio': []}).to_excel(writer, sheet_name='Headings_Vazios', index=False)
                
                # 7. ABA TITLE AUSENTE CIRÚRGICA
                try:
                    TitleAusenteSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Title_Ausente' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Title_Ausente: {e}")
                    try:
                        TitleAusenteSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'problema': []}).to_excel(writer, sheet_name='Title_Ausente', index=False)
                
                # 8. ABA DESCRIPTION AUSENTE CIRÚRGICA
                try:
                    DescriptionAusenteSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Description_Ausente' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Description_Ausente: {e}")
                    try:
                        DescriptionAusenteSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'problema': []}).to_excel(writer, sheet_name='Description_Ausente', index=False)
                
                # 9. ABA TITLE DUPLICADO CIRÚRGICA
                try:
                    TitleDuplicadoSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Title_Duplicado' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Title_Duplicado: {e}")
                    try:
                        TitleDuplicadoSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'title_duplicado': []}).to_excel(writer, sheet_name='Title_Duplicado', index=False)
                
                # 10. ABA DESCRIPTION DUPLICADO CIRÚRGICA
                try:
                    DescriptionDuplicadoSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Description_Duplicado' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Description_Duplicado: {e}")
                    try:
                        DescriptionDuplicadoSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'description_duplicado': []}).to_excel(writer, sheet_name='Description_Duplicado', index=False)
                
                # 11. ABA HTTP INSEGURO CIRÚRGICA
                try:
                    HTTPInseguroSheet(df_clean, writer).export()
                    print("   ✅ Aba 'HTTP_Inseguro' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba HTTP_Inseguro: {e}")
                    try:
                        HTTPInseguroSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'link_inseguro': []}).to_excel(writer, sheet_name='HTTP_Inseguro', index=False)
                
                # 12. ABA REDIRECTS 3XX CIRÚRGICA
                try:
                    Redirects3xxSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Redirects_3xx' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Redirects_3xx: {e}")
                    try:
                        Redirects3xxSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'status': [], 'redirect': []}).to_excel(writer, sheet_name='Redirects_3xx', index=False)
                
                # 13. ABA ERRORS 5XX CIRÚRGICA
                try:
                    Errors5xxSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Errors_5xx' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Errors_5xx: {e}")
                    try:
                        Errors5xxSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'status': [], 'erro': []}).to_excel(writer, sheet_name='Errors_5xx', index=False)
                
                # 14. ABA ERRORS 4XX CIRÚRGICA
                try:
                    Errors4xxSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Errors_4xx' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Errors_4xx: {e}")
                    try:
                        Errors4xxSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'status': [], 'erro': []}).to_excel(writer, sheet_name='Errors_4xx', index=False)
                
                # 15. ABA ERRORS HTTP CIRÚRGICA
                try:
                    ErrorsHTTPSheet(df_clean, writer).export()
                    print("   ✅ Aba 'Errors_HTTP' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Errors_HTTP: {e}")
                    try:
                        ErrorsHTTPSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'erro': []}).to_excel(writer, sheet_name='Errors_HTTP', index=False)
                
                # 16. ABA SSL PROBLEMAS CIRÚRGICA
                try:
                    SSLProblemasSheet(df_clean, writer).export()
                    print("   ✅ Aba 'SSL_Problemas' criada (CIRÚRGICA)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba SSL_Problemas: {e}")
                    try:
                        SSLProblemasSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'ssl_problema': []}).to_excel(writer, sheet_name='SSL_Problemas', index=False)
                
                # 🔒 17. ABA MIXED CONTENT - SHEET ENGINE SIMPLES
                try:
                    MixedContentSheet(df_clean, writer).export()
                    print("   🔒 Aba 'Mixed_Content' criada (SHEET ENGINE)")
                except Exception as e:
                    print(f"   ⚠️ Erro na aba Mixed_Content: {e}")
                    try:
                        MixedContentSheet(df_clean, writer)._criar_aba_vazia()
                    except:
                        pd.DataFrame({'url': [], 'mixed_content_status': [], 'issues': []}).to_excel(writer, sheet_name='Mixed_Content', index=False)
                
            else:
                # FALLBACK BÁSICO se engines não disponíveis
                print("🔄 Usando exportação básica (engines não disponíveis)")
                
                try:
                    df_clean.to_excel(writer, sheet_name='Dados_Completos', index=False)
                    print("   ✅ Aba 'Dados_Completos' criada")
                except Exception as e:
                    print(f"   ❌ Erro na criação básica: {e}")
                    raise
        
        # 📊 RELATÓRIO FINAL DETALHADO
        print(f"\n🎉 RELATÓRIO ENTERPRISE CONCLUÍDO!")
        print(f"📁 Arquivo: {output_path}")
        print(f"🔧 Engines ativas:")
        print(f"   1. Resumo (métricas gerais)")
        print(f"   2. Status_HTTP (verificação real)")
        print(f"   3. Metatags (análise SEO completa)")
        print(f"   4. Estrutura_Headings (H1 ausente, hierarquia)")
        print(f"   5. H1_H2_Problemas (duplicação entre páginas)")
        print(f"   6. Headings_Vazios (lixo estrutural)")
        print(f"   7. Title_Ausente (tag ausente/vazia)")
        print(f"   8. Description_Ausente (meta description ausente/vazia)")
        print(f"   9. Title_Duplicado (com separadores visuais)")
        print(f"   10. Description_Duplicado (com separadores visuais)")
        print(f"   11. HTTP_Inseguro (links HTTP em HTTPS)")
        print(f"   12. Redirects_3xx (301, 302, 307, 308)")
        print(f"   13. Errors_5xx (500, 502, 503, 504)")
        print(f"   14. Errors_4xx (400, 403, 404, 410)")
        print(f"   15. Errors_HTTP (timeouts, DNS, SSL)")
        print(f"   16. SSL_Problemas (certificados, chain, expiração)")
        print(f"   🔒 17. Mixed_Content (recursos HTTP em páginas HTTPS)")
        
        # 🔍 VALIDAÇÃO FINAL
        if os.path.exists(output_path):
            tamanho_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"📁 Arquivo criado ({tamanho_mb:.1f} MB)")
            
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
        
        # 🚨 FALLBACK CSV
        try:
            output_csv = output_path.replace('.xlsx', '.csv')
            df_simple = clean_dataframe_for_excel(df)
            df_simple.to_csv(output_csv, index=False, encoding='utf-8')
            print(f"🔄 Dados salvos como CSV: {output_csv}")
            return output_csv
        except Exception as e2:
            print(f"💥 Erro total na exportação: {e2}")
            raise e2