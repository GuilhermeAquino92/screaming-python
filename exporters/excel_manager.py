import os
import pandas as pd
from exporters.sheets.resumo_sheet import ResumoSheet
from exporters.sheets.status_http_sheet import StatusHTTPSheet
from exporters.sheets.metatags_sheet import MetatagsSheet
# from exporters.sheets.headings_sheet import HeadingsSheet  # 🗑️ REMOVIDA - funcionalidade substituída
from exporters.sheets.headings_estrutura_sheet import HeadingsEstruturaSheet  # 🆕 Aba estrutura de headings
from exporters.sheets.headings_vazios_sheet import HeadingsVaziosSheet  # 🆕 Versão limpa e profissional
from exporters.sheets.h1_h2_problemas_sheet import H1H2ProblemasSheet  # 🆕 Nova aba H1/H2 PRIORITÁRIA
from exporters.sheets.http_inseguro_sheet import HTTPInseguroSheet
from exporters.sheets.auditoria_sheets import AuditoriaSheets
from exporters.sheets.errors_sheet import ErrorsSheet

def exportar_relatorio_completo(df, df_http, auditorias, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    writer = pd.ExcelWriter(output_path, engine="xlsxwriter")
    writer.book.default_url_format = writer.book.add_format({'font_color': 'black', 'underline': False})

    # 📊 ABAS PRINCIPAIS (ordem estratégica para SEO)
    print("📋 Gerando abas do relatório SEO...")
    
    # 1. ABA RESUMO - Visão geral
    ResumoSheet(df, writer).export()
    print("   ✅ Aba 'Resumo' criada (visão geral)")
    
    # 2. ABA STATUS HTTP - Problemas técnicos críticos
    StatusHTTPSheet(df, writer).export()
    print("   ✅ Aba 'Status_HTTP' criada (problemas técnicos)")
    
    # 3. ABA METATAGS - SEO básico
    MetatagsSheet(df, writer).export()
    print("   ✅ Aba 'Metatags' criada (SEO básico)")
    
    # 🗑️ ABA HEADINGS ANTIGA REMOVIDA
    # HeadingsSheet(df, writer).export()  # REMOVIDA - substituída pelas específicas abaixo
    
    # 4. 🆕 ABA ESTRUTURA DE HEADINGS - MÁXIMA PRIORIDADE SEO
    HeadingsEstruturaSheet(df, writer).export()
    print("   ✅ Aba 'Estrutura_Headings' criada (PRIORIDADE MÁXIMA)")
    
    # 5. 🆕 ABA H1/H2 PROBLEMAS - Textos duplicados entre páginas
    H1H2ProblemasSheet(df, writer).export()
    print("   ✅ Aba 'H1_H2_Problemas' criada (textos duplicados)")
    
    # 6. 🆕 ABA HEADINGS VAZIOS - Análise técnica detalhada com CSS
    HeadingsVaziosSheet(df, writer, ordenacao_tipo='url_primeiro').export()
    print("   ✅ Aba 'Headings_Vazios' criada (análise técnica CSS)")
    
    # 7. ABA HTTP INSEGURO - Problemas de segurança
    HTTPInseguroSheet(df_http, writer).export()
    print("   ✅ Aba 'HTTP_Inseguro' criada (segurança)")
    
    # 8. ABAS DE AUDITORIA - Problemas específicos de SEO (com Title_Ausente otimizada)
    AuditoriaSheets(df, auditorias, writer).export()
    print("   ✅ Abas de Auditoria criadas:")
    
    # 9. ABA ERRORS - Erros HTTP detalhados
    ErrorsSheet(auditorias.get("df_errors", pd.DataFrame()), writer).export()
    print("   ✅ Aba 'Errors_HTTP' criada (detalhamento de erros)")

    writer.close()
    print(f"\n🎉 RELATÓRIO SEO COMPLETO EXPORTADO PARA: {output_path}")
    
    print(f"\n📋 ESTRUTURA OTIMIZADA DO RELATÓRIO:")
    print(f"   1️⃣ Resumo - Visão geral do site")
    print(f"   2️⃣ Status_HTTP - Problemas técnicos (404, 500, etc.)")
    print(f"   3️⃣ Metatags - Títulos e descrições")
    print(f"   4️⃣ Estrutura_Headings - PRIORIDADE MÁXIMA:")
    print(f"      🚫 H1 ausente (crítico)")
    print(f"      🔄 H1 duplicado = múltiplas tags <h1> na mesma página (crítico)")
    print(f"      📋 H2 ausente quando necessário (alto)")
    print(f"      🏗️ Hierarquia quebrada: H1→H3→H2, H1→H4, etc. (alto)")
    print(f"   5️⃣ H1_H2_Problemas - Textos iguais entre páginas:")
    print(f"      🔄 H1/H2 com mesmo TEXTO em páginas diferentes")
    print(f"      📊 URLs agrupadas por texto duplicado")
    print(f"   6️⃣ Headings_Vazios - Análise técnica avançada:")
    print(f"      🕳️ Detecção de headings vazios")
    print(f"      🎨 Detecção de CSS que oculta headings (display:none, etc.)")
    print(f"      📄 Análise detalhada de contexto e atributos")
    print(f"   7️⃣ HTTP_Inseguro - Links http:// em páginas https://")
    print(f"   8️⃣ Auditorias - OTIMIZADO:")
    print(f"      📋 Title_Ausente - Análise profissional com prioridades")
    print(f"      📋 Description_Ausente - Páginas sem meta description")
    print(f"      🔄 Title_Duplicado - Títulos iguais entre páginas")
    print(f"      🔄 Description_Duplicado - Descriptions iguais entre páginas")
    print(f"   9️⃣ Errors_HTTP - Detalhamento completo de erros")
    
    print(f"\n🗑️ OTIMIZAÇÃO REALIZADA:")
    print(f"   ❌ Aba 'Headings' antiga REMOVIDA (dados genéricos)")
    print(f"   ✅ Substituída por 3 abas especializadas e altamente funcionais")
    print(f"   🎯 Foco em problemas específicos e acionáveis para SEO")
    print(f"   📊 Melhor organização e usabilidade do relatório")