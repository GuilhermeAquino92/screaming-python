import os
import pandas as pd
from exporters.sheets.resumo_sheet import ResumoSheet
from exporters.sheets.status_http_sheet import StatusHTTPSheet
from exporters.sheets.metatags_sheet import MetatagsSheet
from exporters.sheets.headings_sheet import HeadingsSheet
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

    # Abas originais
    ResumoSheet(df, writer).export()
    StatusHTTPSheet(df, writer).export()
    MetatagsSheet(df, writer).export()
    HeadingsSheet(df, writer).export()
    
    # 🆕 NOVA ABA: Estrutura de Headings (MAIS PRIORITÁRIA - problemas estruturais críticos)
    HeadingsEstruturaSheet(df, writer).export()
    
    # 🆕 REESTRUTURADA: H1 e H2 Textos Duplicados entre páginas (agora é sobre textos iguais)
    H1H2ProblemasSheet(df, writer).export()
    
    # 🆕 NOVA ABA: Headings Vazios com Ordenação por URL (análise técnica detalhada)
    HeadingsVaziosSheet(df, writer, ordenacao_tipo='url_primeiro').export()
    
    # Abas restantes
    HTTPInseguroSheet(df_http, writer).export()
    AuditoriaSheets(df, auditorias, writer).export()
    ErrorsSheet(auditorias.get("df_errors", pd.DataFrame()), writer).export()

    writer.close()
    print(f"✅ Relatório exportado para: {output_path}")
    print(f"🆕 Aba 'Estrutura_Headings' criada (5ª aba - MÁXIMA PRIORIDADE):")
    print(f"   - H1 ausente (crítico)")
    print(f"   - H1 duplicado = múltiplas tags <h1> na mesma página (crítico)")
    print(f"   - H2 ausente quando necessário (alto)")
    print(f"   - Hierarquia quebrada: H1→H3→H2, H1→H4, etc. (alto)")
    print(f"🆕 Aba 'H1_H2_Problemas' reestruturada (6ª aba):")
    print(f"   - H1/H2 com mesmo TEXTO em páginas diferentes")
    print(f"   - URLs agrupadas por texto duplicado")
    print(f"🆕 Aba 'Headings_Vazios' (7ª aba - análise técnica):")
    print(f"   - Detecção de CSS que oculta headings")
    print(f"   - Análise detalhada de contexto e atributos")