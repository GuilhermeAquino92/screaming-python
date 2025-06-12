import os
import pandas as pd
from exporters.sheets.resumo_sheet import ResumoSheet
from exporters.sheets.status_http_sheet import StatusHTTPSheet
from exporters.sheets.metatags_sheet import MetatagsSheet
from exporters.sheets.headings_sheet import HeadingsSheet
from exporters.sheets.http_inseguro_sheet import HTTPInseguroSheet
from exporters.sheets.auditoria_sheets import AuditoriaSheets
from exporters.sheets.errors_sheet import ErrorsSheet

def exportar_relatorio_completo(df, df_http, auditorias, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    writer = pd.ExcelWriter(output_path, engine="xlsxwriter")
    writer.book.default_url_format = writer.book.add_format({'font_color': 'black', 'underline': False})

    ResumoSheet(df, writer).export()
    StatusHTTPSheet(df, writer).export()
    MetatagsSheet(df, writer).export()
    HeadingsSheet(df, writer).export()
    HTTPInseguroSheet(df_http, writer).export()
    AuditoriaSheets(df, auditorias, writer).export()
    ErrorsSheet(auditorias.get("df_errors", pd.DataFrame()), writer).export()

    writer.close()
    print(f"✅ Relatório exportado para: {output_path}")
