import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class AuditoriaSheets(BaseSheetExporter):
    def __init__(self, df, auditorias, writer):
        super().__init__(df, writer)
        self.auditorias = auditorias

    def export(self):
        sheets = [
            ("Title_Ausente", self.auditorias.get("df_title_ausente"), ["url", "status_code", "title"]),
            ("Description_Ausente", self.auditorias.get("df_description_ausente"), ["url", "status_code", "description"]),
            ("Title_Duplicado", self.auditorias.get("df_title_duplicado"), ["url", "status_code", "title"]),
            ("Description_Duplicado", self.auditorias.get("df_description_duplicado"), ["url", "status_code", "description"])
        ]

        for nome, df_aba, colunas_padrao in sheets:
            self._export_sheet(df_aba, nome, colunas_padrao)

    def _export_sheet(self, df_aba, nome_aba, colunas_padrao):
        if df_aba is not None and not df_aba.empty:
            colunas_existentes = [col for col in colunas_padrao if col in df_aba.columns]
            if "status_code" in colunas_padrao and "status_code" not in colunas_existentes and "status_code_http" in df_aba.columns:
                colunas_existentes.append("status_code_http")
            df_export = df_aba[colunas_existentes].copy()
        else:
            df_export = pd.DataFrame(columns=colunas_padrao)

        df_export.to_excel(self.writer, index=False, sheet_name=nome_aba)
