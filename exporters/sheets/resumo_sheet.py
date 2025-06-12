from exporters.base_exporter import BaseSheetExporter

class ResumoSheet(BaseSheetExporter):
    def export(self):
        self.df.to_excel(self.writer, index=False, sheet_name="Resumo")
