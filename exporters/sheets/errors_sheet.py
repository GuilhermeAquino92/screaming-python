import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class ErrorsSheet(BaseSheetExporter):
    def export(self):
        if not self.df.empty:
            df_errors = self.df.copy()
        else:
            df_errors = pd.DataFrame(columns=["url", "status_code_http", "tipo_erro"])

        df_errors.to_excel(self.writer, index=False, sheet_name="Errors_HTTP")
