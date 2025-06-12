import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class StatusHTTPSheet(BaseSheetExporter):
    def export(self):
        if 'status_code_http' in self.df.columns:
            df_status = self.df[['url', 'status_code_http']].copy()
        else:
            df_status = pd.DataFrame(columns=['url', 'status_code_http'])
        df_status.to_excel(self.writer, index=False, sheet_name="Status_HTTP")
