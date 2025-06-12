import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class HTTPInseguroSheet(BaseSheetExporter):
    def export(self):
        if not self.df.empty:
            self.df.to_excel(self.writer, index=False, sheet_name="http_inseguro")
        else:
            pd.DataFrame(columns=['url']).to_excel(self.writer, index=False, sheet_name="http_inseguro")
