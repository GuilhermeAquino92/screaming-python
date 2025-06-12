import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class HeadingsSheet(BaseSheetExporter):
    def export(self):
        heading_cols = ['url']
        for col in self.df.columns:
            if any(col.startswith(h) for h in ['h1_', 'h2_', 'h3_', 'h4_', 'h5_', 'h6_', 'heading_']):
                heading_cols.append(col)
        df_head = self.df[heading_cols].copy() if len(heading_cols) > 1 else pd.DataFrame(columns=heading_cols)
        df_head.to_excel(self.writer, index=False, sheet_name="Headings")
