import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class MetatagsSheet(BaseSheetExporter):
    def export(self):
        meta_cols = ['url']
        if 'title' in self.df.columns:
            meta_cols.append('title')
        if 'description' in self.df.columns:
            meta_cols.append('description')
        for col in self.df.columns:
            if col.startswith(('meta_', 'og_', 'twitter_')):
                meta_cols.append(col)
        df_meta = self.df[meta_cols].copy() if len(meta_cols) > 1 else pd.DataFrame(columns=meta_cols)
        df_meta.to_excel(self.writer, index=False, sheet_name="Metatags")
