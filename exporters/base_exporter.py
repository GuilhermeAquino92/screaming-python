import pandas as pd

class BaseSheetExporter:
    def __init__(self, df: pd.DataFrame, writer):
        self.df = df
        self.writer = writer

    def export(self):
        raise NotImplementedError("Subclasse deve implementar export()")
