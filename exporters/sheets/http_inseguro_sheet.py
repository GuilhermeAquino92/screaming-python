import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class HTTPInseguroSheet(BaseSheetExporter):
    def export(self):
        if not self.df.empty:
            # Renomeia colunas para match com sua imagem
            df_formatado = self.df.copy()
            
            # Mapeia as colunas originais para os nomes desejados
            mapeamento_colunas = {
                'url': 'URL',
                'tipo': 'Problema', 
                'trecho': 'Url Externa'
            }
            
            # Renomeia apenas as colunas que existem
            colunas_existentes = {k: v for k, v in mapeamento_colunas.items() if k in df_formatado.columns}
            df_formatado = df_formatado.rename(columns=colunas_existentes)
            
            # Reordena colunas na ordem desejada
            colunas_ordenadas = ['URL', 'Problema', 'Url Externa']
            colunas_finais = [col for col in colunas_ordenadas if col in df_formatado.columns]
            
            # Se houver outras colunas, adiciona no final
            outras_colunas = [col for col in df_formatado.columns if col not in colunas_finais]
            colunas_finais.extend(outras_colunas)
            
            df_formatado = df_formatado[colunas_finais]
            
            # Transforma 'tipo' em maiúscula para ficar como "HTTP"
            if 'Problema' in df_formatado.columns:
                df_formatado['Problema'] = df_formatado['Problema'].str.upper()
            
            df_formatado.to_excel(self.writer, index=False, sheet_name="HTTP_Inseguro")
        else:
            # DataFrame vazio com colunas formatadas
            df_vazio = pd.DataFrame(columns=['URL', 'Problema', 'Url Externa'])
            df_vazio.to_excel(self.writer, index=False, sheet_name="HTTP_Inseguro")