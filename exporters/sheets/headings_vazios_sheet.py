import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class HeadingsVaziosSheet(BaseSheetExporter):
    def export(self):
        """🕳️ Gera aba DETALHADA de headings vazios com contexto do elemento pai"""
        try:
            rows = []
            
            for _, row in self.df.iterrows():
                url = row.get('url', '')
                
                # 🎯 USA DADOS COM CONTEXTO do validador_headings.py
                if 'headings_problematicos' in row and isinstance(row['headings_problematicos'], list):
                    problemas_list = row['headings_problematicos']
                    
                    for problema in problemas_list:
                        if isinstance(problema, dict):
                            motivos = problema.get('motivos', [])
                            
                            # Filtra apenas headings vazios
                            if any('vazio' in str(motivo).lower() or 'empty' in str(motivo).lower() for motivo in motivos):
                                rows.append({
                                    '🔗 URL': url,
                                    'Tag': problema.get('tag', '').upper(),
                                    'Elemento_Pai': problema.get('contexto_pai', 'Não informado'),
                                    'Contexto_Completo': problema.get('contexto_expandido', problema.get('contexto_pai', 'Não informado')),
                                    'Atributos_Heading': problema.get('atributos_heading', 'sem atributos'),
                                    '⚖️ Gravidade': problema.get('gravidade', 'MÉDIO'),
                                    'Descrição': problema.get('descricao_completa', problema.get('descricao', '')),
                                    'Posição': problema.get('posicao', 0),
                                    '🎯 Score_Página': row.get('metatags_score', 0)
                                })
                
                # Fallback: Se tem contagem mas não tem detalhes
                elif row.get('headings_vazios_count', 0) > 0:
                    vazios_count = row.get('headings_vazios_count', 0)
                    
                    rows.append({
                        '🔗 URL': url,
                        'Tag': 'MÚLTIPLOS',
                        'Elemento_Pai': 'Contexto não capturado',
                        'Contexto_Completo': f"{vazios_count} headings vazios detectados (sem contexto detalhado)",
                        'Atributos_Heading': 'diversos',
                        '⚖️ Gravidade': 'MÉDIO',
                        'Descrição': f"{vazios_count} headings vazios detectados",
                        'Posição': 0,
                        '🎯 Score_Página': row.get('metatags_score', 0)
                    })
            
            # Se não encontrou nenhum heading vazio
            if not rows:
                df_vazio = pd.DataFrame(columns=[
                    '🔗 URL', 
                    'Tag', 
                    'Elemento_Pai',
                    'Contexto_Completo',
                    'Atributos_Heading',
                    '⚖️ Gravidade', 
                    'Descrição',
                    'Posição',
                    '🎯 Score_Página'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
                print(f"ℹ️ Aba Headings_Vazios criada sem dados (nenhum heading vazio encontrado)")
                return df_vazio
            
            df_vazios = pd.DataFrame(rows)
            
            # Ordena por gravidade (CRÍTICO primeiro), depois por tag, depois por posição
            gravidade_order = {'CRÍTICO': 0, 'MÉDIO': 1}
            df_vazios['gravidade_sort'] = df_vazios['⚖️ Gravidade'].map(gravidade_order)
            df_vazios = df_vazios.sort_values(['gravidade_sort', 'Tag', 'Posição'], ascending=[True, True, True])
            df_vazios = df_vazios.drop('gravidade_sort', axis=1)
            
            df_vazios.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
            
            # Log para debug
            print(f"✅ Aba Headings_Vazios criada com {len(df_vazios)} entradas (incluindo contexto)")
            
            return df_vazios
            
        except Exception as e:
            print(f"⚠️ Erro gerando aba headings vazios: {e}")
            # Em caso de erro, cria DataFrame vazio
            df_vazio = pd.DataFrame(columns=[
                '🔗 URL', 
                'Tag', 
                'Elemento_Pai',
                'Contexto_Completo',
                'Atributos_Heading',
                '⚖️ Gravidade', 
                'Descrição',
                'Posição',
                '🎯 Score_Página'
            ])
            df_vazio.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
            return df_vazio