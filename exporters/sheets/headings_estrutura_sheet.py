# exporters/sheets/headings_estrutura_sheet.py

import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class HeadingsEstruturaSheet(BaseSheetExporter):
    def export(self):
        """🏗️ Gera aba específica para problemas ESTRUTURAIS de headings"""
        try:
            rows = []
            
            # 🚫 FILTRO: Remove paginações e parâmetros desnecessários
            df_filtrado = self.df[~self.df["url"].str.contains(r"\?page=\d+", na=False)].copy()
            df_filtrado = df_filtrado[~df_filtrado["url"].str.contains(r"&page=\d+", na=False)]
            
            for _, row in df_filtrado.iterrows():
                url = row.get('url', '')
                
                # 🔍 EXTRAI DADOS DE HEADINGS
                h1_count = row.get('h1', 0)
                h2_count = row.get('h2', 0)
                h3_count = row.get('h3', 0)
                h4_count = row.get('h4', 0)
                h5_count = row.get('h5', 0)
                h6_count = row.get('h6', 0)
                
                # 🚨 PROBLEMA 1: H1 AUSENTE
                if h1_count == 0:
                    rows.append({
                        'URL': url,
                        'Tipo_Problema': 'H1_AUSENTE',
                        'Problema_Detectado': 'Página sem H1',
                        'Detalhes': 'Página não possui tag H1, fundamental para SEO',
                        'Gravidade': 'CRITICO',
                        'Impacto_SEO': 'CRITICO - H1 é essencial para estrutura e ranking',
                        'Recomendacao': 'Adicionar uma única tag H1 com o título principal da página'
                    })
                
                # 🚨 PROBLEMA 2: H1 DUPLICADO (múltiplas tags H1 na mesma página)
                elif h1_count > 1:
                    rows.append({
                        'URL': url,
                        'Tipo_Problema': 'H1_DUPLICADO',
                        'Problema_Detectado': f'{h1_count} tags H1 na mesma página',
                        'Detalhes': f'Encontradas {h1_count} tags H1. Deve haver apenas 1 H1 por página',
                        'Gravidade': 'CRITICO',
                        'Impacto_SEO': 'CRITICO - Múltiplos H1 confundem mecanismos de busca',
                        'Recomendacao': f'Manter apenas 1 H1 e converter os outros {h1_count-1} para H2 ou H3'
                    })
                
                # 🚨 PROBLEMA 3: H2 AUSENTE (se tem H1 mas não tem H2)
                if h1_count > 0 and h2_count == 0 and (h3_count > 0 or h4_count > 0 or h5_count > 0 or h6_count > 0):
                    rows.append({
                        'URL': url,
                        'Tipo_Problema': 'H2_AUSENTE',
                        'Problema_Detectado': 'H2 ausente mas possui H3/H4/H5/H6',
                        'Detalhes': f'Tem H1 ({h1_count}) mas pula para H3/H4/H5/H6 sem H2',
                        'Gravidade': 'ALTO',
                        'Impacto_SEO': 'ALTO - Hierarquia quebrada prejudica estrutura do conteúdo',
                        'Recomendacao': 'Adicionar H2 entre H1 e os headings de nível inferior'
                    })
                
                # 🚨 PROBLEMA 4: HIERARQUIA QUEBRADA
                hierarquia_problemas = self._detectar_hierarquia_quebrada(h1_count, h2_count, h3_count, h4_count, h5_count, h6_count)
                for problema_hierarquia in hierarquia_problemas:
                    rows.append({
                        'URL': url,
                        'Tipo_Problema': 'HIERARQUIA_QUEBRADA',
                        'Problema_Detectado': problema_hierarquia['problema'],
                        'Detalhes': problema_hierarquia['detalhes'],
                        'Gravidade': problema_hierarquia['gravidade'],
                        'Impacto_SEO': problema_hierarquia['impacto'],
                        'Recomendacao': problema_hierarquia['recomendacao']
                    })
            
            # Se não encontrou nenhum problema
            if not rows:
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tipo_Problema', 'Problema_Detectado', 'Detalhes', 
                    'Gravidade', 'Impacto_SEO', 'Recomendacao'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Estrutura_Headings")
                print(f"✅ Aba Estrutura_Headings criada sem dados (estrutura perfeita!)")
                return df_vazio
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO POR CRITICIDADE
            tipo_order = {
                'H1_AUSENTE': 0,        # Mais crítico
                'H1_DUPLICADO': 1,      # Muito crítico
                'H2_AUSENTE': 2,        # Crítico
                'HIERARQUIA_QUEBRADA': 3 # Importante
            }
            gravidade_order = {'CRITICO': 0, 'ALTO': 1, 'MEDIO': 2, 'BAIXO': 3}
            
            df_problemas['tipo_sort'] = df_problemas['Tipo_Problema'].map(tipo_order)
            df_problemas['gravidade_sort'] = df_problemas['Gravidade'].map(gravidade_order)
            
            # Ordena por: criticidade → gravidade → URL
            df_problemas = df_problemas.sort_values([
                'tipo_sort',      # 1. Tipo de problema (mais crítico primeiro)
                'gravidade_sort', # 2. Gravidade
                'URL'             # 3. URLs alfabéticas
            ], ascending=[True, True, True])
            
            # Remove colunas auxiliares
            df_problemas = df_problemas.drop(['tipo_sort', 'gravidade_sort'], axis=1, errors='ignore')
            
            df_problemas.to_excel(self.writer, index=False, sheet_name="Estrutura_Headings")
            
            # 📊 ESTATÍSTICAS
            stats = df_problemas['Tipo_Problema'].value_counts()
            h1_ausente = stats.get('H1_AUSENTE', 0)
            h1_duplicado = stats.get('H1_DUPLICADO', 0)
            h2_ausente = stats.get('H2_AUSENTE', 0)
            hierarquia_quebrada = stats.get('HIERARQUIA_QUEBRADA', 0)
            
            print(f"✅ Aba Estrutura_Headings criada com {len(df_problemas)} problemas estruturais:")
            print(f"   🚫 H1 ausente: {h1_ausente} páginas")
            print(f"   🔄 H1 duplicado (múltiplos H1): {h1_duplicado} páginas")
            print(f"   📋 H2 ausente: {h2_ausente} páginas")
            print(f"   🏗️ Hierarquia quebrada: {hierarquia_quebrada} páginas")
            print(f"   🎯 Foco em problemas estruturais críticos para SEO")
            
            return df_problemas
            
        except Exception as e:
            print(f"⚠️ Erro gerando aba estrutura headings: {e}")
            # Em caso de erro, cria DataFrame vazio
            df_vazio = pd.DataFrame(columns=[
                'URL', 'Tipo_Problema', 'Problema_Detectado', 'Detalhes',
                'Gravidade', 'Impacto_SEO', 'Recomendacao'
            ])
            df_vazio.to_excel(self.writer, index=False, sheet_name="Estrutura_Headings")
            return df_vazio
    
    def _detectar_hierarquia_quebrada(self, h1, h2, h3, h4, h5, h6):
        """🔍 Detecta problemas na hierarquia de headings"""
        problemas = []
        
        # Lista com contagens [H1, H2, H3, H4, H5, H6]
        counts = [h1, h2, h3, h4, h5, h6]
        
        # 🚨 DETECTA PULOS NA HIERARQUIA
        for i in range(len(counts)):
            if counts[i] > 0:  # Se existe este nível
                # Verifica se níveis anteriores existem
                for j in range(i):
                    if counts[j] == 0:  # Nível anterior ausente
                        nivel_atual = f"H{i+1}"
                        nivel_faltante = f"H{j+1}"
                        
                        # Evita duplicar problema já detectado
                        if not any(p['problema'].startswith(f'Pula de H{j+1}') for p in problemas):
                            problemas.append({
                                'problema': f'Pula de {nivel_faltante} para {nivel_atual}',
                                'detalhes': f'Tem {nivel_atual} ({counts[i]}) mas não tem {nivel_faltante}. Hierarquia deve ser sequencial.',
                                'gravidade': 'ALTO' if j <= 1 else 'MEDIO',  # H1/H2 mais críticos
                                'impacto': 'ALTO - Hierarquia quebrada prejudica acessibilidade e SEO',
                                'recomendacao': f'Adicionar {nivel_faltante} antes de usar {nivel_atual}, ou converter {nivel_atual} para nível apropriado'
                            })
        
        # 🚨 CASOS ESPECÍFICOS PROBLEMÁTICOS
        
        # H1 existe, mas tem H4/H5/H6 sem H2/H3
        if h1 > 0 and h2 == 0 and h3 == 0 and (h4 > 0 or h5 > 0 or h6 > 0):
            problemas.append({
                'problema': 'Pula de H1 direto para H4/H5/H6',
                'detalhes': f'Estrutura: H1({h1}) → H4({h4}), H5({h5}), H6({h6}). Faltam H2 e H3.',
                'gravidade': 'ALTO',
                'impacto': 'ALTO - Salto muito grande na hierarquia',
                'recomendacao': 'Usar H2 para seções principais e H3 para subseções antes de H4/H5/H6'
            })
        
        # Tem H3 mas não tem H1 nem H2
        if h1 == 0 and h2 == 0 and h3 > 0:
            problemas.append({
                'problema': 'Inicia com H3 sem H1/H2',
                'detalhes': f'Página começa com H3 ({h3}) sem ter H1 ou H2',
                'gravidade': 'CRITICO',
                'impacto': 'CRITICO - Estrutura totalmente inadequada',
                'recomendacao': 'Começar com H1 para título principal, depois H2 para seções, depois H3'
            })
        
        return problemas