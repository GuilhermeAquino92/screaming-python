import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class HeadingsVaziosSheet(BaseSheetExporter):
    def __init__(self, df, writer, ordenacao_tipo='url_primeiro'):
        """
        ordenacao_tipo options:
        - 'url_primeiro': Agrupa por URL, depois por gravidade
        - 'gravidade_primeiro': Problemas críticos primeiro, depois URL
        - 'alfabetica_simples': Apenas ordem alfabética de URL
        """
        super().__init__(df, writer)
        self.ordenacao_tipo = ordenacao_tipo
    
    def export(self):
        """🕳️ Gera aba LIMPA de headings vazios E ocultos por CSS com ordenação configurável"""
        try:
            rows = []
            
            for _, row in self.df.iterrows():
                url = row.get('url', '')
                
                # 🎯 USA DADOS COM CONTEXTO E CSS do validador_headings.py
                if 'headings_problematicos' in row and isinstance(row['headings_problematicos'], list):
                    problemas_list = row['headings_problematicos']
                    
                    for problema in problemas_list:
                        if isinstance(problema, dict):
                            motivos = problema.get('motivos', [])
                            
                            # Filtra headings vazios OU ocultos por CSS
                            tem_vazio = any('vazio' in str(motivo).lower() or 'empty' in str(motivo).lower() for motivo in motivos)
                            tem_css = any('css' in str(motivo).lower() or 'oculto' in str(motivo).lower() for motivo in motivos)
                            
                            if tem_vazio or tem_css:
                                # 🎨 DETERMINA TIPO DO PROBLEMA
                                if tem_vazio and tem_css:
                                    tipo_problema = "Vazio + CSS Oculto"
                                elif tem_vazio:
                                    tipo_problema = "Vazio"
                                elif tem_css:
                                    tipo_problema = "CSS Oculto"
                                else:
                                    tipo_problema = "Outro"
                                
                                # 🎨 MONTA DESCRIÇÃO CSS DETALHADA
                                css_detalhes = []
                                
                                # CSS do próprio elemento
                                css_problemas = problema.get('css_problemas', [])
                                if css_problemas:
                                    css_detalhes.append(f"Elemento: {', '.join(css_problemas)}")
                                
                                # CSS do elemento pai
                                css_pai_problemas = problema.get('css_pai_problemas', [])
                                if css_pai_problemas:
                                    css_detalhes.append(f"Pai: {', '.join(css_pai_problemas)}")
                                
                                # Style inline (apenas se relevante)
                                style_inline = problema.get('style_inline', '')
                                if style_inline and len(style_inline.strip()) > 0:
                                    style_resumido = style_inline[:60] + ('...' if len(style_inline) > 60 else '')
                                    css_detalhes.append(f"Style: {style_resumido}")
                                
                                css_resumo = " | ".join(css_detalhes) if css_detalhes else "Sem CSS detectado"
                                
                                # 🎨 DETERMINA GRAVIDADE
                                gravidade_original = problema.get('gravidade', 'MEDIO')
                                if tipo_problema == "Vazio + CSS Oculto":
                                    gravidade_final = "CRITICO"
                                elif "CSS Oculto" in tipo_problema and problema.get('tag', '').upper() == 'H1':
                                    gravidade_final = "CRITICO"
                                elif tipo_problema == "Vazio" and problema.get('tag', '').upper() == 'H1':
                                    gravidade_final = "CRITICO"
                                else:
                                    gravidade_final = gravidade_original
                                
                                # 🎨 TEXTO DO HEADING (limita tamanho)
                                texto_heading = problema.get('texto', '')
                                if not texto_heading or texto_heading.strip() == '':
                                    texto_display = "[VAZIO]"
                                else:
                                    texto_display = texto_heading[:80] + ('...' if len(texto_heading) > 80 else '')
                                
                                rows.append({
                                    'URL': url,
                                    'Tag': problema.get('tag', '').upper(),
                                    'Tipo_Problema': tipo_problema,
                                    'Texto_Heading': texto_display,
                                    'CSS_Detalhes': css_resumo,
                                    'CSS_Elemento_Oculto': 'SIM' if problema.get('css_oculto', False) else 'NAO',
                                    'CSS_Pai_Oculto': 'SIM' if problema.get('css_pai_oculto', False) else 'NAO',
                                    'Contexto_Completo': problema.get('contexto_expandido', problema.get('contexto_pai', 'Nao informado')),
                                    'Atributos_Heading': problema.get('atributos_heading', 'sem atributos'),
                                    'Gravidade': gravidade_final,
                                    'Score_Pagina': row.get('metatags_score', 0)
                                })
                
                # Fallback: Se tem contagem mas não tem detalhes
                elif (row.get('headings_vazios_count', 0) > 0 or row.get('headings_ocultos_count', 0) > 0):
                    vazios_count = row.get('headings_vazios_count', 0)
                    ocultos_count = row.get('headings_ocultos_count', 0)
                    
                    if vazios_count > 0 and ocultos_count > 0:
                        tipo_problema = f"Multiplos ({vazios_count} vazios + {ocultos_count} CSS ocultos)"
                    elif vazios_count > 0:
                        tipo_problema = f"Multiplos ({vazios_count} vazios)"
                    else:
                        tipo_problema = f"Multiplos ({ocultos_count} CSS ocultos)"
                    
                    rows.append({
                        'URL': url,
                        'Tag': 'MULTIPLOS',
                        'Tipo_Problema': tipo_problema,
                        'Texto_Heading': 'Multiplos problemas',
                        'CSS_Detalhes': 'Analise nao disponivel (dados resumidos)',
                        'CSS_Elemento_Oculto': 'DESCONHECIDO',
                        'CSS_Pai_Oculto': 'DESCONHECIDO',
                        'Contexto_Completo': f"Problemas detectados sem contexto detalhado",
                        'Atributos_Heading': 'diversos',
                        'Gravidade': 'MEDIO',
                        'Score_Pagina': row.get('metatags_score', 0)
                    })
            
            # Se não encontrou nenhum problema
            if not rows:
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tag', 'Tipo_Problema', 'Texto_Heading', 'CSS_Detalhes',
                    'CSS_Elemento_Oculto', 'CSS_Pai_Oculto', 'Contexto_Completo',
                    'Atributos_Heading', 'Gravidade', 'Score_Pagina'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
                print(f"ℹ️ Aba Headings_Vazios criada sem dados (nenhum problema encontrado)")
                return df_vazio
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 APLICA ORDENAÇÃO CONFIGURÁVEL
            df_problemas = self._aplicar_ordenacao(df_problemas)
            
            # Reordena colunas para melhor visualização
            colunas_ordenadas = [
                'URL', 'Tag', 'Tipo_Problema', 'Gravidade', 'Texto_Heading',
                'CSS_Elemento_Oculto', 'CSS_Pai_Oculto', 'CSS_Detalhes',
                'Contexto_Completo', 'Atributos_Heading', 'Score_Pagina'
            ]
            
            df_problemas = df_problemas[colunas_ordenadas]
            
            df_problemas.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
            
            # Log para debug
            total_vazios = len([r for r in rows if 'Vazio' in r['Tipo_Problema']])
            total_css = len([r for r in rows if 'CSS' in r['Tipo_Problema']])
            total_mistos = len([r for r in rows if '+' in r['Tipo_Problema']])
            
            print(f"✅ Aba Headings_Vazios criada com {len(df_problemas)} problemas:")
            print(f"   🕳️ Headings vazios: {total_vazios}")
            print(f"   🎨 Headings ocultos por CSS: {total_css}")
            print(f"   🔥 Headings vazios + CSS oculto: {total_mistos}")
            print(f"   📊 Ordenação aplicada: {self.ordenacao_tipo}")
            
            return df_problemas
            
        except Exception as e:
            print(f"⚠️ Erro gerando aba headings vazios com CSS: {e}")
            # Em caso de erro, cria DataFrame vazio
            df_vazio = pd.DataFrame(columns=[
                'URL', 'Tag', 'Tipo_Problema', 'Texto_Heading', 'CSS_Detalhes',
                'CSS_Elemento_Oculto', 'CSS_Pai_Oculto', 'Contexto_Completo',
                'Atributos_Heading', 'Gravidade', 'Score_Pagina'
            ])
            df_vazio.to_excel(self.writer, index=False, sheet_name="Headings_Vazios")
            return df_vazio
    
    def _aplicar_ordenacao(self, df_problemas):
        """🔄 Aplica diferentes tipos de ordenação conforme configuração"""
        
        # Prepara colunas auxiliares de ordenação
        gravidade_order = {'CRITICO': 0, 'ALTO': 1, 'MEDIO': 2, 'BAIXO': 3}
        tipo_order = {
            'Vazio + CSS Oculto': 0,
            'Vazio': 1, 
            'CSS Oculto': 2,
            'Outro': 3
        }
        
        df_problemas['gravidade_sort'] = df_problemas['Gravidade'].map(gravidade_order)
        df_problemas['tipo_sort'] = df_problemas['Tipo_Problema'].apply(
            lambda x: min([tipo_order.get(key, 999) for key in tipo_order.keys() if key in str(x)])
        )
        df_problemas['tag_sort'] = df_problemas['Tag'].str.extract('(\d+)').astype(float).fillna(99)
        
        # Aplica ordenação conforme tipo escolhido
        if self.ordenacao_tipo == 'url_primeiro':
            # Agrupa por URL, depois problemas críticos
            df_problemas = df_problemas.sort_values([
                'URL',               # 1. URLs em ordem alfabética
                'gravidade_sort',    # 2. Críticos primeiro
                'tipo_sort',         # 3. Tipo do problema
                'tag_sort'           # 4. H1, H2, H3...
            ], ascending=[True, True, True, True])
            
        elif self.ordenacao_tipo == 'gravidade_primeiro':
            # Problemas críticos primeiro, depois agrupa por URL
            df_problemas = df_problemas.sort_values([
                'gravidade_sort',    # 1. Críticos primeiro
                'URL',               # 2. URLs em ordem alfabética
                'tipo_sort',         # 3. Tipo do problema
                'tag_sort'           # 4. H1, H2, H3...
            ], ascending=[True, True, True, True])
            
        elif self.ordenacao_tipo == 'alfabetica_simples':
            # Apenas ordem alfabética simples
            df_problemas = df_problemas.sort_values([
                'URL',               # 1. URLs em ordem alfabética
                'tag_sort'           # 2. H1, H2, H3...
            ], ascending=[True, True])
        
        # Remove colunas auxiliares
        df_problemas = df_problemas.drop(['gravidade_sort', 'tipo_sort', 'tag_sort'], axis=1, errors='ignore')
        
        return df_problemas