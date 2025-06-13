# exporters/sheets/h1_h2_problemas_sheet.py

import pandas as pd
from exporters.base_exporter import BaseSheetExporter

class H1H2ProblemasSheet(BaseSheetExporter):
    def export(self):
        """📋 Gera aba específica para problemas de H1 e H2 (duplicados e ausentes)"""
        try:
            rows = []
            
            # 🔍 COLETA DADOS DE H1 E H2 DE TODAS AS URLs (APENAS PARA DUPLICADOS DE TEXTO)
            h1_dados = {}  # {texto_h1: [lista_urls]}
            h2_dados = {}  # {texto_h2: [lista_urls]}
            
            # 🚫 FILTRO: Remove paginações e parâmetros desnecessários
            df_filtrado = self.df[~self.df["url"].str.contains(r"\?page=\d+", na=False)].copy()
            df_filtrado = df_filtrado[~df_filtrado["url"].str.contains(r"&page=\d+", na=False)]
            
            for _, row in df_filtrado.iterrows():
                url = row.get('url', '')
                
                # 🆕 MÉTODO MELHORADO: usa dados diretos do validador
                h1_texts = row.get('h1_texts', [])
                h2_texts = row.get('h2_texts', [])
                
                # Fallback: verifica dados detalhados se dados diretos não disponíveis
                if not h1_texts and not h2_texts:
                    if 'headings_problematicos' in row and isinstance(row['headings_problematicos'], list):
                        # Extrai H1 e H2 dos dados detalhados
                        for problema in row['headings_problematicos']:
                            if isinstance(problema, dict):
                                tag = problema.get('tag', '').lower()
                                texto = problema.get('texto', '').strip()
                                
                                # Só considera headings com texto (não vazios ou ocultos apenas)
                                if texto and not any('vazio' in str(motivo).lower() for motivo in problema.get('motivos', [])):
                                    if tag == 'h1':
                                        h1_texts.append(texto)
                                    elif tag == 'h2':
                                        h2_texts.append(texto)
                
                # 🔍 PROCESSA H1 (APENAS para detectar textos duplicados)
                for h1_text in h1_texts:
                    if h1_text.strip():  # Só textos não vazios
                        if h1_text not in h1_dados:
                            h1_dados[h1_text] = []
                        h1_dados[h1_text].append(url)
                
                # 🔍 PROCESSA H2 (APENAS para detectar textos duplicados)
                for h2_text in h2_texts:
                    if h2_text.strip():  # Só textos não vazios
                        if h2_text not in h2_dados:
                            h2_dados[h2_text] = []
                        h2_dados[h2_text].append(url)
            
            # 🔥 GERA LINHAS APENAS PARA H1 DUPLICADO (mesmo texto em múltiplas páginas)
            for h1_text, urls_list in h1_dados.items():
                if len(urls_list) > 1:  # Duplicado entre páginas
                    urls_ordenadas = sorted(urls_list)
                    grupo_key = f'H1_DUPLICADO_{h1_text[:50]}'
                    
                    # Primeira linha: mostra o texto duplicado e total
                    rows.append({
                        'URL': f'>>> H1 DUPLICADO EM {len(urls_list)} PÁGINAS <<<',
                        'Tag': 'H1',
                        'Tipo_Problema': 'DUPLICADO',
                        'Texto_Heading': h1_text[:100] + ('...' if len(h1_text) > 100 else ''),
                        'Total_URLs_Afetadas': len(urls_list),
                        'Gravidade': 'CRITICO',
                        'Impacto_SEO': 'CRITICO - H1 duplicado entre páginas confunde mecanismos de busca',
                        'Grupo_Ordenacao': f'{grupo_key}_000_CABECALHO'  # Fica no topo do grupo
                    })
                    
                    # Linhas seguintes: uma para cada URL com esse H1
                    for i, url in enumerate(urls_ordenadas):
                        rows.append({
                            'URL': url,
                            'Tag': 'H1',
                            'Tipo_Problema': 'DUPLICADO',
                            'Texto_Heading': f'[MESMO H1 #{i+1}] {h1_text[:80]}{"..." if len(h1_text) > 80 else ""}',
                            'Total_URLs_Afetadas': len(urls_list),
                            'Gravidade': 'CRITICO',
                            'Impacto_SEO': 'CRITICO - H1 duplicado entre páginas confunde mecanismos de busca',
                            'Grupo_Ordenacao': f'{grupo_key}_{i+1:03d}_{url}'  # Ordena após cabeçalho
                        })
            
            # 🔥 GERA LINHAS APENAS PARA H2 DUPLICADO (mesmo texto em múltiplas páginas)
            for h2_text, urls_list in h2_dados.items():
                if len(urls_list) > 1:  # Duplicado entre páginas
                    urls_ordenadas = sorted(urls_list)
                    grupo_key = f'H2_DUPLICADO_{h2_text[:50]}'
                    
                    # Primeira linha: mostra o texto duplicado e total
                    rows.append({
                        'URL': f'>>> H2 DUPLICADO EM {len(urls_list)} PÁGINAS <<<',
                        'Tag': 'H2',
                        'Tipo_Problema': 'DUPLICADO',
                        'Texto_Heading': h2_text[:100] + ('...' if len(h2_text) > 100 else ''),
                        'Total_URLs_Afetadas': len(urls_list),
                        'Gravidade': 'ALTO',
                        'Impacto_SEO': 'ALTO - H2 duplicado entre páginas pode prejudicar SEO',
                        'Grupo_Ordenacao': f'{grupo_key}_000_CABECALHO'  # Fica no topo do grupo
                    })
                    
                    # Linhas seguintes: uma para cada URL com esse H2
                    for i, url in enumerate(urls_ordenadas):
                        rows.append({
                            'URL': url,
                            'Tag': 'H2',
                            'Tipo_Problema': 'DUPLICADO',
                            'Texto_Heading': f'[MESMO H2 #{i+1}] {h2_text[:80]}{"..." if len(h2_text) > 80 else ""}',
                            'Total_URLs_Afetadas': len(urls_list),
                            'Gravidade': 'ALTO',
                            'Impacto_SEO': 'ALTO - H2 duplicado entre páginas pode prejudicar SEO',
                            'Grupo_Ordenacao': f'{grupo_key}_{i+1:03d}_{url}'  # Ordena após cabeçalho
                        })
            
            # Se não encontrou nenhum problema
            if not rows:
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Tag', 'Tipo_Problema', 'Texto_Heading', 'Total_URLs_Afetadas',
                    'Gravidade', 'Impacto_SEO'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="H1_H2_Problemas")
                print(f"✅ Aba H1_H2_Problemas criada sem dados (nenhum texto duplicado encontrado)")
                return df_vazio
            
            df_problemas = pd.DataFrame(rows)
            
            # 🔄 ORDENAÇÃO SIMPLIFICADA - apenas por tag e agrupamento
            tag_order = {'H1': 0, 'H2': 1}
            
            df_problemas['tag_sort'] = df_problemas['Tag'].map(tag_order)
            
            # Ordena por: tag → grupo_ordenacao (que já mantém cabeçalhos e URLs juntos)
            df_problemas = df_problemas.sort_values([
                'tag_sort',           # 1. H1 primeiro
                'Grupo_Ordenacao'     # 2. Mantém agrupamento correto
            ], ascending=[True, True])
            
            # Remove colunas auxiliares
            df_problemas = df_problemas.drop(['tag_sort', 'Grupo_Ordenacao'], axis=1, errors='ignore')
            
            df_problemas.to_excel(self.writer, index=False, sheet_name="H1_H2_Problemas")
            
            # 📊 ESTATÍSTICAS ATUALIZADAS
            total_h1_duplicado = len([h1 for h1, urls in h1_dados.items() if len(urls) > 1])
            total_h2_duplicado = len([h2 for h2, urls in h2_dados.items() if len(urls) > 1])
            urls_h1_duplicado = sum([len(urls) for h1, urls in h1_dados.items() if len(urls) > 1])
            urls_h2_duplicado = sum([len(urls) for h2, urls in h2_dados.items() if len(urls) > 1])
            
            print(f"✅ Aba H1_H2_Problemas criada focada em DUPLICAÇÃO DE TEXTO:")
            print(f"   🔄 H1s com texto duplicado: {total_h1_duplicado} textos afetando {urls_h1_duplicado} URLs")
            print(f"   🔄 H2s com texto duplicado: {total_h2_duplicado} textos afetando {urls_h2_duplicado} URLs")
            print(f"   📋 Total de linhas na planilha: {len(df_problemas)}")
            print(f"   🎯 Foco: mesmo texto H1/H2 usado em múltiplas páginas")
            print(f"   ℹ️ H1/H2 ausentes agora estão na aba 'Estrutura_Headings'")
            
            return df_problemas
            
        except Exception as e:
            print(f"⚠️ Erro gerando aba H1/H2 problemas: {e}")
            # Em caso de erro, cria DataFrame vazio
            df_vazio = pd.DataFrame(columns=[
                'URL', 'Tag', 'Tipo_Problema', 'Texto_Heading', 'Total_URLs_Afetadas',
                'Gravidade', 'Impacto_SEO'
            ])
            df_vazio.to_excel(self.writer, index=False, sheet_name="H1_H2_Problemas")
            return df_vazio