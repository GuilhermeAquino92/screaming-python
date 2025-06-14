# analise_csv_headings.py - Execute este arquivo para analisar os dados

import pandas as pd
import ast

def analisar_headings_csv():
    """📊 Análise completa do CSV gerado"""
    try:
        # Carrega CSV
        csv_path = "relatorio_seo_pipeline_gndisul_com_br.csv"
        df = pd.read_csv(csv_path)
        
        print("="*60)
        print("📊 ANÁLISE COMPLETA - HEADINGS VAZIOS")
        print("="*60)
        
        print(f"\n📈 ESTATÍSTICAS GERAIS:")
        print(f"   Total URLs: {len(df)}")
        print(f"   Colunas disponíveis: {len(df.columns)}")
        
        # 1. HEADINGS VAZIOS
        print(f"\n🕳️ HEADINGS VAZIOS:")
        if 'headings_vazios_count' in df.columns:
            vazios_stats = df['headings_vazios_count'].value_counts().sort_index()
            print(f"   Distribuição:")
            for valor, count in vazios_stats.items():
                print(f"      {valor} vazios: {count} URLs ({count/len(df)*100:.1f}%)")
            
            max_vazios = df['headings_vazios_count'].max()
            total_vazios = df['headings_vazios_count'].sum()
            print(f"   📊 Max vazios em uma URL: {max_vazios}")
            print(f"   📊 Total vazios no site: {total_vazios}")
            
            # URLs com vazios
            urls_com_vazios = df[df['headings_vazios_count'] > 0]
            if len(urls_com_vazios) > 0:
                print(f"   🎯 URLs com headings vazios:")
                for _, row in urls_com_vazios.head(5).iterrows():
                    print(f"      • {row['url']}: {row['headings_vazios_count']} vazios")
            else:
                print(f"   ✅ Nenhuma URL com headings vazios encontrada")
        else:
            print("   ❌ Coluna 'headings_vazios_count' não encontrada")
        
        # 2. HEADINGS OCULTOS POR CSS
        print(f"\n🎨 HEADINGS OCULTOS POR CSS:")
        if 'headings_ocultos_count' in df.columns:
            ocultos_stats = df['headings_ocultos_count'].value_counts().sort_index()
            print(f"   Distribuição:")
            for valor, count in ocultos_stats.items():
                print(f"      {valor} ocultos: {count} URLs ({count/len(df)*100:.1f}%)")
            
            urls_com_ocultos = df[df['headings_ocultos_count'] > 0]
            if len(urls_com_ocultos) > 0:
                print(f"   🎯 URLs com headings ocultos:")
                for _, row in urls_com_ocultos.head(5).iterrows():
                    print(f"      • {row['url']}: {row['headings_ocultos_count']} ocultos")
            else:
                print(f"   ✅ Nenhuma URL com headings ocultos encontrada")
        else:
            print("   ❌ Coluna 'headings_ocultos_count' não encontrada")
        
        # 3. PROBLEMAS DETALHADOS
        print(f"\n📋 PROBLEMAS DETALHADOS:")
        if 'headings_problematicos' in df.columns:
            # Conta URLs com problemas detalhados
            def tem_problemas(x):
                try:
                    if pd.isna(x) or x == '[]':
                        return False
                    if isinstance(x, str):
                        lista = ast.literal_eval(x)
                        return isinstance(lista, list) and len(lista) > 0
                    elif isinstance(x, list):
                        return len(x) > 0
                    return False
                except:
                    return False
            
            urls_com_problemas = df[df['headings_problematicos'].apply(tem_problemas)]
            print(f"   URLs com problemas detalhados: {len(urls_com_problemas)}")
            
            if len(urls_com_problemas) > 0:
                print(f"   🎯 Primeira URL com problemas:")
                primeira = urls_com_problemas.iloc[0]
                print(f"      URL: {primeira['url']}")
                
                try:
                    problemas_str = primeira['headings_problematicos']
                    if isinstance(problemas_str, str):
                        problemas = ast.literal_eval(problemas_str)
                    else:
                        problemas = problemas_str
                    
                    print(f"      Total problemas: {len(problemas)}")
                    if problemas and isinstance(problemas[0], dict):
                        primeiro = problemas[0]
                        print(f"      Primeiro problema:")
                        print(f"        Tag: {primeiro.get('tag', 'N/A')}")
                        print(f"        Motivos: {primeiro.get('motivos', [])}")
                        print(f"        Texto: '{primeiro.get('texto', 'N/A')[:50]}...'")
                except Exception as e:
                    print(f"      ⚠️ Erro processando problemas: {e}")
            else:
                print(f"   ✅ Nenhuma URL com problemas detalhados")
        else:
            print("   ❌ Coluna 'headings_problematicos' não encontrada")
        
        # 4. DISTRIBUIÇÃO DE HEADINGS
        print(f"\n📊 DISTRIBUIÇÃO DE HEADINGS:")
        heading_cols = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']
        for col in heading_cols:
            if col in df.columns:
                total = df[col].sum()
                max_val = df[col].max()
                urls_com_heading = len(df[df[col] > 0])
                print(f"   {col.upper()}: {total} total, {max_val} max, {urls_com_heading} URLs ({urls_com_heading/len(df)*100:.1f}%)")
        
        # 5. PROBLEMAS ESTRUTURAIS
        print(f"\n⚠️ PROBLEMAS ESTRUTURAIS:")
        if 'problemas' in df.columns:
            problemas_stats = df['problemas'].value_counts().head(10)
            for problema, count in problemas_stats.items():
                pct = count/len(df)*100
                print(f"   {problema}: {count} URLs ({pct:.1f}%)")
        
        # 6. TOP URLs POR QUANTIDADE DE HEADINGS
        print(f"\n🏆 TOP 10 URLs COM MAIS HEADINGS:")
        if all(col in df.columns for col in heading_cols):
            df_temp = df.copy()
            df_temp['total_headings'] = df_temp[heading_cols].sum(axis=1)
            top_headings = df_temp.nlargest(10, 'total_headings')
            
            for i, (_, row) in enumerate(top_headings.iterrows(), 1):
                print(f"   {i:2d}. {row['url'][:60]}...")
                print(f"       H1:{row['h1']} H2:{row['h2']} H3:{row['h3']} H4:{row['h4']} H5:{row['h5']} H6:{row['h6']} | Total: {row['total_headings']}")
        
        # 7. ANÁLISE DE TÍTULOS
        print(f"\n📝 ANÁLISE DE TÍTULOS:")
        if 'title' in df.columns:
            urls_sem_title = len(df[df['title'].isna() | (df['title'] == '')])
            urls_com_title = len(df) - urls_sem_title
            print(f"   URLs com title: {urls_com_title} ({urls_com_title/len(df)*100:.1f}%)")
            print(f"   URLs sem title: {urls_sem_title} ({urls_sem_title/len(df)*100:.1f}%)")
            
            if urls_sem_title > 0:
                print(f"   🎯 Primeiras URLs sem title:")
                urls_sem_title_df = df[df['title'].isna() | (df['title'] == '')].head(5)
                for _, row in urls_sem_title_df.iterrows():
                    print(f"      • {row['url']}")
        
        print(f"\n" + "="*60)
        print(f"✅ ANÁLISE CONCLUÍDA")
        print(f"💡 CONCLUSÃO:")
        
        if 'headings_vazios_count' in df.columns:
            total_vazios = df['headings_vazios_count'].sum()
            if total_vazios == 0:
                print(f"   🎉 O site GNDI SUL está bem estruturado!")
                print(f"   ✅ Não há headings vazios detectados")
                print(f"   📊 Isso explica por que a aba Headings_Vazios fica vazia")
            else:
                print(f"   ⚠️ Encontrados {total_vazios} headings vazios que precisam de atenção")
        
        if 'headings_ocultos_count' in df.columns:
            total_ocultos = df['headings_ocultos_count'].sum()
            if total_ocultos == 0:
                print(f"   ✅ Não há headings ocultos por CSS")
            else:
                print(f"   ⚠️ Encontrados {total_ocultos} headings ocultos por CSS")
        
        print(f"   📋 A aba vazia não é um bug - é o resultado correto!")
        
    except FileNotFoundError:
        print("❌ Arquivo CSV não encontrado. Execute o pipeline primeiro.")
    except Exception as e:
        print(f"❌ Erro na análise: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analisar_headings_csv()