# debug_headings.py - Para descobrir por que a aba sumiu

import pandas as pd
import requests
from bs4 import BeautifulSoup

def debug_headings_simples(excel_path):
    """🔍 Debug simples para ver o que está acontecendo"""
    
    print(f"🔍 DEBUGANDO: {excel_path}")
    
    # 1. Lê o Excel
    try:
        df = pd.read_excel(excel_path, sheet_name='Resumo')
        print(f"✅ Excel lido: {len(df)} URLs")
    except Exception as e:
        print(f"❌ Erro lendo Excel: {e}")
        return
    
    # 2. Pega algumas URLs para testar
    urls_teste = df['url'].head(5).tolist()
    print(f"🎯 Testando {len(urls_teste)} URLs:")
    
    for i, url in enumerate(urls_teste, 1):
        print(f"\n{i}. Testando: {url}")
        
        try:
            # Request simples
            response = requests.get(url, timeout=10, verify=False)
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Conta headings
            headings_vazios = 0
            
            for level in range(1, 7):
                tags = soup.find_all(f'h{level}')
                for tag in tags:
                    texto = tag.get_text().strip()
                    if not texto:
                        headings_vazios += 1
                        print(f"   🕳️ H{level} vazio encontrado!")
            
            if headings_vazios == 0:
                print(f"   ✅ Nenhum heading vazio")
            else:
                print(f"   ⚠️ Total vazios: {headings_vazios}")
                
        except Exception as e:
            print(f"   ❌ Erro: {e}")
    
    print(f"\n🎯 CONCLUSÃO:")
    print(f"Se você viu headings vazios acima, o revalidador deveria ter criado a aba.")
    print(f"Se não viu nenhum, é por isso que a aba não foi criada.")

if __name__ == "__main__":
    import os
    import glob
    
    # Procura arquivos Excel na pasta
    excel_files = glob.glob("*.xlsx")
    
    if not excel_files:
        print("❌ Nenhum arquivo Excel encontrado na pasta atual")
        print(f"📁 Pasta atual: {os.getcwd()}")
        print("📋 Arquivos na pasta:")
        for f in os.listdir('.'):
            if f.endswith('.xlsx'):
                print(f"   {f}")
    else:
        print(f"📁 Arquivos Excel encontrados:")
        for i, f in enumerate(excel_files, 1):
            print(f"   {i}. {f}")
        
        # Usa o primeiro arquivo encontrado
        arquivo_teste = excel_files[0]
        print(f"\n🎯 Usando: {arquivo_teste}")
        debug_headings_simples(arquivo_teste)