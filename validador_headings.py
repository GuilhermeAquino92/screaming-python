# validador_headings.py - COM CONTEXTO DO ELEMENTO PAI

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import warnings
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings("ignore")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9"
}

def extrair_contexto_pai(tag):
    """🔍 Extrai contexto útil do elemento pai"""
    try:
        if not tag.parent:
            return "Sem elemento pai"
        
        pai = tag.parent
        contexto_parts = []
        
        # 1. Nome da tag pai
        contexto_parts.append(f"<{pai.name}>")
        
        # 2. Classes do pai (se houver)
        classes_pai = pai.get('class', [])
        if classes_pai:
            classes_str = ' '.join(classes_pai)
            contexto_parts.append(f"class='{classes_str}'")
        
        # 3. ID do pai (se houver)
        id_pai = pai.get('id')
        if id_pai:
            contexto_parts.append(f"id='{id_pai}'")
        
        # 4. Data attributes importantes (se houver)
        data_attrs = []
        for attr, value in pai.attrs.items():
            if attr.startswith('data-') and len(str(value)) < 30:
                data_attrs.append(f"{attr}='{value}'")
        
        if data_attrs:
            contexto_parts.extend(data_attrs[:2])  # Máximo 2 data attributes
        
        # 5. Monta contexto final
        contexto = f"<{pai.name}"
        if len(contexto_parts) > 1:
            atributos = ' '.join(contexto_parts[1:])
            contexto += f" {atributos}"
        contexto += ">"
        
        # 6. Se o contexto ficou muito longo, encurta
        if len(contexto) > 100:
            contexto = contexto[:97] + "..."
        
        return contexto
        
    except Exception as e:
        return f"Erro extraindo contexto: {str(e)}"

def extrair_contexto_expandido(tag):
    """🔍 Extrai contexto expandido incluindo hierarquia"""
    try:
        contextos = []
        
        # Contexto do elemento pai direto
        contexto_pai = extrair_contexto_pai(tag)
        contextos.append(f"Pai: {contexto_pai}")
        
        # Contexto do avô (se útil)
        if tag.parent and tag.parent.parent:
            avo = tag.parent.parent
            if avo.name not in ['body', 'html', 'head']:
                contexto_avo = f"<{avo.name}"
                
                # Adiciona class do avô se houver
                classes_avo = avo.get('class', [])
                if classes_avo:
                    contexto_avo += f" class='{' '.join(classes_avo[:2])}'..."  # Máximo 2 classes
                
                # Adiciona ID do avô se houver
                id_avo = avo.get('id')
                if id_avo:
                    contexto_avo += f" id='{id_avo}'"
                
                contexto_avo += ">"
                
                if len(contexto_avo) < 60:  # Só adiciona se não for muito longo
                    contextos.append(f"Avô: {contexto_avo}")
        
        # Contexto de irmãos próximos (se tiver texto)
        irmao_anterior = tag.find_previous_sibling()
        if irmao_anterior and hasattr(irmao_anterior, 'get_text'):
            texto_irmao = irmao_anterior.get_text(strip=True)
            if texto_irmao and len(texto_irmao) < 50:
                contextos.append(f"Após: '{texto_irmao}'")
        
        return " | ".join(contextos)
        
    except Exception:
        return extrair_contexto_pai(tag)

def validar_headings_em_url(url):
    try:
        response = requests.get(url, timeout=10, headers=HEADERS, verify=False)
        soup = BeautifulSoup(response.text, "lxml")

        # 🔥 DADOS ORIGINAIS (mantidos)
        headings = {f"h{i}": [] for i in range(1, 7)}
        for i in range(1, 7):
            for tag in soup.find_all(f"h{i}"):
                texto = tag.get_text(strip=True)
                if texto:
                    headings[f"h{i}"].append(texto)

        erros = []

        if not headings["h1"]:
            erros.append("H1 ausente")
        elif len(headings["h1"]) > 1:
            erros.append("H1 duplicado")

        if not headings["h2"]:
            erros.append("H2 ausente")

        ordem = []
        for i in range(1, 7):
            if headings[f"h{i}"]:
                ordem.append(i)

        if ordem != sorted(ordem):
            erros.append("Hierarquia invertida")

        # 🆕 DETECÇÃO AVANÇADA: Headings vazios com contexto
        headings_vazios = []
        headings_vazios_count = 0
        
        for i in range(1, 7):
            tags_vazias = soup.find_all(f"h{i}")
            posicao_geral = 0
            
            for tag in tags_vazias:
                posicao_geral += 1
                texto = tag.get_text(strip=True)
                
                if not texto:  # Heading vazio
                    headings_vazios_count += 1
                    
                    # 🔥 EXTRAI CONTEXTO DO PAI
                    contexto_pai = extrair_contexto_pai(tag)
                    contexto_expandido = extrair_contexto_expandido(tag)
                    
                    # 🔥 EXTRAI ATRIBUTOS DO PRÓPRIO HEADING
                    atributos_heading = []
                    if tag.get('class'):
                        atributos_heading.append(f"class='{' '.join(tag.get('class'))}'")
                    if tag.get('id'):
                        atributos_heading.append(f"id='{tag.get('id')}'")
                    
                    atributos_str = ' '.join(atributos_heading) if atributos_heading else 'sem atributos'
                    
                    headings_vazios.append({
                        'tag': f'h{i}',
                        'posicao': posicao_geral,
                        'contexto_pai': contexto_pai,
                        'contexto_expandido': contexto_expandido,
                        'atributos_heading': atributos_str,
                        'descricao': f'H{i} vazio na posição {posicao_geral}',
                        'descricao_completa': f'H{i} vazio: {contexto_pai}',
                        'texto': '',
                        'motivos': ['Vazio'],
                        'gravidade': 'CRÍTICO' if i == 1 else 'MÉDIO'
                    })

        # 🆕 DADOS ESTRUTURADOS para a aba
        resultado = {
            # Dados originais (mantidos para compatibilidade)
            "url": url,
            "h1": len(headings["h1"]),
            "h2": len(headings["h2"]),
            "h3": len(headings["h3"]),
            "h4": len(headings["h4"]),
            "h5": len(headings["h5"]),
            "h6": len(headings["h6"]),
            "problemas": "; ".join(erros) if erros else "OK",
            
            # 🆕 Novos dados com contexto
            "headings_vazios_count": headings_vazios_count,
            "headings_problematicos": headings_vazios,  # Lista com contexto
            "tem_headings_vazios": headings_vazios_count > 0
        }

        return resultado

    except Exception as e:
        return {
            # Dados originais em caso de erro
            "url": url,
            "h1": 0,
            "h2": 0,
            "h3": 0,
            "h4": 0,
            "h5": 0,
            "h6": 0,
            "problemas": f"Erro: {str(e)}",
            
            # Novos dados vazios em caso de erro
            "headings_vazios_count": 0,
            "headings_problematicos": [],
            "tem_headings_vazios": False
        }

def validar_headings(lista_urls, max_threads=30):
    print(f"🔄 Validando headings (com contexto do elemento pai) com até {max_threads} threads...")
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        resultados = list(tqdm(executor.map(validar_headings_em_url, lista_urls), total=len(lista_urls), desc="🧠 Headings + Contexto"))
    
    # 🆕 Log estatísticas
    total_vazios = sum(r.get('headings_vazios_count', 0) for r in resultados)
    urls_com_vazios = len([r for r in resultados if r.get('tem_headings_vazios', False)])
    
    if total_vazios > 0:
        print(f"🕳️ Detectados {total_vazios} headings vazios em {urls_com_vazios} URLs (com contexto)")
    else:
        print(f"✅ Nenhum heading vazio encontrado")
    
    return resultados


# 🧪 Função de teste para verificar contexto
def testar_extracao_contexto():
    """Testa a extração de contexto do elemento pai"""
    print("🧪 Testando extração de contexto...")
    
    # HTML de teste com diferentes contextos
    html_test = """
    <html>
    <body>
        <div class="main-content" id="content">
            <h1>Título Principal</h1>
            <section class="article-section" data-section="intro">
                <h2></h2><!-- H2 VAZIO em section -->
            </section>
            <div class="sidebar widget" data-widget="recent">
                <h3></h3><!-- H3 VAZIO em sidebar -->
            </div>
            <article id="post-123" class="post">
                <header class="post-header">
                    <h2>Título do Post</h2>
                    <h4>   </h4><!-- H4 VAZIO em header -->
                </header>
            </article>
        </div>
    </body>
    </html>
    """
    
    soup = BeautifulSoup(html_test, 'html.parser')
    
    print(f"🎯 Testando extração de contexto:")
    
    # Encontra headings vazios e mostra contexto
    for i in range(1, 7):
        tags = soup.find_all(f"h{i}")
        for tag in tags:
            texto = tag.get_text(strip=True)
            if not texto:
                contexto = extrair_contexto_pai(tag)
                contexto_expandido = extrair_contexto_expandido(tag)
                
                print(f"  H{i} vazio:")
                print(f"    Contexto Pai: {contexto}")
                print(f"    Contexto Expandido: {contexto_expandido}")
                print(f"    ---")
    
    print(f"✅ Teste de contexto concluído")


if __name__ == "__main__":
    testar_extracao_contexto()