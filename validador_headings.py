# validador_headings.py - COM DETECÇÃO DE CSS QUE OCULTA ELEMENTOS

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import warnings
from concurrent.futures import ThreadPoolExecutor
import re

warnings.filterwarnings("ignore")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9"
}

def analisar_css_ocultacao(tag, css_global=""):
    """🕵️ Analisa se o elemento está oculto por CSS"""
    problemas_css = []
    
    # 1. STYLE INLINE do próprio elemento
    style_inline = tag.get('style', '')
    if style_inline:
        style_lower = style_inline.lower()
        
        # Detecta propriedades que ocultam
        if 'display:none' in style_lower.replace(' ', ''):
            problemas_css.append("display:none (inline)")
        if 'display: none' in style_lower:
            problemas_css.append("display:none (inline)")
        if 'visibility:hidden' in style_lower.replace(' ', ''):
            problemas_css.append("visibility:hidden (inline)")
        if 'visibility: hidden' in style_lower:
            problemas_css.append("visibility:hidden (inline)")
        
        # Detecta cores que "escondem" o texto
        if any(color_hack in style_lower for color_hack in [
            'color:white', 'color:#fff', 'color:#ffffff', 'color:transparent',
            'color: white', 'color: #fff', 'color: #ffffff', 'color: transparent'
        ]):
            problemas_css.append("cor de texto invisível (inline)")
        
        # Detecta posicionamento que remove do fluxo
        if 'position:absolute' in style_lower.replace(' ', '') and 'left:-' in style_lower.replace(' ', ''):
            problemas_css.append("posicionamento fora da tela (inline)")
        if 'text-indent:-' in style_lower.replace(' ', ''):
            problemas_css.append("text-indent negativo (inline)")
    
    # 2. CLASSES e IDs que podem ter CSS oculto
    suspeitas_classe = []
    classes = tag.get('class', [])
    if classes:
        for classe in classes:
            classe_lower = classe.lower()
            # Classes suspeitas de ocultação
            if any(termo in classe_lower for termo in [
                'hidden', 'hide', 'invisible', 'sr-only', 'screen-reader',
                'visually-hidden', 'visuallyhidden', 'off-screen', 'offscreen'
            ]):
                suspeitas_classe.append(f"classe suspeita: {classe}")
    
    id_elemento = tag.get('id', '')
    if id_elemento:
        id_lower = id_elemento.lower()
        if any(termo in id_lower for termo in ['hidden', 'hide', 'invisible']):
            suspeitas_classe.append(f"ID suspeito: {id_elemento}")
    
    # 3. ANALISA CSS GLOBAL (básico)
    css_global_problemas = []
    if css_global and (classes or id_elemento):
        css_lower = css_global.lower()
        
        # Verifica se há regras CSS para as classes/ID do elemento
        for classe in classes:
            if f".{classe}" in css_lower:
                # Busca a regra CSS desta classe
                pattern = rf'\.{re.escape(classe)}\s*\{{[^}}]*\}}'
                matches = re.findall(pattern, css_lower, re.DOTALL)
                for match in matches:
                    if any(prop in match for prop in ['display:none', 'visibility:hidden', 'color:white', 'color:#fff']):
                        css_global_problemas.append(f"regra CSS oculta para .{classe}")
        
        if id_elemento and f"#{id_elemento}" in css_lower:
            pattern = rf'#{re.escape(id_elemento)}\s*\{{[^}}]*\}}'
            matches = re.findall(pattern, css_lower, re.DOTALL)
            for match in matches:
                if any(prop in match for prop in ['display:none', 'visibility:hidden', 'color:white', 'color:#fff']):
                    css_global_problemas.append(f"regra CSS oculta para #{id_elemento}")
    
    # 4. CONSOLIDA RESULTADO
    todos_problemas = problemas_css + suspeitas_classe + css_global_problemas
    
    return {
        'tem_ocultacao': len(todos_problemas) > 0,
        'problemas_css': todos_problemas,
        'style_inline': style_inline,
        'classes_suspeitas': suspeitas_classe,
        'css_global_issues': css_global_problemas
    }

def analisar_css_pai(tag, css_global=""):
    """🔍 Analisa se o elemento PAI está oculto por CSS"""
    if not tag.parent:
        return {'pai_oculto': False, 'motivos_pai': []}
    
    pai = tag.parent
    analise_pai = analisar_css_ocultacao(pai, css_global)
    
    return {
        'pai_oculto': analise_pai['tem_ocultacao'],
        'motivos_pai': analise_pai['problemas_css'],
        'pai_tag': pai.name,
        'pai_classes': pai.get('class', []),
        'pai_id': pai.get('id', '')
    }

def extrair_css_global(soup):
    """🎨 Extrai CSS inline das tags <style> para análise"""
    css_content = ""
    
    # Extrai CSS de todas as tags <style>
    for style_tag in soup.find_all('style'):
        css_content += style_tag.get_text() + "\n"
    
    return css_content

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

        # 🎨 EXTRAI CSS GLOBAL para análise
        css_global = extrair_css_global(soup)

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

        # 🆕 DETECÇÃO AVANÇADA: Headings vazios E ocultos por CSS
        headings_vazios = []
        headings_ocultos = []
        headings_vazios_count = 0
        headings_ocultos_count = 0
        
        for i in range(1, 7):
            tags_heading = soup.find_all(f"h{i}")
            posicao_geral = 0
            
            for tag in tags_heading:
                posicao_geral += 1
                texto = tag.get_text(strip=True)
                
                # 🔥 ANALISA CSS OCULTAÇÃO (para todos os headings)
                analise_css = analisar_css_ocultacao(tag, css_global)
                analise_pai_css = analisar_css_pai(tag, css_global)
                
                # 🔥 EXTRAI CONTEXTO E ATRIBUTOS
                contexto_pai = extrair_contexto_pai(tag)
                contexto_expandido = extrair_contexto_expandido(tag)
                
                atributos_heading = []
                if tag.get('class'):
                    atributos_heading.append(f"class='{' '.join(tag.get('class'))}'")
                if tag.get('id'):
                    atributos_heading.append(f"id='{tag.get('id')}'")
                
                atributos_str = ' '.join(atributos_heading) if atributos_heading else 'sem atributos'
                
                # 🔥 HEADING VAZIO
                if not texto:
                    headings_vazios_count += 1
                    
                    # Adiciona informação de CSS se estiver oculto também
                    motivos = ['Vazio']
                    descricao_completa = f'H{i} vazio: {contexto_pai}'
                    
                    if analise_css['tem_ocultacao']:
                        motivos.append('Oculto por CSS')
                        descricao_completa += f' [CSS: {", ".join(analise_css["problemas_css"])}]'
                    
                    if analise_pai_css['pai_oculto']:
                        motivos.append('Pai oculto por CSS')
                        descricao_completa += f' [PAI CSS: {", ".join(analise_pai_css["motivos_pai"])}]'
                    
                    headings_vazios.append({
                        'tag': f'h{i}',
                        'posicao': posicao_geral,
                        'contexto_pai': contexto_pai,
                        'contexto_expandido': contexto_expandido,
                        'atributos_heading': atributos_str,
                        'descricao': f'H{i} vazio na posição {posicao_geral}',
                        'descricao_completa': descricao_completa,
                        'texto': '',
                        'motivos': motivos,
                        'gravidade': 'CRÍTICO' if i == 1 else 'MÉDIO',
                        # 🆕 DADOS CSS
                        'css_oculto': analise_css['tem_ocultacao'],
                        'css_problemas': analise_css['problemas_css'],
                        'css_pai_oculto': analise_pai_css['pai_oculto'],
                        'css_pai_problemas': analise_pai_css['motivos_pai'],
                        'style_inline': analise_css['style_inline']
                    })
                
                # 🆕 HEADING COM TEXTO MAS OCULTO POR CSS
                elif analise_css['tem_ocultacao'] or analise_pai_css['pai_oculto']:
                    headings_ocultos_count += 1
                    
                    motivos = ['Oculto por CSS']
                    descricao_completa = f'H{i} oculto por CSS: {contexto_pai}'
                    
                    if analise_css['tem_ocultacao']:
                        descricao_completa += f' [ELEMENTO: {", ".join(analise_css["problemas_css"])}]'
                    
                    if analise_pai_css['pai_oculto']:
                        motivos.append('Pai oculto por CSS')
                        descricao_completa += f' [PAI: {", ".join(analise_pai_css["motivos_pai"])}]'
                    
                    headings_ocultos.append({
                        'tag': f'h{i}',
                        'posicao': posicao_geral,
                        'contexto_pai': contexto_pai,
                        'contexto_expandido': contexto_expandido,
                        'atributos_heading': atributos_str,
                        'descricao': f'H{i} oculto por CSS na posição {posicao_geral}',
                        'descricao_completa': descricao_completa,
                        'texto': texto[:100] + ('...' if len(texto) > 100 else ''),  # Limita o texto
                        'motivos': motivos,
                        'gravidade': 'ALTO' if i == 1 else 'MÉDIO',
                        # 🆕 DADOS CSS
                        'css_oculto': analise_css['tem_ocultacao'],
                        'css_problemas': analise_css['problemas_css'],
                        'css_pai_oculto': analise_pai_css['pai_oculto'],
                        'css_pai_problemas': analise_pai_css['motivos_pai'],
                        'style_inline': analise_css['style_inline']
                    })

        # 🆕 CONSOLIDA TODOS OS PROBLEMAS
        todos_headings_problematicos = headings_vazios + headings_ocultos

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
            
            # 🆕 Novos dados com contexto E CSS
            "headings_vazios_count": headings_vazios_count,
            "headings_ocultos_count": headings_ocultos_count,  # 🆕 NOVO CAMPO
            "headings_problematicos": todos_headings_problematicos,  # 🆕 INCLUI OCULTOS
            "tem_headings_vazios": headings_vazios_count > 0,
            "tem_headings_ocultos": headings_ocultos_count > 0,  # 🆕 NOVO CAMPO
            "tem_css_global": len(css_global.strip()) > 0,  # 🆕 INDICA SE TEM CSS
            
            # 🆕 DADOS ESPECÍFICOS PARA ABA H1_H2_PROBLEMAS
            "h1_texts": headings["h1"],  # Lista de textos H1 encontrados
            "h2_texts": headings["h2"],  # Lista de textos H2 encontrados
            "h1_ausente": len(headings["h1"]) == 0,
            "h2_ausente": len(headings["h2"]) == 0
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
            "headings_ocultos_count": 0,
            "headings_problematicos": [],
            "tem_headings_vazios": False,
            "tem_headings_ocultos": False,
            "tem_css_global": False,
            
            # 🆕 DADOS PARA H1_H2_PROBLEMAS em caso de erro
            "h1_texts": [],
            "h2_texts": [],
            "h1_ausente": True,
            "h2_ausente": True
        }

def validar_headings(lista_urls, max_threads=30):
    print(f"🔄 Validando headings (com detecção de CSS oculto) com até {max_threads} threads...")
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        resultados = list(tqdm(executor.map(validar_headings_em_url, lista_urls), total=len(lista_urls), desc="🧠 Headings + CSS"))
    
    # 🆕 Log estatísticas ATUALIZADAS
    total_vazios = sum(r.get('headings_vazios_count', 0) for r in resultados)
    total_ocultos = sum(r.get('headings_ocultos_count', 0) for r in resultados)
    urls_com_vazios = len([r for r in resultados if r.get('tem_headings_vazios', False)])
    urls_com_ocultos = len([r for r in resultados if r.get('tem_headings_ocultos', False)])
    
    if total_vazios > 0:
        print(f"🕳️ Detectados {total_vazios} headings vazios em {urls_com_vazios} URLs")
    
    if total_ocultos > 0:
        print(f"🕵️ Detectados {total_ocultos} headings OCULTOS POR CSS em {urls_com_ocultos} URLs")
    
    if total_vazios == 0 and total_ocultos == 0:
        print(f"✅ Nenhum heading vazio ou oculto por CSS encontrado")
    
    return resultados


# 🧪 Função de teste para verificar detecção de CSS
def testar_deteccao_css():
    """Testa a detecção de CSS que oculta elementos"""
    print("🧪 Testando detecção de CSS que oculta headings...")
    
    # HTML de teste com diferentes tipos de ocultação CSS
    html_test = """
    <html>
    <head>
        <style>
            .hidden { display: none; }
            .invisible { visibility: hidden; }
            .white-text { color: white; }
            .sr-only { position: absolute; left: -10000px; }
            #hidden-header { display: none; }
        </style>
    </head>
    <body>
        <div class="main-content">
            <h1>Título Principal Visível</h1>
            
            <!-- HEADING VAZIO -->
            <h2></h2>
            
            <!-- HEADING OCULTO POR CSS INLINE -->
            <h2 style="display: none;">Heading oculto por display none</h2>
            
            <!-- HEADING OCULTO POR CLASSE CSS -->
            <h3 class="hidden">Heading oculto por classe</h3>
            
            <!-- HEADING OCULTO POR ID CSS -->
            <h4 id="hidden-header">Heading oculto por ID</h4>
            
            <!-- HEADING COM COR INVISÍVEL -->
            <h5 style="color: white;">Heading com cor branca</h5>
            
            <!-- HEADING EM CONTAINER OCULTO -->
            <div style="display: none;">
                <h6>Heading em div oculta</h6>
            </div>
            
            <!-- HEADING COM CLASSE SUSPEITA -->
            <h2 class="sr-only">Screen reader only</h2>
            
            <!-- HEADING NORMAL -->
            <h3>Heading normal e visível</h3>
        </div>
    </body>
    </html>
    """
    
    soup = BeautifulSoup(html_test, 'html.parser')
    css_global = """
        .hidden { display: none; }
        .invisible { visibility: hidden; }
        .white-text { color: white; }
        .sr-only { position: absolute; left: -10000px; }
        #hidden-header { display: none; }
    """
    
    print(f"🎯 Testando detecção de CSS em diferentes cenários:")
    
    # Testa todos os headings
    for i in range(1, 7):
        tags = soup.find_all(f"h{i}")
        for idx, tag in enumerate(tags):
            texto = tag.get_text(strip=True)
            analise_css = analisar_css_ocultacao(tag, css_global)
            analise_pai = analisar_css_pai(tag, css_global)
            
            print(f"\n  H{i} #{idx+1}: '{texto[:30]}{'...' if len(texto) > 30 else ''}'")
            print(f"    Vazio: {'SIM' if not texto else 'NÃO'}")
            print(f"    CSS Oculto: {'SIM' if analise_css['tem_ocultacao'] else 'NÃO'}")
            if analise_css['tem_ocultacao']:
                print(f"    Problemas CSS: {analise_css['problemas_css']}")
            print(f"    Pai Oculto: {'SIM' if analise_pai['pai_oculto'] else 'NÃO'}")
            if analise_pai['pai_oculto']:
                print(f"    Problemas Pai: {analise_pai['motivos_pai']}")
    
    print(f"\n✅ Teste de detecção CSS concluído")


if __name__ == "__main__":
    testar_deteccao_css()