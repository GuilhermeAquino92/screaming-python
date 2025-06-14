# exporters/sheets/title_ausente_sheet.py - VERSÃO CIRÚRGICA

import pandas as pd
from exporters.base_exporter import BaseSheetExporter

# 🚫 BLACKLIST PLACEHOLDERS
PLACEHOLDERS_BLACKLIST = [
    'loading', 'carregando', 'app', 'react app', 'vue app', 'angular app',
    'untitled', 'no title', 'sem título', 'title', 'página', 'page',
    'document', 'default', 'index', 'main', 'home', 'null', 'undefined'
]

# 🚫 EXTENSÕES BLOQUEADAS
EXTENSOES_BLOQUEADAS = (
    '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.ico',
    '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.rar',
    '.js', '.css', '.mp3', '.mp4', '.avi', '.mov', '.xml', '.json'
)

def is_title_ausente(title):
    """🎯 Detecta se title está ausente ou inválido"""
    # 1. None ou não-string
    if title is None:
        return True, 'TAG_AUSENTE'
    
    # 2. Converte para string se necessário
    if not isinstance(title, str):
        title_str = str(title).strip()
        if title_str.lower() in ['nan', 'none', 'null', '']:
            return True, 'TAG_AUSENTE'
        title = title_str
    
    # 3. String vazia
    title_clean = title.strip()
    if not title_clean:
        return True, 'TITLE_VAZIO'
    
    # 4. Muito curto (≤3 caracteres)
    if len(title_clean) <= 3:
        return True, 'TITLE_MUITO_CURTO'
    
    # 5. Placeholder blacklist
    if title_clean.lower() in PLACEHOLDERS_BLACKLIST:
        return True, 'PLACEHOLDER'
    
    # 6. OK - título válido
    return False, 'VALIDO'

def is_url_valida(url):
    """🚫 Remove arquivos não-HTML"""
    if not url or not isinstance(url, str):
        return False
    
    return not url.lower().endswith(EXTENSOES_BLOQUEADAS)

class TitleAusenteSheetHardened(BaseSheetExporter):
    def export(self):
        """🎯 Validador CIRÚRGICO - direto ao ponto"""
        try:
            print(f"🎯 Validando titles ausentes...")
            
            rows = []
            total_inicial = len(self.df)
            filtradas = 0
            analisadas = 0
            problemas = 0
            
            for _, row in self.df.iterrows():
                url = row.get('url', '')
                
                # 🚫 Filtra URLs inválidas
                if not is_url_valida(url):
                    filtradas += 1
                    continue
                
                # 🚫 Filtra paginações
                if '?page=' in url or '&page=' in url:
                    filtradas += 1
                    continue
                
                # 🚫 Só páginas 200 ou 3xx
                status = row.get('status_code_http', row.get('status_code', None))
                if status is not None:
                    try:
                        status_num = int(float(status))
                        if status_num != 200 and not (300 <= status_num < 400):
                            filtradas += 1
                            continue
                    except:
                        filtradas += 1
                        continue
                
                # 🚫 Só HTML
                content_type = row.get('tipo_conteudo_http', row.get('tipo_conteudo', ''))
                if content_type and 'text/html' not in content_type.lower():
                    filtradas += 1
                    continue
                
                analisadas += 1
                
                # 🎯 VALIDAÇÃO DO TITLE
                title = row.get('title', None)
                tem_problema, tipo_problema = is_title_ausente(title)
                
                if tem_problema:
                    problemas += 1
                    
                    # Tipo de página
                    if url.endswith('/') or url.count('/') <= 3:
                        tipo_pagina = 'Homepage/Principal'
                    elif any(x in url.lower() for x in ['/produto', '/product', '/servico']):
                        tipo_pagina = 'Produto/Serviço'
                    elif any(x in url.lower() for x in ['/blog', '/artigo', '/post']):
                        tipo_pagina = 'Blog/Conteúdo'
                    else:
                        tipo_pagina = 'Página Interna'
                    
                    # Gravidade
                    if tipo_problema in ['TAG_AUSENTE', 'TITLE_VAZIO']:
                        gravidade = 'CRÍTICO'
                        prioridade = 'URGENTE'
                    elif tipo_problema == 'PLACEHOLDER':
                        gravidade = 'ALTO'
                        prioridade = 'ALTA'
                    else:
                        gravidade = 'MÉDIO'
                        prioridade = 'MÉDIA'
                    
                    # Recomendação
                    if tipo_problema == 'TAG_AUSENTE':
                        recomendacao = f'Adicionar tag <title> à página'
                    elif tipo_problema == 'TITLE_VAZIO':
                        recomendacao = f'Preencher conteúdo da tag <title>'
                    elif tipo_problema == 'PLACEHOLDER':
                        recomendacao = f'Substituir placeholder "{title}" por título descritivo'
                    else:
                        recomendacao = f'Expandir título para ser mais descritivo'
                    
                    rows.append({
                        'URL': url,
                        'Status_HTTP': status or 'N/A',
                        'Tipo_Problema': tipo_problema,
                        'Title_Atual': title if title else '[AUSENTE]',
                        'Tamanho': len(str(title)) if title else 0,
                        'Tipo_Pagina': tipo_pagina,
                        'Gravidade': gravidade,
                        'Prioridade': prioridade,
                        'Recomendacao': recomendacao
                    })
            
            # 📊 ESTATÍSTICAS
            print(f"   📊 URLs inicial: {total_inicial}")
            print(f"   🚫 Filtradas: {filtradas}")
            print(f"   ✅ Analisadas: {analisadas}")
            print(f"   ⚠️ Problemas: {problemas}")
            
            if problemas == 0:
                print(f"   🎉 PERFEITO: Todos os títulos estão OK!")
                df_vazio = pd.DataFrame(columns=[
                    'URL', 'Status_HTTP', 'Tipo_Problema', 'Title_Atual', 
                    'Tamanho', 'Tipo_Pagina', 'Gravidade', 'Prioridade', 'Recomendacao'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
                return df_vazio
            
            # 📋 CRIA DATAFRAME E ORDENA
            df_problemas = pd.DataFrame(rows)
            
            # Ordena: CRÍTICO primeiro, depois por URL
            gravidade_order = {'CRÍTICO': 1, 'ALTO': 2, 'MÉDIO': 3}
            df_problemas['sort_gravidade'] = df_problemas['Gravidade'].map(gravidade_order)
            df_problemas = df_problemas.sort_values(['sort_gravidade', 'URL'])
            df_problemas = df_problemas.drop('sort_gravidade', axis=1)
            
            # 📤 EXPORTA
            df_problemas.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            
            print(f"   📋 Title_Ausente: {len(df_problemas)} problemas encontrados")
            
            # Resumo por tipo
            for tipo, count in df_problemas['Tipo_Problema'].value_counts().items():
                print(f"      • {tipo}: {count} URLs")
            
            return df_problemas
            
        except Exception as e:
            print(f"❌ Erro no validador: {e}")
            import traceback
            traceback.print_exc()
            
            df_erro = pd.DataFrame(columns=[
                'URL', 'Status_HTTP', 'Tipo_Problema', 'Title_Atual', 
                'Tamanho', 'Tipo_Pagina', 'Gravidade', 'Prioridade', 'Recomendacao'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="Title_Ausente")
            return df_erro

# Compatibilidade com imports antigos
class TitleAusenteSheetSemantico(TitleAusenteSheetHardened):
    pass