# priorizacao_pipeline.py - INTELIGÊNCIA ESTRATÉGICA SEO ENTERPRISE 3.0
# 🧠 Sistema de Priorização Automática para Auditorias SEO

import pandas as pd
import os
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

class SEOPriorizationEngine:
    """🧠 Engine de Inteligência para Priorização Automática de SEO"""
    
    def __init__(self, excel_path: str):
        self.excel_path = excel_path
        self.scoring_matrix = self._init_scoring_matrix()
        self.problemas_detectados = []
        
    def _init_scoring_matrix(self) -> dict:
        """📊 Matriz de Scoring Inteligente"""
        return {
            # PROBLEMAS ESTRUTURAIS CRÍTICOS (Afetam indexação)
            'title_ausente': {
                'gravidade': 100, 'impacto_seo': 100, 'esforco': 20, 
                'categoria': 'ESTRUTURAL', 'descricao': 'Tags <title> ausentes ou vazias'
            },
            'h1_ausente': {
                'gravidade': 95, 'impacto_seo': 90, 'esforco': 25,
                'categoria': 'ESTRUTURAL', 'descricao': 'H1 ausente compromete hierarquia'
            },
            'h1_duplicado': {
                'gravidade': 90, 'impacto_seo': 85, 'esforco': 30,
                'categoria': 'CONTEUDO', 'descricao': 'H1 duplicado entre páginas'
            },
            'ssl_problemas': {
                'gravidade': 100, 'impacto_seo': 80, 'esforco': 60,
                'categoria': 'INFRAESTRUTURA', 'descricao': 'Problemas SSL afetam crawling'
            },
            'errors_5xx': {
                'gravidade': 100, 'impacto_seo': 90, 'esforco': 40,
                'categoria': 'INFRAESTRUTURA', 'descricao': 'Erros de servidor críticos'
            },
            'errors_4xx': {
                'gravidade': 70, 'impacto_seo': 60, 'esforco': 35,
                'categoria': 'CONTEUDO', 'descricao': 'Páginas não encontradas (404, 403)'
            },
            'redirects_longos': {
                'gravidade': 60, 'impacto_seo': 70, 'esforco': 50,
                'categoria': 'INFRAESTRUTURA', 'descricao': 'Cadeias de redirect muito longas'
            },
            
            # PROBLEMAS DE CONTEÚDO (Afetam relevância)
            'description_ausente': {
                'gravidade': 60, 'impacto_seo': 40, 'esforco': 10,
                'categoria': 'CONTEUDO', 'descricao': 'Meta descriptions ausentes'
            },
            'title_duplicado': {
                'gravidade': 80, 'impacto_seo': 75, 'esforco': 25,
                'categoria': 'CONTEUDO', 'descricao': 'Titles duplicados entre páginas'
            },
            'description_duplicado': {
                'gravidade': 50, 'impacto_seo': 45, 'esforco': 20,
                'categoria': 'CONTEUDO', 'descricao': 'Descriptions duplicadas'
            },
            'headings_vazios': {
                'gravidade': 30, 'impacto_seo': 20, 'esforco': 15,
                'categoria': 'CONTEUDO', 'descricao': 'Headings com lixo estrutural'
            },
            'headings_estrutura': {
                'gravidade': 50, 'impacto_seo': 40, 'esforco': 30,
                'categoria': 'CONTEUDO', 'descricao': 'Hierarquia de headings quebrada'
            },
            
            # PROBLEMAS DE SEGURANÇA
            'http_inseguro': {
                'gravidade': 85, 'impacto_seo': 70, 'esforco': 45,
                'categoria': 'SEGURANCA', 'descricao': 'Links HTTP em páginas HTTPS'
            }
        }
    
    def _calcular_score_final(self, gravidade: int, impacto: int, esforco: int) -> int:
        """🎯 Fórmula cirúrgica de scoring"""
        # Score = min(Gravidade, Impacto) - (Esforço * 0.2)
        score_base = min(gravidade, impacto)
        penalidade_esforco = esforco * 0.2
        score_final = max(0, score_base - penalidade_esforco)
        return int(score_final)
    
    def _classificar_prioridade(self, score: int) -> str:
        """🏷️ Classificação automática de prioridade"""
        if score >= 85:
            return "🔥 URGENTE"
        elif score >= 70:
            return "⚠️ ALTA"
        elif score >= 50:
            return "🚧 MÉDIA"
        else:
            return "⏳ BAIXA"
    
    def analisar_title_ausente(self, df: pd.DataFrame) -> list:
        """🎯 Analisa problemas de title ausente"""
        problemas = []
        if not df.empty:
            for _, row in df.iterrows():
                if row.get('Tipo_Problema') in ['TAG_AUSENTE', 'TAG_VAZIA']:
                    config = self.scoring_matrix['title_ausente']
                    score = self._calcular_score_final(
                        config['gravidade'], config['impacto_seo'], config['esforco']
                    )
                    
                    problemas.append({
                        'problema': 'Title Ausente',
                        'url': row.get('URL', ''),
                        'score': score,
                        'prioridade': self._classificar_prioridade(score),
                        'categoria': config['categoria'],
                        'aba_origem': 'Title_Ausente',
                        'recomendacao': 'Criar titles únicos e descritivos (30-60 chars)',
                        'impacto_estimado': 'ALTO - Impacta diretamente SERP e CTR'
                    })
        return problemas
    
    def analisar_ssl_problemas(self, df: pd.DataFrame) -> list:
        """🔒 Analisa problemas SSL detectados"""
        problemas = []
        if not df.empty:
            # Verifica domínios com problemas SSL
            dominios_com_problema = df[df['Problema'] != 'OK']
            
            for _, row in dominios_com_problema.iterrows():
                problema_ssl = row.get('Problema', '')
                grade = row.get('Grade', '')
                impacto = row.get('Impacto_SEO', 'MÉDIO')
                
                # Determina score baseado no tipo de problema
                if 'expirado' in problema_ssl.lower() or 'timeout' in problema_ssl.lower():
                    config = self.scoring_matrix['ssl_problemas']
                    config['gravidade'] = 100  # Crítico
                elif 'cadeia' in problema_ssl.lower():
                    config = self.scoring_matrix['ssl_problemas'] 
                    config['gravidade'] = 95   # Muito alto
                elif 'expirando' in problema_ssl.lower():
                    config = self.scoring_matrix['ssl_problemas']
                    config['gravidade'] = 80   # Alto
                else:
                    config = self.scoring_matrix['ssl_problemas']
                    config['gravidade'] = 70   # Padrão
                
                score = self._calcular_score_final(
                    config['gravidade'], config['impacto_seo'], config['esforco']
                )
                
                problemas.append({
                    'problema': f'SSL: {problema_ssl}',
                    'url': row.get('Dominio', ''),
                    'score': score,
                    'prioridade': self._classificar_prioridade(score),
                    'categoria': config['categoria'],
                    'aba_origem': 'SSL_Problemas',
                    'recomendacao': row.get('Recomendacao', 'Verificar configuração SSL'),
                    'impacto_estimado': f'{impacto} - SSL afeta crawling e trust'
                })
        
        return problemas
    
    def analisar_description_ausente(self, df: pd.DataFrame) -> list:
        """📝 Analisa problemas de description ausente"""
        problemas = []
        if not df.empty:
            for _, row in df.iterrows():
                if row.get('Tipo_Problema') in ['TAG_AUSENTE', 'TAG_VAZIA']:
                    config = self.scoring_matrix['description_ausente']
                    score = self._calcular_score_final(
                        config['gravidade'], config['impacto_seo'], config['esforco']
                    )
                    
                    problemas.append({
                        'problema': 'Description Ausente',
                        'url': row.get('URL', ''),
                        'score': score,
                        'prioridade': self._classificar_prioridade(score),
                        'categoria': config['categoria'],
                        'aba_origem': 'Description_Ausente',
                        'recomendacao': 'Criar descriptions únicas (120-160 chars)',
                        'impacto_estimado': 'MÉDIO - Impacta CTR na SERP'
                    })
        return problemas
    
    def analisar_estrutura_headings(self, df: pd.DataFrame) -> list:
        """🔬 Analisa problemas estruturais de headings"""
        problemas = []
        if not df.empty:
            for _, row in df.iterrows():
                h1_ausente = row.get('H1_Ausente') == 'Sim'
                h1_duplicado = row.get('H1_Duplicado') == 'Sim'
                hierarquia_ok = row.get('Hierarquia_OK') == 'Não'
                
                if h1_ausente:
                    config = self.scoring_matrix['h1_ausente']
                    score = self._calcular_score_final(
                        config['gravidade'], config['impacto_seo'], config['esforco']
                    )
                    
                    problemas.append({
                        'problema': 'H1 Ausente',
                        'url': row.get('URL', ''),
                        'score': score,
                        'prioridade': self._classificar_prioridade(score),
                        'categoria': config['categoria'],
                        'aba_origem': 'Estrutura_Headings',
                        'recomendacao': 'Adicionar H1 único por página',
                        'impacto_estimado': 'ALTO - H1 é fundamental para relevância'
                    })
                
                if h1_duplicado:
                    config = self.scoring_matrix['h1_duplicado']
                    score = self._calcular_score_final(
                        config['gravidade'], config['impacto_seo'], config['esforco']
                    )
                    
                    problemas.append({
                        'problema': 'H1 Duplicado',
                        'url': row.get('URL', ''),
                        'score': score,
                        'prioridade': self._classificar_prioridade(score),
                        'categoria': config['categoria'],
                        'aba_origem': 'Estrutura_Headings',
                        'recomendacao': 'Tornar H1 único por página',
                        'impacto_estimado': 'ALTO - Confunde relevância da página'
                    })
        return problemas
    
    def analisar_duplicatas(self, df_title: pd.DataFrame, df_desc: pd.DataFrame) -> list:
        """🔄 Analisa problemas de duplicação"""
        problemas = []
        
        # Title duplicado
        if not df_title.empty:
            urls_title_dup = df_title[df_title['Tipo_Linha'] == 'URL_INDIVIDUAL']
            
            config = self.scoring_matrix['title_duplicado']
            score = self._calcular_score_final(
                config['gravidade'], config['impacto_seo'], config['esforco']
            )
            
            if len(urls_title_dup) > 0:
                problemas.append({
                    'problema': 'Titles Duplicados',
                    'url': f"{len(urls_title_dup)} URLs afetadas",
                    'score': score,
                    'prioridade': self._classificar_prioridade(score),
                    'categoria': config['categoria'],
                    'aba_origem': 'Title_Duplicado',
                    'recomendacao': 'Revisar template e criar titles únicos',
                    'impacto_estimado': 'ALTO - Canibalização de palavras-chave'
                })
        
        # Description duplicado
        if not df_desc.empty:
            urls_desc_dup = df_desc[df_desc['Tipo_Linha'] == 'URL_INDIVIDUAL']
            
            config = self.scoring_matrix['description_duplicado']
            score = self._calcular_score_final(
                config['gravidade'], config['impacto_seo'], config['esforco']
            )
            
            if len(urls_desc_dup) > 0:
                problemas.append({
                    'problema': 'Descriptions Duplicadas',
                    'url': f"{len(urls_desc_dup)} URLs afetadas",
                    'score': score,
                    'prioridade': self._classificar_prioridade(score),
                    'categoria': config['categoria'],
                    'aba_origem': 'Description_Duplicado',
                    'recomendacao': 'Criar descriptions únicas por página',
                    'impacto_estimado': 'MÉDIO - Reduz atratividade na SERP'
                })
        
        return problemas
    
    def analisar_erros(self, df_4xx: pd.DataFrame, df_5xx: pd.DataFrame) -> list:
        """💥 Analisa erros 4xx e 5xx"""
        problemas = []
        
        # Erros 5xx
        if not df_5xx.empty:
            config = self.scoring_matrix['errors_5xx']
            score = self._calcular_score_final(
                config['gravidade'], config['impacto_seo'], config['esforco']
            )
            
            problemas.append({
                'problema': 'Erros 5xx (Servidor)',
                'url': f"{len(df_5xx)} URLs com erro de servidor",
                'score': score,
                'prioridade': self._classificar_prioridade(score),
                'categoria': config['categoria'],
                'aba_origem': 'Errors_5xx',
                'recomendacao': 'Correção urgente na infraestrutura',
                'impacto_estimado': 'CRÍTICO - Páginas inacessíveis ao Googlebot'
            })
        
        # Erros 4xx
        if not df_4xx.empty:
            config = self.scoring_matrix['errors_4xx']
            score = self._calcular_score_final(
                config['gravidade'], config['impacto_seo'], config['esforco']
            )
            
            problemas.append({
                'problema': 'Erros 4xx (Cliente)',
                'url': f"{len(df_4xx)} URLs com erro 404/403",
                'score': score,
                'prioridade': self._classificar_prioridade(score),
                'categoria': config['categoria'],
                'aba_origem': 'Errors_4xx',
                'recomendacao': 'Corrigir URLs ou implementar redirects 301',
                'impacto_estimado': 'ALTO - Links quebrados prejudicam experiência'
            })
        
        return problemas
    
    def analisar_redirects(self, df: pd.DataFrame) -> list:
        """🔄 Analisa problemas de redirects"""
        problemas = []
        if not df.empty:
            # Verifica redirects com muitas etapas na cadeia
            redirects_longos = df[df['Cadeia_Redirects'] > 2]
            
            if len(redirects_longos) > 0:
                config = self.scoring_matrix['redirects_longos']
                score = self._calcular_score_final(
                    config['gravidade'], config['impacto_seo'], config['esforco']
                )
                
                problemas.append({
                    'problema': 'Cadeias de Redirect Longas',
                    'url': f"{len(redirects_longos)} URLs com cadeia >2 redirects",
                    'score': score,
                    'prioridade': self._classificar_prioridade(score),
                    'categoria': config['categoria'],
                    'aba_origem': 'Redirects_3xx',
                    'recomendacao': 'Simplificar cadeias para 1 redirect máximo',
                    'impacto_estimado': 'MÉDIO - Desperdício de crawl budget'
                })
        return problemas
    
    def processar_excel_completo(self) -> dict:
        """📊 Processa todas as abas do Excel e gera inteligência"""
        
        print(f"🧠 PROCESSANDO INTELIGÊNCIA ESTRATÉGICA...")
        print(f"   📁 Arquivo: {os.path.basename(self.excel_path)}")
        
        try:
            # Lê todas as abas do Excel
            excel_data = pd.read_excel(self.excel_path, sheet_name=None)
            print(f"   📋 {len(excel_data)} abas detectadas")
            
            # Processa cada aba com análise específica
            todos_problemas = []
            
            # Title Ausente
            if 'Title_Ausente' in excel_data:
                problemas = self.analisar_title_ausente(excel_data['Title_Ausente'])
                todos_problemas.extend(problemas)
                print(f"      🎯 Title_Ausente: {len(problemas)} problemas")
            
            # Description Ausente
            if 'Description_Ausente' in excel_data:
                problemas = self.analisar_description_ausente(excel_data['Description_Ausente'])
                todos_problemas.extend(problemas)
                print(f"      📝 Description_Ausente: {len(problemas)} problemas")
            
            # Estrutura Headings
            if 'Estrutura_Headings' in excel_data:
                problemas = self.analisar_estrutura_headings(excel_data['Estrutura_Headings'])
                todos_problemas.extend(problemas)
                print(f"      🔬 Estrutura_Headings: {len(problemas)} problemas")
            
            # Duplicatas
            df_title_dup = excel_data.get('Title_Duplicado', pd.DataFrame())
            df_desc_dup = excel_data.get('Description_Duplicado', pd.DataFrame())
            problemas = self.analisar_duplicatas(df_title_dup, df_desc_dup)
            todos_problemas.extend(problemas)
            print(f"      🔄 Duplicatas: {len(problemas)} problemas")
            
            # Erros
            df_4xx = excel_data.get('Errors_4xx', pd.DataFrame())
            df_5xx = excel_data.get('Errors_5xx', pd.DataFrame())
            problemas = self.analisar_erros(df_4xx, df_5xx)
            todos_problemas.extend(problemas)
            print(f"      💥 Erros: {len(problemas)} problemas")
            
            # Redirects
            if 'Redirects_3xx' in excel_data:
                problemas = self.analisar_redirects(excel_data['Redirects_3xx'])
                todos_problemas.extend(problemas)
                print(f"      🔄 Redirects: {len(problemas)} problemas")
            
            # SSL Problemas 🆕
            if 'SSL_Problemas' in excel_data:
                problemas = self.analisar_ssl_problemas(excel_data['SSL_Problemas'])
                todos_problemas.extend(problemas)
                print(f"      🔒 SSL_Problemas: {len(problemas)} problemas")
            
            # Ordena por score (maior primeiro)
            todos_problemas.sort(key=lambda x: x['score'], reverse=True)
            
            print(f"   ✅ TOTAL: {len(todos_problemas)} problemas analisados")
            
            return {
                'problemas': todos_problemas,
                'excel_data': excel_data,
                'total_abas': len(excel_data)
            }
            
        except Exception as e:
            print(f"❌ Erro no processamento: {e}")
            return {'problemas': [], 'excel_data': {}, 'total_abas': 0}
    
    def gerar_backlog_prioridades(self, analise: dict, output_path: str) -> str:
        """📊 Gera planilha de backlog priorizado"""
        
        problemas = analise['problemas']
        
        if not problemas:
            print("   ⚠️ Nenhum problema detectado para priorização")
            return ""
        
        # Cria DataFrame do backlog
        df_backlog = pd.DataFrame(problemas)
        
        # Reordena colunas
        colunas_order = [
            'prioridade', 'problema', 'url', 'score', 'categoria',
            'aba_origem', 'recomendacao', 'impacto_estimado'
        ]
        df_backlog = df_backlog[colunas_order]
        
        # Renomeia colunas para output final
        df_backlog.columns = [
            'Prioridade', 'Problema', 'URLs_Afetadas', 'Score', 'Categoria',
            'Aba_Origem', 'Recomendação', 'Impacto_Estimado'
        ]
        
        # Gera arquivo
        backlog_path = output_path.replace('.xlsx', '_BACKLOG_PRIORIDADES.xlsx')
        
        with pd.ExcelWriter(backlog_path, engine='xlsxwriter') as writer:
            df_backlog.to_excel(writer, sheet_name='Backlog_Prioridades', index=False)
            
            # Adiciona resumo executivo
            self._gerar_resumo_executivo(problemas, writer)
        
        print(f"   📊 Backlog gerado: {os.path.basename(backlog_path)}")
        return backlog_path
    
    def _gerar_resumo_executivo(self, problemas: list, writer) -> None:
        """📈 Gera aba de resumo executivo"""
        
        # Estatísticas
        total = len(problemas)
        urgentes = len([p for p in problemas if p['prioridade'] == '🔥 URGENTE'])
        altas = len([p for p in problemas if p['prioridade'] == '⚠️ ALTA'])
        medias = len([p for p in problemas if p['prioridade'] == '🚧 MÉDIA'])
        baixas = len([p for p in problemas if p['prioridade'] == '⏳ BAIXA'])
        
        # Por categoria
        categorias = {}
        for problema in problemas:
            cat = problema['categoria']
            categorias[cat] = categorias.get(cat, 0) + 1
        
        # Cria resumo
        resumo_data = [
            ['📊 RESUMO EXECUTIVO SEO', ''],
            ['', ''],
            ['Total de problemas mapeados:', total],
            ['🔥 Problemas URGENTES:', urgentes],
            ['⚠️ Problemas ALTA prioridade:', altas],
            ['🚧 Problemas MÉDIA prioridade:', medias],
            ['⏳ Problemas BAIXA prioridade:', baixas],
            ['', ''],
            ['📋 DISTRIBUIÇÃO POR CATEGORIA:', ''],
            ['', ''],
        ]
        
        for categoria, count in sorted(categorias.items()):
            resumo_data.append([f'• {categoria}:', count])
        
        resumo_data.extend([
            ['', ''],
            ['🎯 RECOMENDAÇÃO ESTRATÉGICA:', ''],
            ['', ''],
            ['1. Resolver problemas URGENTES imediatamente', ''],
            ['2. Planejar correção de problemas ALTA prioridade', ''],
            ['3. Incluir melhorias MÉDIA/BAIXA no roadmap', ''],
            ['', ''],
            [f'📅 Relatório gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M")}', '']
        ])
        
        df_resumo = pd.DataFrame(resumo_data, columns=['Métrica', 'Valor'])
        df_resumo.to_excel(writer, sheet_name='Resumo_Executivo', index=False)

def executar_priorizacao_completa(excel_path: str) -> str:
    """🚀 Execução completa da priorização automática"""
    
    print(f"\n🧠 INICIANDO INTELIGÊNCIA ESTRATÉGICA SEO...")
    
    # Inicializa engine
    engine = SEOPriorizationEngine(excel_path)
    
    # Processa análise completa
    analise = engine.processar_excel_completo()
    
    # Gera backlog priorizado
    backlog_path = engine.gerar_backlog_prioridades(analise, excel_path)
    
    print(f"\n🎉 INTELIGÊNCIA ESTRATÉGICA CONCLUÍDA!")
    print(f"📁 Backlog: {os.path.basename(backlog_path)}")
    
    return backlog_path

# Exemplo de uso
if __name__ == "__main__":
    # Exemplo de execução
    excel_exemplo = "relatorio_seo_ccgsaude_com_br.xlsx"
    
    if os.path.exists(excel_exemplo):
        backlog_path = executar_priorizacao_completa(excel_exemplo)
        print(f"✅ Priorização concluída: {backlog_path}")
    else:
        print(f"❌ Arquivo não encontrado: {excel_exemplo}")