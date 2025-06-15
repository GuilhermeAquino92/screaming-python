# exporters/sheets/ssl_problemas_sheet.py - ENGINE CIRÚRGICA SSL
# 🔒 ENGINE CIRÚRGICA: Análise SSL completa para auditoria de infraestrutura SEO

import ssl
import socket
import pandas as pd
import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from exporters.base_exporter import BaseSheetExporter

class SSLProblemasSheet(BaseSheetExporter):
    def __init__(self, df, writer):
        super().__init__(df, writer)
        
    def export(self):
        """🔒 Gera aba CIRÚRGICA de problemas SSL"""
        try:
            print("🔒 SSL PROBLEMAS - ENGINE CIRÚRGICA")
            
            # Extrai domínios únicos
            urls = self.df['url'].dropna().unique().tolist()
            dominios = self._extrair_dominios_unicos(urls)
            
            print(f"   📊 URLs no DataFrame: {len(urls)}")
            print(f"   🌐 Domínios únicos detectados: {len(dominios)}")
            print(f"   🎯 Foco: Análise SSL que impacta crawling do Googlebot")
            
            if not dominios:
                print("   ⚠️ Nenhum domínio válido para análise SSL")
                df_vazio = pd.DataFrame(columns=[
                    'Dominio', 'Subject', 'Emitido_Por', 'Valido_Ate', 'Dias_Restantes',
                    'Chain_OK', 'Grade', 'Problema', 'Impacto_SEO', 'Recomendacao'
                ])
                df_vazio.to_excel(self.writer, index=False, sheet_name="SSL_Problemas")
                return df_vazio
            
            # Análise SSL paralela
            resultados = self._analisar_ssl_paralelo(dominios)
            
            # Cria DataFrame
            df_ssl = pd.DataFrame(resultados)
            
            # Ordena por problemas primeiro
            df_ssl['sort_problema'] = df_ssl['Problema'].apply(lambda x: 0 if x != 'OK' else 1)
            df_ssl = df_ssl.sort_values(['sort_problema', 'Grade', 'Dominio'])
            df_ssl = df_ssl.drop('sort_problema', axis=1)
            
            # Exporta
            df_ssl.to_excel(self.writer, index=False, sheet_name="SSL_Problemas")
            
            # Estatísticas
            total_dominios = len(resultados)
            problemas_ssl = len([r for r in resultados if r['Problema'] != 'OK'])
            ssl_ok = total_dominios - problemas_ssl
            
            print(f"   ✅ Domínios analisados: {total_dominios}")
            print(f"   🔒 SSL OK: {ssl_ok}")
            print(f"   🚨 Problemas SSL: {problemas_ssl}")
            
            # Stats por tipo de problema
            tipos_problema = {}
            for resultado in resultados:
                if resultado['Problema'] != 'OK':
                    problema = resultado['Problema']
                    tipos_problema[problema] = tipos_problema.get(problema, 0) + 1
            
            for problema, count in tipos_problema.items():
                print(f"      • {problema}: {count} domínios")
            
            print(f"   📋 Aba 'SSL_Problemas' criada com análise CIRÚRGICA")
            print(f"   🎯 Impacto: SSL quebrado = Googlebot limitado")
            
            return df_ssl
            
        except Exception as e:
            print(f"❌ Erro no engine SSL: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback
            df_erro = pd.DataFrame(columns=[
                'Dominio', 'Subject', 'Emitido_Por', 'Valido_Ate', 'Dias_Restantes',
                'Chain_OK', 'Grade', 'Problema', 'Impacto_SEO', 'Recomendacao'
            ])
            df_erro.to_excel(self.writer, index=False, sheet_name="SSL_Problemas")
            return df_erro

    def _extrair_dominios_unicos(self, urls: list) -> list:
        """🌐 Extrai domínios raiz únicos das URLs"""
        dominios = set()
        
        for url in urls:
            try:
                parsed = urlparse(url)
                hostname = parsed.hostname
                
                if hostname:
                    # Remove subdominios comuns para focar no domínio principal
                    parts = hostname.split('.')
                    if len(parts) >= 2:
                        # Mantém apenas domínio.tld (remove www, api, etc.)
                        if len(parts) >= 3 and parts[0] in ['www', 'api', 'blog', 'shop']:
                            dominio_principal = '.'.join(parts[1:])
                        else:
                            dominio_principal = hostname
                        dominios.add(dominio_principal)
                    
            except Exception:
                continue
        
        return list(dominios)

    def _analisar_ssl_cirurgico(self, dominio: str) -> dict:
        """🔒 Análise SSL cirúrgica de um domínio"""
        
        try:
            # Cria contexto SSL
            contexto = ssl.create_default_context()
            
            # Conecta via SSL
            with socket.create_connection((dominio, 443), timeout=10) as sock:
                with contexto.wrap_socket(sock, server_hostname=dominio) as ssock:
                    # Obtém certificado
                    cert = ssock.getpeercert()
                    
                    # Analisa certificado
                    valido_ate = datetime.datetime.strptime(cert['notAfter'], "%b %d %H:%M:%S %Y %Z")
                    valido_desde = datetime.datetime.strptime(cert['notBefore'], "%b %d %H:%M:%S %Y %Z")
                    
                    dias_restantes = (valido_ate - datetime.datetime.utcnow()).days
                    
                    # Extrai informações do certificado
                    issuer = dict(x[0] for x in cert['issuer']).get('organizationName', 
                             dict(x[0] for x in cert['issuer']).get('O', 'Desconhecido'))
                    
                    subject = dict(x[0] for x in cert['subject']).get('commonName', dominio)
                    
                    # Verifica chain SSL
                    chain_ok = self._validar_chain_ssl(ssock, cert)
                    
                    # Calcula grade SSL
                    grade = self._calcular_grade_ssl(chain_ok, dias_restantes, cert)
                    
                    # Detecta problemas
                    problema, impacto, recomendacao = self._detectar_problemas_ssl(
                        chain_ok, dias_restantes, cert, dominio
                    )
                    
                    return {
                        'Dominio': dominio,
                        'Subject': subject,
                        'Emitido_Por': issuer,
                        'Valido_Ate': valido_ate.strftime("%Y-%m-%d"),
                        'Dias_Restantes': dias_restantes,
                        'Chain_OK': 'Sim' if chain_ok else 'Não',
                        'Grade': grade,
                        'Problema': problema,
                        'Impacto_SEO': impacto,
                        'Recomendacao': recomendacao
                    }
        
        except socket.timeout:
            return {
                'Dominio': dominio,
                'Subject': '',
                'Emitido_Por': '',
                'Valido_Ate': '',
                'Dias_Restantes': 0,
                'Chain_OK': 'Não',
                'Grade': 'FAIL',
                'Problema': 'Timeout SSL',
                'Impacto_SEO': 'CRÍTICO',
                'Recomendacao': 'Servidor SSL não responde - verificar configuração'
            }
        
        except ssl.SSLError as e:
            ssl_error = str(e)
            return {
                'Dominio': dominio,
                'Subject': '',
                'Emitido_Por': '',
                'Valido_Ate': '',
                'Dias_Restantes': 0,
                'Chain_OK': 'Não',
                'Grade': 'FAIL',
                'Problema': f'Erro SSL: {ssl_error[:50]}',
                'Impacto_SEO': 'CRÍTICO',
                'Recomendacao': 'Corrigir configuração SSL - Googlebot pode falhar'
            }
        
        except socket.gaierror:
            return {
                'Dominio': dominio,
                'Subject': '',
                'Emitido_Por': '',
                'Valido_Ate': '',
                'Dias_Restantes': 0,
                'Chain_OK': 'Não',
                'Grade': 'FAIL',
                'Problema': 'DNS não resolve',
                'Impacto_SEO': 'CRÍTICO',
                'Recomendacao': 'Verificar configuração DNS'
            }
        
        except Exception as e:
            return {
                'Dominio': dominio,
                'Subject': '',
                'Emitido_Por': '',
                'Valido_Ate': '',
                'Dias_Restantes': 0,
                'Chain_OK': 'Não',
                'Grade': 'FAIL',
                'Problema': f'Erro: {str(e)[:50]}',
                'Impacto_SEO': 'ALTO',
                'Recomendacao': 'Investigar problema SSL específico'
            }

    def _validar_chain_ssl(self, ssock, cert) -> bool:
        """🔗 Valida se a cadeia SSL está completa"""
        try:
            # Verifica se conseguiu obter certificado completo
            if not cert:
                return False
            
            # Verifica se há issuer válido
            issuer = dict(x[0] for x in cert.get('issuer', []))
            if not issuer:
                return False
            
            # Se chegou até aqui sem erro SSL, chain provavelmente está OK
            # (Análise mais profunda requereria bibliotecas específicas)
            return True
            
        except Exception:
            return False

    def _calcular_grade_ssl(self, chain_ok: bool, dias_restantes: int, cert: dict) -> str:
        """📊 Calcula grade SSL simplificada"""
        
        # Grade base por validade
        if dias_restantes < 0:
            return "F"  # Expirado
        elif not chain_ok:
            return "C"  # Chain incompleta
        elif dias_restantes < 15:
            return "D"  # Expirando muito em breve
        elif dias_restantes < 30:
            return "C"  # Expirando em breve
        elif dias_restantes < 90:
            return "B"  # Válido por pouco tempo
        else:
            # Verifica outros fatores para grade A
            try:
                # Verifica algoritmo de assinatura
                sig_algorithm = cert.get('signatureAlgorithm', '').lower()
                if 'sha1' in sig_algorithm:
                    return "B"  # SHA1 é fraco
                elif 'sha256' in sig_algorithm or 'sha384' in sig_algorithm:
                    return "A"  # SHA256+ é bom
                else:
                    return "A-"  # Desconhecido
            except:
                return "A-"

    def _detectar_problemas_ssl(self, chain_ok: bool, dias_restantes: int, 
                               cert: dict, dominio: str) -> tuple:
        """🚨 Detecta problemas específicos de SSL"""
        
        # Certificado expirado
        if dias_restantes < 0:
            return (
                "SSL expirado",
                "CRÍTICO",
                "Renovar certificado SSL imediatamente"
            )
        
        # Chain incompleta
        if not chain_ok:
            return (
                "Cadeia SSL incompleta",
                "ALTO",
                "Configurar CA intermediário completo"
            )
        
        # Expirando em breve
        if dias_restantes < 15:
            return (
                "SSL expirando em breve",
                "ALTO",
                f"Renovar SSL - expira em {dias_restantes} dias"
            )
        elif dias_restantes < 30:
            return (
                "SSL expirando",
                "MÉDIO",
                f"Planejar renovação SSL - expira em {dias_restantes} dias"
            )
        
        # Verifica mismatch de domínio
        try:
            subject = dict(x[0] for x in cert['subject']).get('commonName', '')
            if subject and dominio not in subject and subject not in dominio:
                # Verifica SAN (Subject Alternative Names)
                san_domains = []
                for ext in cert.get('subjectAltName', []):
                    if ext[0] == 'DNS':
                        san_domains.append(ext[1])
                
                if dominio not in san_domains:
                    return (
                        "Mismatch de domínio",
                        "ALTO",
                        f"Certificado para '{subject}' não cobre '{dominio}'"
                    )
        except:
            pass
        
        # Algoritmo fraco
        try:
            sig_algorithm = cert.get('signatureAlgorithm', '').lower()
            if 'sha1' in sig_algorithm:
                return (
                    "Algoritmo SSL fraco",
                    "MÉDIO",
                    "Atualizar para SHA256 ou superior"
                )
        except:
            pass
        
        # SSL OK
        return ("OK", "BAIXO", "SSL configurado corretamente")

    def _analisar_ssl_paralelo(self, dominios: list) -> list:
        """🚀 Análise SSL paralela"""
        
        print(f"🔒 Análise SSL iniciada: {len(dominios)} domínios")
        
        resultados = []
        
        # Menos threads para SSL (pode ser lento)
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_dominio = {
                executor.submit(self._analisar_ssl_cirurgico, dominio): dominio
                for dominio in dominios
            }
            
            for future in as_completed(future_to_dominio):
                resultado = future.result()
                resultados.append(resultado)
                
                # Progress
                print(f"      🔒 Analisado: {resultado['Dominio']} - Grade: {resultado['Grade']}")
        
        return resultados