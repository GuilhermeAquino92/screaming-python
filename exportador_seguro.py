
def exportar_dados_seguros(df, output_path):
    """📊 Exportador seguro baseado no debug"""
    
    # Limpa dados problemáticos
    df_safe = df.copy()
    
    for col in df_safe.columns:
        if df_safe[col].dtype == 'object':
            def safe_clean(x):
                if x is None:
                    return ''
                elif isinstance(x, (list, dict, tuple)):
                    return str(x)[:300]  # Trunca objetos complexos
                elif isinstance(x, str):
                    # Remove caracteres não-ASCII e trunca
                    clean_str = x.encode('ascii', errors='ignore').decode('ascii')
                    return clean_str[:500]
                else:
                    return str(x)
            
            df_safe[col] = df_safe[col].apply(safe_clean)
    
    # Exporta com context manager
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        df_safe.to_excel(writer, sheet_name='Dados_Seguros', index=False)
    
    return output_path
