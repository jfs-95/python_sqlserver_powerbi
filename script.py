import pandas as pd
import pyodbc

# ABRIR O ARQUIVO
file = 'dados/vendas_att.csv'
df = pd.read_csv(file, encoding='utf-8', sep=';')

# CONVERTER , PRA .
df['dinheiro'] = df['dinheiro'].str.replace(',', '.', regex=False)
df['total_vendas'] = df['total_vendas'].str.replace(',', '.', regex=False)

# CONVERTER OBJETO PARA NUMÉRICO
df['quantidade'] = pd.to_numeric(df['quantidade'])
df['dinheiro'] = pd.to_numeric(df['dinheiro'])
df['total_vendas'] = pd.to_numeric(df['total_vendas'])

# CONVERTER OBJETO PRA DATETIME
df['data'] = pd.to_datetime(df['data'], format='mixed', dayfirst=True)
df['mes'] = df['data'].dt.to_period('M').astype(str)

# AGRUPAMENTOS E TRANSFORMAÇÕES
df_pagamentos = df.groupby(['mes', 'café_nome', 'tipo_dinheiro']).size().reset_index(name='qtd_modo_pg')
df_pivot = df_pagamentos.pivot_table(index=['mes', 'café_nome'], columns='tipo_dinheiro', values='qtd_modo_pg', aggfunc='sum', fill_value=0).reset_index()

df_agrupado = df.groupby(['mes', 'café_nome']).agg(total_pedidos=('quantidade', 'sum')).reset_index()
df_total_vendas = df.groupby(['mes', 'café_nome']).agg(total_vendas_rs=('total_vendas', 'sum')).reset_index()

df_final = pd.merge(df_pivot, df_agrupado, on=['mes', 'café_nome'])
df_final = pd.merge(df_final, df_total_vendas, on=['mes', 'café_nome'])

# CONFIGURAÇÃO DO SQL SERVER
server = 'DESKTOP-NV2QR6N'
database = 'vendas_cafe'
username = 'sa'
password = '1234'

# AUMENTAR O TEMPO LIMITE DE LOGON COM LoginTimeout E USAR try-except-finally
conn = None
cursor = None
try:
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    conn.autocommit = True
    cursor = conn.cursor()

    # CRIAR A TABELA
    create_table_vendas_combinado = '''
        CREATE TABLE vendas_combinado (
            mes CHAR(7),
            café_nome VARCHAR(20),
            cartao INT,   
            dinheiro INT,
            total_pedidos INT,
            total_vendas_rs FLOAT
        )
    '''
    cursor.execute(create_table_vendas_combinado)

    # INSERIR DADOS NA TABELA
    for index, row in df_final.iterrows():
        cursor.execute("""
            INSERT INTO vendas_combinado(mes, café_nome, cartao, dinheiro, total_pedidos, total_vendas_rs)
            VALUES (?, ?, ?, ?, ?, ?)
        """, row['mes'], row['café_nome'], row['cartao'], row['dinheiro'], row['total_pedidos'], row['total_vendas_rs'])

    # CONFIRMAR AS ALTERAÇÕES
    conn.commit()

except pyodbc.Error as e:
    print(f"Erro ao conectar ao SQL Server: {e}")

finally:
    # GARANTIR QUE O CURSOR E A CONEXÃO SEJAM FECHADOS SOMENTE SE FOREM CRIADOS
    if cursor:
        cursor.close()
    if conn:
        conn.close()

print("Processo concluído!")
