#!/usr/bin/env python
# coding: utf-8

# In[24]:


#!pip install pandas
#!pip install pyodbc
#!pip install datetime

import pandas as pd
import pyodbc
import datetime
import openpyxl
import warnings

warnings.filterwarnings('ignore')

#usuário insere a data do brv

data = input('insira a data do arquivo do BRV a ser usado, no formato yyyy-mm-dd. Exemplo "2023-01-30"')

data = data.replace('-','')

ano = '/' + str(data[0:4])


########################################################################################################
########################################################################################################
########################################################################################################
#################################### BRV DA ML #########################################################

try:
    print ('Procurando o arquivo na pasta...')
#ler o aquivo csv de acordo com a data passada pelo usuário e inserir em um dataframe
    df = pd.read_csv(r'\\10.10.5.83\remessas_cargas\BRADESCO VEICULO' + ano + r'\BRV_ML_' + data + '.TXT',sep=';',error_bad_lines=False,encoding = "ISO-8859-1")

except FileNotFoundError:
    print("O arquivo BRV referente a essa data não foi localizado na pasta")

else:
#coluna D substituindo os meses

    print ('Arquivo encontrado. Realizando os ajustes...')

    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('JAN','/01/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('FEB','/02/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('MAR','/03/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('APR','/04/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('MAY','/05/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('JUN','/06/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('JUL','/07/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('AUG','/08/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('SEP','/09/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('OCT','/10/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('NOV','/11/')
    df['DT_VENCTO_PCELA2'] = df['DT_VENCTO_PCELA2'].str.replace('DEC','/12/')


#coluna F e G substituir ponto por barra

    df['DT_INIC_OPER'] = df['DT_INIC_OPER'].str.replace('.','/')
    df['DT_FIM_OPER'] = df['DT_FIM_OPER'].str.replace('.','/')


#coluna AP subitituir ponto por barra e os vazios pela data preenchida

    df['DATA_DA_TRANSACAO'] = df['DATA_DA_TRANSACAO'].str.replace('.','/')

    df2 = df[df.DATA_DA_TRANSACAO.isna() == False]

    data = list(df2['DATA_DA_TRANSACAO'])

    df['DATA_DA_TRANSACAO'] = df['DATA_DA_TRANSACAO'].fillna(data[0])

#Penultima coluna substituir mes

    df['DT_MVTO'] = df['DT_MVTO'].str.replace('JAN','/01/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('FEB','/02/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('MAR','/03/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('APR','/04/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('MAY','/05/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('JUN','/06/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('JUL','/07/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('AUG','/08/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('SEP','/09/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('OCT','/10/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('NOV','/11/')
    df['DT_MVTO'] = df['DT_MVTO'].str.replace('DEC','/12/')


#adicionando coluna escritório
    df = df.assign(ESCRITORIO = "ML")

    df = df.fillna('NULL')

    
    print ('ajustes realizados. Conectando com o banco de dados SQL...')

#conecta no banco de dados
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=plan01spo15;"
                     "Trusted_Connection=yes;")
    
    print ('Conexão realizada. iniciando a importação para a tabela...')

#cria o cursor
    cursor = cnxn.cursor()


    for index, row in df.iterrows():

#Importa para a tabela
        cursor.execute("INSERT INTO [HOMOLOGACAO].[dbo].[16_20191003_BATIMENTO_BRV_BASE]" 
                        "(AGENCIA, CONTA, CONTRATO, DT_VENCTO_PCELA2, ID, " +
                        "NOME_CLIENTE, VL_INICIAL_DIV, CARTEIRA, DATA_DA_TRANSACAO, DT_MVTO, ESCRITORIO) " +
                        "values(?,?,?,?,?,?,?,?,?,?,?)"
                                           ,row.AGENCIA, row.CONTA, row.CONTRATO, row.DT_VENCTO_PCELA2,
                                            row.ID, row.NOME_CLIENTE, row.VL_INICIAL_DIV, row.CARTEIRA,
                                            row.DATA_DA_TRANSACAO, row.DT_MVTO, row.ESCRITORIO)
        if index == round(len(df.index) * 0.05):
            print ('5% do arquivo importado...')
        
        if index == round(len(df.index) * 0.10):
            print ('10% do arquivo importado...')
        
        if index == round(len(df.index) * 0.20):
            print ('20% do arquivo importado...')
        
        if index == round(len(df.index) * 0.30):
            print ('30% do arquivo importado...')
        
        if index == round(len(df.index) * 0.40):
            print ('40% do arquivo importado...')
        
        if index == round(len(df.index) * 0.50):
            print ('50% do arquivo importado...')
        
        if index == round(len(df.index) * 0.60):
            print ('60% do arquivo importado...')
            
        if index == round(len(df.index) * 0.70):
            print ('70% do arquivo importado...')
            
        if index == round(len(df.index) * 0.80):
            print ('80% do arquivo importado...')
        
        if index == round(len(df.index) * 0.90):
            print ('90% do arquivo importado...')
            
        if index == round(len(df.index) * 1):
            print ('100% do arquivo importado...')
    
    print('finalizando importação...')
    cnxn.commit()

    cursor.close()

    cnxn.close()

    print ('importação finalizada. verificando as datas para atualizar o BRV...')
#roda a procedure do BRV
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=plan01spo15;"
                     "Trusted_Connection=yes;")

    
    sql_query = pd.read_sql_query (''' EXEC PRODUCAO.DBO.[16_VARIAVEIS_BRV] ''', cnxn)

    
    df = pd.DataFrame(sql_query)

    variavel = list(df['DATA'])
    
    print ('Datas registradas... executando procedure de atualização do BRV')
    query = "EXEC HOMOLOGACAO.[DBO].[P_BRAD_VEIC_MENSURACAO_BRV] '" + str(variavel[2]) + "','" +  str(variavel[1]) + "','" + str(variavel [0]) + "','" + str(variavel[3]) + "','" + str(variavel [4]) + " 00:00:00.000','" + str(variavel[3]) + " 00:00:00.000','" + str(variavel[1]) + "','ML'"

    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=plan01spo15;"
                     "Trusted_Connection=yes;")

    cursor = cnxn.cursor()

    cursor.execute(query)

    cnxn.commit()
    
    cursor.close()

    cnxn.close()
    
    print('BRV atualizado. Gerando o analítico...')

#EXTRAI O ANALÍTICO VIA SQL E O SALVA EM EXCEL
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=plan01spo15;"
                     "Trusted_Connection=yes;")

    sql_query = pd.read_sql_query (''' EXEC PRODUCAO.[dbo].[P_16_ANALITICO_BRV] ''', cnxn)


    df_analitico = pd.DataFrame(sql_query)

    cnxn.close()

    data = datetime.date.today()
    
    writer = pd.ExcelWriter('N:\Planejamento\MIS SQUAD\ROTINAS\BRV - VEICULOS\PYTHON\ML_analitico '+str(data)+'.xlsx') 
    
    df_analitico.to_excel(writer, sheet_name='ANALITICO', index = False)
      
    for column in df_analitico:
        column_width = max(df_analitico[column].astype(str).map(len).max(), len(column))
        col_idx = df_analitico.columns.get_loc(column)
        writer.sheets['ANALITICO'].set_column(col_idx, col_idx, column_width)

    writer.save()
    writer.close()   
        
    print ('Analítico gerado. Salvando as entradas...')
  
    #EXTRAI AS ENTRADAS E SALVA EM CSV

    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=plan01spo15;"
                     "Trusted_Connection=yes;")

    sql_query = pd.read_sql_query (''' EXEC PRODUCAO.[DBO].[P_16_ENTRADAS_BRV] ''', cnxn)


    df_entradas = pd.DataFrame(sql_query)

    cnxn.close()

    data = datetime.date.today()

    df_entradas.to_csv('N:\Planejamento\MIS SQUAD\ROTINAS\BRV - VEICULOS\PYTHON\ML_entradas ' + str(data) +'.csv',index = False)
    
    print ('entradas Salvas. Gerando o arquivo de pagamentos...')

#EXTRAI OS PAGAMENTOS VIA BANCO E SALVA EM XLSX
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=plan01spo15;"
                     "Trusted_Connection=yes;")

    sql_query = pd.read_sql_query (''' EXEC PRODUCAO.[dbo].[P_16_BRV_PGTOS] ''', cnxn)


    df_pgtos = pd.DataFrame(sql_query)

    cnxn.close()

    pgto = list(df_pgtos['DT_PGTO'])
    
    writer = pd.ExcelWriter('N:\Planejamento\MIS SQUAD\ROTINAS\BRV - VEICULOS\PYTHON\pagtos - ' + pgto[0].replace('/','-') +'.xlsx')
    
    df_pgtos.to_excel(writer,sheet_name='PGTOS',index = False)

    for column in df_pgtos:
        column_width = max(df_pgtos[column].astype(str).map(len).max(), len(column))
        col_idx = df_pgtos.columns.get_loc(column)
        writer.sheets['PGTOS'].set_column(col_idx, col_idx, column_width)

    writer.save()
    writer.close()

    print('BRV ATUALIZADO COM SUCESSO!! OS ARQUIVOS ESTARÃO SALVOS NA PASTA: N:\Planejamento\MIS SQUAD\ROTINAS\BRV - VEICULOS\PYTHON')

finally:
    print ("Processo Finalizado!! Você já pode fechar o prompt de comando.")

