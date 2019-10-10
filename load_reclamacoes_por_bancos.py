# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 18:33:46 2019

@author: abonna
"""

import os
import csv
import credentials
import psycopg2
from subprocess import call

### Definicao das variaveis
indir = '/home/ubuntu/dump/dados_banco_central/ranking_reclamacoes/'
outdir = '/home/ubuntu/scripts/load-dados-banco-central/parsed/'
file = 'Bancos+e+financeiras+-+Reclamacoes+e+quantidades+de+clientes+por+instituicao+financeira.csv'
new_file = 'reclamacoes_quantidades_clientes_por_instituicao_financeira.csv'
folders = [f for f in os.listdir(indir) if os.path.isdir(indir+f)]
tablename = 'dados_banco_central.reclamacoes_por_banco_stg'

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

### funcao que cria data no formato banco de dados
def create_date(folder):
    if folder >= '2017':
        year  = folder[:4]
        quarter = folder[-2:]
        month = str(int(quarter) * 3).zfill(2)
        date = year+'-'+month+'-01'
    elif folder < '2017' and folder >= '2016_07':
        year  = folder[:4]
        month = folder[-2:]
        date = year+'-'+month+'-01'
    else:
        date = folder.replace('_','-')+'-01'
    
    return date

### funcao que normaliza o nome dos bancos
def norm_banks(bankname):
    ### Limpeza
    name = bankname.upper().replace(' (CONGLOMERADO)','')
    name = name.replace('-','').replace(',','')
    name = name.replace('S.A.','').replace('S A','').replace('S/A','').replace('S.A','')
    name = name.replace('LTDA.','')
    name = name.replace('CRÉDITO FINANCIAMENTO E INVESTIMENTO','').replace('CREDITO FINANCIAMENTO E INVESTIMENTO','').replace('CREDITO FINANCIAMENTO E INVESTIMENTOS','')
    name = name.replace('SOCIEDADE DE','')
    name = name.replace('FINANCIADORA','')
    name = name.strip()
    name = ' '.join(name.split())
    name = name.replace('CRÉDITO FINANCIAMENTO E INVESTIMENTO','').replace('CREDITO FINANCIAMENTO E INVESTIMENTO','').replace('CREDITO FINANCIAMENTO E INVESTIMENTOS','').strip()
    
    ### padronizacao
    if name == 'BB':
        name = 'BANCO DO BRASIL'
    else:
        name = name.replace('INTERMEDIUM', 'INTER').replace('BANCO INTER','INTER')
        name = name.replace('PANAMERICANO', 'PAN').replace('BANCO PAN', 'PAN')
        name = name.replace('BONSUCESSO', 'BS2').replace('BANCO BS2', 'BS2').replace('GRUPO BS2 BS2','BS2')
        name = name.replace('CAIXA ECONÔMICA FEDERAL', 'CAIXA ECONOMICA FEDERAL')
        name = name.replace('SANTANDER BANESPA', 'SANTANDER')
        name = name.replace('HSBC BANK BRASIL BANCO MULTIPLO', 'HSBC')
        name = name.replace('BANCO DAYCOVAL','DAYCOVAL')
        name = name.replace('BANCO BMG','BMG')
        name = name.replace('BANCO CITIBANK','CITIBANK')
        name = name.replace('BANCO ORIGINAL','ORIGINAL')
        name = name.replace('PAGSEGURO INTERNET','PAGSEGURO')
        name = name.replace('BANCO BMC','BMC')
        name = name.replace('ABCBRASIL','ABC-BRASIL')
        name = name.replace('BANCO BGN','BGN')
        name = name.replace('BANCO TOPÁZIO','BANCO TOPAZIO')
        name = name.replace('NOVO BANCO CONTINENTAL BANCO MÚLTIPLO','NOVO BANCO CONTINENTAL BANCO MULTIPLO')
        name = name.replace('AGIPLAN FINANCEIRA','AGIPLAN')
        name = name.replace('BANIF INTERNACIONAL DO FUNCHAL (BRASIL) EM LIQUIDAÇÃO ORDINÁRIA','BANIF')
        name = name.replace('BANCO DO ESTADO DO PARÁ','BANCO DO ESTADO DO PARA')
        name = name.replace('BANCO DE TOKYO MITSUBISHI UFJ BRASIL','BANCO DE TOKYOMITSUBISHI UFJ BRASIL')
        name = name.replace('BANCO A J RENNER','BANCO RENNER').replace('BANCO A.J. RENNER','BANCO RENNER')
        name = name.replace('BANCO ABN AMRO','ABN AMRO')
        name = name.replace('BANCO BM&FBOVESPA DE SERVIÇOS DE LIQUIDAÇÃO E CUSTÓDIA','BANCO BM&FBOVESPA').replace('BANCO BM FBOVESPA DE SERVICOS DE LIQUIDACAO E CUSTODIA','BANCO BM&FBOVESPA')
    if name.endswith(' S'):
        name = name[:-2]
    
    return name

### Iteracao sobre os diretorios e parser dos CSVs
with open(outdir+new_file,'w', newline="\n", encoding="utf-8") as ofile:
    writer = csv.writer(ofile, delimiter=';')
    for folder in folders:
        date = create_date(folder)
        with open(indir+folder+'/'+file, 'r', encoding='latin-1') as ifile:
            reader = csv.reader(ifile, delimiter=';')
            header = next(reader, None)  ### Pula o cabeçalho
            for row in reader:
                if len(row) == 16:
                    del row[-1] ### Remove colunas nao utilizadas
                del row[:2] 
                del row[2]
                row[2] = norm_banks(row[2])
                ### padrao numerico ENG-US
                row[3:] = [r.replace('.','').replace(',','.').replace(' ','') for r in row[3:]]
                row.insert(0,date)
                writer.writerow(row)
### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')
### copy
with open(outdir+new_file, 'r') as ifile:
    SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    print("Executing Copy in "+tablename)
    cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    db_conn.commit()
cursor.close()
db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM VERBOSE ANALYZE '+tablename+'";',shell=True)