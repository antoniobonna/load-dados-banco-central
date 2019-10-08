# -*- coding: utf-8 -*-
"""
Created on Fri Oct  4 17:33:21 2019

@author: abonna
"""

import os
import csv
from collections import OrderedDict
import credentials
import psycopg2
from subprocess import call

### Definicao das variaveis
indir = '/home/ubuntu/dump/dados_banco_central/if.data/'
outdir = '/home/ubuntu/scripts/load-dados-banco-central/parsed/'
new_file = 'pj_porte.csv'
files = [f for f in os.listdir(indir) if os.path.isfile(indir+f) and 'pj_porte' in f]
columns = ['Instituição financeira','Código','Segmento','Cidade','UF','Data','Total da Carteira de Pessoa Jurídica','Micro','Pequena','Média','Grande','Indisponível','Total não Individualizado Pessoa Jurídica','Total Exterior Pessoa Jurídica']
tablename = 'dados_banco_central.pj_porte_stg'

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

### funcao que cria data no formato banco de dados
def parse_date(date):
    newdate = date[3:] + '-' + date[:2] + '-01'
    return newdate

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
    elif 'VIACREDI' in name:
        name = 'VIACREDI'
    elif 'SICOOB' in name:
        name = 'SICOOB'
    elif 'SICREDI' in name:
        name = 'SICREDI'
    elif 'UNIPRIME' in name:
        name = 'UNIPRIME' 
    elif 'UNICRED' in name:
        name = 'UNICRED'
    elif 'CRESOL' in name:
        name = 'CRESOL'
    elif 'ASCOOB' in name:
        name = 'ASCOOB'
    elif 'CREDJUST' in name:
        name = 'CREDJUST'
    elif name.startswith('NU '):
        name = 'NUBANK'
    elif name.startswith('COOPERATIVA') and name[-2] == '-':
        name = name[-1]
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
        name = name.replace('BANCO BM&FBOVESPA DE SERVIÇOS DE LIQUIDAÇÃO E CUSTÓDIA','BANCO BM&FBOVESPA').replace('BANCO BM FBOVESPA DE SERVICOS DE LIQUIDACAO E CUSTODIA','BANCO BM&FBOVESPA')
        
    if name.endswith(' S'):
        name = name[:-2]
    
    return name

### Iteracao sobre os diretorios e parser dos CSVs
with open(outdir+new_file+'_aux','w', newline="\n", encoding="utf-8") as ofile: ### arquivo temporario
    writer = csv.DictWriter(ofile, fieldnames=columns,restval='', extrasaction='ignore',delimiter=';')
    for file in files:
        with open(indir+file, 'r', encoding='utf-8') as ifile:
            reader = csv.DictReader(ifile, delimiter=';') ### le CSV como dicionario      
            for row in reader:
                ### corrige erros no cabecalho
                row = OrderedDict([('Instituição financeira', v) if k == '\ufeffInstituição financeira' else (k, v) for k, v in row.items()])
                writer.writerow(row)
error = 0
with open(outdir+new_file,'w', newline="\n", encoding="utf-8") as ofile:
    writer = csv.writer(ofile, delimiter=';')
    with open(outdir+new_file+'_aux', 'r', encoding='utf-8') as ifile:
        reader = csv.reader(ifile, delimiter=';')    
        for row in reader:
            try:
                if not row[1] or row[-1] == '??? area':
                    continue
                row[0] = norm_banks(row[0])
                ### ajusta data para o formato do banco de dados
                row[5] = parse_date(row[5])
                ### converte numeros para padrao en-US
                row[1] = int(row[1].replace('.',''))
                row[6:] = [int(v.replace('.','').replace('NI','0').replace('NA','0')) if v else v for v in row[6:]]
                writer.writerow(row)
            except:
                print('Error in line:\n{}\n'.format(row))
                error += 1
                pass
print('Errors: {}'.format(error))
os.remove(outdir+new_file+'_aux')

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