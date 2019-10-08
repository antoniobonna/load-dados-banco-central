# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 17:43:02 2019

@author: abonna
"""

import os
import csv
import credentials
import psycopg2
from subprocess import call

### Definicao das variaveis
indir = '/home/ubuntu/dump/dados_banco_central/ranking_ouvidoria/'
outdir = '/home/ubuntu/scripts/load-dados-banco-central/parsed/'
files = [('ouvidora_mais_4M.csv','new_ouvidora_mais_4M.csv'),
         ('ouvidora_menos_4M.csv','new_ouvidora_menos_4M.csv')]
folders = [f for f in os.listdir(indir) if os.path.isdir(indir+f)]
tablename = 'dados_banco_central.ranking_ouvidoria_stg'

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

### funcao que cria data no formato banco de dados
def create_date(folder):
    year  = folder[:4]
    quarter = folder[-2:]
    month = str(int(quarter) * 3).zfill(2)
    date = year+'-'+month+'-01'    
    return date

### funcao que normaliza o nome dos bancos
def norm_banks(bankname):
    ### Limpeza
    name = bankname.upper().replace(' (CONGLOMERADO)','')
    name = name.replace('-','').replace(',','')
    name = name.replace('S.A.','').replace('S A','').replace('S/A','')
    name = name.replace('LTDA.','')
    name = name.replace('CRÉDITO FINANCIAMENTO E INVESTIMENTO','').replace('CREDITO FINANCIAMENTO E INVESTIMENTO','')
    name = name.replace('FINANCIADORA','')
    name = name.strip()
    name = ' '.join(name.split())
    
    ### padronizacao
    name = name.replace('BB', 'BANCO DO BRASIL')
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
    name = name.replace('BANCO BGN','BGN')
    name = name.replace('ABCBRASIL','ABC-BRASIL')
    name = name.replace('BANCO A J RENNER','BANCO RENNER').replace('BANCO A.J. RENNER','BANCO RENNER')
    
    return name

### Iteracao sobre os diretorios e parser dos CSVs
for (file,new_file) in files:
    with open(outdir+new_file,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        for folder in folders:
            date = create_date(folder)
            with open(indir+folder+'/'+file, 'r', encoding="utf-8") as ifile:
                for row in csv.reader(ifile, delimiter='\t'):
                    ### Pega o valor do rank
                    if len(row) == 4:
                        rank = row[0].replace('º','').strip()
                        row[0] = rank
                    else:
                        row.insert(0,rank)
                    row[1] = norm_banks(row[1])
                    ### padrao numerico ENG-US
                    row[2] = row[2].replace(',','.')
                    row[3] = row[3].replace('.','')
                    row.insert(0,date)
                    writer.writerow(row)
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')
    ### copy
    with open(outdir+new_file, 'r') as ifile:
        cursor.copy_from(ifile, tablename, sep=';')
        db_conn.commit()
    cursor.close()
    db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM VERBOSE ANALYZE '+tablename+'";',shell=True)