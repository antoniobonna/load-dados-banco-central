# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 10:34:30 2019

@author: abonna
"""

import os
import pandas as pd
import csv
import credentials
import psycopg2
from subprocess import call

### Definicao das variaveis
indir = '/home/ubuntu/dump/dados_banco_central/ranking_reclamacoes/'
outdir = '/home/ubuntu/scripts/load-dados-banco-central/parsed/'
files = [('ranking_mais_4M.csv','new_ranking_mais_4M.csv'),
         ('ranking_menos_4M.csv','new_ranking_menos_4M.csv')]
folders = [f for f in os.listdir(indir) if os.path.isdir(indir+f)]
tablename = 'dados_banco_central.ranking_reclamacoes_stg'

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
    name = name.replace('S.A.','').replace('S A','').replace('S/A','')
    name = name.replace('LTDA.','')
    name = name.replace('CRÉDITO FINANCIAMENTO E INVESTIMENTO','').replace('CREDITO FINANCIAMENTO E INVESTIMENTO','')
    name = name.replace('FINANCIADORA','')
    name = name.strip()
    name = ' '.join(name.split())
    
    ### padronizacao
    name = name.replace('BB', 'BANCO DO BRASIL')
    name = name.replace('CAIXA ECONÔMICA FEDERAL', 'CAIXA ECONOMICA FEDERAL')
    name = name.replace('INTERMEDIUM', 'INTER')
    name = name.replace('PANAMERICANO', 'PAN')
    name = name.replace('BONSUCESSO', 'BS2').replace('BANCO BS2', 'BS2')
    name = name.replace('BANCO DAYCOVAL','DAYCOVAL')
    name = name.replace('BANCO CITIBANK','CITIBANK')
    name = name.replace('ABCBRASIL','ABC-BRASIL')
    name = name.replace('BANCO ABN AMRO','ABN AMRO')
    name = name.replace('PARANA BANCO','PARANÁ BANCO')
    name = name.replace('PARANA SAFRA','SAFRA') 
    
    return name

### Iteracao sobre os diretorios e parser dos CSVs
for (file,new_file) in files:
    with open(outdir+new_file,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        for folder in folders:
            date = create_date(folder)
            with open(indir+folder+'/'+file, 'r', encoding="utf-8") as ifile:
                for row in csv.reader(ifile, delimiter='\t'):
                    if row[0]:
                        rank = row[0].replace('º','').strip()
                    row[0] = rank
                    row[1] = norm_banks(row[1])
                    ### padrao numerico ENG-US
                    row[2] = row[2].replace('.','').replace(',','.')
                    row[3] = row[3].replace('.','')
                    row[4] = row[4].replace(',','')
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
call('psql -d torkcapital -c "VACUUM VERBOSE ANALYZE dados_banco_central.ranking_reclamacoes_stg";',shell=True)