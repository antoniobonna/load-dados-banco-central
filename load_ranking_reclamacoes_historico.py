# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 15:55:53 2019

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
files = [('ranking_mais_4M_historico.csv','new_ranking_mais_4M_historico.csv'),
         ('ranking_menos_4M_historico.csv','new_ranking_menos_4M_historico.csv')]
folders = [f for f in os.listdir(indir) if os.path.isdir(indir+f)]
tablename = 'dados_banco_central.ranking_reclamacoes_stg'

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

### funcao que cria data no formato banco de dados
def create_date(date):  
    return date.replace('_','-')+'-01'

### funcao que normaliza o nome dos bancos
def norm_banks(bankname):
    ### Limpeza
    name = bankname.upper().replace(' (CONGLOMERADO)','').replace('CONGLOMERADO','')
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
    name = name.replace('BONSUCESSO', 'BS2').replace('BANCO BS2', 'BS2')
    name = name.replace('CAIXA ECONÔMICA FEDERAL', 'CAIXA ECONOMICA FEDERAL')
    name = name.replace('SANTANDER BANESPA', 'SANTANDER')
    name = name.replace('HSBC BANK BRASIL BANCO MULTIPLO', 'HSBC')
    name = name.replace('BANCO DAYCOVAL','DAYCOVAL')
    name = name.replace('BANCO BMG','BMG')
    name = name.replace('BANCO CITIBANK','CITIBANK')
    name = name.replace('BANCO ORIGINAL','ORIGINAL')
    name = name.replace('BANCO BMC','BMC')
    name = name.replace('BANCO BGN','BGN')
    name = name.replace('ABCBRASIL','ABC-BRASIL')
    name = name.replace('BANCO ABN AMRO','ABN AMRO')
    
    return name

### Iteracao sobre os arquivos historicos
for (file,new_file) in files:
    with open(outdir+new_file,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        with open(indir+file, 'r', encoding="utf-8") as ifile:
            for row in csv.reader(ifile, delimiter='\t'):
                if len(row) == 1:
                    date = create_date(row[0])
                    continue
                ### parsear no mesmo formato do banco de dados
                if date >= '2005_11':
                    rank = row[0].replace('º','').strip()
                    bank = norm_banks(row[1])
                    index = row[4].replace('.','').replace(',','.')
                    claims = row[2].replace('.','')
                    clients = row[3].replace(',','').replace(' ','')
                else:
                    rank = row[0].replace('º','').strip()
                    bank = norm_banks(row[1])
                    index = row[4].replace('.','').replace(',','.')
                    claims = row[2].replace('.','')
                    clients = row[3].replace(',','').replace(' ','')
                writer.writerow([date,rank,bank,index,claims,clients])
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