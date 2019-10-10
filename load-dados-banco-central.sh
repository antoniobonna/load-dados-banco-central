!#/bin/bash

### Definicao de variaveis
LOG="/var/log/scripts/scripts.log"
DIR="/home/ubuntu/scripts/load-dados-banco-central"
DUMP="/home/ubuntu/scripts/dump"
STARTDATE=$(date +'%F %T')
SCRIPTNAME="load-dados-banco-central.sh"

horario()
{
	date +%d/%m/%Y" - "%H:%M:%S
}
export -f horario

stagingDados()
{
	FILE=$1
	time python ${DIR}/${FILE}
	echo -e "$(horario):Script $FILE executado.\n-\n"
}
export -f stagingDados

LoadDW()
{
	FILE=$1
	time psql -d torkcapital -f ${DIR}/${FILE}
	echo -e "$(horario):Script $FILE executado.\n-\n"
}
export -f LoadDW

LoadHist()
{
	TABLE=$1
	time (psql -d torkcapital -c "COPY (table dados_banco_central.${TABLE}_stg) TO '/home/ubuntu/dump/${TABLE}.txt';"
	psql -d torkcapital -c "COPY dados_banco_central.${TABLE}_hist FROM '/home/ubuntu/dump/${TABLE}.txt';"
	psql -d torkcapital -c "VACUUM ANALYZE dados_banco_central.${TABLE}_hist;"
	psql -d torkcapital -c "TRUNCATE dados_banco_central.${TABLE}_stg;")
	echo -e "$(horario):Tabela $TABLE transferida para dados historicos.\n-\n"
}
export -f LoadHist

### Carrega arquivos nas tabelas staging

echo -e "$(horario):Inicio do staging.\n-\n"

ListaArquivos="load_clientes_operacoes.py load_credito_regiao.py load_irregularidades_por_bancos.py load_ouvidoria.py load_pj_porte.py load_ranking_reclamacoes.py load_reclamacoes_por_bancos.py load_resumo.py pj_porte.py"
for FILE in $ListaArquivos; do
	stagingDados $FILE

### Carrega dados no DW

echo -e "$(horario):Inicio da carga no DW.\n-\n"

ListaArquivos="etl_dw_carteira_pj_porte.sql etl_dw_dados_bancos.sql etl_dw_irregularidades_por_banco.sql etl_dw_operacoes_creditos.sql"
for FILE in $ListaArquivos; do
	LoadDW $FILE

### Limpa tabelas staging e carrega no historico

echo -e "$(horario):Inicio da limpeza do staging.\n-\n"

ListaTabelas="clientes_operacoes credito_regiao irregularidades_por_banco pj_porte ranking_ouvidoria reclamacoes_por_banco resumo"
for TABLE in $ListaTabelas; do
	LoadHist $TABLE

### Remove arquivos temporarios e escreve no log

rm -f ${DUMP}/*.txt

ENDDATE=$(date +'%F %T')
echo "$SCRIPTNAME;$STARTDATE;$ENDDATE" >> $LOG

echo -e "$(horario):Fim da execucao.\n"

exit 0