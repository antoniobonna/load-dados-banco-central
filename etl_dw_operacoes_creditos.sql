-- dados_banco_central_dw.operacoes_credito_regiao

\! echo "Carregando dados tabela fato operacoes_credito_regiao..."

COPY (
WITH credito_regiao AS
	(SELECT instituicao_financeira banco, data, SUM(sudeste) sudeste, SUM(centrooeste) centrooeste, SUM(nordeste) nordeste,  SUM(norte) norte, 
	 SUM(sul) sul, SUM(regiao_nao_informada) regiao_nao_informada, SUM(total_exterior) total_exterior
	FROM dados_banco_central.credito_regiao_stg 
	 GROUP BY instituicao_financeira, data)
SELECT d.data, b.banco_id, 
unnest(string_to_array('1,2,3,4,5,6,7', ','))::int as region_id,
unnest(string_to_array(concat_ws(',',sudeste, sul, centrooeste, nordeste, norte, total_exterior,regiao_nao_informada), ','))::bigint qtd_credito
	FROM credito_regiao c
	JOIN dados_banco_central_dw.data d ON d.data=c.data
	JOIN dados_banco_central_dw.banco b ON b.banco=c.banco
) to '/home/ubuntu/dump/operacoes_credito.txt';
COPY dados_banco_central_dw.operacoes_credito_regiao FROM '/home/ubuntu/dump/operacoes_credito.txt';

VACUUM VERBOSE ANALYZE dados_banco_central_dw.operacoes_credito_regiao;