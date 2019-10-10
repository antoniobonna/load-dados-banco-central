-- dados_banco_central_dw.carteira_pj_porte

\! echo "Carregando dados tabela fato carteira_pj_porte..."

COPY (
WITH carteira_pj_porte AS
	(SELECT instituicao_financeira banco, data, SUM(micro) micro, SUM(pequena) pequena, SUM(media) media,  SUM(grande) grande, 
	 SUM(total_exterior_pj) total_exterior, SUM(total_nao_individualizado_pj) total_nao_individualizado
	FROM dados_banco_central.pj_porte_stg 
	 GROUP BY instituicao_financeira, data)
SELECT d.data, b.banco_id, 
unnest(string_to_array('1,2,3,4,5,6', ','))::int as porte_id,
unnest(string_to_array(concat_ws(',',micro, pequena, media, grande, total_exterior, total_nao_individualizado), ','))::bigint qtd_credito
	FROM carteira_pj_porte c
	JOIN dados_banco_central_dw.data d ON d.data=c.data
	JOIN dados_banco_central_dw.banco b ON b.banco=c.banco
) to '/home/ubuntu/dump/carteira_pj_porte.txt';
COPY dados_banco_central_dw.carteira_pj_porte FROM '/home/ubuntu/dump/carteira_pj_porte.txt';

VACUUM VERBOSE ANALYZE dados_banco_central_dw.carteira_pj_porte;