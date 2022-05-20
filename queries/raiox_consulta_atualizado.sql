	with aux_rx as (
	SELECT
	nrcpfcnpj,
  bandeira.nmbandeira bandeira,
	class.dsprodutotipoclassificacao produto,
  case when adquirente.nmadquirente='iFood Pagamentos' then 'IFOOD' else adquirente.nmadquirente end adq,
  lotevenda.nrparcelas nrparcelas,
  DATE(lotevenda.DTLOTE) dtlote, 
  SUM(lotevenda.vlliquido) 	tpv,
	from `dataplatform-prd.equals_oracle.crclotevenda`                      lotevenda
	left join `dataplatform-prd.equals_oracle.crcadquirente` 					      adquirente on lotevenda.idadquirente = adquirente.idadquirente
	left join `dataplatform-prd.equals_oracle.crcbandeira` 					        bandeira on lotevenda.idbandeira = bandeira.idbandeira
	left join `dataplatform-prd.equals_oracle.crcproduto` 					        produto on lotevenda.idproduto = produto.idproduto
	left join `dataplatform-prd.equals_oracle.crcprodutotipo` 				      tipo on produto.idprodutotipo = tipo.idprodutotipo
	left join `dataplatform-prd.equals_oracle.crcprodutotipoclassificacao` 	class on tipo.idprodutotipoclassificacao = class.idprodutotipoclassificacao
	left join (select idcliente, max(nrcpfcnpj) nrcpfcnpj, from `dataplatform-prd.equals_oracle.pltcliente` group by 1) cl ON lotevenda.idcliente = cl.idcliente 
	where 1=1 
	AND lotevenda.DTLOTE >= '2022-05-10'
	AND lotevenda.DTLOTE < '2022-05-17'
	AND CLASS.dsprodutotipoclassificacao IN('Crédito à vista','Parcelado','Débito')  /*'Débito'*/
	AND bandeira.nmbandeira in ('MasterCard', 'Visa', 'Elo')  
	GROUP BY 1, 2, 3, 4, 5, 6 )



select rx.*
from aux_rx rx
inner join `dataplatform-prd.economic_research.tmp_raiox_sample_consulta` c on c.CNPJ_CPF=rx.nrcpfcnpj and c.Concorrente=rx.adq