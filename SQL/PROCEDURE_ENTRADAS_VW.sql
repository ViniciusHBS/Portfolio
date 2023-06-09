USE [PRODUCAO]
GO
/****** Object:  StoredProcedure [dbo].[P_06_ABASTECER_ENTRADAS]    Script Date: 28/03/2023 09:02:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[P_06_ABASTECER_ENTRADAS]


AS BEGIN

-------------------------------------------------------------
--------------DECLARAÇÃO DE VARÍAVEIS------------------------
-------------------------------------------------------------
DECLARE @COD_IMP INT
DECLARE @DATA_ONTEM DATE
DECLARE @DATA DATE = (SELECT MAX (DATA) FROM HOMOLOGACAO.DBO.DIAS_UTEIS WHERE CONVERT(DATE,DATA,103) < CONVERT(DATE,GETDATE(),103))


------------------------------------------------------------
----------CONFIRMAR SE A DATA É UM DIA ÚTIL ----------------
------------------------------------------------------------

IF @DATA IN (SELECT DATA FROM HOMOLOGACAO.DBO.DIAS_UTEIS WHERE DATA BETWEEN @DATA AND GETDATE())
BEGIN


--> seta o dia útil anterior à variável data
SET @DATA_ONTEM	= (SELECT MAX(DATA) FROM HOMOLOGACAO.DBO.DIAS_UTEIS WHERE DATA < @DATA) 


------------------------------------------------------------
----------PEGA O COD_IMP DO ARQUIVO DE REMESSA--------------
------------------------------------------------------------


IF OBJECT_ID('tempdb.dbo.#COD_IMP') IS NOT NULL DROP TABLE #COD_IMP

SELECT 
	 COD_IMP
	,ROW_NUMBER() OVER (ORDER BY TOT_DEVEDORES  DESC) AS RK 
	INTO #COD_IMP 
FROM [CobSystems3].[dbo].[IMPORTACAO] 
	WHERE COD_CRED = 6
	AND CONVERT(DATE,DATA_IMP,103) = @DATA 
	AND ARQ_IMP LIKE '%REMESSA%'


--> DELETA CASO HAJA ARQUIVO REPETIDO, DEIXANDO APENAS O COM MAIOR NÚMERO DE CONTRATOS IMPORTADOS
DELETE FROM #COD_IMP WHERE RK > 1


--> SETA A VARÁIVEL COM O CÓDIGO DE IMPORTAÇÃO DO ARQUIVO
SET @COD_IMP = (SELECT COD_IMP FROM #COD_IMP)




--------------------------------------------------------------------------------
----------CRIA BASE COM TODAS AS PARCELAS IMPORTADAS COM O COD_IMP--------------
--------------------------------------------------------------------------------


IF OBJECT_ID('tempdb.dbo.#BASE_HOJE') IS NOT NULL DROP TABLE #BASE_HOJE 

SELECT DISTINCT
     COD_TIT
	,COD_PARC
	INTO #BASE_HOJE
FROM [CobSystems3].[dbo].[IMPORTACAO_HISTORICO]				
	WHERE COD_IMP = @COD_IMP


-----------------------------------------------------------------------------
----------PEGA O COD_IMP DO ARQUIVO DE REMESSA NA DATA ANTERIOR--------------
-----------------------------------------------------------------------------


IF OBJECT_ID('tempdb.dbo.#COD_IMP_ONTEM') IS NOT NULL DROP TABLE #COD_IMP_ONTEM
SELECT 
	 COD_IMP
	,ROW_NUMBER() OVER (ORDER BY TOT_DEVEDORES  DESC) AS RK 
	INTO #COD_IMP_ONTEM 
FROM [CobSystems3].[dbo].[IMPORTACAO] 
	WHERE COD_CRED = 6
	AND CONVERT(DATE,DATA_IMP,103) = @DATA_ONTEM 
	AND ARQ_IMP LIKE '%REMESSA%'



--> DELETA CASO HAJA ARQUIVO REPETIDO, DEIXANDO APENAS O COM MAIOR NÚMERO DE CONTRATOS IMPORTADOS
DELETE FROM #COD_IMP_ONTEM WHERE RK > 1


--> TRAZ O CÓDIGO DE IMPORTAÇÃO DO ARQUIVO ANTERIOR
SET @COD_IMP = (SELECT COD_IMP FROM #COD_IMP_ONTEM)



-------------------------------------------------------------------------------------
----------CRIA BASE COM TODOS OS COD_TIT IMPORTADOS NO ARQUIVO ANTERIOR--------------
-------------------------------------------------------------------------------------


IF OBJECT_ID('tempdb.dbo.#BASE_ONTEM') IS NOT NULL DROP TABLE #BASE_ONTEM

SELECT DISTINCT
    COD_TIT
	INTO #BASE_ONTEM
FROM [CobSystems3].[dbo].[IMPORTACAO_HISTORICO]				
	WHERE COD_IMP = @COD_IMP


--------------------------------------------------------------------------------------------------
----------COMPARA A BASE ATUAL E ANTIGA E TRAS OS CONTRATOS DIFERENTES COMO ENTRADAS--------------
--------------------------------------------------------------------------------------------------


IF OBJECT_ID('tempdb.dbo.#ENTRADAS') IS NOT NULL DROP TABLE #ENTRADAS

SELECT
	 A.COD_TIT
	,A.COD_PARC
	INTO #ENTRADAS
FROM #BASE_HOJE AS A
WHERE NOT EXISTS (	SELECT
						B.COD_TIT
					FROM #BASE_ONTEM AS B
					WHERE B.COD_TIT = A.COD_TIT)


--------------------------------------------------------------------------------
----------DEFINE O ATRASO E VALOR DAS PARCELAS DAS ENTRADAS---------------------
--------------------------------------------------------------------------------


IF OBJECT_ID('tempdb.dbo.#ENTRADAS_COM_VCTO') IS NOT NULL DROP TABLE #ENTRADAS_COM_VCTO

SELECT 
	 A.COD_TIT
	,MIN(CONVERT(DATE,B.VCTO_PARC,103))			AS MENOR_VCTO
	,SUM(VR_PARC)								AS VALOR_PRINC_DIVIDA
	INTO #ENTRADAS_COM_VCTO
FROM #ENTRADAS									AS A
	LEFT JOIN PRODUCAO.DBO.V_ML_PARCELAS		AS B
	ON A.COD_PARC = B.COD_PARC
		WHERE (B.DATA_RESOLVIDO IS NULL OR B.DATA_RESOLVIDO >= @DATA)
		GROUP BY A.COD_TIT




--------------------------------------------------------------------------------
-----------------------------ABASTECE A BASE FINAL------------------------------
--------------------------------------------------------------------------------


INSERT INTO PRODUCAO.DBO.ML_ENTRADAS

SELECT
	 CONVERT(DATETIME, @DATA, 103)						AS DATA_ENTRADA
	,'BANCO VOLKSWAGEN'									AS CARTEIRA
	,'6'												AS COD_CRED
	,B.CPFCGC_PES										AS CPF_CNPJ
	,B.TIPO_PES											AS TIPOPESSOA
	,B.CONTRATO_TIT										AS CONTRATO
	,B.CA_N_ACAO										AS NUMERO_ACAO
	,A.COD_TIT
	,DATEDIFF(DD,A.MENOR_VCTO,@DATA)					AS ATRASO
	,'AMIGÁVEL'											AS STATUS_COBRANCA
	,B.UF
	,B.REGIAO
	,B.PRODUTO
	,NULL												AS PRODUTO_2
	,NULL												AS PRODUTO_3
	,'COB'												AS CRM
	,A.VALOR_PRINC_DIVIDA								AS VALOR_DIVIDA
	,CASE
		WHEN B.INFORMACAO_FILA_VW = 'MIDD'				THEN '40.01.05.07'
		WHEN DATEDIFF(DD,A.MENOR_VCTO,@DATA) < 60		THEN '40.01.05.01'
		ELSE '40.01.05.05'
	 END												AS CENTRO_DE_CUSTO
	 ,NULL												AS DT_BX_DIRETA
	 ,NULL												AS DT_PGTO
	 ,NULL												AS TIPO_BAIXA
FROM #ENTRADAS_COM_VCTO											AS A
	LEFT JOIN CobReports_Diversos.DBO.POSICAO_CARTEIRA_ML		AS B				
	ON A.COD_TIT = B.COD_TIT
	WHERE DATEDIFF(DD,A.MENOR_VCTO,@DATA) <=120



END


END
