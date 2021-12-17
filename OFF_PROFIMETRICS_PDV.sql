create or replace package OFF_PROFIMETRICS_PDV is

  procedure PRECOS_OFERTAS_PROFIMETRICS_TODAS(V_DIA_ATUAL date := sysdate);
  procedure VENDAS_PROMOCIONAIS_LOG8(V_DIA_ATUAL date := trunc(sysdate));
  procedure APURAR_SELLOUT(V_DIA_ATUAL date := trunc(sysdate));
  procedure PROC_COMP_OFERTAS;

end OFF_PROFIMETRICS_PDV;


create or replace package body OFF_PROFIMETRICS_PDV is

  PROCEDURE PRECOS_OFERTAS_PROFIMETRICS_TODAS(V_DIA_ATUAL date := sysdate) is
  
    -- A DIFERENCA ENTRE ESTE E O PRECOS_OFERTAS_PROFIMETRICS Eh QUE NESTA PROCEDERE
    -- NAO FAZEMOS O FILTRO DE APENAS SIMPLES E PUBLICO GERAL
    -- PROCESSO USADO PARA APURACAO DE SELLOUT
  BEGIN
  
    MERGE INTO OFERTAS_PROFIMETRICS_TODAS G
    USING (SELECT OFFER_ID,
                  FILIAL,
                  PRODUTO,
                  APPROVED_PRICE,
                  CUSTO,
                  DATA_INI,
                  DATA_FIM,
                  PRECO_REGULAR,
                  DIA_ATUAL,
                  DATA_PROC,
                  PRECO_EXPORTAR,
                  DESCRICAO,
                  EAN,
                  offer_id || filial || produto || dia_atual AS CHV
             FROM (SELECT DISTINCT O.OFFER_ID,
                                   CASE
                                     WHEN (to_number(AP.LOC_ID)) between 6000 and 6999 then
                                      (TRUNC(to_number(AP.LOC_ID) / 10) - 600) * 10 +
                                      dac(TRUNC(to_number(AP.LOC_ID) / 10) - 600)
                                     ELSE
                                      (to_number(AP.LOC_ID))
                                   END AS FILIAL,
                                   AP.PRODUCT_ID AS PRODUTO,
                                   AP.APPROVED_PRICE,
                                   nvl(PE.cm, 0) AS CUSTO,
                                   O.OFFER_START_DATE AS DATA_INI,
                                   O.OFFER_ATTR_02_DATE AS DATA_FIM,
                                   PC_RMS_CAL.F_CALCSTPR(2,
                                                         TRUNC(AP.PRODUCT_ID / 10),
                                                         '1' ||
                                                         TO_CHAR(SYSDATE,
                                                                 'RRMMDD'),
                                                         (CASE
                                                           when (to_number(AP.LOC_ID)) between 6000 and 6999 then
                                                            (TRUNC(to_number(AP.LOC_ID) / 10) - 600)
                                                           ELSE
                                                            TRUNC(to_number(AP.LOC_ID) / 10)
                                                         END)) AS PRECO_REGULAR,
                                   
                                   TRUNC(V_DIA_ATUAL) + 1 AS DIA_ATUAL,
                                   V_DIA_ATUAL AS DATA_PROC,
                                   
                                   CASE
                                     WHEN TRUNC(TRUNC(V_DIA_ATUAL) + 1) =
                                          O.OFFER_START_DATE THEN
                                      AP.APPROVED_PRICE
                                     WHEN TRUNC(V_DIA_ATUAL) =
                                          O.OFFER_ATTR_02_DATE THEN
                                      PC_RMS_CAL.F_CALCSTPR(2,
                                                            TRUNC(AP.PRODUCT_ID / 10),
                                                            '1' ||
                                                            TO_CHAR(SYSDATE,
                                                                    'RRMMDD'),
                                                            (CASE
                                                              when (to_number(AP.LOC_ID)) between 6000 and 6999 then
                                                               (TRUNC(to_number(AP.LOC_ID) / 10) - 600)
                                                              ELSE
                                                               TRUNC(to_number(AP.LOC_ID) / 10)
                                                            END))
                                     ELSE
                                      NULL
                                   END PRECO_EXPORTAR,
                                   ITEM.GIT_DESCRICAO AS DESCRICAO,
                                   ITEM.GIT_CODIGO_EAN13 AS EAN,
                                   rank() over(partition by AP.PRODUCT_ID, AP.LOC_ID order by O.OFFER_ATTR_02_DATE DESC) AS RNK
                   
                     FROM GS_PRF_APPROVEDPRICE AP
                   
                     LEFT JOIN gs_posestq pe
                       ON pe.produto = TRUNC(to_number(AP.product_id) / 10)
                      AND pe.filial = TRUNC(to_number(AP.loc_id) / 10)
                   
                    INNER JOIN OFERTAS_PRF_OFFER_CAPA O
                       ON AP.OFFER_ID = O.OFFER_ID
                   --AND O.OFFER_TYPE_PARENT_ID = 'SIMPLE'
                   --AND O.OFFER_ID NOT IN (select C.OFFER_ID from OFERTAS_PRF_FIDEXC C)
                   
                    INNER JOIN AA3CITEM ITEM
                       ON TRUNC(to_number(AP.product_id) / 10) =
                          ITEM.GIT_COD_ITEM
                   
                    WHERE TRUNC(V_DIA_ATUAL) >= O.OFFER_START_DATE - 1
                      AND TRUNC(V_DIA_ATUAL) <= O.OFFER_ATTR_02_DATE)
           
           ) A
    ON (G.CHV = A.CHV)
    WHEN MATCHED THEN
      UPDATE
         SET G.OFFER_ID       = A.OFFER_ID,
             G.FILIAL         = A.FILIAL,
             G.PRODUTO        = A.PRODUTO,
             G.APPROVED_PRICE = A.APPROVED_PRICE,
             G.PRECO_REGULAR  = A.PRECO_REGULAR,
             G.DATA_INI       = A.DATA_INI,
             G.DATA_FIM       = A.DATA_FIM
    WHEN NOT MATCHED THEN
      INSERT
        (G.OFFER_ID,
         G.FILIAL,
         G.PRODUTO,
         G.APPROVED_PRICE,
         G.CUSTO,
         G.DATA_INI,
         G.DATA_FIM,
         G.PRECO_REGULAR,
         G.DIA_ATUAL,
         G.PRECO_EXPORTAR,
         G.DESCRICAO,
         G.EAN,
         G.CHV,
         G.DATA_PROC)
      VALUES
        (A.OFFER_ID,
         A.FILIAL,
         A.PRODUTO,
         A.APPROVED_PRICE,
         A.CUSTO,
         A.DATA_INI,
         A.DATA_FIM,
         A.PRECO_REGULAR,
         A.DIA_ATUAL,
         A.PRECO_EXPORTAR,
         A.DESCRICAO,
         A.EAN,
         A.CHV,
         A.DATA_PROC);
  
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
    
  END PRECOS_OFERTAS_PROFIMETRICS_TODAS;

  PROCEDURE APURAR_SELLOUT(V_DIA_ATUAL date := trunc(sysdate)) is
  
    -- RELACIONA:
    -- OFERTAS_PROFIMETRICS_TODAS
    -- OFERTAS_VENDAS_PROMOCIONAIS
    -- OFERTAS_PRF_SELLOUT
    -- ATENCAO !!!
    -- OFERTAS_VENDAS_PROMOCIONAIS NAO TEM ID DE OFERTA A APURACAO Eh ESTIMADA
    -- SE FIZEREM DUAS OFERTAS IGUAIS, VAI DUPLICAR O VALOR APURADO
  
  BEGIN
  
    MERGE INTO OFERTAS_RESULTADO_SELLOUT G
    USING (SELECT PDV.DATA_LOG AS DIA_VDA,
                  PROF.OFFER_ID,
                  PROF.FILIAL,
                  PROF.PRODUTO,
                  I.GIT_DESCRICAO AS DESC_ITEM,
                  PDV.QTD_VDA,
                  VLRSELL.VLR_SELLOUT,
                  PDV.QTD_VDA * VLRSELL.VLR_SELLOUT AS SELL_OUT,
                  F.TIP_CODIGO * 10 + F.TIP_DIGITO AS FORN,
                  F.TIP_RAZAO_SOCIAL AS DESC_FORN,
                  PROF.OFFER_ID || PDV.DATA_LOG || PROF.FILIAL ||
                  PROF.PRODUTO AS CHV,
                  SYSDATE AS APURADO
             FROM OFERTAS_PROFIMETRICS_TODAS PROF
            INNER JOIN OFERTAS_VENDAS_PROMOCIONAIS PDV
               ON PROF.FILIAL = PDV.LOJA_LOG * 10 + DAC(PDV.LOJA_LOG)
              AND PROF.PRODUTO = PDV.COD_ITEM
              AND PROF.DIA_ATUAL = PDV.DATA_LOG
            INNER JOIN (select DISTINCT OFFER_ID,
                                       SKU_ID,
                                       PROMO_PRICE,
                                       OFFER_PRODUCT_ATTR_01_NO AS VLR_SELLOUT
                         from OFERTAS_PRF_SELLOUT t) VLRSELL
               ON PROF.PRODUTO = VLRSELL.SKU_ID
              AND PROF.OFFER_ID = VLRSELL.OFFER_ID
            INNER JOIN AA3CITEM I
               ON PROF.PRODUTO = I.GIT_COD_ITEM * 10 + I.GIT_DIGITO
            INNER JOIN AA2CTIPO F
               ON I.GIT_COD_FOR = F.TIP_CODIGO
            WHERE PROF.DIA_ATUAL BETWEEN PROF.DATA_INI AND PROF.DATA_FIM
              AND PDV.DATA_LOG >= V_DIA_ATUAL - 7) A
    ON (G.CHV = A.CHV)
    WHEN MATCHED THEN
      UPDATE
         SET G.QTD_VDA     = A.QTD_VDA,
             G.VLR_SELLOUT = A.VLR_SELLOUT,
             G.SELL_OUT    = A.SELL_OUT,
             G.APURADO     = APURADO
    WHEN NOT MATCHED THEN
      INSERT
        (G.DIA_VDA,
         G.OFFER_ID,
         G.FILIAL,
         G.PRODUTO,
         G.DESC_ITEM,
         G.QTD_VDA,
         G.VLR_SELLOUT,
         G.SELL_OUT,
         G.FORN,
         G.DESC_FORN,
         G.CHV,
         G.APURADO)
      VALUES
        (A.DIA_VDA,
         A.OFFER_ID,
         A.FILIAL,
         A.PRODUTO,
         A.DESC_ITEM,
         A.QTD_VDA,
         A.VLR_SELLOUT,
         A.SELL_OUT,
         A.FORN,
         A.DESC_FORN,
         A.CHV,
         A.APURADO);
  
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
    
  END APURAR_SELLOUT;

  PROCEDURE VENDAS_PROMOCIONAIS_LOG8(V_DIA_ATUAL date := trunc(sysdate)) is
  
    -- AGRUPA VENDAS DA TABELA LOG8_DESCONTO
    -- TABELA LOG8_DESCONTO SOh TEM VALORES DE VENDAS EM PROMOCAO
    -- NAO TEM ID OFERTA
  BEGIN
  
    MERGE INTO OFERTAS_VENDAS_PROMOCIONAIS G
    USING (select loja_log,
                  data_log,
                  cod_item,
                  nom_prd,
                  sum(qtd_log) AS QTD_VDA,
                  sum(preco_log) AS PRECO_LOG,
                  sum(vlr_desc) AS VLR_DESC,
                  loja_log || data_log || cod_item AS CHV
             from LOG8_DESCONTO t
            where data_log >= V_DIA_ATUAL - 7
            group by loja_log, data_log, cod_item, nom_prd) A
    ON (G.CHV = A.CHV)
    WHEN MATCHED THEN
      UPDATE
         SET G.QTD_VDA   = A.QTD_VDA,
             G.PRECO_LOG = A.PRECO_LOG,
             G.VLR_DESC  = A.VLR_DESC
    WHEN NOT MATCHED THEN
      INSERT
        (G.LOJA_LOG,
         G.DATA_LOG,
         G.COD_ITEM,
         G.NOM_PRD,
         G.QTD_VDA,
         G.PRECO_LOG,
         G.VLR_DESC,
         G.CHV)
      VALUES
        (A.LOJA_LOG,
         A.DATA_LOG,
         A.COD_ITEM,
         A.NOM_PRD,
         A.QTD_VDA,
         A.PRECO_LOG,
         A.VLR_DESC,
         A.CHV);
  
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
    
  END VENDAS_PROMOCIONAIS_LOG8;

  PROCEDURE PROC_COMP_OFERTAS is
  
    -- AGRUPA VENDAS DA TABELA LOG8_DESCONTO
    -- TABELA LOG8_DESCONTO SOh TEM VALORES DE VENDAS EM PROMOCAO
    -- NAO TEM ID OFERTA
  BEGIN
  
    ETQ_PROFIMETRICS_PDV();
    PRECOS_OFERTAS_PROFIMETRICS();
    PRECOS_OFERTAS_PROFIMETRICS_TODAS();
    VENDAS_PROMOCIONAIS_LOG8();
    APURAR_SELLOUT();
  
  END PROC_COMP_OFERTAS;

END OFF_PROFIMETRICS_PDV;
