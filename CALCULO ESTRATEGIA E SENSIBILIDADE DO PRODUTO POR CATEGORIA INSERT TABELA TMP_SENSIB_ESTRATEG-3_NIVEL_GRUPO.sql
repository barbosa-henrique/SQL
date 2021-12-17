DECLARE
  --CALCULO ESTRATEGIA E SENSIBILIDADE DO PRODUTO POR CATEGORIA
  --Dentro do CURSOR, inserir o DEPARTAMENTO, SECAO E GRUPO das categorias que terão a sensibilidade e estratégia calculadas
  CURSOR drive IS
    SELECT DISTINCT ncc_departamento as dpto,
                    ncc_secao        as sec,
                    ncc_grupo        as grp
      FROM AA3CNVCC
     WHERE ncc_departamento IN (3)
       /*AND ncc_secao IN ()
       AND ncc_grupo IN ()*/
       AND ncc_secao > 0
       AND ncc_grupo > 0
       AND ncc_subgrupo > 0;

  V_INI PLS_INTEGER;
  V_FIM PLS_INTEGER;

BEGIN

  V_INI := 1190601;
  V_FIM := 1210601;

  FOR rec IN drive LOOP
    --------------------------------------------------------------------------------------------------- INI INSERT BASE_VDA_ESTRAT_SENSIB - GRAVA TABELA TEMPORARIA COM BASE AGREGADA DE VENDAS
    begin
      INSERT INTO BASE_VDA_ESTRAT_SENSIB
        (produto,
         dia,
         vda,
         qtd,
         custo,
         day_count,
         merch_l4,
         min_dia,
         max_dia,
         descr,
         dta_ent,
         dep,
         sec,
         grp,
         sgrp,
         cat)
        (SELECT V.PRODUTO,
                V.DIA,
                SUM(V.VDA_VLR) - SUM(V.VDA_DEV_VLR) AS VDA,
                SUM(V.VDA_QTD) - SUM(V.VDA_DEV_QTD) AS QTD,
                (SUM(V.VDA_CMV) - SUM(V.VDA_DEV_CMV)) /*CMV*/
                + (SUM(V.VDA_ICMS) - SUM(V.VDA_DEV_ICMS)) /*ICMS*/
                + (SUM(V.VDA_PIS) - SUM(V.VDA_DEV_PIS)) /*PIS*/
                + (SUM(V.VDA_COFINS) - SUM(V.VDA_DEV_COFINS)) /*COFINS*/
                + (SUM(V.VDA_CPMF) - SUM(V.VDA_DEV_CPMF)) /*CPMF*/
                + (SUM(V.VDA_ST) - SUM(V.VDA_DEV_ST)) /*ST*/
                + (SUM(V.VDA_ICMS_ST) - SUM(V.VDA_DEV_ICMS_ST)) /*IMCS_ST*/
                + (SUM(V.VDA_IPI) - SUM(V.VDA_DEV_IPI)) /*IPI*/ AS CUSTO,
                COUNT(DISTINCT V.DIA) AS DAY_COUNT,
                1 || '.' || LPAD(V.DEP, 3, 0) || '.' || LPAD(V.SEC, 3, 0) || '.' ||
                LPAD(V.GRP, 3, 0) AS MERCH_L4,
                MIN(V.DIA) AS MIN_DIA,
                MAX(V.DIA) AS MAX_DIA,
                TRIM(INITCAP(I.GIT_DESCRICAO)) AS DESCR,
                rms6to_rms7(I.GIT_DAT_ENT_LIN) AS DTA_ENT,
                V.DEP,
                V.SEC,
                V.GRP,
                V.SGRP,
                I.GIT_CATEGORIA AS CAT -- V.*
           from GS_AGG_COML_PROD V
           LEFT JOIN AA3CITEM I
             ON V.PRODUTO = I.GIT_COD_ITEM
            AND DAC(V.PRODUTO) = I.GIT_DIGITO
            AND V.DEP = I.GIT_DEPTO
            AND V.SEC = I.GIT_SECAO
            AND V.GRP = I.GIT_GRUPO
            AND V.SGRP = I.GIT_SUBGRUPO
            AND V.FORNECEDOR = I.GIT_COD_FOR
          WHERE V.DIA BETWEEN V_INI AND V_FIM
            AND V.FILIAL > 0 --INDICE
            AND V.FILIAL < 900 --INDICE
            AND V.PRODUTO > 0 --INDICE
            AND V.FORNECEDOR > 0 --INDICE
            AND I.GIT_DAT_SAI_LIN = 0
            AND I.GIT_COD_ITEM > 0 --INDICE
            AND I.GIT_COD_FOR > 0 --INDICE
            AND I.GIT_DEPTO = rec.dpto --INDICE
            AND I.GIT_SECAO = rec.sec --INDICE
            AND I.GIT_GRUPO = rec.grp --INDICE
            --AND I.GIT_SUBGRUPO = rec.sgrp --INDICE
            AND V.DEP = rec.dpto --INDICE
            AND V.SEC = rec.sec --INDICE
            AND V.GRP = rec.grp --INDICE
            --AND V.SGRP = rec.sgrp --INDICE
          GROUP BY V.DIA,
                   V.PRODUTO,
                   V.DEP,
                   V.SEC,
                   V.GRP,
                   I.GIT_DESCRICAO,
                   I.GIT_DAT_ENT_LIN,
                   V.DEP,
                   V.SEC,
                   V.GRP,
                   V.SGRP,
                   I.GIT_CATEGORIA);
      COMMIT;
    END;
    --------------------------------------------------------------------------------------------------- FIM INSERT BASE_VDA_ESTRAT_SENSIB - GRAVA TABELA TEMPORARIA COM BASE AGREGADA DE VENDAS

    --------------------------------------------------------------------------------------------------- INI INSERT TMP_SENSIB_ESTRATEG - GRAVA ESTRATEGIA E SENSIBILIDADE
    INSERT into TMP_SENSIB_ESTRATEG
    --------------------------------------------------------------------------------------------------- INI QUERY GERA ESTRATEGIA E SENSIBILIDADE
      SELECT ( --CASE SENSIBILIDADE E ESTRATEGIA 
             CASE
               WHEN (GERAL.quartile_mrg_pct IN (3, 4) AND
                    /*GERAL.quartile_sls_qty IN (1)*/
                    GERAL.CURVA_M_CUPOM_DIA = 'A') OR -- 'TRAFEGO'
                    (GERAL.quartile_sls_qty = 1 AND
                    GERAL.CURVA_M_CUPOM_DIA = 'A') --'Volume MAIS representativo'
                THEN
                'SUPER SENSIVEL'
               WHEN (GERAL.quartile_sls_qty in (1, 2) OR
                    GERAL.quartile_sls_val IN (1, 2) OR
                    (GERAL.quartile_mrg_pct = 1 AND
                    GERAL.quartile_sls_qty = 3)) --'MARGEM vendas mais representativas'
                THEN
                'SENSIVEL'
               ELSE
                'POUCO SENSIVEL'
             END) AS sensitivity,
             CASE
               WHEN (GERAL.quartile_mrg_pct IN (1) AND
                    GERAL.quartile_sls_qty NOT IN (1, 2)) THEN
                'MARGEM'
               WHEN (GERAL.quartile_mrg_pct IN (3, 4) AND
                    GERAL.CURVA_M_CUPOM_DIA = 'A') OR
                    (GERAL.quartile_mrg_pct IN (2) AND
                    GERAL.quartile_M_CUPOM IN (1) AND
                    GERAL.CURVA_M_CUPOM_DIA = 'A') THEN
                'TRAFEGO'
               WHEN (GERAL.quartile_sls_val IN (1, 2) AND
                    GERAL.quartile_M_CUPOM IN (1, 2)) OR
                    (GERAL.quartile_sls_qty IN (1, 2) AND
                    GERAL.quartile_M_CUPOM IN (1, 2)) THEN
                'VOLUME'
               WHEN (GERAL.quartile_sls_qty > 3) THEN
                'IMAGEM'
               ELSE
                'IMAGEM'
             END AS strategy,
             
             GERAL.*
        FROM ( --CURVAS
              SELECT CASE
                        WHEN FIM.ACUM_TOTAL < 51 THEN
                         'A'
                        WHEN FIM.ACUM_TOTAL < 81 THEN
                         'B'
                        ELSE
                         'C'
                      END AS CURVA_TOTAL,
                      CASE
                        WHEN FIM.ACUM_M_CUPOM_DIA < 51 THEN
                         'A'
                        WHEN FIM.ACUM_M_CUPOM_DIA < 81 THEN
                         'B'
                        ELSE
                         'C'
                      END AS CURVA_M_CUPOM_DIA,
                      FIM.*
                FROM ( --DEFINE QUARTIL E ACUMULADOS
                       SELECT QUARTIL.PRODUTO * 10 + DAC(QUARTIL.PRODUTO) AS PRODUTO,
                               QUARTIL.DESCR,
                               QUARTIL.VDA,
                               QUARTIL.QTD,
                               QUARTIL.CUSTO,
                               QUARTIL.MG_VAL,
                               QUARTIL.MG_PCT,
                               QUARTIL.QTD_CUPOM,
                               ROUND(QUARTIL.M_CUPOM_DIA, 2) AS M_CUPOM_DIA,
                               QUARTIL.DIA_ANALISE,
                               QUARTIL.PEN_TOTAL,
                               SUM(QUARTIL.PEN_TOTAL) OVER(ORDER BY QUARTIL.PEN_TOTAL DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ACUM_TOTAL,
                               QUARTIL.PEN_M_CUPOM_DIA,
                               SUM(QUARTIL.PEN_M_CUPOM_DIA) OVER(ORDER BY QUARTIL.PEN_M_CUPOM_DIA DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ACUM_M_CUPOM_DIA,
                               QUARTIL.DAY_COUNT,
                               QUARTIL.MERCH_L4,
                               ntile(4) over(ORDER BY QUARTIL.MG_PCT DESC) AS quartile_mrg_pct,
                               ntile(4) over(ORDER BY QUARTIL.MG_VAL DESC) AS quartile_mrg_valor,
                               ntile(4) over(ORDER BY QUARTIL.QTD DESC) AS quartile_sls_qty,
                               ntile(4) over(ORDER BY QUARTIL.VDA DESC) AS quartile_sls_val,
                               ntile(4) over(ORDER BY QUARTIL.DAY_COUNT DESC) quartile_day_count,
                               ntile(4) over(ORDER BY QUARTIL.QTD_CUPOM DESC) quartile_QTD_CUPOM,
                               ntile(4) over(ORDER BY QUARTIL.M_CUPOM_DIA DESC) quartile_M_CUPOM,
                               ntile(4) over(ORDER BY QUARTIL.PEN_TOTAL DESC) quartile_PEN_CUPOM,
                               ntile(4) over(ORDER BY QUARTIL.PEN_M_CUPOM_DIA DESC) quartile_PEN_M_CUPOM_DIA,
                               TO_CHAR(TRUNC(SYSDATE), 'YYYY') ||
                               TO_CHAR(TRUNC(SYSDATE), 'MM') ||
                               TO_CHAR(TRUNC(SYSDATE), 'DD') || '_NIVEL_GRUPO' AS DATA_CALCULADO,
                               QUARTIL.MIN_DIA,
                               QUARTIL.MAX_DIA,
                               QUARTIL.DTA_ENT,
                               QUARTIL.DIAS_LINHA
                         FROM ( --CALCULOS  : DIAS EM LINHA; PENETRACAO; MEDIA_CUPOM_DIA; DIA_ANALISE; 
                                SELECT BASE.PRODUTO,
                                        ROUND(SUM(BASE.VDA), 2) AS VDA,
                                        ROUND(SUM(BASE.QTD), 2) AS QTD,
                                        ROUND(SUM(BASE.CUSTO), 2) AS CUSTO,
                                        (SUM(BASE.VDA) - SUM(BASE.CUSTO)) AS MG_VAL,
                                        CASE
                                          WHEN SUM(BASE.CUSTO) > 0 THEN
                                           ROUND((SUM(BASE.VDA) - SUM(BASE.CUSTO)) /
                                                 SUM(BASE.VDA) * 100,
                                                 2)
                                          ELSE
                                           0
                                        END AS MG_PCT,
                                        SUM(BASE.DAY_COUNT) AS DAY_COUNT,
                                        BASE.MERCH_L4,
                                        SUM(CP.QTD_CUPOM) AS QTD_CUPOM,
                                        ROUND(SUM(CP.QTD_CUPOM) * 100 /
                                              (SELECT SUM(SUM(CP.QTD_CUPOM)) AS QTD_CUPOM
                                                 FROM (SELECT V.PRODUTO, V.DIA
                                                         from BASE_VDA_ESTRAT_SENSIB V
                                                        WHERE V.DIA BETWEEN V_INI AND
                                                              V_FIM
                                                          AND V.PRODUTO > 0 --INDICE
                                                          AND V.DEP = rec.dpto --INDICE
                                                          AND V.SEC = rec.sec --INDICE
                                                          AND V.GRP = rec.grp --INDICE
                                                          --AND V.SGRP = rec.sgrp --INDICE
                                                       ) BASE
                                                 LEFT JOIN DET_CUPOM_CONSOLIDADO_DIA CP
                                                   ON BASE.PRODUTO * 10 +
                                                      DAC(BASE.PRODUTO) =
                                                      CP.COD_PROD_CUPOM
                                                  AND BASE.DIA = CP.DATA_CUPOM
                                                WHERE CP.COD_PROD_CUPOM > 0 --INDICE
                                                  AND CP.DATA_CUPOM >= V_INI --INDICE
                                                GROUP BY BASE.PRODUTO),
                                              2) AS PEN_TOTAL,
                                        CASE
                                          WHEN rms7to_date(BASE.DTA_ENT) <
                                               rms7to_date(V_INI) THEN
                                           SUM(CP.QTD_CUPOM) /
                                           (rms7to_date(V_FIM) - rms7to_date(V_INI))
                                          ELSE
                                           SUM(CP.QTD_CUPOM) /
                                           (rms7to_date(V_FIM) -
                                            rms7to_date(BASE.DTA_ENT))
                                        END AS M_CUPOM_DIA,
                                        ROUND((CASE
                                                WHEN rms7to_date(BASE.DTA_ENT) <
                                                     rms7to_date(V_INI) THEN
                                                 SUM(CP.QTD_CUPOM) /
                                                 (rms7to_date(V_FIM) - rms7to_date(V_INI))
                                                ELSE
                                                 SUM(CP.QTD_CUPOM) /
                                                 (rms7to_date(V_FIM) -
                                                  rms7to_date(BASE.DTA_ENT))
                                              END) * 100 /
                                              (SELECT SUM(CASE
                                                            WHEN rms7to_date(BASE.DTA_ENT) <
                                                                 rms7to_date(V_INI) THEN
                                                             SUM(CP.QTD_CUPOM) /
                                                             (rms7to_date(V_FIM) -
                                                              rms7to_date(V_INI))
                                                            ELSE
                                                             SUM(CP.QTD_CUPOM) /
                                                             (rms7to_date(V_FIM) -
                                                              rms7to_date(BASE.DTA_ENT))
                                                          END) AS QTD_CUPOM
                                                 FROM (SELECT PRODUTO, DIA, DTA_ENT
                                                         from BASE_VDA_ESTRAT_SENSIB V
                                                        WHERE V.DIA BETWEEN V_INI AND
                                                              V_FIM
                                                          AND V.PRODUTO > 0 --INDICE
                                                          AND V.DEP = rec.dpto --INDICE
                                                          AND V.SEC = rec.sec --INDICE
                                                          AND V.GRP = rec.grp --INDICE
                                                          --AND V.SGRP = rec.sgrp --INDICE
                                                       ) BASE
                                                 LEFT JOIN DET_CUPOM_CONSOLIDADO_DIA CP
                                                   ON BASE.PRODUTO * 10 +
                                                      DAC(BASE.PRODUTO) =
                                                      CP.COD_PROD_CUPOM
                                                  AND BASE.DIA = CP.DATA_CUPOM
                                                WHERE CP.COD_PROD_CUPOM > 0 --INDICE
                                                  AND CP.DATA_CUPOM >= V_INI --INDICE
                                                GROUP BY BASE.PRODUTO, BASE.DTA_ENT),
                                              2) AS PEN_M_CUPOM_DIA,
                                        CASE
                                          WHEN rms7to_date(BASE.DTA_ENT) <
                                               rms7to_date(V_INI) THEN
                                           (rms7to_date(V_FIM) - rms7to_date(V_INI))
                                          ELSE
                                           (rms7to_date(V_FIM) - 7 -
                                           rms7to_date(BASE.DTA_ENT))
                                        END AS DIA_ANALISE,
                                        MIN(BASE.MIN_DIA) AS MIN_DIA,
                                        MAX(BASE.MAX_DIA) AS MAX_DIA,
                                        BASE.DESCR,
                                        BASE.DTA_ENT,
                                        rms7to_date(V_FIM) -
                                        rms7to_date(BASE.DTA_ENT) AS DIAS_LINHA
                                  FROM ( --BASE DOS DADOS DE VENDAS E ESTRUTURA MERCADOLOLICA
                                         SELECT *
                                           from BASE_VDA_ESTRAT_SENSIB V
                                          WHERE V.DIA BETWEEN V_INI AND V_FIM
                                            AND V.PRODUTO > 0 --INDICE
                                            AND V.DEP = rec.dpto --INDICE
                                            AND V.SEC = rec.sec --INDICE
                                            AND V.GRP = rec.grp --INDICE
                                            --AND V.SGRP = rec.sgrp --INDICE
                                         ) BASE
                                  LEFT JOIN DET_CUPOM_CONSOLIDADO_DIA CP
                                    ON BASE.PRODUTO * 10 + DAC(BASE.PRODUTO) =
                                       CP.COD_PROD_CUPOM
                                   AND BASE.DIA = CP.DATA_CUPOM
                                 WHERE CP.COD_PROD_CUPOM > 0 --INDICE
                                   AND CP.DATA_CUPOM >= V_INI --INDICE
                                 GROUP BY BASE.PRODUTO,
                                           BASE.MERCH_L4,
                                           BASE.DESCR,
                                           BASE.DTA_ENT,
                                           BASE.DTA_ENT) QUARTIL) FIM) GERAL;
    --------------------------------------------------------------------------------------------------- FIM QUERY GERA ESTRATEGIA E SENSIBILIDADE
    COMMIT;
    --------------------------------------------------------------------------------------------------- FIM INSERT TMP_SENSIB_ESTRATEG - GRAVA ESTRATEGIA E SENSIBILIDADE  

    --------------------------------------------------------------------------------------------------- INI DELETE BASE_VDA_ESTRAT_SENSIB - DELETA TABELA TEMPORARIA COM BASE AGREGADA DE VENDAS
    BEGIN
      DELETE FROM BASE_VDA_ESTRAT_SENSIB
       WHERE DIA BETWEEN V_INI AND V_FIM
         AND DEP = rec.dpto
         AND SEC = rec.sec
         AND GRP = rec.grp
         /*AND SGRP = rec.sgrp*/;
      COMMIT;
    END;
    --------------------------------------------------------------------------------------------------- FIM DELETE BASE_VDA_ESTRAT_SENSIB - DELETA TABELA TEMPORARIA COM BASE AGREGADA DE VENDAS
  
  END LOOP;

END;
