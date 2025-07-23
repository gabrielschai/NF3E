import sys
sys.path.append('/home/airflow/airflow/include')  # Adiciona diretório com funções utilitárias
import Functions as fUnc
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import nzrcipy  # Biblioteca para conexão com o Netezza
import logging

# Define tipo de execução baseado em argumento recebido
if len(sys.argv) > 1:
    par = int(sys.argv[1])
else:
    par = 0

tpEx = par
print("⚠️  Tipo de Execução:", tpEx)

# Define variáveis de ambiente e caminhos de arquivos
os.environ['PYSPARK_SUBMIT_ARGS'] = fUnc.vEnviron
vNtzTable = "NF3E.ODS_NF3E_TOTAL_IMPOSTO"
vNtzCtrlTabCdProc = 7
vNtzTableIdGapDel = f"NF3E.IDS_REMOVER_NF3E_EX{tpEx}"
vNtzIdGapDel = 'ID_NF3E'
vTarPath = fUnc.vDirNF3E + 'NF3e/Files/TAR_Files/' + str(tpEx) + '/'
vUltReg = fUnc.leUltimaRotina(cod_rotina=vNtzCtrlTabCdProc, tip_execucao=tpEx)

try:
    # Inicializa a SparkSession definindo as regras para execução 
    spark = SparkSession.builder \
        .appName("pysprk-parse-ins-ods-nf3e-total-imposto-" + str(tpEx)) \
        .master(fUnc.vMaster) \
        .config("spark.cores.max", fUnc.vParsInsSparkCoresMax) \
        .config("spark.executor.memory", fUnc.vParsInsSparkExecMem) \
        .config("spark.executor.cores", fUnc.vParsInsSparkExecutorCores) \
        .config("spark.driver.memory", fUnc.vParsInsSparkDriverMemory) \
        .config("spark.network.timeout", fUnc.vNetworkTimeout) \
        .getOrCreate()

    # Verifica se há arquivos válidos e condições para continuar
    if os.listdir(vTarPath) != [] and (int(vUltReg["sts_rotina"]) in (1,3)) and vUltReg["cod_estagio"] >= 3:

        # Lê arquivos XML previamente parseados para DataFrame
        df_Nf3e = spark.read.parquet(fUnc.vDirNF3E + 'NF3e/Files/' + str(tpEx) + '/DF_XML_NF3E')

        # PARSE PARA COLUNAS DO XML EM NOVO DATAFRAME, o código está extraindo dados estruturados do XML que foi previamente transformado em DataFrame — ou seja,
        # está “parseando” (lendo e estruturando) os dados de um XML para colunas que farão sentido no banco de destino (Netezza).
        df_Nf3e = df_Nf3e.select(
            col("_NSU").alias("ID_NF3E"),
            col("NF3eProc.NF3e.infNF3e.total.ICMSTot.vBC").alias("VLR_TOT_ICMS_BASE_CALCULO"),
            col("NF3eProc.NF3e.infNF3e.total.ICMSTot.vICMS").alias("VLR_TOT_ICMS"),
            col("NF3eProc.NF3e.infNF3e.total.ICMSTot.vICMSDeson").alias("VLR_TOT_ICMS_DESONERADO"),
            col("NF3eProc.NF3e.infNF3e.total.ICMSTot.vFCP").alias("VLR_TOT_FCP"),
            col("NF3eProc.NF3e.infNF3e.total.ICMSTot.vBCST").alias("VLR_TOT_ICMS_ST_BASE_CALCULO"),
            col("NF3eProc.NF3e.infNF3e.total.ICMSTot.vST").alias("VLR_TOT_ICMS_ST"),
            col("NF3eProc.NF3e.infNF3e.total.ICMSTot.vFCPST").alias("VLR_TOT_FCP_ST"),
            col("NF3eProc.NF3e.infNF3e.total.vRetTribTot.vRetPIS").alias("VLR_TOT_PIS_RETIDO"),
            col("NF3eProc.NF3e.infNF3e.total.vRetTribTot.vRetCofins").alias("VLR_TOT_COFINS_RETIDO"),
            col("NF3eProc.NF3e.infNF3e.total.vRetTribTot.vRetCSLL").alias("VLR_TOT_CSLL_RETIDO"),
            col("NF3eProc.NF3e.infNF3e.total.vRetTribTot.vIRRF").alias("VLR_TOT_IRRF_RETIDO"),
            col("NF3eProc.NF3e.infNF3e.total.vCOFINS").alias("VLR_TOT_COFINS"),
            col("NF3eProc.NF3e.infNF3e.total.vCOFINSEfet").alias("VLR_TOT_COFINS_EFETIVO"),
            col("NF3eProc.NF3e.infNF3e.total.vPIS").alias("VLR_TOT_PIS"),
            col("NF3eProc.NF3e.infNF3e.total.vPISEfet").alias("VLR_TOT_PIS_EFETIVO"),
            col("NF3eProc.NF3e.infNF3e.total.vNF").alias("VLR_TOT_NFE"),
            col("NF3eProc.NF3e.infNF3e.total.vTotDFe").alias("VLR_TOT_DFE"), 
            col("NF3eProc.NF3e.infNF3e.total.IBSCBSTot.gIBS.gIBSUF.vDevTrib").alias("VLR_IBS_UF_TOTAL_DEVOLUCAO"),
            col("NF3eProc.NF3e.infNF3e.total.IBSCBSTot.gIBS.gIBSUF.vDif").alias("VLR_IBS_UF_TOTAL_DIFERIMENTO"),
            col("NF3eProc.NF3e.infNF3e.total.IBSCBSTot.gIBS.gIBSUF.vIBSUF").alias("VLR_IBS_UF_TOTAL"),      
            col("NF3eProc.NF3e.infNF3e.total.IBSCBSTot.gIBS.vCredPres").alias("VLR_IBS_CREDITO_PRESUMIDO_TOTAL"),
            col("NF3eProc.NF3e.infNF3e.total.IBSCBSTot.gIBS.vCredPresCondSus").alias("VLR_IBS_CRED_PRES_COND_SUSP_TOT"),
            col("NF3eProc.NF3e.infNF3e.total.IBSCBSTot.gIBS.vIBS").alias("VLR_IBS_TOTAL"),
            col("NF3eProc.NF3e.infNF3e.total.IBSCBSTot.vBCIBSCBS").alias("VLR_IBS_CBS_TOTAL_BASE_CALCULO"),
            current_timestamp().alias("DAT_ULT_ALTERACAO")
        )
        
        print("✅  PARSE ")

        # Realiza join com IDs para adicionar data de recebimento
        df_IdNF3e = spark.read.parquet(fUnc.vDirNF3E + 'NF3e/Files/' + str(tpEx) + '/DF_ID_NF3E*')
        df_Nf3e = df_Nf3e.join(df_IdNF3e, df_Nf3e.ID_NF3E == df_IdNF3e.COD_NSU, "INNER") \
                         .select("*").drop("COD_NSU", "STS_NF3E_SITUACAO")

        # Monta query de DELETE para evitar duplicidade na tabela de destino 
        sql_del = ''
        if not(df_Nf3e.isEmpty()):
            if tpEx in (fUnc.cINCREMENTAL, fUnc.cINTERVALO, fUnc.cHISTORICO):
                if vUltReg['cod_chave_inicial'].isnumeric():
                    sql_del = f"DELETE FROM {vNtzTable} WHERE ID_NF3E > {vUltReg['cod_chave_inicial']} AND ID_NF3E <= {vUltReg['cod_chave_final']}"
                else:
                    dflimits = df_Nf3e.select(
                        max(col('DAT_RECEBIMENTO_ES')).cast('string').alias("max"),
                        min(col('DAT_RECEBIMENTO_ES')).cast('string').alias('min')
                    )
                    max_dt = dflimits.select(col('max')).first()[0]
                    min_dt = dflimits.select(col('min')).first()[0]
                    if max_dt:
                        sql_del = f"DELETE FROM {vNtzTable} WHERE DAT_RECEBIMENTO_ES >= '{min_dt}'::timestamp AND DAT_RECEBIMENTO_ES <= '{max_dt}'::timestamp"
            elif tpEx == fUnc.cLISTA:
                sql_del = f"DELETE FROM {vNtzTable} WHERE {vNtzIdGapDel} IN (SELECT {vNtzIdGapDel} FROM {vNtzTableIdGapDel})"

              ### MONTANDO SQL PARA DELEÇÃO
        sql_del=''
        if not(df_Nf3e.isEmpty()):
            if tpEx in (fUnc.cINCREMENTAL ,fUnc.cINTERVALO ,fUnc.cHISTORICO):                
                bEhNumerico=vUltReg['cod_chave_inicial'].isnumeric()
                if bEhNumerico==True:
                    sql_del="DELETE FROM " + vNtzTable + \
                        " WHERE ID_NF3E  > " + str(vUltReg['cod_chave_inicial']) + \
                        " AND ID_NF3E  <= " + str(vUltReg['cod_chave_final'])
                else:                 
                    dflimits=df_Nf3e.select( max(col('DAT_RECEBIMENTO_ES')).cast('string').alias("max"),min(col('DAT_RECEBIMENTO_ES')).cast('string').alias('min'))
                    max=dflimits.select(col('max')).first()[0]
                    min=dflimits.select(col('min')).first()[0]
                    if max != None:
                        sql_del="DELETE FROM " + vNtzTable + \
                            f" WHERE DAT_RECEBIMENTO_ES >= '{min}'::timestamp"+\
                            f" AND DAT_RECEBIMENTO_ES <= '{max}'::timestamp"
                    else:
                        sql_del=''
            elif tpEx==fUnc.cLISTA:
                sql_del=f"DELETE FROM {vNtzTable} " + \
                            f"WHERE {vNtzIdGapDel} IN (SELECT {vNtzIdGapDel} FROM  {vNtzTableIdGapDel})"     


            ### DELETANDO A TABELA PARA EVITAR DUPLICIDADE
            print("⚠️ DELETANDO --> ",vNtzTable)

            if sql_del!='':
                conn = nzrcipy.connect(host=fUnc.vNtzHost, port=fUnc.vNtzPort,
                                    database=fUnc.vNtzDatabase, user=fUnc.vNtzUserNF3E,  password=fUnc.vNtzPassNF3E,logLevel=logging.WARN, timeout=fUnc.vLoginTimeout)
                with conn.cursor() as cursor:  
                    cursor.execute(sql_del)
                    q=cursor.rowcount
                    cursor.close()
                conn.close()    
                print (f"✅ DELETADOS  :{q} registros")
            q=df_Nf3e.count()
            

            # Insere novos dados na tabela ODS do Netezza
              df_Nf3e.write.mode("append").format("jdbc") \
                                            .option("url", fUnc.vNtzUrl) \
                                            .option("dbtable", vNtzTable) \
                                            .option("batchsize", fUnc.vNtzWrtBatchSize) \
                                            .option("user", fUnc.vNtzUserNF3E) \
                                            .option("password", fUnc.vNtzPassNF3E) \
                                            .option("connectTimeout", fUnc.vConnectTimeout) \
                                            .option("loginTimeout", fUnc.vLoginTimeout) \
                                            .option("socketTimeout", fUnc.vSocketTimeout) \
                                            .option("createTableColumnTypes", "ID_NF3E BIGINT NOT NULL," + \
                                                                                "VLR_TOT_ICMS_BASE_CALCULO NUMERIC(16,2)," + \
                                                                                "VLR_TOT_ICMS NUMERIC(16,2)," + \
                                                                                "VLR_TOT_ICMS_DESONERADO NUMERIC(16,2)," + \
                                                                                "VLR_TOT_FCP NUMERIC(16,2)," + \
                                                                                "VLR_TOT_ICMS_ST_BASE_CALCULO NUMERIC(16,2)," + \
                                                                                "VLR_TOT_ICMS_ST NUMERIC(16,2)," + \
                                                                                "VLR_TOT_FCP_ST NUMERIC(16,2)," + \
                                                                                "VLR_TOT_PIS_RETIDO NUMERIC(16,2)," + \
                                                                                "VLR_TOT_COFINS_RETIDO NUMERIC(16,2)," + \
                                                                                "VLR_TOT_CSLL_RETIDO NUMERIC(16,2)," + \
                                                                                "VLR_TOT_IRRF_RETIDO NUMERIC(16,2)," + \
                                                                                "VLR_TOT_COFINS NUMERIC(16,2)," + \
                                                                                "VLR_TOT_COFINS_EFETIVO NUMERIC(16,2)," + \
                                                                                "VLR_TOT_PIS NUMERIC(16,2)," + \
                                                                                "VLR_TOT_PIS_EFETIVO NUMERIC(16,2)," + \
                                                                                "VLR_TOT_NFE NUMERIC(16,2)," + \
                                                                                "VLR_TOT_DFE NUMERIC(16,2)," + \
                                                                                "DAT_RECEBIMENTO_ES TIMESTAMP," + \
                                                                                "DAT_ULT_ALTERACAO TIMESTAMP").save()

        else:
            print("⚠️ DATAFRAME vazio, não há dados a serem inseridos!")

    else:
        print("⚠️ Sem condição de execução")

except Exception as ERR:
    # Log e atualização de controle em caso de erro
    ERR2 = 'TypeError: ' + str(ERR)
    nome_arq = os.path.basename(__file__).replace('PYSPRK_', '').replace('.py', '.log')
    with open(fUnc.vLogDirNF3E + '/NF3e/' + nome_arq, 'w') as file_log:
        file_log.write(ERR2)
    fUnc.atualizaControle(cod_rotina=vNtzCtrlTabCdProc, cod_tipo_execucao=tpEx,
                          cod_acao=fUnc.cFIMERRO,
                          dsc_erro=f"{nome_arq} - " + str(ERR).replace("'", "´").replace('"', "¨"))
    print(f"❌ ERRO em {nome_arq}:", ERR)
    raise ERR
