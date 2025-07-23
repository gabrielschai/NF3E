# Migração do Documento Fiscal Eletrônico (NF3e) de Oracle para IBM Netezza utilizando PySpark

## 🔎 **Sobre o projeto**

Participei de um projeto estratégico de migração e reprocessamento de documentos fiscais eletrônicos do setor de energia elétrica, mais especificamente da Nota Fiscal de Energia Elétrica Eletrônica (NF3e – Modelo 66), cuja principal finalidade era a transferência da base de dados de um ambiente Oracle para um novo ambiente em Mainframe IBM Netezza, otimizando o armazenamento e a análise de dados fiscais no contexto do Big Data.

## 🧩 **Desafio**

A empresa necessitava:
- Migrar dados fiscais sensíveis e complexos (arquivos XML da NF3e) do banco Oracle para IBM Netezza.
- Garantir integridade, confiabilidade e rastreabilidade do processo.
- Realizar parsing eficiente dos documentos XML.
- Evitar duplicidade de dados e assegurar a performance do novo ambiente.
- Automatizar o processo via Apache Airflow e Spark em ambiente distribuído.

## 🚀 **Resultado**

Esse pipeline garante a carga segura, incremental e rastreável dos dados fiscais da NF3e para uma camada ODS (Operational Data Store), permitindo análises posteriores, integração com BI e cumprimento de requisitos legais.

⚙️ Principais etapas do processo:
1. Inicialização do ambiente PySpark:
- Configura e inicia a SparkSession com parâmetros customizados para execução em cluster.
- Define o tipo de execução (incremental, histórico ou por lista) com base em argumentos recebidos via sys.argv.~

```
  os.environ['PYSPARK_SUBMIT_ARGS'] = fUnc.vEnviron
  vNtzTable = "NF3E.ODS_NF3E_TOTAL_IMPOSTO"
  vNtzCtrlTabCdProc = 7
  vNtzTableIdGapDel = f"NF3E.IDS_REMOVER_NF3E_EX{tpEx}"
  vNtzIdGapDel = 'ID_NF3E'
  vTarPath = fUnc.vDirNF3E + 'NF3e/Files/TAR_Files/' + str(tpEx) + '/'
  vUltReg = fUnc.leUltimaRotina(cod_rotina=vNtzCtrlTabCdProc, tip_execucao=tpEx)
```

2. Validação de execução:
- Verifica se existem arquivos .tar contendo os XMLs da NF3e previamente extraídos.
- Garante que o processo só continuará se a execução anterior estiver bem-sucedida ou marcada para reinício.

```
  if os.listdir(vTarPath) != [] and (int(vUltReg["sts_rotina"]) in (1,3)) and vUltReg["cod_estagio"] >= 3:
```

3. Leitura e parse dos XMLs convertidos (Parquet):
- Lê o DataFrame DF_XML_NF3E, contendo os XMLs previamente parseados.
- Realiza a extração de campos específicos da seção <total> do XML, como valores de ICMS, FCP, PIS, COFINS, IBS e outros tributos.
- Mapeia cada campo XML para um nome de coluna mais descritivo (ex: vICMS → VLR_TOT_ICMS).

```
  df_Nf3e = spark.read.parquet(fUnc.vDirNF3E + 'NF3e/Files/' + str(tpEx) + '/DF_XML_NF3E')

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
```

4. Enriquecimento com metadados:
- Faz join com o DataFrame DF_ID_NF3E, para adicionar a coluna DAT_RECEBIMENTO_ES à base de dados que será inserida.

```
  df_IdNF3e = spark.read.parquet(fUnc.vDirNF3E + 'NF3e/Files/' + str(tpEx) + '/DF_ID_NF3E*')
  df_Nf3e = df_Nf3e.join(df_IdNF3e, df_Nf3e.ID_NF3E == df_IdNF3e.COD_NSU, "INNER") \
            .select("*").drop("COD_NSU", "STS_NF3E_SITUACAO")
```

5. Deleção de dados antigos na tabela destino:
- Gera dinamicamente um SQL para excluir registros existentes no Netezza que pertencem ao mesmo intervalo de carga atual.
- Utiliza o driver nzrcipy para executar a instrução DELETE e evitar duplicidade de dados.

```
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
```

```
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
```

```
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
```

6. Carga dos dados no Netezza:
- Realiza a escrita dos dados na tabela NF3E.ODS_NF3E_TOTAL_IMPOSTO, no modo append.
- Define explicitamente os tipos das colunas no momento da escrita para garantir a consistência com o schema do banco.

```
  df_Nf3e.write.mode("append").format("jdbc") \
    .option("url", fUnc.vNtzUrl) \
    .option("dbtable", vNtzTable) \
    .option("batchsize", fUnc.vNtzWrtBatchSize) \
    .option("user", fUnc.vNtzUserNF3E) \
    .option("password", fUnc.vNtzPassNF3E) \
    .option("connectTimeout", fUnc.vConnectTimeout) \
    .option("loginTimeout", fUnc.vLoginTimeout) \
    .option("socketTimeout", fUnc.vSocketTimeout) \
    .option("createTableColumnTypes",
      "ID_NF3E BIGINT NOT NULL," + \
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
```

7. Tratamento de exceções e logging:
- Em caso de erro, registra logs em arquivos .log específicos.
- Atualiza a tabela de controle com status e mensagens de erro detalhadas.

```
  ERR2 = 'TypeError: ' + str(ERR)
    nome_arq = os.path.basename(__file__).replace('PYSPRK_', '').replace('.py', '.log')
    with open(fUnc.vLogDirNF3E + '/NF3e/' + nome_arq, 'w') as file_log:
        file_log.write(ERR2)
    fUnc.atualizaControle(cod_rotina=vNtzCtrlTabCdProc, cod_tipo_execucao=tpEx,
                          cod_acao=fUnc.cFIMERRO,
                          dsc_erro=f"{nome_arq} - " + str(ERR).replace("'", "´").replace('"', "¨"))
    print(f"❌ ERRO em {nome_arq}:", ERR)
```
