%python
 
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame,Row
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date,date_add,when,col,lit,coalesce,concat,lag,lead,current_timestamp,date_format,to_timestamp
from pyspark.sql.types import StringType
from delta.tables import *
 
sapHANAUrl    = "jdbc:sap://10.50.194.148:30015"
sapHANADriver = "com.sap.db.jdbc.Driver"
hanaUser      = <username>
hanaPassword  = <password>
 
#dbutils.widgets.text("IN_SUBSCRIPTION_CATEGORY", "")
#in_subscription_category = dbutils.widgets.get("IN_SUBSCRIPTION_CATEGORY")
#in_calc_days_back = dbutils.widgets.get("IN_CALC_DAYS_BACK")
#in_full_load = dbutils.widgets.get("IN_FULL_LOAD")
#default_start_dt = dbutils.widgets.get("DEFAULT_START_DT")
 
in_subscription_category = 'SERV'
in_calc_days_back = 3
in_full_load = 'no'
 
#variables
st_shema_name = "TRNFRM_ECC_CCM"
st_group_name = "SCD_IN_SUBS_STATUS"
st_workflow_name = "SP_UPDATE_SCD_IN_SUBS_STATUS"
st_attribute_name = "UPDATE_CUTOFF_DATE"
 
spark = SparkSession.builder \
    .appName("update_scd_in_subscription_status") \
    .getOrCreate()
 
#Default_start_date
default_start_dt = "2021-09-10"
  
#user defined function to get subscription category dimension
def get_dim_subs_category(sapHANAUrl,sapHANADriver,hanaUser,hanaPassword,in_subscription_category) -> DataFrame:
 
    dim_subs_category_sql = '''(select subscription_category
                                       ,subscription_type
                                       ,sales_document_type
                                  from TRNFRM_ECC_CCM.dim_subscription dim_sub
                                  where dim_sub.subscription_category = upper('{in_subscription_category}')
                            )'''.format(in_subscription_category=in_subscription_category)
    df_src_data = spark.read.format("jdbc") \
        .option("url", sapHANAUrl) \
        .option("driver", sapHANADriver) \
        .option("user", hanaUser) \
        .option("password", hanaPassword) \
        .option("dbtable", dim_subs_category_sql) \
        .load()
 
    return df_src_data
 
def get_cutoff_date(sapHANAUrl,sapHANADriver,hanaUser,hanaPassword,st_subscription_category,st_shema_name,st_group_name,st_workflow_name,st_attribute_name,in_calc_days_back,default_start_dt) -> DataFrame:
 
    get_cutoff_date_sql = '''(SELECT CASE WHEN (select
                                                    count(*) as int_date_exists
                                                 from common.process_delta_log
                                                 where schma_nm = '{st_shema_name}'
                                                    and grp_nm = '{st_group_name}'
                                                    and wrkflw_nm = '{st_workflow_name}' ||'_'||'{st_subscription_category}'
                                                    and attr_nm = '{st_attribute_name}') = 0
                                            THEN '{default_start_dt}'
                                            ELSE
                                                (select
                                                    add_days(attr_val_dt,
                                                    cast ((-1 * {in_calc_days_back}) as integer) ) as ts_cutoff_date
                                                from common.process_delta_log
                                                where schma_nm = '{st_shema_name}'
                                                    and grp_nm = '{st_group_name}'
                                                    and wrkflw_nm = '{st_workflow_name}' ||'_'||'{st_subscription_category}'
                                                    and attr_nm = '{st_attribute_name}')
                                            END AS CUT_OFF_DATE
                                FROM DUMMY)'''.format(st_shema_name=st_shema_name,                                  
                                st_group_name=st_group_name,
                                st_workflow_name=st_workflow_name,
                                st_subscription_category=st_subscription_category,
                                st_attribute_name=st_attribute_name,
                                in_calc_days_back=in_calc_days_back,default_start_dt=default_start_dt)                          
    df_src_data = spark.read.format("jdbc") \
        .option("url", sapHANAUrl) \
        .option("driver", sapHANADriver) \
        .option("user", hanaUser) \
        .option("password", hanaPassword) \
        .option("dbtable", get_cutoff_date_sql) \
        .load()
 
    return df_src_data   
 
#get distinct subsription category
df_sub_cat = get_dim_subs_category(sapHANAUrl,sapHANADriver,hanaUser,hanaPassword,in_subscription_category)
df_distinct_sub_cat = df_sub_cat.where(
                            (df_sub_cat.SUBSCRIPTION_TYPE == 'IN') &
                            (df_sub_cat.SALES_DOCUMENT_TYPE == 'ZCSB')
                      ).select(*['SUBSCRIPTION_CATEGORY']
                      ).distinct()
 
for row in df_distinct_sub_cat.select("SUBSCRIPTION_CATEGORY").collect():
    st_subscription_category = row["SUBSCRIPTION_CATEGORY"]
     
    #get subscription dimension
    df_dim_subscription = spark.read.table("dpaas_uccatalog_stg.adxdemo.DIM_SUBSCRIPTION")
    #Filter by subscription type, sales document type and subscription category
    df_dim_subs = df_dim_subscription.where(
                                        (df_dim_subscription.SUBSCRIPTION_TYPE == 'IN') &
                                        (df_dim_subscription.SALES_DOCUMENT_TYPE == 'ZCSB') &
                                        (df_dim_subscription.SUBSCRIPTION_CATEGORY == st_subscription_category)
                                    )
     
    #get subscription params
    df_sub_params_raw = spark.read.table("dpaas_uccatalog_stg.adxdemo.SUBSCRIPTION_PARAMS")
    #Filter by parameter category and parameter name
    df_subs_params_cat_name = df_sub_params_raw.where(
                                        (df_sub_params_raw.PARAM_CATEG == 'TWP_OFFERS') &
                                        (df_sub_params_raw.PARAM_NAME == 'OFFER_TYPE')
                                    )
    df_dim_subs = df_dim_subs.join(df_subs_params_cat_name,
                          df_dim_subs.OFFER_TYPE == df_subs_params_cat_name.PARAM_VAL,
                    "left_outer"
                    ).withColumn("TRIAL_ORDER_FLAG",
                        when(col("OFFER_TYPE") == col("PARAM_VAL"),"Y").otherwise("N")
                    )           
 
    df_cutoff_dt = get_cutoff_date(sapHANAUrl,sapHANADriver,hanaUser,hanaPassword,st_subscription_category,
                                           st_shema_name,st_group_name,st_workflow_name,st_attribute_name,in_calc_days_back,default_start_dt)
     
    df_dates = spark.createDataFrame(
        data = [ ("1",df_cutoff_dt.head()[0])],
        schema=["id","TS_CUTOFF_DATE"])
    df_cutoff_dt_types = df_dates.withColumn("TS_CUTOFF_DATE_SCD",
                date_add(to_date(df_dates.TS_CUTOFF_DATE).cast('date'),-1)
            ).withColumn("TS_CUTOFF_DATE_SCD_SAP",
                 date_format(date_add(to_date(df_dates.TS_CUTOFF_DATE).cast('date'),-1),"yyyyMMdd")
            ).withColumn("TS_CUTOFF_DATE_SAP",
                date_format(df_dates.TS_CUTOFF_DATE,"yyyyMMdd")
            ).withColumn("TS_CURRENT_DATE_SAP",
                date_format(current_timestamp().cast('date'),"yyyyMMdd")
            ).withColumn("TS_CURRENT_DATE_SAP_LESS_BY_DAY",
                date_format(date_add(current_timestamp().cast('date'),-1),"yyyyMMdd")
            )
 
    ts_cutoff_date_scd_sap = df_cutoff_dt_types.head()[3]
    ts_cutoff_date_scd = df_cutoff_dt_types.head()[3]
    ts_cutoff_date_sap = df_cutoff_dt_types.head()[4]
    ts_current_date_sap = df_cutoff_dt_types.head()[5]
    ts_current_date_less_by_day = df_cutoff_dt_types.head()[6]
 
    #Hardcoded to match the records - remove later
    ts_cutoff_date_sap = "20210904"
    ts_cutoff_date_scd_sap = "20210903"
    ts_cutoff_date_scd = "20210904"
    ts_current_date_sap =  "20211003"
    ts_current_date_less_by_day =  "20211002"
 
    #get billing plan raw
    df = spark.read.table("dpaas_uccatalog_stg.adxdemo.FPLT")                       
    #Filter default dates
    df_billing_plan = df.where(
                                ((df.OFKDAT != '00000000') & (df.FKDAT != '00000000')) &
                                ((df.OFKDAT >= ts_cutoff_date_sap) &
                                    (df.OFKDAT < ts_current_date_sap)) |
                                ((df.OFKDAT <= ts_cutoff_date_scd_sap) &
                                    (df.BP_END_DATE >= ts_cutoff_date_scd_sap)) |
                                ((df.OFKDAT <= ts_current_date_sap) &
                                    (df.BP_END_DATE >= ts_current_date_sap))
                            ).withColumn("BP_START_DATE",
                                when(df.OFKDAT > df.FKDAT,df.FKDAT).otherwise(df.OFKDAT)
                            )
 
    df_delta_fplt = df_billing_plan.join(df_dim_subs,
                         df_billing_plan.BP_NO == df_dim_subs.BILL_PLAN_NUMBER,
                         "inner"
                        ).select(*['CLIENT',
                                    'SALES_DOCUMENT',
                                    'SALES_DOCUMENT_ITEM',
                                    'BP_NO',
                                    'BP_ITEM',
                                    'BP_START_DATE',
                                    'BP_END_DATE',
                                    'BP_BILLING_BLOCK',
                                    'BP_NET_PRICE',
                                    'BP_UPD_IND'])
    #df_delta_fplt.show()
    df_delta_fplt.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT_2")
 
    df_delta_fplt2 = df_delta_fplt.where(
                                        (df_delta_fplt.BP_START_DATE >= ts_cutoff_date_sap) & \
                                        (df_delta_fplt.BP_START_DATE <= ts_current_date_sap)
                                    ).select(*['CLIENT',
                                            'SALES_DOCUMENT',
                                            'SALES_DOCUMENT_ITEM',
                                            'BP_NO',
                                            'BP_ITEM',
                                            'BP_START_DATE',
                                            'BP_END_DATE',
                                            'BP_BILLING_BLOCK',
                                            'BP_NET_PRICE'])
    #df_delta_fplt2.show()
    df_delta_fplt2.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT2_2")
     
    df_delta_fplt3 = df_delta_fplt.where(
                                        (df_delta_fplt.BP_END_DATE >= ts_cutoff_date_scd) & \
                                        (df_delta_fplt.BP_END_DATE <= ts_current_date_less_by_day)
                                    ).withColumn("EVENT_DATE",to_timestamp(date_add(to_date(df_delta_fplt.BP_END_DATE,'yyyyMMdd'),1),"yyyy-MM-dd HH:mm:ss")).select(*['CLIENT',
                                            'SALES_DOCUMENT',
                                            'SALES_DOCUMENT_ITEM',
                                            'BP_NO',
                                            'BP_ITEM',
                                            'BP_START_DATE',
                                            'EVENT_DATE',
                                            'BP_BILLING_BLOCK',
                                            'BP_NET_PRICE'])
    #df_delta_fplt3.show(10)
     
    df_delta_fplt3.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT3_2")                                   
     
    df_bdocs_item_data_vbrp = spark.read.table("dpaas_uccatalog_stg.adxdemo.VBRP")
     
    df_bdocs_header_data_vbrk = spark.read.table("dpaas_uccatalog_stg.adxdemo.VBRK")
 
    df_bdocs_header_data_vbrk_invoice = df_bdocs_header_data_vbrk.where(
        df_bdocs_header_data_vbrk.FKART == 'F2'
    )
     
    df_bill_docs = df_bdocs_item_data_vbrp.join(df_bdocs_header_data_vbrk_invoice,
                                (df_bdocs_item_data_vbrp.MANDT == df_bdocs_header_data_vbrk_invoice.MANDT) &
                                (df_bdocs_item_data_vbrp.BILLING_DOCUMENT == df_bdocs_header_data_vbrk_invoice.VBELN),
                                "inner"
                           ).join(df_delta_fplt,
                                 (df_delta_fplt.CLIENT == df_bdocs_item_data_vbrp.MANDT) &
                                 (df_delta_fplt.BP_NO == df_bdocs_item_data_vbrp.BP_NO) &
                                 (df_delta_fplt.BP_ITEM == df_bdocs_item_data_vbrp.BP_ITEM),
                                 "inner"
                           ).select(*['BILLING_DOCUMENT',
                                     'BILLING_DOCUMENT_ITEM',
                                     df_delta_fplt.SALES_DOCUMENT,
                                     df_delta_fplt.SALES_DOCUMENT_ITEM,
                                     df_delta_fplt.BP_NO,
                                     df_delta_fplt.BP_ITEM,
                                     'CREATED_DATE'])         
    df_bill_docs.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_2")                                   
     
    #Empty dataset here
    df_bill_docs2 = df_bdocs_item_data_vbrp.join(df_bdocs_header_data_vbrk_invoice,
                              (df_bdocs_item_data_vbrp.MANDT == df_bdocs_header_data_vbrk_invoice.MANDT) &
                              (df_bdocs_item_data_vbrp.BILLING_DOCUMENT == df_bdocs_header_data_vbrk_invoice.VBELN),
                              "inner"
                        ).join(df_delta_fplt,
                              (df_delta_fplt.CLIENT == df_bdocs_item_data_vbrp.MANDT) &
                              (df_delta_fplt.BP_NO == df_bdocs_item_data_vbrp.BP_NO) &
                              (df_delta_fplt.BP_ITEM != df_bdocs_item_data_vbrp.BP_ITEM) &
                              (df_delta_fplt.BP_END_DATE == df_bdocs_item_data_vbrp.BP_END_DATE),
                              "inner"
                        ).select(*['BILLING_DOCUMENT',
                                'BILLING_DOCUMENT_ITEM',
                                df_delta_fplt.SALES_DOCUMENT,
                                df_delta_fplt.SALES_DOCUMENT_ITEM,
                                df_delta_fplt.BP_NO,
                                df_delta_fplt.BP_END_DATE,
                                'CREATED_DATE'])
    df_bill_docs2.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.BILL_DOCS2_2")                                   
     
    df_scd_sales_docs = spark.read.table("dpaas_uccatalog_stg.adxdemo.SCD_SALES_DOCUMENT")
     
    df_scd_sales_docs_cancel_reason = df_scd_sales_docs.where(
                            (df_scd_sales_docs.CANCELLATION_REASON == 'S3') |
                            (df_scd_sales_docs.CANCELLATION_REASON == 'SC')
                        ).select(*['SALES_DOCUMENT',
                                   'SALES_DOCUMENT_ITEM'])
    df_scd_sales_docs_cancel_reason.write.mode("overwrite").saveAsTable("dpaas_uccatalog_stg.adxdemo.SALES_DOCS_CANCEL_REASON_2")
                         
    df_bill_docs_s3 = df_bdocs_item_data_vbrp.where(
                                (df_bdocs_item_data_vbrp.CREATED_DATE >= ts_cutoff_date_sap) &
                                (df_bdocs_item_data_vbrp.CREATED_DATE <= ts_current_date_sap)
                        ).join(df_bdocs_header_data_vbrk_invoice,
                                (df_bdocs_item_data_vbrp.MANDT == df_bdocs_header_data_vbrk_invoice.MANDT) &
                                (df_bdocs_item_data_vbrp.BILLING_DOCUMENT == df_bdocs_header_data_vbrk_invoice.VBELN),
                            "inner"
                        ).join(df_scd_sales_docs_cancel_reason,
                                (df_scd_sales_docs_cancel_reason.SALES_DOCUMENT == df_bdocs_item_data_vbrp.SALES_DOCUMENT) &
                                (df_scd_sales_docs_cancel_reason.SALES_DOCUMENT_ITEM == df_bdocs_item_data_vbrp.SALES_DOCUMENT_ITEM),
                            "inner"   
                        ).join(df_dim_subs,
                                (df_scd_sales_docs_cancel_reason.SALES_DOCUMENT == df_dim_subs.SALES_DOCUMENT) &
                                (df_scd_sales_docs_cancel_reason.SALES_DOCUMENT_ITEM == df_dim_subs.SALES_DOCUMENT_ITEM),
                            "inner"   
                        ).select(*['CLIENT',
                                    df_bdocs_item_data_vbrp.BILLING_DOCUMENT,
                                    df_bdocs_item_data_vbrp.BILLING_DOCUMENT_ITEM,
                                    df_scd_sales_docs_cancel_reason.SALES_DOCUMENT,
                                    df_scd_sales_docs_cancel_reason.SALES_DOCUMENT_ITEM,
                                    df_bdocs_item_data_vbrp.BP_NO,
                                    df_bdocs_item_data_vbrp.BP_ITEM,
                                    'BP_END_DATE',
                                    'CREATED_DATE'])
    df_bill_docs_s3.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_S3_2")
     
    df_bdocs_header_data_vbrk_credit_memo = df_bdocs_header_data_vbrk.where(
        df_bdocs_header_data_vbrk.FKART == 'G2'
    )
     
    #df_sales_docs_item_data_vbap = get_sales_document_vbap(sapHANAUrl,sapHANADriver,hanaUser,hanaPassword)
    df_sales_docs_item_data_vbap = spark.read.table("dpaas_uccatalog_stg.adxdemo.VBAP")
     
    df_sales_docs_header_data_vbak = spark.read.table("dpaas_uccatalog_stg.adxdemo.VBAK")
    df_bill_docs_g2 = df_bdocs_item_data_vbrp.where(
                            (df_bdocs_item_data_vbrp.CREATED_DATE >= ts_cutoff_date_sap) &
                            (df_bdocs_item_data_vbrp.CREATED_DATE <= ts_current_date_sap)
                        ).join(df_bdocs_header_data_vbrk_credit_memo,
                                (df_bdocs_item_data_vbrp.MANDT == df_bdocs_header_data_vbrk_invoice.MANDT) &
                                (df_bdocs_item_data_vbrp.BILLING_DOCUMENT == df_bdocs_header_data_vbrk_invoice.VBELN),
                            "inner"
                        ).join(df_sales_docs_item_data_vbap,
                                (df_bdocs_item_data_vbrp.MANDT == df_sales_docs_item_data_vbap.MANDT) &
                                (df_bdocs_item_data_vbrp.SALES_DOCUMENT == df_sales_docs_item_data_vbap.VBELN) &
                                (df_bdocs_item_data_vbrp.SALES_DOCUMENT_ITEM == df_sales_docs_item_data_vbap.POSNR),
                            "inner"
                        ).join(df_dim_subs,
                                (df_dim_subs.CLIENT == df_sales_docs_item_data_vbap.MANDT) &
                                (df_dim_subs.SALES_DOCUMENT == df_sales_docs_item_data_vbap.VGBEL) &
                                (df_dim_subs.SALES_DOCUMENT_ITEM == df_sales_docs_item_data_vbap.VGPOS),
                            "inner"
                        ).join(df_sales_docs_header_data_vbak,
                                (df_sales_docs_header_data_vbak.MANDT == df_sales_docs_item_data_vbap.MANDT) &
                                (df_sales_docs_header_data_vbak.VBELN == df_bdocs_item_data_vbrp.SALES_DOCUMENT),
                            "inner"
                        ).select(*[df_bdocs_item_data_vbrp.MANDT,
                                   'BILLING_DOCUMENT',
                                   'BILLING_DOCUMENT_ITEM',
                                   df_dim_subs.SALES_DOCUMENT,
                                   df_dim_subs.SALES_DOCUMENT_ITEM,
                                   df_bdocs_item_data_vbrp.CREATED_DATE,
                                   'BILLING_FREQUENCY',
                                   'ORDER_REASON']
                        ).withColumn("CREATED_DATE_YYYY_MM_DD",to_date(df_bdocs_item_data_vbrp.CREATED_DATE,"yyyyMMdd")
                        ).withColumnRenamed('MANDT','CLIENT')
    df_bill_docs_g2.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_G2_2")
     
    df_dim_subs_formatted = df_dim_subs.withColumn("TS_CONTRACT_START_DATE_BP",date_format(df_dim_subs.CONTRACT_START_DATE_BP,"yyyyMMdd"))             
    df_contract_start_dt_recs = df_dim_subs_formatted.where(
                                    ((df_dim_subs_formatted.TS_CONTRACT_START_DATE_BP >= ts_cutoff_date_sap) &
                                            (df_dim_subs_formatted.TS_CONTRACT_START_DATE_BP <= ts_current_date_sap)) &
                                     (df_dim_subs_formatted.CONTRACT_START_DATE_BP > df_dim_subs_formatted.CREATED_DATE_ITEM)
                                ).select(*['CLIENT',
                                           'SALES_DOCUMENT',
                                           'SALES_DOCUMENT_ITEM',
                                           'CREATED_DATE_ITEM',
                                           'CONTRACT_START_DATE_BP']
                                ).withColumnsRenamed({'CONTRACT_START_DATE_BP':'CONTRACT_START_DATE',
                                                      'CREATED_DATE_ITEM':'CREATED_DATE'})                
    df_contract_start_dt_recs.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.CONTRACT_START_DT_RECS_2")
     
    df_scd_sales_docs_formatted = df_scd_sales_docs.withColumn("TS_START_DATE",
                                                date_format(df_scd_sales_docs.START_DATE,"yyyyMMdd")
                                            ).withColumn("TS_END_DATE",
                                                date_format(df_scd_sales_docs.END_DATE,"yyyyMMdd")
                                            )
 
    df_dim_sub_scd = df_scd_sales_docs_formatted.where(
                            ((df_scd_sales_docs_formatted.TS_START_DATE >= ts_cutoff_date_sap) &
                                (df_scd_sales_docs_formatted.TS_START_DATE <= ts_current_date_sap))
                    ).join(df_dim_subs,
                            (df_dim_subs.SALES_DOCUMENT == df_scd_sales_docs_formatted.SALES_DOCUMENT) &
                            (df_dim_subs.SALES_DOCUMENT_ITEM == df_scd_sales_docs_formatted.SALES_DOCUMENT_ITEM) &
                            (df_dim_subs.CLIENT == df_scd_sales_docs_formatted.CLIENT)
                    ).select(*[df_scd_sales_docs_formatted.CLIENT,
                               df_scd_sales_docs_formatted.SALES_DOCUMENT,
                               df_scd_sales_docs_formatted.SALES_DOCUMENT_ITEM,
                               'START_DATE',
                               'BILLING_BLOCK_HEADER',
                               'BILLING_BLOCK_ITEM',
                               'CANCELLATION_REASON',
                               'CANCELLATION_REASON_OLD',
                               'REASON_FOR_REJECTION',
                               'SUBSCRIPTION_QUANTITY',
                               'CREATED_DATE',
                               'FIRST_PMT_SUCCESS_DATE'])
    df_dim_sub_scd.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.DIM_SUB_SCD_2")
     
    df_cont_st_recs_scd = df_scd_sales_docs.join(df_contract_start_dt_recs,
                                (df_contract_start_dt_recs.CLIENT == df_scd_sales_docs.CLIENT) &
                                (df_contract_start_dt_recs.SALES_DOCUMENT == df_scd_sales_docs.SALES_DOCUMENT) &
                                (df_contract_start_dt_recs.SALES_DOCUMENT_ITEM == df_scd_sales_docs.SALES_DOCUMENT_ITEM),
                            "inner"   
                        ).where(
                            (df_scd_sales_docs.START_DATE <= df_contract_start_dt_recs.CONTRACT_START_DATE) &
                            (df_scd_sales_docs.END_DATE >= df_contract_start_dt_recs.CONTRACT_START_DATE)
                        ).select(*[df_scd_sales_docs.CLIENT,
                                    df_scd_sales_docs.SALES_DOCUMENT,
                                    df_scd_sales_docs.SALES_DOCUMENT_ITEM,
                                    df_contract_start_dt_recs.CONTRACT_START_DATE,
                                    'BILLING_BLOCK_HEADER',
                                    'BILLING_BLOCK_ITEM',
                                    'CANCELLATION_REASON',
                                    'CANCELLATION_REASON_OLD',
                                    'REASON_FOR_REJECTION',
                                    'SUBSCRIPTION_QUANTITY',
                                    df_scd_sales_docs.CREATED_DATE,
                                    'FIRST_PMT_SUCCESS_DATE']).withColumnRenamed("CONTRACT_START_DATE","START_DATE")
    df_cont_st_recs_scd.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.CONT_ST_RECS_SCD_2")
     
    df_delta_fplt2_scd =  df_scd_sales_docs_formatted.join(df_delta_fplt2,
                                (df_delta_fplt2.CLIENT == df_scd_sales_docs_formatted.CLIENT) &
                                (df_delta_fplt2.SALES_DOCUMENT == df_scd_sales_docs_formatted.SALES_DOCUMENT) &
                                (df_delta_fplt2.SALES_DOCUMENT_ITEM == df_scd_sales_docs_formatted.SALES_DOCUMENT_ITEM),
                            "inner"
                        ).where(
                            (df_scd_sales_docs_formatted.TS_START_DATE <= df_delta_fplt2.BP_START_DATE) &
                            (df_scd_sales_docs_formatted.TS_END_DATE >= df_delta_fplt2.BP_START_DATE)
                        ).withColumn(
                            "FPLT2_START_DATE_YYYY_MM_DD",to_date(df_delta_fplt2.BP_START_DATE,"yyyyMMdd")
                        ).select(*[df_scd_sales_docs_formatted.CLIENT,
                                    df_scd_sales_docs_formatted.SALES_DOCUMENT,
                                    df_scd_sales_docs_formatted.SALES_DOCUMENT_ITEM,
                                    'FPLT2_START_DATE_YYYY_MM_DD',
                                    'BILLING_BLOCK_HEADER',
                                    'BILLING_BLOCK_ITEM',
                                    'CANCELLATION_REASON',
                                    'CANCELLATION_REASON_OLD',
                                    'REASON_FOR_REJECTION',
                                    'SUBSCRIPTION_QUANTITY',
                                    'CREATED_DATE',
                                    'FIRST_PMT_SUCCESS_DATE']).withColumnRenamed("FPLT2_START_DATE_YYYY_MM_DD","START_DATE")     
    df_delta_fplt2_scd.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT2_SCD_2")
     
    df_delta_fplt3_formatted = df_delta_fplt3.withColumn("EVENT_DATE_YYYY_MM_DD",to_date(df_delta_fplt3.EVENT_DATE))
    df_delta_fplt3_scd = df_scd_sales_docs.join(df_delta_fplt3_formatted,
                                (df_delta_fplt3_formatted.CLIENT == df_scd_sales_docs.CLIENT) &
                                (df_delta_fplt3_formatted.SALES_DOCUMENT == df_scd_sales_docs.SALES_DOCUMENT) &
                                (df_delta_fplt3_formatted.SALES_DOCUMENT_ITEM == df_scd_sales_docs.SALES_DOCUMENT_ITEM),
                            "inner"   
                        ).where(
                            (df_scd_sales_docs.START_DATE <= df_delta_fplt3_formatted.EVENT_DATE_YYYY_MM_DD) &
                            (df_scd_sales_docs.END_DATE >= df_delta_fplt3_formatted.EVENT_DATE_YYYY_MM_DD)
                        ).select(*[df_scd_sales_docs.CLIENT,
                                    df_scd_sales_docs.SALES_DOCUMENT,
                                    df_scd_sales_docs.SALES_DOCUMENT_ITEM,
                                    df_delta_fplt3_formatted.EVENT_DATE_YYYY_MM_DD,
                                    'BILLING_BLOCK_HEADER',
                                    'BILLING_BLOCK_ITEM',
                                    'CANCELLATION_REASON',
                                    'CANCELLATION_REASON_OLD',
                                    'REASON_FOR_REJECTION',
                                    'SUBSCRIPTION_QUANTITY',
                                    'CREATED_DATE',
                                    'FIRST_PMT_SUCCESS_DATE']).withColumnRenamed("EVENT_DATE_YYYY_MM_DD","START_DATE")
                         
    df_delta_fplt3_scd.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT3_SCD_2")
                             
    df_bill_docs_s3_formatted = df_bill_docs_s3.withColumn("CREATED_DATE_YYYY_MM_DD",to_date(df_bill_docs_s3.CREATED_DATE,"yyyyMMdd"))
    df_bill_docs_s3_scd = df_scd_sales_docs.alias("scd").join(df_bill_docs_s3_formatted,
                                (col("scd.CLIENT") == df_bill_docs_s3_formatted.CLIENT) &
                                (col("scd.SALES_DOCUMENT") == df_bill_docs_s3_formatted.SALES_DOCUMENT) &
                                (col("scd.SALES_DOCUMENT_ITEM") == df_bill_docs_s3_formatted.SALES_DOCUMENT_ITEM) &
                                ((col("scd.START_DATE") <= df_bill_docs_s3_formatted.CREATED_DATE_YYYY_MM_DD) &
                                (col("scd.END_DATE") >= df_bill_docs_s3_formatted.CREATED_DATE_YYYY_MM_DD)),
                          "inner"     
                        ).select(*[col("scd.CLIENT"),
                                   col("scd.SALES_DOCUMENT"),
                                   col("scd.SALES_DOCUMENT_ITEM"),
                                   df_bill_docs_s3_formatted.CREATED_DATE_YYYY_MM_DD,
                                   'BILLING_BLOCK_HEADER',
                                   'BILLING_BLOCK_ITEM',
                                   'CANCELLATION_REASON',
                                   'CANCELLATION_REASON_OLD',
                                   'REASON_FOR_REJECTION',
                                   'SUBSCRIPTION_QUANTITY',
                                   col("scd.CREATED_DATE"),
                                   'FIRST_PMT_SUCCESS_DATE']).withColumnRenamed("CREATED_DATE_YYYY_MM_DD","START_DATE")
                         
    df_bill_docs_s3_scd.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_S3_SCD_2")
         
    df_bill_docs_g2_formatted = df_bill_docs_g2.withColumn("CREATED_DATE_YYYY_MM_DD",to_date(df_bill_docs_g2.CREATED_DATE,"yyyyMMdd"))
    df_bill_docs_g2_scd = df_scd_sales_docs.join(df_bill_docs_g2_formatted,
                                (df_bill_docs_g2_formatted.CLIENT == df_scd_sales_docs.CLIENT) &
                                (df_bill_docs_g2_formatted.SALES_DOCUMENT == df_scd_sales_docs.SALES_DOCUMENT) &
                                (df_bill_docs_g2_formatted.SALES_DOCUMENT_ITEM == df_scd_sales_docs.SALES_DOCUMENT_ITEM),
                            "inner"   
                        ).where(
                            (df_scd_sales_docs.START_DATE <= df_bill_docs_g2_formatted.CREATED_DATE_YYYY_MM_DD) &
                            (df_scd_sales_docs.END_DATE >= df_bill_docs_g2_formatted.CREATED_DATE_YYYY_MM_DD)
                        ).select(*[df_scd_sales_docs.CLIENT,
                                    df_scd_sales_docs.SALES_DOCUMENT,
                                    df_scd_sales_docs.SALES_DOCUMENT_ITEM,
                                    df_bill_docs_g2_formatted.CREATED_DATE_YYYY_MM_DD,
                                    'BILLING_BLOCK_HEADER',
                                    'BILLING_BLOCK_ITEM',
                                    'CANCELLATION_REASON',
                                    'CANCELLATION_REASON_OLD',
                                    'REASON_FOR_REJECTION',
                                    'SUBSCRIPTION_QUANTITY',
                                    df_scd_sales_docs.CREATED_DATE,
                                    'FIRST_PMT_SUCCESS_DATE']).withColumnRenamed("CREATED_DATE_YYYY_MM_DD","START_DATE")
    df_bill_docs_g2_scd.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_G2_SCD_2")
         
    df_bill_docs_formatted = df_bill_docs.withColumn("CREATED_DATE_YYYY_MM_DD",to_date(df_bill_docs.CREATED_DATE,"yyyyMMdd"))
    df_bill_docs_scd = df_scd_sales_docs.join(df_bill_docs_formatted,
                                (df_bill_docs_formatted.SALES_DOCUMENT == df_scd_sales_docs.SALES_DOCUMENT) &
                                (df_bill_docs_formatted.SALES_DOCUMENT_ITEM == df_scd_sales_docs.SALES_DOCUMENT_ITEM) ,
                            "inner"
                        ).where(
                            ((df_bill_docs_formatted.CREATED_DATE >= ts_cutoff_date_sap) &
                                (df_bill_docs_formatted.CREATED_DATE <= ts_current_date_sap)) &
                            (df_scd_sales_docs.START_DATE <= df_bill_docs_formatted.CREATED_DATE_YYYY_MM_DD) &
                            (df_scd_sales_docs.END_DATE >= df_bill_docs_formatted.CREATED_DATE_YYYY_MM_DD)
                        ).select(*[df_scd_sales_docs.CLIENT,
                                    df_scd_sales_docs.SALES_DOCUMENT,
                                    df_scd_sales_docs.SALES_DOCUMENT_ITEM,
                                    df_bill_docs_formatted.CREATED_DATE_YYYY_MM_DD,
                                    'BILLING_BLOCK_HEADER',
                                    'BILLING_BLOCK_ITEM',
                                    'CANCELLATION_REASON',
                                    'CANCELLATION_REASON_OLD',
                                    'REASON_FOR_REJECTION',
                                    'SUBSCRIPTION_QUANTITY',
                                    df_scd_sales_docs.CREATED_DATE,
                                    'FIRST_PMT_SUCCESS_DATE']).withColumnRenamed("CREATED_DATE_YYYY_MM_DD","START_DATE")
    df_bill_docs_scd.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_SCD_2")
         
    df_bill_docs2_formatted = df_bill_docs2.withColumn("CREATED_DATE_YYYY_MM_DD",to_date(df_bill_docs2.CREATED_DATE,"yyyyMMdd"))
    df_bill_docs2_scd = df_scd_sales_docs.join(df_bill_docs2_formatted,
                                 (df_bill_docs2_formatted.SALES_DOCUMENT == df_scd_sales_docs.SALES_DOCUMENT) &
                                 (df_bill_docs2_formatted.SALES_DOCUMENT_ITEM == df_scd_sales_docs.SALES_DOCUMENT_ITEM),            
                            "inner"   
                        ).where(
                            (df_scd_sales_docs.START_DATE <=  df_bill_docs2_formatted.CREATED_DATE_YYYY_MM_DD) &
                            (df_scd_sales_docs.END_DATE >= df_bill_docs2_formatted.CREATED_DATE_YYYY_MM_DD) &
                            ((df_bill_docs2_formatted.CREATED_DATE >= ts_cutoff_date_sap) &
                                (df_bill_docs2_formatted.CREATED_DATE <= ts_current_date_sap))
                        ).select(*[df_scd_sales_docs.CLIENT,
                                   df_scd_sales_docs.SALES_DOCUMENT,
                                   df_scd_sales_docs.SALES_DOCUMENT_ITEM,
                                   df_bill_docs2_formatted.CREATED_DATE_YYYY_MM_DD,
                                   'BILLING_BLOCK_HEADER',
                                   'BILLING_BLOCK_ITEM',
                                   'CANCELLATION_REASON',
                                   'CANCELLATION_REASON_OLD',
                                   'REASON_FOR_REJECTION',
                                   'SUBSCRIPTION_QUANTITY',
                                   df_scd_sales_docs.CREATED_DATE,
                                   'FIRST_PMT_SUCCESS_DATE']).withColumnRenamed("CREATED_DATE_YYYY_MM_DD","START_DATE")
    df_bill_docs2_scd.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.BILL_DOCS2_SCD_2")
