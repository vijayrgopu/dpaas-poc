%python
 
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame,Row
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date,date_add,when,col,lit,coalesce,concat,lag,lead,current_timestamp,date_format,isnull,isnotnull
from pyspark.sql.types import StringType
from delta.tables import *
 
sapHANAUrl    = "jdbc:sap://10.50.194.148:30015"
sapHANADriver = "com.sap.db.jdbc.Driver"
 
#use dbutils package
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
        .option("numPartitions", 4) \
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
    #Read all data from subsets
    df_delta_fplt = spark.read.table("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT_2")
 
    df_delta_fplt2 = spark.read.table("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT2_2")
 
    df_delta_fplt3 = spark.read.table("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT3_2")
     
    df_bill_docs = spark.read.table("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_2")
    df_bill_docs_formatted = df_bill_docs.withColumn("CREATED_DATE_YYYY_MM_DD",to_date(df_bill_docs.CREATED_DATE,"yyyyMMdd"))
 
    df_bill_docs2 = spark.read.table("dpaas_uccatalog_stg.adxdemo.BILL_DOCS2_2")
 
    df_scd_sales_docs_cancel_reason = spark.read.table("dpaas_uccatalog_stg.adxdemo.SALES_DOCS_CANCEL_REASON_2")                    
                         
    df_bill_docs_s3 = spark.read.table("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_S3_2")
    df_bill_docs_s3_formatted = df_bill_docs_s3.withColumn("CREATED_DATE_YYYY_MM_DD",to_date(df_bill_docs_s3.CREATED_DATE,"yyyyMMdd"))                                
     
    df_bill_docs_g2 = spark.read.table("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_G2_2")
 
    df_contract_start_dt_recs = spark.read.table("dpaas_uccatalog_stg.adxdemo.CONTRACT_START_DT_RECS_2")
 
    df_dim_sub_scd = spark.read.table("dpaas_uccatalog_stg.adxdemo.DIM_SUB_SCD_2")
 
    df_cont_st_recs_scd = spark.read.table("dpaas_uccatalog_stg.adxdemo.CONT_ST_RECS_SCD_2")
 
    df_delta_fplt2_scd = spark.read.table("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT2_SCD_2")
 
    df_delta_fplt3_scd = spark.read.table("dpaas_uccatalog_stg.adxdemo.DELTA_FPLT3_SCD_2")
                         
    df_bill_docs_s3_scd = spark.read.table("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_S3_SCD_2") 
     
    df_bill_docs_g2_scd = spark.read.table("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_G2_SCD_2")  
     
    df_bill_docs_scd = spark.read.table("dpaas_uccatalog_stg.adxdemo.BILL_DOCS_SCD_2")  
     
    df_bill_docs2_scd = spark.read.table("dpaas_uccatalog_stg.adxdemo.BILL_DOCS2_SCD_2")     
     
    df_delta_scd_sd_all = df_dim_sub_scd.union(
                                df_cont_st_recs_scd
                            ).union(
                                df_delta_fplt2_scd          
                            ).union(
                                df_delta_fplt3_scd
                            ).union(
                                df_bill_docs_s3_scd
                            ).union(
                                df_bill_docs_g2_scd
                            ).union(
                                df_bill_docs_scd
                            ).union(
                                df_bill_docs2_scd
                            ).distinct().withColumn('END_DATE', lit(None).cast(StringType()))
                           
    df_delta_scd_sd_all_formatted = df_delta_scd_sd_all.withColumn("START_DATE_yyyyMMdd",date_format(df_delta_scd_sd_all.START_DATE,"yyyyMMdd"))
    df_tmp_delta_scd_sd_bp = df_delta_scd_sd_all_formatted.join(df_delta_fplt.alias("delta_fplt"),
                                        (col("delta_fplt.CLIENT") == df_delta_scd_sd_all_formatted.CLIENT) &
                                        (col("delta_fplt.SALES_DOCUMENT") == df_delta_scd_sd_all_formatted.SALES_DOCUMENT) &
                                        (col("delta_fplt.SALES_DOCUMENT_ITEM") == df_delta_scd_sd_all_formatted.SALES_DOCUMENT_ITEM) &
                                         (df_delta_scd_sd_all_formatted.START_DATE_yyyyMMdd >= col("delta_fplt.BP_START_DATE")) &
                                        (df_delta_scd_sd_all_formatted.START_DATE_yyyyMMdd <= col("delta_fplt.BP_END_DATE")),
                                    "left_outer"
                                ).select(*[df_delta_scd_sd_all_formatted.CLIENT,
                                           df_delta_scd_sd_all_formatted.SALES_DOCUMENT,
                                           df_delta_scd_sd_all_formatted.SALES_DOCUMENT_ITEM,
                                           df_delta_scd_sd_all_formatted.START_DATE,
                                           'END_DATE',
                                           df_delta_scd_sd_all_formatted.BILLING_BLOCK_HEADER,
                                           df_delta_scd_sd_all_formatted.BILLING_BLOCK_ITEM,
                                           df_delta_scd_sd_all_formatted.CANCELLATION_REASON,
                                           df_delta_scd_sd_all_formatted.CANCELLATION_REASON_OLD,
                                           df_delta_scd_sd_all_formatted.REASON_FOR_REJECTION,
                                           df_delta_scd_sd_all_formatted.SUBSCRIPTION_QUANTITY,
                                           df_delta_scd_sd_all_formatted.CREATED_DATE,
                                           col("delta_fplt.BP_NO"),
                                           col("delta_fplt.BP_ITEM"),
                                           col("delta_fplt.BP_START_DATE"),
                                           col("delta_fplt.BP_END_DATE"),
                                           col("delta_fplt.BP_BILLING_BLOCK"),
                                           col("delta_fplt.BP_NET_PRICE"),
                                           df_delta_scd_sd_all_formatted.FIRST_PMT_SUCCESS_DATE,
                                           col("delta_fplt.BP_UPD_IND")])
     
    df_tmp_bill_doc = df_tmp_delta_scd_sd_bp.join(df_bill_docs_formatted,
                                       (df_bill_docs_formatted.SALES_DOCUMENT == df_tmp_delta_scd_sd_bp.SALES_DOCUMENT) &
                                       (df_bill_docs_formatted.SALES_DOCUMENT_ITEM == df_tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM) &
                                       (df_bill_docs_formatted.BP_NO == df_tmp_delta_scd_sd_bp.BP_NO) &
                                       (df_bill_docs_formatted.BP_ITEM == df_tmp_delta_scd_sd_bp.BP_ITEM) &
                                       (df_bill_docs_formatted.CREATED_DATE_YYYY_MM_DD <= df_tmp_delta_scd_sd_bp.START_DATE),
                                    "inner"            
                            ).select(*['CLIENT',
                                       df_tmp_delta_scd_sd_bp.SALES_DOCUMENT,
                                       df_tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM,
                                       'START_DATE']
                            ).distinct()
    df_tmp_bill_doc.show(2)                       
 
    #returns empty dataset
    df_tmp_bill_doc2 = df_tmp_delta_scd_sd_bp.alias("tmp_delta_scd_sd_bp").join(df_bill_docs2,
                                       (df_bill_docs2.SALES_DOCUMENT == col("tmp_delta_scd_sd_bp.SALES_DOCUMENT")) &
                                       (df_bill_docs2.SALES_DOCUMENT_ITEM == col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM")) &
                                       (df_bill_docs2.BP_NO == col("tmp_delta_scd_sd_bp.BP_NO")) & 
                                       (df_bill_docs2.BP_END_DATE <= col("tmp_delta_scd_sd_bp.BP_END_DATE")) &
                                       (df_bill_docs2.CREATED_DATE <= col("tmp_delta_scd_sd_bp.START_DATE")),
                                    "inner"            
                            ).select(*['CLIENT',
                                       col("tmp_delta_scd_sd_bp.SALES_DOCUMENT"),
                                       col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM"),
                                       'START_DATE']
                            ).distinct()
    df_tmp_bill_doc2.show(2)                      
    df_sales_docs_header_data_vbak = spark.read.table("dpaas_uccatalog_stg.adxdemo.VBAK")
 
    df_delta_rec_status_case = df_tmp_delta_scd_sd_bp.alias("tmp_delta_scd_sd_bp").join(df_dim_subs.alias("dim_subs"),
                                (col("dim_subs.CLIENT") == col("tmp_delta_scd_sd_bp.CLIENT")) &
                                (col("dim_subs.SALES_DOCUMENT") == col("tmp_delta_scd_sd_bp.SALES_DOCUMENT")) &
                                (col("dim_subs.SALES_DOCUMENT_ITEM") == col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM")),
                            "inner"   
                        ).join(df_sales_docs_header_data_vbak.alias("vbak"),
                                (col("tmp_delta_scd_sd_bp.CLIENT") == col("vbak.MANDT")) &
                                (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT") == col("vbak.VBELN")),
                            "inner"   
                        ).join(df_bill_docs_g2.alias("credit_memo"),
                               (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT") == col("credit_memo.SALES_DOCUMENT")) &
                               (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM") == col("credit_memo.SALES_DOCUMENT_ITEM")) &
                               (col("tmp_delta_scd_sd_bp.START_DATE") == col("credit_memo.CREATED_DATE_YYYY_MM_DD")),
                            "left_outer"  
                        ).join(df_bill_docs_s3_formatted.alias("bill_docs_s3"),
                                (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT") == col("bill_docs_s3.SALES_DOCUMENT")) &
                                (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM") == col("bill_docs_s3.SALES_DOCUMENT_ITEM")) &
                                (col("tmp_delta_scd_sd_bp.BP_NO") == col("bill_docs_s3.BP_NO")) &
                                (col("tmp_delta_scd_sd_bp.BP_ITEM") == col("bill_docs_s3.BP_ITEM")) &
                                (col("tmp_delta_scd_sd_bp.START_DATE") == col("bill_docs_s3.CREATED_DATE_YYYY_MM_DD")),
                            "left_outer"
                        ).join(df_bill_docs_s3_formatted.alias("bill_docs_s3_bill2"),
                                (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT") == col("bill_docs_s3_bill2.SALES_DOCUMENT")) &
                                (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM") == col("bill_docs_s3_bill2.SALES_DOCUMENT_ITEM")) &
                                (col("tmp_delta_scd_sd_bp.BP_NO") == col("bill_docs_s3_bill2.BP_NO")) &
                                (col("tmp_delta_scd_sd_bp.BP_END_DATE") == col("bill_docs_s3_bill2.BP_END_DATE")) &
                                (col("tmp_delta_scd_sd_bp.START_DATE") == col("bill_docs_s3_bill2.CREATED_DATE_YYYY_MM_DD")),
                            "left_outer"
                        ).join(df_tmp_bill_doc.alias("bill_doc"),
                               (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT") == col("bill_doc.SALES_DOCUMENT")) &
                               (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM") == col("bill_doc.SALES_DOCUMENT_ITEM")) &
                               (col("tmp_delta_scd_sd_bp.START_DATE") == col("bill_doc.START_DATE")),
                            "left_outer"  
                        ).join(df_tmp_bill_doc2.alias("bill_doc2"),
                               (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT") == col("bill_doc2.SALES_DOCUMENT")) &
                               (col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM") == col("bill_doc2.SALES_DOCUMENT_ITEM")) &
                               (col("tmp_delta_scd_sd_bp.START_DATE") == col("bill_doc2.START_DATE")),
                            "left_outer"
                        ).withColumn(
                            "BP_START_DATE_YYYY_MM_DD", to_date(col("tmp_delta_scd_sd_bp.BP_START_DATE"),"yyyyMMdd")
                        ).withColumn("SUBSCRIPTION_STATUS",
                            when(col("tmp_delta_scd_sd_bp.START_DATE") < col("dim_subs.CONTRACT_START_DATE_BP"), "INACTIVE")
                           .when(col("tmp_delta_scd_sd_bp.START_DATE") > col("dim_subs.CONTRACT_END_DATE_BP"), "INACTIVE")
                           .when((col("tmp_delta_scd_sd_bp.BILLING_BLOCK_HEADER") != '') &
                                 (col("tmp_delta_scd_sd_bp.BILLING_BLOCK_HEADER").isNotNull()),"INACTIVE")
                           .when((col("tmp_delta_scd_sd_bp.BILLING_BLOCK_ITEM") != '') &
                                 (col("tmp_delta_scd_sd_bp.BILLING_BLOCK_ITEM").isNotNull()),"INACTIVE")
                           .when((col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'SC') &
                                    (col("tmp_delta_scd_sd_bp.BP_UPD_IND") == 'RTNE'), "INACTIVE")
                           .when((col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'SA') &
                                 (col("dim_subs.BILLING_FREQUENCY") != 'PUF') &
                                 (col("tmp_delta_scd_sd_bp.START_DATE") == col("BP_START_DATE_YYYY_MM_DD")),"INACTIVE")
                           .when((col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'S2') |
                                 (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'Z8') |
                                 (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'HC') |
                                 (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'HD') |
                                 (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'HE') |
                                 (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'HA') |
                                 (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'SD') |
                                 (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'SF') |
                                 (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'HL'),"INACTIVE")
                           .when(((col("tmp_delta_scd_sd_bp.REASON_FOR_REJECTION") != 'S4') &
                                  (col("tmp_delta_scd_sd_bp.REASON_FOR_REJECTION") != '') &
                                  (col("tmp_delta_scd_sd_bp.REASON_FOR_REJECTION").isNotNull()) ) |
                                 ((col("tmp_delta_scd_sd_bp.REASON_FOR_REJECTION") == 'S4') &
                                  ((col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") != 'S5') &
                                   (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") != 'HB'))),"INACTIVE")
                           .when(((col("dim_subs.SUBSCRIPTION_CATEGORY") != 'SERV') &
                                    ((col("vbak.ORDER_REASON") != '911') &
                                     (col("vbak.ORDER_REASON") != '912') &
                                     (col("vbak.ORDER_REASON") != '913') &
                                     (col("vbak.ORDER_REASON") != '921')) &
                                 (col("tmp_delta_scd_sd_bp.BP_NET_PRICE").isNull())) ,"INACTIVE")
                           .when(((col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'S3') |
                                    (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'SC')
                                 ) &
                                 ((col("dim_subs.BILLING_FREQUENCY") == 'PUF') |
                                    (col("vbak.ORDER_REASON") == '921')
                                 ),"INACTIVE")
                           .when((col("dim_subs.BILLING_FREQUENCY") == 'PUF') &
                                 (col("credit_memo.SALES_DOCUMENT") != ''),"INACTIVE")
                           .when(
                                 (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'NB') &
                                 (col("dim_subs.BILLING_FREQUENCY") != 'PUF') &
                                 ((col("credit_memo.ORDER_REASON") == 'SCC') |
                                  (col("credit_memo.ORDER_REASON") == 'HCC')),"INACTIVE")
                           .when((col("dim_subs.PAYMENT_METHOD") != '06') &
                                 ((col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'S3') |
                                  (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'SC')) &
                                 ((col("tmp_delta_scd_sd_bp.CANCELLATION_REASON_OLD") != 'S4') &
                                  (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON_OLD") != 'SB')) &
                                 (col("dim_subs.CONTRACT_START_DATE_BP") == col("BP_START_DATE_YYYY_MM_DD")),"INACTIVE")
                           .when(((col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'S3') |
                                     (col("tmp_delta_scd_sd_bp.CANCELLATION_REASON") == 'SC')) &
                                    (col("tmp_delta_scd_sd_bp.START_DATE") <= col("BP_START_DATE_YYYY_MM_DD")) &
                                    ((col("tmp_delta_scd_sd_bp.BP_BILLING_BLOCK") != '30') &
                                      (col("tmp_delta_scd_sd_bp.BP_BILLING_BLOCK") != '43') &
                                      (col("tmp_delta_scd_sd_bp.BP_BILLING_BLOCK") != '44')) &
                                    ((col("bill_doc.SALES_DOCUMENT").isNull()) &
                                     (col("bill_doc2.SALES_DOCUMENT").isNull())),"INACTIVE")
                           .when((col("dim_subs.TRIAL_ORDER_FLAG")=='Y') &
                                 ( (col("tmp_delta_scd_sd_bp.FIRST_PMT_SUCCESS_DATE").isNull()) | (col("tmp_delta_scd_sd_bp.FIRST_PMT_SUCCESS_DATE") == '')),"INACTIVE")
                           .when( (col("dim_subs.SUBSCRIPTION_CATEGORY") != 'SERV') &
                                 ( (col("vbak.ORDER_REASON") != '911') &
                                  (col("vbak.ORDER_REASON") != '912') &
                                  (col("vbak.ORDER_REASON") != '913') &
                                  (col("vbak.ORDER_REASON") != '921') ) &
                                 ( col("tmp_delta_scd_sd_bp.BP_BILLING_BLOCK") != '' ) &
                                 ( col("tmp_delta_scd_sd_bp.BP_BILLING_BLOCK").isNotNull() ) &
                                 ( (col("tmp_delta_scd_sd_bp.BP_BILLING_BLOCK") != '30') &
                                   (col("tmp_delta_scd_sd_bp.BP_BILLING_BLOCK") != '43') &
                                   (col("tmp_delta_scd_sd_bp.BP_BILLING_BLOCK") != '44')
                                 ) &
                                 ( ((col("bill_doc.SALES_DOCUMENT") == '') | (col("bill_doc.SALES_DOCUMENT").isNull())) &
                                   ( (col("bill_doc2.SALES_DOCUMENT") == '') | ((col("bill_doc2.SALES_DOCUMENT").isNull())) )
                                ),"INACTIVE")
                            .otherwise("ACTIVE")
                        ).withColumn("LINK_ID",
                                     concat(col("tmp_delta_scd_sd_bp.CLIENT"),
                                            col("tmp_delta_scd_sd_bp.SALES_DOCUMENT"),
                                            col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM"),
                                            col("tmp_delta_scd_sd_bp.START_DATE"))
                        ).withColumn("PREV_LINK_ID",lit(None).cast(StringType())
                        ).withColumn("END_DATE", lit(None).cast(StringType())
                        ).drop(col("tmp_delta_scd_sd_bp.BP_START_DATE")
                        ).withColumnRenamed(
                            "BP_START_DATE_YYYY_MM_DD","BP_START_DATE"
                        ).select(*[ col("tmp_delta_scd_sd_bp.CLIENT"),
                                   col("tmp_delta_scd_sd_bp.SALES_DOCUMENT"),
                                   col("tmp_delta_scd_sd_bp.SALES_DOCUMENT_ITEM"),
                                   col("tmp_delta_scd_sd_bp.START_DATE"),
                                   'END_DATE',
                                   col("dim_subs.SALES_DOCUMENT_TYPE"),
                                   col("tmp_delta_scd_sd_bp.BILLING_BLOCK_HEADER"),
                                   col("tmp_delta_scd_sd_bp.BILLING_BLOCK_ITEM"),
                                   col("tmp_delta_scd_sd_bp.BP_BILLING_BLOCK"),
                                   col("tmp_delta_scd_sd_bp.BP_NET_PRICE"),
                                   'BP_START_DATE',
                                   col("tmp_delta_scd_sd_bp.CANCELLATION_REASON"),
                                   col("tmp_delta_scd_sd_bp.CANCELLATION_REASON_OLD"),
                                   col("dim_subs.CONTRACT_START_DATE_BP"),
                                   col("dim_subs.CONTRACT_END_DATE_BP"),
                                   col("tmp_delta_scd_sd_bp.CREATED_DATE"),
                                   col("tmp_delta_scd_sd_bp.REASON_FOR_REJECTION"),
                                   col("tmp_delta_scd_sd_bp.SUBSCRIPTION_QUANTITY"),
                                   'SUBSCRIPTION_STATUS',
                                   'LINK_ID',
                                   'PREV_LINK_ID',
                                   'SUBSCRIPTION_CATEGORY',
                                   col("tmp_delta_scd_sd_bp.BP_UPD_IND")
                                   ])
     
    df_delta_rec_status = df_delta_rec_status_case.withColumn("INIT_INACTIVE_FLAG",
                                when((col('CREATED_DATE') == col('START_DATE')) & (col('SUBSCRIPTION_STATUS') == 'INACTIVE'),"Y")
                                .when(((col('START_DATE') < col('CONTRACT_START_DATE_BP')) &
                                       (col('SUBSCRIPTION_STATUS') == 'INACTIVE')),"Y")
                                .otherwise("N")
                                ).select(*['CLIENT',
                                          'SALES_DOCUMENT',
                                          'SALES_DOCUMENT_ITEM',
                                          'START_DATE',
                                          'END_DATE',
                                          'SALES_DOCUMENT_TYPE',
                                          'BILLING_BLOCK_HEADER',
                                          'BILLING_BLOCK_ITEM',
                                          'BP_BILLING_BLOCK',
                                          'BP_NET_PRICE',
                                          'BP_START_DATE',
                                          'CANCELLATION_REASON',
                                          'CANCELLATION_REASON_OLD',
                                          'CONTRACT_START_DATE_BP',
                                          'CONTRACT_END_DATE_BP',
                                          'CREATED_DATE',
                                          'REASON_FOR_REJECTION',
                                          'SUBSCRIPTION_STATUS',
                                          'INIT_INACTIVE_FLAG',
                                          'SUBSCRIPTION_QUANTITY',
                                          'LINK_ID',
                                          'PREV_LINK_ID',
                                          'SUBSCRIPTION_CATEGORY',
                                          'BP_UPD_IND']
                                        ).distinct().withColumnsRenamed({'CONTRACT_START_DATE_BP':'CONTRACT_START_DATE',
                                                      'CONTRACT_END_DATE_BP':'CONTRACT_END_DATE'})
                                                                         
    df_scd_in_sub_status = spark.read.table("dpaas_uccatalog_stg.adxdemo.SCD_IN_SUBS_STATUS")
    df_scd_in_sub_status_formatted = df_scd_in_sub_status.withColumn("START_DATE_yyyyMMdd",date_format(df_scd_in_sub_status.START_DATE,"yyyyMMdd")
                                        ).withColumn("END_DATE_yyyyMMdd",date_format(df_scd_in_sub_status.END_DATE,"yyyyMMdd"))
     
    df_deletion_start_rec = df_scd_in_sub_status_formatted.alias("del_status_scd").join(df_delta_rec_status.alias("delta_st"),
                                          ( (col("del_status_scd.CLIENT") == col("delta_st.CLIENT")) &
                                           (col("del_status_scd.SALES_DOCUMENT") == col("delta_st.SALES_DOCUMENT")) &
                                           (col("del_status_scd.SALES_DOCUMENT_ITEM") == col("delta_st.SALES_DOCUMENT_ITEM"))),
                                        "inner"
                                    ).where((col("del_status_scd.START_DATE_yyyyMMdd") <= ts_cutoff_date_scd) &
                                            (col("del_status_scd.END_DATE_yyyyMMdd") >= ts_cutoff_date_scd )
                                    ).select(*[col("del_status_scd.CLIENT"),   
                                                col("del_status_scd.SALES_DOCUMENT"),
                                                col("del_status_scd.SALES_DOCUMENT_ITEM"),
                                                col("del_status_scd.START_DATE"),
                                                col("del_status_scd.END_DATE"),
                                                col("del_status_scd.SALES_DOCUMENT_TYPE"),
                                                col("del_status_scd.BILLING_BLOCK_HEADER"),
                                                col("del_status_scd.BILLING_BLOCK_ITEM"),
                                                col("del_status_scd.BP_BILLING_BLOCK"),
                                                col("del_status_scd.BP_NET_PRICE"),
                                                col("del_status_scd.BP_START_DATE"),
                                                col("del_status_scd.CANCELLATION_REASON"),
                                                col("del_status_scd.CANCELLATION_REASON_OLD"),
                                                col("del_status_scd.CONTRACT_START_DATE"),
                                                col("del_status_scd.CONTRACT_END_DATE"),
                                                col("del_status_scd.CREATED_DATE"),
                                                col("del_status_scd.REASON_FOR_REJECTION"),
                                                col("del_status_scd.SUBSCRIPTION_STATUS"),
                                                col("del_status_scd.INIT_INACTIVE_FLAG"),
                                                col("del_status_scd.SUBSCRIPTION_QUANTITY"),
                                                col("del_status_scd.LINK_ID"),
                                                col("del_status_scd.PREV_LINK_ID"),
                                                col("delta_st.SUBSCRIPTION_CATEGORY"),
                                                col("delta_st.BP_UPD_IND")])  
 
    df_delta_rec_union =  df_delta_rec_status.union(df_deletion_start_rec)
 
    #df_delta_rec_status.where(df_delta_rec_status.SALES_DOCUMENT == "5025299344").show()
    #df_deletion_start_rec.where(df_deletion_start_rec.SALES_DOCUMENT == "5025299344").show()
     
     
    delta_rec_union_partition_cols = ['CLIENT','SALES_DOCUMENT','SALES_DOCUMENT_ITEM']
    delta_rec_union_window = Window.partitionBy(delta_rec_union_partition_cols).orderBy('START_DATE')
    df_delta_rec_union_lag = df_delta_rec_union.withColumn("LAG_SUBSCRIPTION_STATUS",
                                                           lag(col("SUBSCRIPTION_STATUS")).over(delta_rec_union_window))
    
    df_delta_rec_union_grp = df_delta_rec_union_lag.withColumn("GRP",
                                        when((col("SUBSCRIPTION_STATUS") == 'ACTIVE') &
                                             (col("LAG_SUBSCRIPTION_STATUS") == 'INACTIVE') &
                                             (col("CANCELLATION_REASON") == 'SC') &
                                             (col("CANCELLATION_REASON_OLD") == 'SA'),0).otherwise(1)).where(col("GRP") == 1)
    df_delta_status_rec = df_delta_rec_union_grp.withColumn("GRP_2",
                                        when(col("SUBSCRIPTION_STATUS") == lag(col("SUBSCRIPTION_STATUS")).over(delta_rec_union_window),0).otherwise(1)).where(col("GRP_2") == 1)
    #Added Line
    #df_delta_status_rec.where(df_delta_status_rec.SALES_DOCUMENT == "7050284188").show()
    df_delta_switch_rec = df_delta_status_rec.withColumn("PREV_LINK_ID",
                                                         when(lag(col("LINK_ID")).over(delta_rec_union_window).isNull(),
                                                              col("PREV_LINK_ID"))
                                            ).withColumn("END_DATE",
                                                        when(lead(date_add(col("START_DATE"),-1)).over(delta_rec_union_window).isNull(),'9999-12-31')
                                            )
                                                          
    df_vbkd = spark.read.table("dpaas_uccatalog_stg.adxdemo.VBKD") 
 
    df_delta_rec = df_delta_switch_rec.alias("delta_rec").join(df_dim_subscription.alias("dim_sub"),
                                            (col("delta_rec.SALES_DOCUMENT") == col("dim_sub.SALES_DOCUMENT")) &
                                            (col("delta_rec.SALES_DOCUMENT_ITEM") == col("dim_sub.SALES_DOCUMENT_ITEM")),
                                        "inner"
                                        ).join(df_vbkd.alias("vbkd"),
                                            (df_vbkd.VBELN == col("delta_rec.SALES_DOCUMENT")) &
                                            (df_vbkd.POSNR == col("delta_rec.SALES_DOCUMENT_ITEM")),
                                        "left_outer"
                                        ).withColumn("REASON_FOR_REJECTION",
                                            when((col("delta_rec.SUBSCRIPTION_STATUS") == 'INACTIVE') &
                                                 (col("dim_sub.ROUTE_TO_MARKET") == 'APP STORE') &
                                                 (col("vbkd.INCO3_L") == 'BILLING_ERROR'),"")
                                           .when((col("delta_rec.SUBSCRIPTION_STATUS") == 'INACTIVE') &
                                                 (col("delta_rec.CANCELLATION_REASON") == 'SC') &
                                                 (col("delta_rec.BP_UPD_IND") == 'RTNE'), "")
                                           .otherwise(col("delta_rec.REASON_FOR_REJECTION"))
                                        ).withColumn("CANCELLATION_REASON",
                                            when((col("delta_rec.SUBSCRIPTION_STATUS") == 'INACTIVE') &
                                                 (col("dim_sub.ROUTE_TO_MARKET") == 'APP STORE') &
                                                 (col("vbkd.INCO3_L") == 'BILLING_ERROR'),"SA")
                                           .when((col("delta_rec.SUBSCRIPTION_STATUS") == 'INACTIVE') &
                                                 (col("delta_rec.CANCELLATION_REASON") == 'SC') &
                                                 (col("delta_rec.BP_UPD_IND") == 'RTNE'), "SA")
                                           .otherwise(col("delta_rec.CANCELLATION_REASON"))
                                        ).withColumn("LAST_MODIFIED", current_timestamp()
                                        ).select(*[col("delta_rec.CLIENT"),
                                                   col("delta_rec.SALES_DOCUMENT"),
                                                   col("delta_rec.SALES_DOCUMENT_ITEM"),
                                                   'START_DATE',
                                                   'END_DATE',
                                                   col("delta_rec.SALES_DOCUMENT_TYPE"),
                                                   'BILLING_BLOCK_HEADER',
                                                   'BILLING_BLOCK_ITEM',
                                                   'BP_BILLING_BLOCK',
                                                   'BP_NET_PRICE',
                                                   'BP_START_DATE',
                                                   'CANCELLATION_REASON',
                                                   'CANCELLATION_REASON_OLD',
                                                   'CONTRACT_START_DATE',
                                                   'CONTRACT_END_DATE',
                                                   'CREATED_DATE',
                                                   'REASON_FOR_REJECTION',
                                                   'SUBSCRIPTION_STATUS',
                                                   'INIT_INACTIVE_FLAG',
                                                   'SUBSCRIPTION_QUANTITY',
                                                   'LINK_ID',
                                                   'PREV_LINK_ID',
                                                   'LAST_MODIFIED'])
     
    df_fplt = df_billing_plan.alias("dim_bp").join(df_dim_subscription.alias("dim_sub"),
                                                            col("dim_bp.BP_NO") == col("dim_sub.BILL_PLAN_NUMBER"),
                                                     "inner"
                                                     ).select(*[col("dim_sub.CLIENT"),
                                                                col("dim_sub.SALES_DOCUMENT"),
                                                                col("dim_sub.SALES_DOCUMENT_ITEM"),
                                                                'BP_NO',
                                                                'BP_ITEM',
                                                                'BP_START_DATE',
                                                                'BP_END_DATE',
                                                                'BP_BILLING_BLOCK',
                                                                'BP_NET_PRICE',
                                                                'BP_UPD_IND'])
    df_fplt = df_fplt.withColumn("BP_START_DATE_yyyy_MM_dd",to_date(df_fplt.BP_START_DATE,"yyyyMMdd")
                ).withColumn("BP_END_DATE_yyyy_MM_dd",to_date(df_fplt.BP_END_DATE,"yyyyMMdd")
                ).drop(df_fplt.BP_START_DATE
                ).drop(df_fplt.BP_END_DATE
                ).withColumnsRenamed({"BP_START_DATE_yyyy_MM_dd" : "BP_START_DATE",
                                     "BP_END_DATE_yyyy_MM_dd" : "BP_END_DATE"})
    #df_fplt.show()
       
    delta_tbl_scd_in_status = DeltaTable.forName(spark,"dpaas_uccatalog_stg.adxdemo.SCD_IN_SUBS_STATUS_2")
    delta_tbl_scd_in_status.alias("dtbl") \
                    .merge(df_delta_rec.alias("updates"),
                        'dtbl.CLIENT = updates.CLIENT AND \
                         dtbl.SALES_DOCUMENT = updates.SALES_DOCUMENT AND \
                         dtbl.SALES_DOCUMENT_ITEM = updates.SALES_DOCUMENT_ITEM AND \
                         dtbl.START_DATE = updates.START_DATE'
                    ).whenMatchedUpdate(
                        set = {
                            "dtbl.END_DATE" : "updates.END_DATE",         
                            "dtbl.SALES_DOCUMENT_TYPE" : "updates.SALES_DOCUMENT_TYPE",
                            "dtbl.BILLING_BLOCK_HEADER" : "updates.BILLING_BLOCK_HEADER",
                            "dtbl.BILLING_BLOCK_ITEM" : "updates.BILLING_BLOCK_ITEM",  
                            "dtbl.BP_BILLING_BLOCK" : "updates.BP_BILLING_BLOCK",    
                            "dtbl.BP_NET_PRICE" : "updates.BP_NET_PRICE",    
                            "dtbl.BP_START_DATE" : "updates.BP_START_DATE",     
                            "dtbl.CANCELLATION_REASON" : "updates.CANCELLATION_REASON",
                            "dtbl.CANCELLATION_REASON_OLD" : "updates.CANCELLATION_REASON_OLD",
                            "dtbl.CONTRACT_START_DATE" : "updates.CONTRACT_START_DATE",
                            "dtbl.CONTRACT_END_DATE" : "updates.CONTRACT_END_DATE",
                            "dtbl.CREATED_DATE" : "updates.CREATED_DATE",     
                            "dtbl.REASON_FOR_REJECTION" : "updates.REASON_FOR_REJECTION",
                            "dtbl.SUBSCRIPTION_QUANTITY" : "updates.SUBSCRIPTION_QUANTITY",
                            "dtbl.SUBSCRIPTION_STATUS" : "updates.SUBSCRIPTION_STATUS",  
                            "dtbl.INIT_INACTIVE_FLAG" : "updates.INIT_INACTIVE_FLAG",
                            "dtbl.LINK_ID" : "updates.LINK_ID",          
                            "dtbl.PREV_LINK_ID" : "updates.PREV_LINK_ID",     
                            "dtbl.LAST_MODIFIED" : "updates.LAST_MODIFIED" 
                        }
                    ).whenNotMatchedInsert(
                        values = {
                            "dtbl.CLIENT" : "updates.CLIENT",
                            "dtbl.SALES_DOCUMENT" : "updates.SALES_DOCUMENT",
                            "dtbl.SALES_DOCUMENT_ITEM" : "updates.SALES_DOCUMENT_ITEM",
                            "dtbl.START_DATE" : "updates.START_DATE",
                            "dtbl.END_DATE" : "updates.END_DATE",         
                            "dtbl.SALES_DOCUMENT_TYPE" : "updates.SALES_DOCUMENT_TYPE",
                            "dtbl.BILLING_BLOCK_HEADER" : "updates.BILLING_BLOCK_HEADER",
                            "dtbl.BILLING_BLOCK_ITEM" : "updates.BILLING_BLOCK_ITEM",  
                            "dtbl.BP_BILLING_BLOCK" : "updates.BP_BILLING_BLOCK",    
                            "dtbl.BP_NET_PRICE" : "updates.BP_NET_PRICE",    
                            "dtbl.BP_START_DATE" : "updates.BP_START_DATE",     
                            "dtbl.CANCELLATION_REASON" : "updates.CANCELLATION_REASON",
                            "dtbl.CANCELLATION_REASON_OLD" : "updates.CANCELLATION_REASON_OLD",
                            "dtbl.CONTRACT_START_DATE" : "updates.CONTRACT_START_DATE",
                            "dtbl.CONTRACT_END_DATE" : "updates.CONTRACT_END_DATE",
                            "dtbl.CREATED_DATE" : "updates.CREATED_DATE",     
                            "dtbl.REASON_FOR_REJECTION" : "updates.REASON_FOR_REJECTION",
                            "dtbl.SUBSCRIPTION_QUANTITY" : "updates.SUBSCRIPTION_QUANTITY",
                            "dtbl.SUBSCRIPTION_STATUS" : "updates.SUBSCRIPTION_STATUS",  
                            "dtbl.INIT_INACTIVE_FLAG" : "updates.INIT_INACTIVE_FLAG",
                            "dtbl.LINK_ID" : "updates.LINK_ID",          
                            "dtbl.PREV_LINK_ID" : "updates.PREV_LINK_ID",     
                            "dtbl.LAST_MODIFIED" : "updates.LAST_MODIFIED"
                        }
                    ).execute()
 
    delta_tbl_scd_in_status.alias("dtbl") \
                    .merge(df_fplt.alias("updates"),
                        'dtbl.SALES_DOCUMENT = updates.SALES_DOCUMENT AND \
                         dtbl.SALES_DOCUMENT_ITEM = updates.SALES_DOCUMENT_ITEM AND \
                         dtbl.START_DATE >= updates.BP_START_DATE AND dtbl.START_DATE <= updates.BP_END_DATE'
                    ).whenMatchedUpdate(
                        set = {"dtbl.UPD_IND" : "updates.BP_UPD_IND"}
                    ).execute()
 
         
